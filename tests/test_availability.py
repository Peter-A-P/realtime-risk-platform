"""The live window's two numbers, and the rules that separate them (ADR 25).

The rule that excludes a reclaim's recovery from the latency figure is the
one a reader will check hardest, so these hold its edges: only a recorded
notice excludes anything, a stop the platform caused itself stays in, the
window ends on throughput and never on latency, and every excluded minute
is still counted in availability.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

import pytest

from verdict.observe.availability import (
    Minute,
    Notice,
    assemble_minutes,
    caught_up,
    quantile,
    read_notices,
    recovery_windows,
    report,
)

START = dt.datetime(2026, 10, 1, 0, 0, tzinfo=dt.UTC)
FAST = {0.01: 60_000.0, 0.05: 61_000.0, 1.0: 61_000.0, 300.0: 61_000.0, math.inf: 61_000.0}
"""A normal minute: 61,000 decisions, nearly all under 10 ms, all under 50."""


def late(decided: float) -> dict[float, float]:
    """A catch-up minute: everything decided minutes after its event."""
    return {0.01: 0.0, 0.05: 0.0, 1.0: 0.0, 300.0: decided, math.inf: decided}


def normal(at: dt.datetime) -> Minute:
    return Minute(at=at, decisions=61_000, sent=61_000, feed_lag=0.01, buckets=FAST)


def down(at: dt.datetime) -> Minute:
    """No instance: nothing sent, nothing decided, no feed to report a lag."""
    return Minute(at=at, decisions=0, sent=0, feed_lag=None, buckets={})


def catching_up(at: dt.datetime) -> Minute:
    """The replacement working off the backlog at twice the live rate."""
    return Minute(at=at, decisions=130_000, sent=61_000, feed_lag=0.02, buckets=late(130_000))


def minute(n: int) -> dt.datetime:
    return START + dt.timedelta(minutes=n)


def a_reclaim_at(n: int) -> list[Minute]:
    """Twenty minutes: normal, a notice at `n`, two more normal, four down, three catching up."""
    series: list[Minute] = []
    for i in range(20):
        if i < n + 2:
            series.append(normal(minute(i)))
        elif i < n + 6:
            series.append(down(minute(i)))
        elif i < n + 9:
            series.append(catching_up(minute(i)))
        else:
            series.append(normal(minute(i)))
    return series


def a_notice_at(n: int, seconds: int = 20) -> Notice:
    return Notice(
        instance_id="i-0123456789abcdef0",
        noticed_at=minute(n) + dt.timedelta(seconds=seconds),
        action="terminate",
        action_at=minute(n + 2) + dt.timedelta(seconds=seconds),
    )


def test_a_reclaims_window_runs_from_its_notice_to_the_platform_catching_up() -> None:
    [window] = recovery_windows(a_reclaim_at(5), [a_notice_at(5)])
    assert window.start == minute(5)
    assert window.end == minute(14)
    assert window.minutes_without_decisions == 4
    assert window.decided_late == 3 * 130_000


def test_latency_while_serving_leaves_out_the_reclaim_and_nothing_else() -> None:
    series = a_reclaim_at(5)
    result = report(series, [a_notice_at(5)])
    serving = result["latency_while_serving"]
    assert serving["minutes"] == 20 - 9
    assert serving["p99_ms"] < 50
    assert result["latency_every_minute"]["p99_ms"] > 1_000


def test_without_a_notice_nothing_is_left_out() -> None:
    """A stop the platform caused itself, a crash or a deploy, counts against it."""
    result = report(a_reclaim_at(5), [])
    assert result["latency_while_serving"]["minutes"] == 20
    assert result["latency_while_serving"]["p99_ms"] > 1_000
    assert result["availability"]["spot_reclaims"] == []
    assert result["availability"]["other_stops"] == [
        {"first": minute(7).isoformat(), "last": minute(10).isoformat(), "minutes": 4}
    ]


def test_every_minute_still_counts_in_availability() -> None:
    result = report(a_reclaim_at(5), [a_notice_at(5)])
    availability = result["availability"]
    assert availability["minutes_without_decisions"] == 4
    assert availability["uptime_percent"] == pytest.approx(100 * 16 / 20)
    [reclaim] = availability["spot_reclaims"]
    assert reclaim["recovery_minutes"] == 9
    assert reclaim["decided_late"] == 3 * 130_000
    assert availability["other_stops"] == []


def test_a_notice_the_stream_never_felt_excludes_nothing() -> None:
    series = [normal(minute(i)) for i in range(30)]
    assert recovery_windows(series, [a_notice_at(5)]) == []


def test_the_window_ends_on_throughput_not_on_latency() -> None:
    """A slow minute after catching up is the platform's, and stays in."""
    series = a_reclaim_at(5)
    slow = Minute(at=minute(16), decisions=61_000, sent=61_000, feed_lag=0.01, buckets=late(61_000))
    series[16] = slow
    [window] = recovery_windows(series, [a_notice_at(5)])
    assert window.end == minute(14)
    assert not window.covers(minute(16))


def test_one_caught_up_minute_is_not_the_end_of_a_recovery() -> None:
    series = a_reclaim_at(5)
    series[12] = normal(minute(12))  # one good minute in the middle of the catch-up
    [window] = recovery_windows(series, [a_notice_at(5)])
    assert window.end == minute(14)


def test_a_platform_still_catching_up_at_the_end_of_the_data_stays_excluded() -> None:
    series = a_reclaim_at(5)[:12]
    [window] = recovery_windows(series, [a_notice_at(5)])
    assert window.end is None
    assert window.minutes is None


@pytest.mark.parametrize(
    ("minute_", "expected"),
    [
        (Minute(START, 61_000, 61_000, 0.01), True),
        (Minute(START, 61_000, 61_000, 3.0), False),  # the feed is behind its clock
        (Minute(START, 130_000, 61_000, 0.01), False),  # the scorer is working a backlog
        (Minute(START, 0, 0, None), False),  # nothing running
        (Minute(START, 61_000, 61_000, None), False),  # no feed reported
    ],
)
def test_caught_up_is_judged_by_the_feed_and_the_scorers_rates(
    minute_: Minute, expected: bool
) -> None:
    assert caught_up(minute_) is expected


def test_the_quantile_matches_prometheus_histogram_quantile() -> None:
    buckets = {0.01: 50.0, 0.05: 100.0, math.inf: 100.0}
    assert quantile(0.5, buckets) == pytest.approx(0.01)
    assert quantile(0.75, buckets) == pytest.approx(0.03)
    assert quantile(0.25, buckets) == pytest.approx(0.005)
    assert quantile(0.99, {0.01: 50.0, 0.05: 60.0, math.inf: 100.0}) == pytest.approx(0.05)
    assert math.isnan(quantile(0.99, {}))


def test_daily_figures_carry_an_interval_across_days() -> None:
    series = [normal(START + dt.timedelta(days=d, minutes=m)) for d in range(3) for m in range(5)]
    daily = report(series, [])["latency_while_serving"]["daily_mean_95ci"]["p99_ms"]
    assert daily is not None
    assert daily["runs"] == 3


def test_notices_are_read_as_the_watcher_writes_them(tmp_path: Path) -> None:
    (tmp_path / "i-0123456789abcdef0.json").write_text(
        json.dumps(
            {
                "instance_id": "i-0123456789abcdef0",
                "noticed_at": "2026-10-01T00:05:20Z",
                "notice": {"action": "terminate", "time": "2026-10-01T00:07:20Z"},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "boots.jsonl").write_text("{}\n", encoding="utf-8")
    [notice] = read_notices(tmp_path)
    assert notice.noticed_at == minute(5) + dt.timedelta(seconds=20)
    assert notice.action == "terminate"
    assert notice.action_at == minute(7) + dt.timedelta(seconds=20)


def _ending(n: int) -> float:
    """A `query_range` timestamp: the sample at t describes the minute ending at t."""
    return (minute(n) + dt.timedelta(minutes=1)).timestamp()


def test_a_minute_prometheus_has_nothing_for_is_kept_as_a_stopped_minute() -> None:
    """A replacement leaves a hole in the scrape; the hole is the outage, not missing data."""
    two: list[list[Any]] = [[_ending(0), "61000"], [_ending(2), "61000"]]
    results: dict[str, list[dict[str, Any]]] = {
        "decisions": [{"metric": {}, "values": two}],
        "sent": [{"metric": {}, "values": two}],
        "feed_lag": [{"metric": {}, "values": [[_ending(0), "0.01"], [_ending(2), "0.01"]]}],
        "buckets": [
            {"metric": {"le": "0.01"}, "values": [[_ending(0), "60000"], [_ending(2), "60000"]]},
            {"metric": {"le": "+Inf"}, "values": two},
        ],
    }
    minutes = assemble_minutes(minute(0), minute(3), results)
    assert [m.at for m in minutes] == [minute(0), minute(1), minute(2)]
    assert minutes[1] == Minute(at=minute(1), decisions=0.0, sent=0.0, feed_lag=None, buckets={})
    assert minutes[0].buckets == {0.01: 60_000.0, math.inf: 61_000.0}
    assert caught_up(minutes[0])
    assert not caught_up(minutes[1])
