"""The live window's two numbers: latency while serving, and availability.

The live stack is one spot instance (ADR 14). AWS may reclaim it with two
minutes' notice; the group launches a replacement, the stream waits, and the
replacement then decides the backlog minutes late. A p99 over every minute
would report that catch-up as the scorer's latency, and a p99 with those
minutes quietly dropped would hide that the platform stopped. ADR 25 reports
both, under rules fixed before the window starts:

1. **Latency while serving**: decision latency over every minute outside a
   reclaim's recovery window. Only a reclaim AWS announced is excluded,
   with the notice the instance recorded as the evidence
   (`/data/interruptions/<instance>.json`, written by the boot script's
   watcher). A crash, a hang, a deploy or anything else that is the
   platform's own doing stays in.
2. **Availability**: every minute counted, reclaims listed with how long
   each took to recover and how many decisions came late because of it, and
   every stop that was not a reclaim listed beside them.

A recovery window starts at the notice and ends when the platform has
caught up, judged by throughput and never by latency, so the rule cannot
choose its own answer: the first run of `SUSTAIN` minutes in which the
transaction feed is on time (`FEED_ON_TIME` seconds behind its clock or
less) and the scorer decides no more than `CATCH_UP_FACTOR` times what the
feed sends. A scorer working off a backlog decides faster than the feed
sends; one that has caught up decides what arrives.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from verdict.scoring.timing import t_interval

FEED_ON_TIME: Final = 1.0
"""Seconds behind its clock a feed may be and still count as on time."""

CATCH_UP_FACTOR: Final = 1.1
"""Decisions per transaction sent above which the scorer is working off a backlog."""

SUSTAIN: Final = 2
"""Consecutive caught-up minutes that end a recovery window."""

LATE: Final = 1.0
"""Seconds after which a decision counts as late in the availability report."""

NOTICE_MUST_STOP_WITHIN: Final = dt.timedelta(minutes=15)
"""How long after a notice the stream must be seen to stop.

A spot notice is followed by termination two minutes later. If the stream
never falters within this long, the notice did not cost anything and no
minutes are excluded for it.
"""


@dataclass(frozen=True, slots=True)
class Minute:
    """One minute of the live stack, from Prometheus.

    Attributes:
        at: The minute's start, UTC.
        decisions: Transactions decided in the minute.
        sent: Transactions the feed sent in the minute.
        feed_lag: The transaction feed's worst lag in the minute, in
            seconds; None if the feed reported nothing.
        buckets: Decisions in the minute at or under each latency bound, in
            seconds, cumulative as Prometheus histograms are. `inf` is the
            total.
    """

    at: dt.datetime
    decisions: float
    sent: float
    feed_lag: float | None
    buckets: Mapping[float, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Notice:
    """A spot interruption notice, as the instance recorded it.

    Attributes:
        instance_id: The instance AWS reclaimed.
        noticed_at: When the watcher saw the notice.
        action: What AWS said it would do (`terminate`, `stop`).
        action_at: When AWS said it would do it.
    """

    instance_id: str
    noticed_at: dt.datetime
    action: str
    action_at: dt.datetime | None


@dataclass(frozen=True, slots=True)
class RecoveryWindow:
    """The minutes a reclaim cost, from its notice until the platform caught up.

    Attributes:
        notice: The notice.
        start: The first minute excluded (the notice's minute).
        end: The first minute counted again, or None if the platform had not
            caught up by the end of the data.
        minutes_without_decisions: Minutes in the window with no decision.
        decided: Decisions made inside the window.
        decided_late: Of those, decided more than `LATE` seconds after their
            event.
    """

    notice: Notice
    start: dt.datetime
    end: dt.datetime | None
    minutes_without_decisions: int
    decided: float
    decided_late: float

    def covers(self, at: dt.datetime) -> bool:
        """Whether a minute falls inside the window.

        Args:
            at: The minute's start.

        Returns:
            True if the minute is excluded from the serving latency.
        """
        return at >= self.start and (self.end is None or at < self.end)

    @property
    def minutes(self) -> int | None:
        """The window's length in minutes, or None if it never closed."""
        if self.end is None:
            return None
        return int((self.end - self.start).total_seconds() // 60)


def read_notices(directory: Path) -> list[Notice]:
    """Every spot notice the watcher recorded on the data volume.

    Args:
        directory: `/data/interruptions`, or a copy of it.

    Returns:
        The notices, oldest first.
    """
    notices: list[Notice] = []
    if not directory.exists():
        return notices
    for path in sorted(directory.glob("i-*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        notice = raw.get("notice") or {}
        action_time = notice.get("time")
        notices.append(
            Notice(
                instance_id=raw["instance_id"],
                noticed_at=_parse(raw["noticed_at"]),
                action=str(notice.get("action", "")),
                action_at=_parse(action_time) if action_time else None,
            )
        )
    return sorted(notices, key=lambda n: n.noticed_at)


def _parse(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(dt.UTC)


def caught_up(minute: Minute) -> bool:
    """Whether the platform was current in a minute, by throughput alone.

    Args:
        minute: The minute.

    Returns:
        True if the feed was on time and the scorer decided about what the
        feed sent. A minute with nothing sent or nothing decided is not
        caught up: something was stopped.
    """
    if minute.feed_lag is None or minute.feed_lag > FEED_ON_TIME:
        return False
    if minute.sent <= 0 or minute.decisions <= 0:
        return False
    return minute.decisions <= CATCH_UP_FACTOR * minute.sent


def recovery_windows(minutes: Sequence[Minute], notices: Iterable[Notice]) -> list[RecoveryWindow]:
    """The recovery window of each reclaim.

    Args:
        minutes: The live stack, minute by minute, in order.
        notices: The spot notices.

    Returns:
        One window per notice after which the stream was seen to stop.
    """
    windows: list[RecoveryWindow] = []
    for notice in sorted(notices, key=lambda n: n.noticed_at):
        start = notice.noticed_at.replace(second=0, microsecond=0)
        after = [m for m in minutes if m.at >= start]
        stopped = next(
            (
                i
                for i, m in enumerate(after)
                if not caught_up(m) and m.at < start + NOTICE_MUST_STOP_WITHIN
            ),
            None,
        )
        if stopped is None:
            continue
        end: dt.datetime | None = None
        for i in range(stopped, len(after) - SUSTAIN + 1):
            if all(caught_up(m) for m in after[i : i + SUSTAIN]):
                end = after[i].at
                break
        inside = [m for m in after if end is None or m.at < end]
        windows.append(
            RecoveryWindow(
                notice=notice,
                start=start,
                end=end,
                minutes_without_decisions=sum(1 for m in inside if m.decisions <= 0),
                decided=sum(m.decisions for m in inside),
                decided_late=sum(_over(m.buckets, LATE) for m in inside),
            )
        )
    return windows


def _over(buckets: Mapping[float, float], bound: float) -> float:
    """Decisions in a minute that took longer than a bound, from its buckets."""
    total = buckets.get(math.inf, 0.0)
    at_or_under = max((count for le, count in buckets.items() if le <= bound), default=0.0)
    return max(total - at_or_under, 0.0)


def quantile(q: float, buckets: Mapping[float, float]) -> float:
    """A quantile from cumulative histogram buckets, as Prometheus computes it.

    Linear within the bucket the quantile falls in; the lowest bucket
    interpolates from zero; a quantile in the `inf` bucket reports the
    highest finite bound, as `histogram_quantile` does.

    Args:
        q: The quantile, 0 to 1.
        buckets: Upper bound to cumulative count, including `inf`.

    Returns:
        Seconds, or NaN with no observations.
    """
    total = buckets.get(math.inf, 0.0)
    if total <= 0:
        return math.nan
    rank = q * total
    bounds = sorted(buckets)
    previous_bound, previous_count = 0.0, 0.0
    for bound in bounds:
        count = buckets[bound]
        if count >= rank:
            if math.isinf(bound):
                return previous_bound
            if count == previous_count:
                return bound
            return previous_bound + (bound - previous_bound) * (
                (rank - previous_count) / (count - previous_count)
            )
        previous_bound, previous_count = bound, count
    return previous_bound


def _summed(minutes: Iterable[Minute]) -> dict[float, float]:
    total: dict[float, float] = {}
    for minute in minutes:
        for le, count in minute.buckets.items():
            total[le] = total.get(le, 0.0) + count
    return total


def _latency(minutes: Sequence[Minute]) -> dict[str, float]:
    buckets = _summed(minutes)
    return {
        "decisions": buckets.get(math.inf, 0.0),
        "p50_ms": quantile(0.50, buckets) * 1000,
        "p95_ms": quantile(0.95, buckets) * 1000,
        "p99_ms": quantile(0.99, buckets) * 1000,
    }


def report(minutes: Sequence[Minute], notices: Iterable[Notice]) -> dict[str, Any]:
    """Both numbers, and everything needed to check them.

    Args:
        minutes: The live stack, minute by minute, in order.
        notices: The spot notices the instances recorded.

    Returns:
        The report: latency while serving (overall and by day, with an
        interval across days), latency over every minute for comparison,
        and availability with each reclaim and each other stop listed.
    """
    windows = recovery_windows(minutes, notices)
    serving = [m for m in minutes if not any(w.covers(m.at) for w in windows)]
    days = sorted({m.at.date() for m in serving})
    by_day = {day.isoformat(): _latency([m for m in serving if m.at.date() == day]) for day in days}
    daily: dict[str, Any] = {}
    for key in ("p50_ms", "p95_ms", "p99_ms"):
        values = [v[key] for v in by_day.values() if math.isfinite(v[key])]
        daily[key] = t_interval(values).rounded(3) if len(values) >= 2 else None

    excluded = {m.at for m in minutes if any(w.covers(m.at) for w in windows)}
    stops = _other_stops([m for m in minutes if m.at not in excluded])
    total_minutes = len(minutes)
    without = sum(1 for m in minutes if m.decisions <= 0)
    return {
        "track": "synthetic live",
        "rules": {
            "feed_on_time_s": FEED_ON_TIME,
            "catch_up_factor": CATCH_UP_FACTOR,
            "sustain_minutes": SUSTAIN,
            "late_s": LATE,
        },
        "window": {
            "first_minute": minutes[0].at.isoformat() if minutes else None,
            "last_minute": minutes[-1].at.isoformat() if minutes else None,
            "minutes": total_minutes,
        },
        "latency_while_serving": {
            **_latency(serving),
            "minutes": len(serving),
            "daily_mean_95ci": daily,
            "by_day": by_day,
        },
        "latency_every_minute": _latency(minutes),
        "availability": {
            "minutes_with_decisions": total_minutes - without,
            "minutes_without_decisions": without,
            "uptime_percent": 100.0 * (total_minutes - without) / total_minutes
            if total_minutes
            else math.nan,
            "decisions": sum(m.decisions for m in minutes),
            "decided_late": sum(_over(m.buckets, LATE) for m in minutes),
            "spot_reclaims": [
                {
                    "instance_id": w.notice.instance_id,
                    "noticed_at": w.notice.noticed_at.isoformat(),
                    "action": w.notice.action,
                    "recovered_at": w.end.isoformat() if w.end else None,
                    "recovery_minutes": w.minutes,
                    "minutes_without_decisions": w.minutes_without_decisions,
                    "decided": w.decided,
                    "decided_late": w.decided_late,
                }
                for w in windows
            ],
            "other_stops": stops,
        },
    }


def _other_stops(minutes: Sequence[Minute]) -> list[dict[str, Any]]:
    """Runs of minutes with no decision that no reclaim explains: the platform's own."""
    runs: list[dict[str, Any]] = []
    for minute in minutes:
        if minute.decisions > 0:
            continue
        if runs and minute.at - dt.datetime.fromisoformat(runs[-1]["last"]) <= dt.timedelta(
            minutes=1
        ):
            runs[-1]["last"] = minute.at.isoformat()
            runs[-1]["minutes"] += 1
        else:
            runs.append(
                {"first": minute.at.isoformat(), "last": minute.at.isoformat(), "minutes": 1}
            )
    return runs


QUERIES: Final = {
    "decisions": "sum(increase(verdict_decisions_total[1m]))",
    "sent": 'sum(increase(verdict_feed_records_total{feed="transactions"}[1m]))',
    "feed_lag": 'max(max_over_time(verdict_feed_lag_seconds{feed="transactions"}[1m]))',
    "buckets": "sum by (le) (increase(verdict_event_to_decision_seconds_bucket[1m]))",
}
"""What each minute is built from. `increase` over a counter survives the
scorer restarting, which is every replacement."""

_CHUNK: Final = dt.timedelta(days=1)
"""Prometheus caps points per query; a day of minutes is well inside it."""


def assemble_minutes(
    start: dt.datetime, end: dt.datetime, results: Mapping[str, Sequence[Mapping[str, Any]]]
) -> list[Minute]:
    """Every minute from start to end, from Prometheus `query_range` results.

    A minute Prometheus has nothing for is a minute nothing ran, which is
    what a replacement looks like from the volume: it is kept, as zero,
    rather than dropped.

    Args:
        start: The first minute.
        end: The minute after the last.
        results: Each of `QUERIES` to its `data.result` list, evaluated at a
            60 s step; a sample at t describes the minute ending at t.

    Returns:
        The minutes, in order, every one present.
    """

    def by_minute(result: Sequence[Mapping[str, Any]]) -> dict[dt.datetime, float]:
        out: dict[dt.datetime, float] = {}
        for series in result:
            for t, value in series["values"]:
                at = dt.datetime.fromtimestamp(float(t), dt.UTC) - dt.timedelta(minutes=1)
                if math.isfinite(float(value)):
                    out[at] = float(value)
        return out

    decisions = by_minute(results.get("decisions", []))
    sent = by_minute(results.get("sent", []))
    lag = by_minute(results.get("feed_lag", []))
    buckets: dict[dt.datetime, dict[float, float]] = {}
    for series in results.get("buckets", []):
        le = float(series["metric"]["le"])
        for at, value in by_minute([series]).items():
            buckets.setdefault(at, {})[le] = value

    minutes: list[Minute] = []
    at = start.replace(second=0, microsecond=0)
    while at < end:
        minutes.append(
            Minute(
                at=at,
                decisions=decisions.get(at, 0.0),
                sent=sent.get(at, 0.0),
                feed_lag=lag.get(at),
                buckets=buckets.get(at, {}),
            )
        )
        at += dt.timedelta(minutes=1)
    return minutes


def fetch_minutes(prometheus: str, start: dt.datetime, end: dt.datetime) -> list[Minute]:
    """Every minute from start to end, read from a Prometheus server.

    Args:
        prometheus: Its base URL, for example `http://prometheus:9090`.
        start: The first minute.
        end: The minute after the last.

    Returns:
        The minutes, in order.
    """
    import urllib.parse
    import urllib.request

    results: dict[str, list[Mapping[str, Any]]] = {name: [] for name in QUERIES}
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + _CHUNK, end)
        for name, expr in QUERIES.items():
            query = urllib.parse.urlencode(
                {
                    "query": expr,
                    "start": (chunk_start + dt.timedelta(minutes=1)).timestamp(),
                    "end": chunk_end.timestamp(),
                    "step": 60,
                }
            )
            url = f"{prometheus.rstrip('/')}/api/v1/query_range?{query}"
            with urllib.request.urlopen(url, timeout=60) as response:
                body = json.loads(response.read())
            results[name].extend(body["data"]["result"])
        chunk_start = chunk_end
    return assemble_minutes(start, end, results)
