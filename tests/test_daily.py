"""The live window day by day: days cut at UTC midnight, rows read from the queries."""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from verdict.observe import daily

START = dt.datetime(2026, 9, 29, 19, 11, tzinfo=dt.UTC)


def test_days_are_utc_days_with_the_first_and_last_cut() -> None:
    end = dt.datetime(2026, 10, 1, 6, 0, tzinfo=dt.UTC)
    got = daily.days(START, end)
    assert [b.isoformat() for b, _ in got] == [
        "2026-09-29T19:11:00+00:00",
        "2026-09-30T00:00:00+00:00",
        "2026-10-01T00:00:00+00:00",
    ]
    assert got[0][1] == dt.datetime(2026, 9, 30, tzinfo=dt.UTC)
    assert got[-1][1] == end


def vector(**values: float) -> list[dict[str, Any]]:
    return [{"metric": {}, "value": [0, str(v)]} for v in values.values()]


def fake(expr: str, at: float) -> list[dict[str, Any]]:
    """A day of 1,000 decisions: 990 inside 50 ms, 10 between 50 ms and 100 ms."""
    del at
    buckets = {"0.01": 600.0, "0.05": 990.0, "0.1": 1000.0, "+Inf": 1000.0}
    if expr.startswith("sum by (le)"):
        return [{"metric": {"le": le}, "value": [0, str(n)]} for le, n in buckets.items()]
    if "by (action)" in expr:
        return [
            {"metric": {"action": "approve"}, "value": [0, "900"]},
            {"metric": {"action": "review"}, "value": [0, "100"]},
        ]
    if "by (model_version)" in expr:
        return [{"metric": {"model_version": "champion-1"}, "value": [0, "1000"]}]
    if 'le="0.05"' in expr:
        return vector(v=990.0)
    if "_count[" in expr or ("decisions_total[" in expr and "rate(" not in expr):
        return vector(v=1000.0)
    if "_sum[" in expr:
        return vector(v=12.0)
    if "histogram_quantile" in expr:
        q = float(re.search(r"histogram_quantile\(([0-9.]+)", expr).group(1))  # type: ignore[union-attr]
        return vector(v={0.5: 0.008, 0.99: 0.05, 0.999: 0.095}[q])
    if "count_over_time" in expr:
        return vector(v=1430.0)
    if "max_over_time" in expr:
        return vector(v=1250.4)
    if "dead_letters" in expr:
        return []
    raise AssertionError(expr)


def test_a_row_counts_the_day_from_the_queries() -> None:
    begin = dt.datetime(2026, 10, 1, tzinfo=dt.UTC)
    row = daily.day_row(fake, begin, begin + dt.timedelta(days=1))
    assert row["day"] == "2026-10-01"
    assert row["decisions"] == 1000
    assert row["mean_ms"] == 12.0
    assert (row["p50_ms"], row["p99_ms"], row["p999_ms"]) == (8.0, 50.0, 95.0)
    assert row["p99_bucket_ms"] == [10.0, 50.0]
    assert row["over_budget_share"] == 0.01
    assert row["actions"] == {"approve": 900, "review": 100}
    assert row["models"] == {"champion-1": 1000}
    assert row["peak_per_second"] == 1250
    assert row["dead_letters"] is None
    assert row["minutes_without_decisions"] == 10


def test_the_window_is_queried_over_each_days_own_length() -> None:
    seen: list[tuple[str, float]] = []

    def recording(expr: str, at: float) -> list[dict[str, Any]]:
        seen.append((expr, at))
        return fake(expr, at)

    end = dt.datetime(2026, 9, 30, 12, tzinfo=dt.UTC)
    got = daily.report(recording, START, end)
    assert [r["day"] for r in got["days"]] == ["2026-09-29", "2026-09-30"]
    first = {expr for expr, at in seen if at == dt.datetime(2026, 9, 30, tzinfo=dt.UTC).timestamp()}
    assert all("[17340s" in expr for expr in first if "increase(" in expr)
    assert got["track"] == "synthetic live"
