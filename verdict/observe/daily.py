"""The live window day by day, from the metrics Prometheus kept (ADR 36).

The dashboard shows the window as the stack saw it; the stack, and with it
the dashboard, ends at teardown. This reads the same metrics into one row a
UTC day, small enough to commit (`docs/live/daily.json`) and to draw on the
demo site, which outlives the stack.

Every row counts every decision of its day, not a sample, so the counts and
shares carry no sampling interval. The latency percentiles are interpolated
within the scorer's histogram buckets (`observe/metrics.py`), so each is
known to its bucket: `p99_bucket_ms` gives the bucket a p99 fell in. A
minute without decisions is one whose rate Prometheus saw as zero or did
not see at all: a scorer restart, or the stack stopped.
Synthetic live track only.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any, Final

Query = Callable[[str, float], list[dict[str, Any]]]
"""An instant query at a moment (Unix seconds), returning Prometheus's result vector."""

AGE: Final = "verdict_event_to_decision_seconds"
DECISIONS: Final = "verdict_decisions_total"
BUDGET_SECONDS: Final = 0.05


def prometheus(url: str, *, timeout: float = 60.0) -> Query:
    """Instant queries against a Prometheus.

    Args:
        url: Its base URL.
        timeout: Seconds a query may take.

    Returns:
        The query function.
    """

    def query(expr: str, at: float) -> list[dict[str, Any]]:
        params = urllib.parse.urlencode({"query": expr, "time": f"{at:.3f}"})
        with urllib.request.urlopen(f"{url}/api/v1/query?{params}", timeout=timeout) as reply:
            body = json.load(reply)
        if body.get("status") != "success":
            msg = f"Prometheus refused {expr!r}: {body}"
            raise RuntimeError(msg)
        result: list[dict[str, Any]] = body["data"]["result"]
        return result

    return query


def days(start: dt.datetime, end: dt.datetime) -> list[tuple[dt.datetime, dt.datetime]]:
    """The UTC days from `start` to `end`, the first and last cut to them.

    Args:
        start: The window's start.
        end: Where the rows stop: the window's end or now.

    Returns:
        Each day's first and last moment.
    """
    out = []
    begin = start
    while begin < end:
        midnight = dt.datetime.combine(begin.date() + dt.timedelta(days=1), dt.time(), dt.UTC)
        stop = min(midnight, end)
        out.append((begin, stop))
        begin = stop
    return out


def _scalar(query: Query, expr: str, at: float) -> float | None:
    result = query(expr, at)
    if not result:
        return None
    value = float(result[0]["value"][1])
    return None if math.isnan(value) else value


def _by(query: Query, expr: str, at: float, label: str) -> dict[str, float]:
    return {r["metric"].get(label, ""): float(r["value"][1]) for r in query(expr, at)}


def _bucket_of(query: Query, window: str, at: float, quantile: float) -> list[float | None] | None:
    """The histogram bucket a quantile of the day fell in, in milliseconds."""
    counts = _by(query, f"sum by (le) (increase({AGE}_bucket[{window}]))", at, "le")
    if not counts:
        return None
    ordered = sorted((math.inf if le == "+Inf" else float(le), n) for le, n in counts.items())
    total = ordered[-1][1]
    lower = 0.0
    for upper, n in ordered:
        if n >= quantile * total:
            return [round(lower * 1000, 3), round(upper * 1000, 3) if upper != math.inf else None]
        lower = upper
    return None


def day_row(query: Query, begin: dt.datetime, stop: dt.datetime) -> dict[str, Any]:
    """One day's row.

    Args:
        query: Instant queries against the stack's Prometheus.
        begin: The day's first moment (the window's start, on its first day).
        stop: Its last.

    Returns:
        The row, JSON-ready.
    """
    at = stop.timestamp()
    seconds = max(1, round((stop - begin).total_seconds()))
    w = f"{seconds}s"
    decided = _scalar(query, f"sum(increase({DECISIONS}[{w}]))", at)
    within = _scalar(query, f'sum(increase({AGE}_bucket{{le="{BUDGET_SECONDS}"}}[{w}]))', at)
    count = _scalar(query, f"sum(increase({AGE}_count[{w}]))", at)
    total = _scalar(query, f"sum(increase({AGE}_sum[{w}]))", at)

    def quantile(q: float) -> float | None:
        expr = f"histogram_quantile({q}, sum by (le) (increase({AGE}_bucket[{w}])))"
        v = _scalar(query, expr, at)
        return None if v is None else round(v * 1000, 2)

    actions = _by(query, f"sum by (action) (increase({DECISIONS}[{w}]))", at, "action")
    models = _by(query, f"sum by (model_version) (increase({DECISIONS}[{w}]))", at, "model_version")
    peak = _scalar(query, f"max_over_time(sum(rate({DECISIONS}[5m]))[{w}:1m])", at)
    dead = _scalar(query, f"sum(increase(verdict_dead_letters_total[{w}]))", at)
    deciding = _scalar(query, f"count_over_time((sum(rate({DECISIONS}[1m])) > 0)[{w}:1m])", at)
    return {
        "day": begin.date().isoformat(),
        "from": begin.isoformat().replace("+00:00", "Z"),
        "to": stop.isoformat().replace("+00:00", "Z"),
        "decisions": None if decided is None else round(decided),
        "peak_per_second": None if peak is None else round(peak),
        "mean_ms": None if not count or total is None else round(1000 * total / count, 2),
        "p50_ms": quantile(0.5),
        "p99_ms": quantile(0.99),
        "p999_ms": quantile(0.999),
        "p99_bucket_ms": _bucket_of(query, w, at, 0.99),
        "over_budget_share": None
        if not count or within is None
        else round(max(0.0, 1 - within / count), 5),
        "actions": {k: round(v) for k, v in sorted(actions.items()) if round(v)},
        "models": {k: round(v) for k, v in sorted(models.items()) if round(v)},
        "dead_letters": None if dead is None else round(dead),
        "minutes_without_decisions": None
        if deciding is None
        else max(0, seconds // 60 - round(deciding)),
    }


def report(query: Query, start: dt.datetime, end: dt.datetime) -> dict[str, Any]:
    """The window day by day.

    Args:
        query: Instant queries against the stack's Prometheus.
        start: The window's start.
        end: Where the rows stop.

    Returns:
        The report, JSON-ready.
    """
    rows = [day_row(query, begin, stop) for begin, stop in days(start, end)]
    return {
        "track": "synthetic live",
        "source": "the live stack's Prometheus (ADR 36)",
        "budget_ms": BUDGET_SECONDS * 1000,
        "note": (
            "Every decision of each UTC day is counted, not sampled. Percentiles are "
            "interpolated within the scorer's latency buckets; p99_bucket_ms is the "
            "bucket the p99 fell in. The first and last days are partial."
        ),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "days": rows,
    }
