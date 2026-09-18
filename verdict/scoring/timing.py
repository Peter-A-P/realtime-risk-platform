"""The per-hop clock, and the statistics latency is reported with.

Every decision records how long each hop took, in integer nanoseconds from
`time.perf_counter_ns`, which is monotonic and never adjusted by the system
clock. The hops are the ones `PLAN.md` section 2.4 budgets:

| Hop | From | To |
|---|---|---|
| `ingest` | the producer handing the event to the stream | the scorer starting on it |
| `features` | starting on it | its features served |
| `model` | features served | score returned |
| `decision` | score returned | action chosen and the decision record built |
| `persist` | decision built | decision handed to the stream |

`ingest` includes the broker round trip and any time the event waited in a
consumed batch behind earlier events; that wait is real latency and belongs to
the stream, not to the scorer. `persist` ends when the decision is handed to
the producer, not when the broker acknowledges it, because acknowledgement is
batched: the time a batch spends in flush and checkpoint is recorded
separately, per batch, as `commit`, and reported beside the hops rather than
spread across them as if every event paid it.

Intervals follow the week 1 measurement: a statistic computed per run, and a
95 percent t interval across runs.
"""

from __future__ import annotations

import contextlib
import platform
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Final

HOPS: Final[tuple[str, ...]] = ("ingest", "features", "model", "decision", "persist")

_T_975: Final[dict[int, float]] = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    25: 2.060,
    30: 2.042,
}
"""Two-sided 95 percent critical values of Student's t, by degrees of freedom.

A table rather than scipy, which this project does not otherwise need. Degrees
of freedom between listed values use the next smaller listed one, which widens
the interval slightly, never narrows it.
"""


@contextlib.contextmanager
def fine_grained_timers() -> Iterator[bool]:
    """Ask Windows for a 1 ms timer while measuring or serving, if it is Windows.

    Windows gives each process a timer whose resolution defaults to about
    15.6 ms, and since Windows 10 version 2004 that resolution is per process:
    another process asking for a finer timer does not grant one here. Every
    wait a client makes, including the one inside the Kafka client's flush,
    is rounded up to the next tick.

    The cost of not asking is not small and it is not noise. Measured on
    2026-09-17, on this project's own load test: the time from producing a
    record to its acknowledgement was 48 ms at p50 without this and 3.7 ms
    with it, against the same broker, with nothing else changed. The first
    figure is a timer artefact, and `docs/latency-budget.md` reports both so
    the difference cannot be mistaken for a platform property.

    On Linux, where the live stack runs, there is nothing to ask for and this
    does nothing.

    Yields:
        Whether a finer timer was requested.
    """
    if platform.system() != "Windows":
        yield False
        return
    import ctypes

    winmm = ctypes.WinDLL("winmm")
    granted = winmm.timeBeginPeriod(1) == 0
    try:
        yield granted
    finally:
        if granted:
            winmm.timeEndPeriod(1)


@dataclass(frozen=True, slots=True)
class HopSample:
    """One decision's timings.

    Attributes:
        started_ns: When the scorer started on the event.
        features_ns: Time to serve features.
        model_ns: Time to score.
        decision_ns: Time to apply the rules and build the record.
        persist_ns: Time to hand the decision to the stream.
    """

    started_ns: int
    features_ns: int
    model_ns: int
    decision_ns: int
    persist_ns: int

    @property
    def finished_ns(self) -> int:
        """When the decision was handed to the stream.

        Returns:
            The perf-counter timestamp.
        """
        return (
            self.started_ns + self.features_ns + self.model_ns + self.decision_ns + self.persist_ns
        )


def percentile(values: Sequence[float], q: float) -> float:
    """The q-th percentile, by linear interpolation between order statistics.

    Args:
        values: The observations. Must not be empty.
        q: The percentile, 0 to 100.

    Returns:
        The percentile.

    Raises:
        ValueError: If there are no values or q is out of range.
    """
    if not values:
        msg = "no values"
        raise ValueError(msg)
    if not 0 <= q <= 100:
        msg = f"percentile must be between 0 and 100, got {q}"
        raise ValueError(msg)
    ordered = sorted(values)
    position = (len(ordered) - 1) * q / 100
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def t_critical(degrees_of_freedom: int) -> float:
    """The two-sided 95 percent critical value of t.

    Args:
        degrees_of_freedom: At least 1.

    Returns:
        The critical value. Above 30 degrees of freedom, the value at 30.

    Raises:
        ValueError: If degrees of freedom is below 1.
    """
    if degrees_of_freedom < 1:
        msg = "an interval needs at least two runs"
        raise ValueError(msg)
    listed = max(df for df in _T_975 if df <= degrees_of_freedom)
    return _T_975[listed]


@dataclass(frozen=True, slots=True)
class Interval:
    """A mean across runs with its 95 percent t interval.

    Attributes:
        mean: The mean.
        low: Lower bound.
        high: Upper bound.
        runs: How many runs it came from.
    """

    mean: float
    low: float
    high: float
    runs: int

    def rounded(self, places: int = 2) -> dict[str, float | int]:
        """Render for a report.

        Args:
            places: Decimal places.

        Returns:
            The fields, rounded.
        """
        return {
            "mean": round(self.mean, places),
            "low": round(self.low, places),
            "high": round(self.high, places),
            "runs": self.runs,
        }


def t_interval(values: Sequence[float]) -> Interval:
    """A 95 percent t interval for the mean of per-run values.

    Args:
        values: One value per run, at least two.

    Returns:
        The interval.
    """
    n = len(values)
    if n < 2:
        msg = "an interval needs at least two runs"
        raise ValueError(msg)
    mean = sum(values) / n
    variance = sum((value - mean) ** 2 for value in values) / (n - 1)
    half = t_critical(n - 1) * (variance / n) ** 0.5
    return Interval(mean=mean, low=mean - half, high=mean + half, runs=n)
