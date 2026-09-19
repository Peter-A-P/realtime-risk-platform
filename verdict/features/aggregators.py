"""Sliding-window aggregations with bounded state.

Each aggregator answers one `FeatureSpec` for one entity. It is given values
as they arrive, evicts what has fallen out of the window, and reports a value
as of a moment. Three properties are non-negotiable, because the whole
platform rests on them:

**Bounded state.** A merchant taking a thousand transactions an hour cannot
be answered by rescanning its history per event. Every aggregator here keeps
what its window needs and nothing more.

**Amortised constant time.** Each value is pushed once and evicted once. The
maximum aggregator uses a monotonic deque, which discards values that can
never be the maximum again, so it too is amortised constant rather than a
scan.

**The window is `[as_of - window, as_of)`.** Eviction happens at query time
against the query's own horizon, never against the time of the last push. An
aggregator that evicted only when something new arrived would report a stale
window for a quiet entity: a card that has not transacted for two hours must
report an empty one-hour window, not the one it had when it last moved.

The event being scored is never in here when it is asked. That is enforced by
the engine, which serves before it observes, rather than by a comparison in
each aggregator; see `engine.py`.

**A feature with a resolution is held in buckets** (ADR 20). Its window's far
edge is rounded down to the resolution (`FeatureSpec.window_start`), so every
event in one bucket leaves the window at the same moment, and a bucket's
summary (a count, a total, a maximum, a set) answers exactly what its events
would. The `Bucketed*` aggregators keep one entry per bucket rather than one
per event: at most 25 for a day at hourly resolution, whatever the rate.
"""

from __future__ import annotations

import datetime as dt
import sys
from abc import ABC, abstractmethod
from array import array
from collections import Counter, deque

from verdict.store.features import EPOCH, NO_EVENTS, Aggregation, FeatureSpec, floor_to


class Aggregator(ABC):
    """One windowed aggregation for one entity.

    Attributes:
        window: How far back the aggregation looks, or None for unbounded.
    """

    __slots__ = ("resolution", "window")

    def __init__(
        self, window: dt.timedelta | None, *, resolution: dt.timedelta | None = None
    ) -> None:
        """Store the window.

        Args:
            window: The window, or None for unbounded.
            resolution: The far edge's rounding, or None for exact.
        """
        self.window = window
        self.resolution = resolution

    def horizon(self, as_of: dt.datetime) -> dt.datetime | None:
        """The oldest timestamp still inside the window at a moment.

        Args:
            as_of: The moment being asked about.

        Returns:
            The horizon, or None when the window is unbounded. With a
            resolution, `as_of - window` rounded down to it, as
            `FeatureSpec.window_start` defines.
        """
        if self.window is None:
            return None
        start = as_of - self.window
        return start if self.resolution is None else floor_to(start, self.resolution)

    def bucket(self, at: dt.datetime) -> dt.datetime:
        """The bucket an observation belongs to: its resolution step, or itself.

        Args:
            at: When it happened.

        Returns:
            The bucket's start.
        """
        return at if self.resolution is None else floor_to(at, self.resolution)

    @abstractmethod
    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Add one observation.

        Args:
            at: When it happened.
            value: The field being aggregated, or None where the aggregation
                needs no field.
        """

    @abstractmethod
    def value(self, as_of: dt.datetime) -> float:
        """Report the aggregation as of a moment, evicting first.

        Args:
            as_of: The moment being asked about.

        Returns:
            The value, or `NO_EVENTS` if the window is empty.
        """

    @abstractmethod
    def is_empty(self) -> bool:
        """Whether the aggregator is holding nothing.

        Returns:
            True if there is no state left, so the engine can drop it.
        """


class CountAggregator(Aggregator):
    """How many events fell in the window."""

    __slots__ = ("_times", "_unbounded_count")

    def __init__(self, window: dt.timedelta | None) -> None:
        """Prepare the state.

        Args:
            window: The window, or None for unbounded.
        """
        super().__init__(window)
        self._times: deque[dt.datetime] = deque()
        self._unbounded_count = 0

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record an event.

        Args:
            at: When it happened.
            value: Ignored; a count needs no field.
        """
        del value
        if self.window is None:
            self._unbounded_count += 1
        else:
            self._times.append(at)

    def value(self, as_of: dt.datetime) -> float:
        """Count the events still in the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The count, or `NO_EVENTS` if the window is empty.
        """
        if self.window is None:
            return float(self._unbounded_count) if self._unbounded_count else NO_EVENTS
        horizon = self.horizon(as_of)
        if horizon is not None:
            while self._times and self._times[0] < horizon:
                self._times.popleft()
        return float(len(self._times)) if self._times else NO_EVENTS

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return not self._times and not self._unbounded_count


class SumAggregator(Aggregator):
    """Sum, and the mean, of a numeric field over the window.

    The running total is maintained rather than recomputed, so eviction
    subtracts. Floating-point addition is not associative, so a total
    maintained this way can drift from one computed by summing the window
    afresh. These are amounts in integer cents, held exactly by a float well
    past any amount this platform will see, so the drift is zero rather than
    small; the leakage test compares against a fresh sum and would show it if
    that ever stopped being true.
    """

    __slots__ = ("_entries", "_mean", "_total")

    def __init__(self, window: dt.timedelta | None, *, mean: bool = False) -> None:
        """Prepare the state.

        Args:
            window: The window, or None for unbounded.
            mean: Report the mean rather than the total.
        """
        super().__init__(window)
        self._entries: deque[tuple[dt.datetime, float]] = deque()
        self._total = 0.0
        self._mean = mean

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record a value.

        Args:
            at: When it happened.
            value: The number to add.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, int | float) or isinstance(value, bool):
            msg = f"sum needs a number, got {type(value).__name__}"
            raise TypeError(msg)
        self._entries.append((at, float(value)))
        self._total += float(value)

    def _evict(self, as_of: dt.datetime) -> None:
        """Drop what has fallen out of the window.

        Args:
            as_of: The moment being asked about.
        """
        horizon = self.horizon(as_of)
        if horizon is None:
            return
        while self._entries and self._entries[0][0] < horizon:
            self._total -= self._entries.popleft()[1]

    def value(self, as_of: dt.datetime) -> float:
        """Report the total or the mean.

        Args:
            as_of: The moment being asked about.

        Returns:
            The value, or `NO_EVENTS` if the window is empty.
        """
        self._evict(as_of)
        if not self._entries:
            return NO_EVENTS
        return self._total / len(self._entries) if self._mean else self._total

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return not self._entries


class ExtremeAggregator(Aggregator):
    """Maximum or minimum of a numeric field over the window.

    A sliding maximum cannot be maintained with a single running value,
    because evicting the current maximum leaves no way to find the next one
    without a scan. The standard answer is a monotonic deque: a value that
    arrives larger than those behind it makes them unreachable as future
    maxima, so they are discarded on arrival. Each value is pushed and popped
    at most once, which keeps this amortised constant rather than linear.
    """

    __slots__ = ("_candidates", "_largest")

    def __init__(self, window: dt.timedelta | None, *, largest: bool = True) -> None:
        """Prepare the state.

        Args:
            window: The window, or None for unbounded.
            largest: Report the maximum rather than the minimum.
        """
        super().__init__(window)
        self._candidates: deque[tuple[dt.datetime, float]] = deque()
        self._largest = largest

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record a value, discarding those it makes unreachable.

        Args:
            at: When it happened.
            value: The number.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, int | float) or isinstance(value, bool):
            msg = f"max or min needs a number, got {type(value).__name__}"
            raise TypeError(msg)
        numeric = float(value)
        while self._candidates and (
            self._candidates[-1][1] <= numeric
            if self._largest
            else self._candidates[-1][1] >= numeric
        ):
            self._candidates.pop()
        self._candidates.append((at, numeric))

    def value(self, as_of: dt.datetime) -> float:
        """Report the extreme value in the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The value, or `NO_EVENTS` if the window is empty.
        """
        horizon = self.horizon(as_of)
        if horizon is not None:
            while self._candidates and self._candidates[0][0] < horizon:
                self._candidates.popleft()
        return self._candidates[0][1] if self._candidates else NO_EVENTS

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return not self._candidates


class DistinctAggregator(Aggregator):
    """How many distinct values of a field appeared in the window.

    This is the entity-graph aggregation: distinct cards on a device is what
    a card-testing burst looks like from the outside. Distinctness needs the
    multiset, not just the count, because evicting one occurrence of a value
    that appeared three times must not reduce the distinct count.
    """

    __slots__ = ("_counts", "_entries")

    def __init__(self, window: dt.timedelta | None) -> None:
        """Prepare the state.

        Args:
            window: The window, or None for unbounded.
        """
        super().__init__(window)
        self._entries: deque[tuple[dt.datetime, str]] = deque()
        self._counts: Counter[str] = Counter()

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record a value.

        Args:
            at: When it happened.
            value: The value whose distinctness is being counted.
        """
        key = sys.intern(str(value))
        self._entries.append((at, key))
        self._counts[key] += 1

    def value(self, as_of: dt.datetime) -> float:
        """Count the distinct values still in the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The count, or `NO_EVENTS` if the window is empty.
        """
        horizon = self.horizon(as_of)
        if horizon is not None:
            while self._entries and self._entries[0][0] < horizon:
                _, key = self._entries.popleft()
                self._counts[key] -= 1
                if self._counts[key] == 0:
                    del self._counts[key]
        return float(len(self._counts)) if self._counts else NO_EVENTS

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return not self._entries


class SecondsSinceLastAggregator(Aggregator):
    """Seconds between the last event in the window and the moment asked about.

    The one feature that is about silence rather than activity: a card that
    has not been used for a month, suddenly used twice in a minute, is the
    shape of a takeover.
    """

    __slots__ = ("_last",)

    def __init__(
        self, window: dt.timedelta | None, *, resolution: dt.timedelta | None = None
    ) -> None:
        """Prepare the state.

        Args:
            window: The window, or None for unbounded.
            resolution: The far edge's rounding, or None for exact.
        """
        super().__init__(window, resolution=resolution)
        self._last: dt.datetime | None = None

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record an event.

        Args:
            at: When it happened.
            value: Ignored.
        """
        del value
        self._last = at if self._last is None or at > self._last else self._last

    def value(self, as_of: dt.datetime) -> float:
        """Report the gap.

        Args:
            as_of: The moment being asked about.

        Returns:
            The gap in seconds, or `NO_EVENTS` if the last event has fallen
            out of the window or there has never been one.
        """
        if self._last is None:
            return NO_EVENTS
        horizon = self.horizon(as_of)
        if horizon is not None and self._last < horizon:
            # Forget it rather than merely ignoring it. A timestamp that has
            # fallen out of the window can never re-enter one, and an
            # aggregator that held on to it would report itself non-empty
            # forever, so `prune` would never drop the entity. Over 87 live
            # days that is one retained aggregator per card that ever
            # transacted, which is a memory leak with a long fuse.
            self._last = None
            return NO_EVENTS
        return (as_of - self._last).total_seconds()

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return self._last is None


class _Buckets(Aggregator):
    """Shared machinery for the bucketed aggregators.

    Buckets are named by their index, whole resolutions since the epoch, and
    held in `array` columns rather than as objects: a day at hourly resolution
    is at most 25 entries, where removing from the front of an array costs
    nothing worth measuring, and an entry costs 8 bytes a column instead of a
    tuple, a datetime and a float. Measured on 2026-09-19: about 5.8 kB for a
    full day held as objects, against well under 1 kB as arrays (ADR 20).
    """

    __slots__ = ("_keys",)

    def __init__(self, window: dt.timedelta, *, resolution: dt.timedelta) -> None:
        """Prepare the state.

        Args:
            window: The window.
            resolution: The bucket size.
        """
        super().__init__(window, resolution=resolution)
        self._keys = array("q")

    def _index(self, at: dt.datetime) -> int:
        assert self.resolution is not None
        return (at - EPOCH) // self.resolution

    def _first_live(self, as_of: dt.datetime) -> int:
        """How many buckets at the front have left the window."""
        horizon = self.horizon(as_of)
        if horizon is None:
            return 0
        oldest = self._index(horizon)
        gone = 0
        while gone < len(self._keys) and self._keys[gone] < oldest:
            gone += 1
        return gone

    def _is_current(self, key: int) -> bool:
        return bool(self._keys) and self._keys[-1] == key

    def is_empty(self) -> bool:
        """Whether anything is held.

        Returns:
            True if nothing is held.
        """
        return not self._keys


class BucketedCountAggregator(_Buckets):
    """A count, one entry per bucket."""

    __slots__ = ("_counts", "_total")

    def __init__(self, window: dt.timedelta, *, resolution: dt.timedelta) -> None:
        """Prepare the state.

        Args:
            window: The window.
            resolution: The bucket size.
        """
        super().__init__(window, resolution=resolution)
        self._counts = array("q")
        self._total = 0

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record an event in its bucket.

        Args:
            at: When it happened.
            value: Ignored.
        """
        del value
        key = self._index(at)
        if self._is_current(key):
            self._counts[-1] += 1
        else:
            self._keys.append(key)
            self._counts.append(1)
        self._total += 1

    def value(self, as_of: dt.datetime) -> float:
        """Count the events in buckets still inside the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The count, or `NO_EVENTS` if the window is empty.
        """
        gone = self._first_live(as_of)
        if gone:
            self._total -= sum(self._counts[:gone])
            del self._keys[:gone]
            del self._counts[:gone]
        return float(self._total) if self._total else NO_EVENTS


class BucketedSumAggregator(_Buckets):
    """Sum or mean, a total and a count per bucket."""

    __slots__ = ("_count", "_counts", "_mean", "_total", "_totals")

    def __init__(
        self, window: dt.timedelta, *, resolution: dt.timedelta, mean: bool = False
    ) -> None:
        """Prepare the state.

        Args:
            window: The window.
            resolution: The bucket size.
            mean: Report the mean rather than the total.
        """
        super().__init__(window, resolution=resolution)
        self._totals = array("d")
        self._counts = array("q")
        self._total = 0.0
        self._count = 0
        self._mean = mean

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Add a value to its bucket.

        Args:
            at: When it happened.
            value: The number.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, int | float) or isinstance(value, bool):
            msg = f"sum needs a number, got {type(value).__name__}"
            raise TypeError(msg)
        key = self._index(at)
        if self._is_current(key):
            self._totals[-1] += float(value)
            self._counts[-1] += 1
        else:
            self._keys.append(key)
            self._totals.append(float(value))
            self._counts.append(1)
        self._total += float(value)
        self._count += 1

    def value(self, as_of: dt.datetime) -> float:
        """Report the total or the mean over buckets still inside the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The value, or `NO_EVENTS` if the window is empty.
        """
        gone = self._first_live(as_of)
        if gone:
            self._total -= sum(self._totals[:gone])
            self._count -= sum(self._counts[:gone])
            del self._keys[:gone]
            del self._totals[:gone]
            del self._counts[:gone]
        if not self._count:
            return NO_EVENTS
        return self._total / self._count if self._mean else self._total


class BucketedExtremeAggregator(_Buckets):
    """Maximum or minimum: a monotonic sequence with at most one entry per bucket.

    Within a bucket every value leaves the window at the same moment, so only
    the bucket's extreme can ever be the answer: a new value that does not
    beat the bucket's current extreme is dropped, and one that does replaces
    it (and, as in the exact version, everything behind it that it beats).
    """

    __slots__ = ("_largest", "_values")

    def __init__(
        self, window: dt.timedelta, *, resolution: dt.timedelta, largest: bool = True
    ) -> None:
        """Prepare the state.

        Args:
            window: The window.
            resolution: The bucket size.
            largest: Report the maximum rather than the minimum.
        """
        super().__init__(window, resolution=resolution)
        self._values = array("d")
        self._largest = largest

    def _beats(self, new: float, old: float) -> bool:
        return new >= old if self._largest else new <= old

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record a value, keeping only what can still be the extreme.

        Args:
            at: When it happened.
            value: The number.

        Raises:
            TypeError: If the value is not a number.
        """
        if not isinstance(value, int | float) or isinstance(value, bool):
            msg = f"max or min needs a number, got {type(value).__name__}"
            raise TypeError(msg)
        numeric = float(value)
        key = self._index(at)
        if self._is_current(key) and not self._beats(numeric, self._values[-1]):
            return
        while self._values and self._beats(numeric, self._values[-1]):
            self._values.pop()
            self._keys.pop()
        self._keys.append(key)
        self._values.append(numeric)

    def value(self, as_of: dt.datetime) -> float:
        """Report the extreme value over buckets still inside the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The value, or `NO_EVENTS` if the window is empty.
        """
        gone = self._first_live(as_of)
        if gone:
            del self._keys[:gone]
            del self._values[:gone]
        return self._values[0] if self._values else NO_EVENTS


class BucketedDistinctAggregator(_Buckets):
    """Distinct count, by each value's latest bucket.

    A value is in the window if any of its occurrences is, which is the same
    as its latest occurrence being in it. So the state is one entry per
    distinct value (the bucket it was last seen in) and, per bucket, how many
    values were last seen there. When a bucket leaves, exactly its values
    leave the count. Values are interned, so an id seen by many entities is
    held once. Measured on 2026-09-19 at the live per-entity rate, a set per
    bucket held each card's merchants up to 25 times over (ADR 20).
    """

    __slots__ = ("_counts", "_latest", "_total")

    def __init__(self, window: dt.timedelta, *, resolution: dt.timedelta) -> None:
        """Prepare the state.

        Args:
            window: The window.
            resolution: The bucket size.
        """
        super().__init__(window, resolution=resolution)
        self._counts = array("q")
        self._latest: dict[str, int] = {}
        self._total = 0

    def _position(self, key: int) -> int | None:
        """Where a bucket is among the held ones, searching from the newest."""
        for position in range(len(self._keys) - 1, -1, -1):
            if self._keys[position] == key:
                return position
            if self._keys[position] < key:
                return None
        return None

    def push(self, at: dt.datetime, value: float | str | None) -> None:
        """Record a value as last seen in this event's bucket.

        Args:
            at: When it happened.
            value: The value whose distinctness is counted.
        """
        key, item = self._index(at), sys.intern(str(value))
        if not self._is_current(key):
            self._keys.append(key)
            self._counts.append(0)
        previous = self._latest.get(item)
        if previous == key:
            return
        position = None if previous is None else self._position(previous)
        if position is None:
            self._total += 1
        else:
            self._counts[position] -= 1
        self._latest[item] = key
        self._counts[-1] += 1

    def value(self, as_of: dt.datetime) -> float:
        """Count the distinct values last seen in buckets still inside the window.

        Args:
            as_of: The moment being asked about.

        Returns:
            The count, or `NO_EVENTS` if the window is empty.
        """
        gone = self._first_live(as_of)
        if gone:
            self._total -= sum(self._counts[:gone])
            del self._keys[:gone]
            del self._counts[:gone]
            oldest = self._keys[0] if self._keys else None
            self._latest = {
                item: key
                for item, key in self._latest.items()
                if oldest is not None and key >= oldest
            }
        return float(self._total) if self._total else NO_EVENTS


def build_aggregator(spec: FeatureSpec) -> Aggregator:
    """Create the aggregator a specification calls for.

    Args:
        spec: The feature.

    Returns:
        A new, empty aggregator: bucketed if the feature has a resolution.
    """
    if spec.resolution is not None and spec.window is not None:
        window, resolution = spec.window, spec.resolution
        match spec.aggregation:
            case Aggregation.COUNT:
                return BucketedCountAggregator(window, resolution=resolution)
            case Aggregation.SUM:
                return BucketedSumAggregator(window, resolution=resolution)
            case Aggregation.MEAN:
                return BucketedSumAggregator(window, resolution=resolution, mean=True)
            case Aggregation.MAX:
                return BucketedExtremeAggregator(window, resolution=resolution, largest=True)
            case Aggregation.MIN:
                return BucketedExtremeAggregator(window, resolution=resolution, largest=False)
            case Aggregation.DISTINCT_COUNT:
                return BucketedDistinctAggregator(window, resolution=resolution)
            case Aggregation.SECONDS_SINCE_LAST:
                return SecondsSinceLastAggregator(window, resolution=resolution)
    match spec.aggregation:
        case Aggregation.COUNT:
            return CountAggregator(spec.window)
        case Aggregation.SUM:
            return SumAggregator(spec.window)
        case Aggregation.MEAN:
            return SumAggregator(spec.window, mean=True)
        case Aggregation.MAX:
            return ExtremeAggregator(spec.window, largest=True)
        case Aggregation.MIN:
            return ExtremeAggregator(spec.window, largest=False)
        case Aggregation.DISTINCT_COUNT:
            return DistinctAggregator(spec.window)
        case Aggregation.SECONDS_SINCE_LAST:
            return SecondsSinceLastAggregator(spec.window)
