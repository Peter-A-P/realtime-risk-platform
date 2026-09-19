"""Each aggregator agrees with a brute-force scan, on any input.

The aggregators are the one place in this platform where a clever data
structure earns its keep: a monotonic deque for the sliding maximum, a
multiset for distinct counts, a running total with subtracting eviction. Each
is a chance to be subtly wrong at a window boundary, and a boundary bug here
is exactly the leak the whole project is built to prevent.

So they are checked against the obvious implementation on random inputs, with
Hypothesis choosing the awkward cases: repeated timestamps, values that all
arrive at once, queries long after everything has expired, windows that hold
nothing.
"""

from __future__ import annotations

import datetime as dt

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from verdict.features.aggregators import (
    CountAggregator,
    DistinctAggregator,
    ExtremeAggregator,
    SecondsSinceLastAggregator,
    SumAggregator,
    build_aggregator,
)
from verdict.store.features import NO_EVENTS, Aggregation, EntityKind, FeatureSpec

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)
WINDOW = dt.timedelta(minutes=60)

# (offset in minutes, value) pairs, in non-decreasing time order.
observations = st.lists(
    st.tuples(st.integers(min_value=0, max_value=300), st.integers(min_value=1, max_value=10_000)),
    min_size=0,
    max_size=60,
).map(lambda pairs: sorted(pairs, key=lambda pair: pair[0]))

query_offsets = st.integers(min_value=0, max_value=400)


def in_window(
    pairs: list[tuple[int, int]], at: int, window_minutes: int | None = 60
) -> list[tuple[int, int]]:
    """The obvious implementation: filter to `[at - window, at)`."""
    if window_minutes is None:
        return [pair for pair in pairs if pair[0] < at]
    return [pair for pair in pairs if at - window_minutes <= pair[0] < at]


def pushable(pairs: list[tuple[int, int]], at: int) -> list[tuple[int, int]]:
    """The observations an aggregator may legitimately have been given.

    An aggregator is only ever handed events that are strictly older than the
    moment it is later asked about: the engine serves before it observes, and
    holds events back until time moves on, so nothing at or after the query
    time can be inside it. Pushing such an event here would test a situation
    that cannot arise and would report a bug that does not exist. The engine
    is what guarantees this, and `tests/test_engine.py` is where that
    guarantee is tested.
    """
    return [pair for pair in pairs if pair[0] < at]


@settings(max_examples=200)
@given(pairs=observations, at=query_offsets)
def test_count_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = CountAggregator(WINDOW)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    assert result == (float(len(expected)) if expected else NO_EVENTS)


@settings(max_examples=200)
@given(pairs=observations, at=query_offsets)
def test_sum_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = SumAggregator(WINDOW)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    assert result == (float(sum(v for _, v in expected)) if expected else NO_EVENTS)


@settings(max_examples=200)
@given(pairs=observations, at=query_offsets)
def test_mean_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = SumAggregator(WINDOW, mean=True)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    if not expected:
        assert result == NO_EVENTS
    else:
        assert result == pytest.approx(sum(v for _, v in expected) / len(expected))


@settings(max_examples=300)
@given(pairs=observations, at=query_offsets)
def test_the_sliding_maximum_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    """The monotonic deque is the least obvious structure here."""
    aggregator = ExtremeAggregator(WINDOW, largest=True)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    assert result == (float(max(v for _, v in expected)) if expected else NO_EVENTS)


@settings(max_examples=300)
@given(pairs=observations, at=query_offsets)
def test_the_sliding_minimum_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = ExtremeAggregator(WINDOW, largest=False)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    assert result == (float(min(v for _, v in expected)) if expected else NO_EVENTS)


@settings(max_examples=300)
@given(pairs=observations, at=query_offsets)
def test_distinct_count_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    """Evicting one occurrence of a repeated value must not lose the value."""
    aggregator = DistinctAggregator(WINDOW)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), f"v{value % 5}")
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    distinct = {f"v{value % 5}" for _, value in expected}
    assert result == (float(len(distinct)) if distinct else NO_EVENTS)


@settings(max_examples=200)
@given(pairs=observations, at=query_offsets)
def test_seconds_since_last_matches_a_scan(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = SecondsSinceLastAggregator(WINDOW)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    expected = in_window(pairs, at)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    if not expected:
        assert result == NO_EVENTS
    else:
        assert result == pytest.approx((at - max(m for m, _ in expected)) * 60.0)


@settings(max_examples=100)
@given(pairs=observations, at=query_offsets)
def test_unbounded_windows_never_evict(pairs: list[tuple[int, int]], at: int) -> None:
    aggregator = CountAggregator(None)
    for minutes, value in pushable(pairs, at):
        aggregator.push(START + dt.timedelta(minutes=minutes), value)
    result = aggregator.value(START + dt.timedelta(minutes=at))
    pushed = pushable(pairs, at)
    assert result == (float(len(pushed)) if pushed else NO_EVENTS)


# --- the properties that make the window convention hold --------------------


def test_eviction_happens_at_query_time_not_push_time() -> None:
    """A quiet entity must report an empty window, not a stale one.

    An aggregator that only evicted when something new arrived would answer
    with the window it held when the card was last used, which for a dormant
    card could be months old and would look like furious recent activity.
    """
    aggregator = CountAggregator(WINDOW)
    aggregator.push(START, 1)
    assert aggregator.value(START + dt.timedelta(minutes=30)) == 1.0
    assert aggregator.value(START + dt.timedelta(minutes=90)) == NO_EVENTS


def test_the_far_edge_of_the_window_is_inclusive() -> None:
    """`[as_of - window, as_of)`: exactly one window old is still in."""
    aggregator = CountAggregator(WINDOW)
    aggregator.push(START, 1)
    assert aggregator.value(START + WINDOW) == 1.0
    assert aggregator.value(START + WINDOW + dt.timedelta(microseconds=1)) == NO_EVENTS


def test_simultaneous_events_are_all_counted() -> None:
    """At a thousand events a second, collisions are normal."""
    aggregator = CountAggregator(WINDOW)
    for _ in range(5):
        aggregator.push(START, 1)
    assert aggregator.value(START + dt.timedelta(minutes=1)) == 5.0


def test_an_empty_aggregator_reports_the_sentinel_not_zero() -> None:
    """Zero is a real count; "nothing here" is a different claim."""
    assert CountAggregator(WINDOW).value(START) == NO_EVENTS
    assert SumAggregator(WINDOW).value(START) == NO_EVENTS
    assert DistinctAggregator(WINDOW).value(START) == NO_EVENTS


def test_emptiness_is_reported_so_state_can_be_dropped() -> None:
    """An 87-day window would otherwise keep every card that ever appeared."""
    aggregator = CountAggregator(WINDOW)
    assert aggregator.is_empty()
    aggregator.push(START, 1)
    assert not aggregator.is_empty()
    aggregator.value(START + dt.timedelta(minutes=90))
    assert aggregator.is_empty()


@pytest.mark.parametrize(
    ("aggregation", "field"),
    [
        (Aggregation.SUM, "amount_cents"),
        (Aggregation.MEAN, "amount_cents"),
        (Aggregation.MAX, "amount_cents"),
        (Aggregation.MIN, "amount_cents"),
    ],
)
def test_numeric_aggregations_refuse_a_non_number(aggregation: Aggregation, field: str) -> None:
    spec = FeatureSpec(
        name="x", entity=EntityKind.CARD, aggregation=aggregation, field=field, window=WINDOW
    )
    with pytest.raises(TypeError, match="needs a number"):
        build_aggregator(spec).push(START, "not a number")


def test_every_aggregation_can_be_built() -> None:
    """Every aggregation has a builder.

    One without would fail at run time, on whichever unlucky event first
    needed it, in the middle of the live window.
    """
    for aggregation in Aggregation:
        needs_field = aggregation not in {
            Aggregation.COUNT,
            Aggregation.SECONDS_SINCE_LAST,
        }
        spec = FeatureSpec(
            name="x",
            entity=EntityKind.CARD,
            aggregation=aggregation,
            field="amount_cents" if needs_field else None,
            window=WINDOW,
        )
        assert build_aggregator(spec) is not None


# --- a window with a resolution (ADR 20) -------------------------------------

RESOLUTION = dt.timedelta(minutes=15)


def in_resolved_window(pairs: list[tuple[int, int]], at: int) -> list[tuple[int, int]]:
    """The definition, by scan: `[floor(at - window), at)` in 15-minute steps."""
    far = ((at - 60) // 15) * 15
    return [pair for pair in pairs if far <= pair[0] < at]


def resolved(aggregation: Aggregation, field: str | None) -> FeatureSpec:
    return FeatureSpec(
        name="resolved",
        entity=EntityKind.CARD,
        aggregation=aggregation,
        field=field,
        window=WINDOW,
        resolution=RESOLUTION,
    )


@settings(max_examples=300)
@given(pairs=observations, at=query_offsets)
def test_every_bucketed_aggregation_matches_the_resolved_definition(
    pairs: list[tuple[int, int]], at: int
) -> None:
    """One summary per bucket must answer exactly what the bucket's events would."""
    # START is 12:00, on a 15-minute boundary, so offsets floor as minutes do.
    expected = in_resolved_window(pairs, at)
    values = [v for _, v in expected]
    cases: list[tuple[Aggregation, str | None, float]] = [
        (Aggregation.COUNT, None, float(len(values))),
        (Aggregation.SUM, "amount_cents", float(sum(values))),
        (Aggregation.MEAN, "amount_cents", sum(values) / len(values) if values else 0.0),
        (Aggregation.MAX, "amount_cents", float(max(values, default=0))),
        (Aggregation.MIN, "amount_cents", float(min(values, default=0))),
        (Aggregation.DISTINCT_COUNT, "merchant_id", float(len({v % 5 for v in values}))),
    ]
    for aggregation, field, want in cases:
        aggregator = build_aggregator(resolved(aggregation, field))
        for minutes, value in pushable(pairs, at):
            item: float | str = f"v{value % 5}" if field == "merchant_id" else value
            aggregator.push(START + dt.timedelta(minutes=minutes), item)
        result = aggregator.value(START + dt.timedelta(minutes=at))
        if not values:
            assert result == NO_EVENTS, aggregation
        else:
            assert result == pytest.approx(want), aggregation


def test_a_bucketed_window_holds_one_entry_per_bucket_whatever_the_rate() -> None:
    """The point of ADR 20: memory set by buckets, not by events."""
    spec = FeatureSpec(
        name="day",
        entity=EntityKind.CARD,
        aggregation=Aggregation.COUNT,
        window=dt.timedelta(hours=24),
        resolution=dt.timedelta(hours=1),
    )
    aggregator = build_aggregator(spec)
    for second in range(0, 26 * 3600, 3):
        aggregator.push(START + dt.timedelta(seconds=second), None)
    aggregator.value(START + dt.timedelta(hours=26))
    assert len(aggregator._keys) <= 25  # type: ignore[attr-defined]


def test_a_resolution_that_does_not_divide_the_window_is_refused() -> None:
    with pytest.raises(ValueError, match="resolution"):
        FeatureSpec(
            name="odd",
            entity=EntityKind.CARD,
            aggregation=Aggregation.COUNT,
            window=dt.timedelta(minutes=50),
            resolution=dt.timedelta(minutes=15),
        )
