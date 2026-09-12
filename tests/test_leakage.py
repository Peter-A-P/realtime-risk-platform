"""The leakage test, tested.

A test that cannot fail is worth nothing, and a leakage test that cannot fail
is worth less than nothing, because it certifies whatever it is pointed at.
So every check here is run twice: once against a serving implementation that
is correct, which must come back clean, and once against a planted leak,
which must come back red and name the feature.

The planted leaks are the three that actually happen:

1. The window includes the event being scored.
2. The window reaches forward past the as-of time.
3. The training join uses the label time instead of the event time.
"""

from __future__ import annotations

import datetime as dt
from collections import deque

import pytest

from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent
from verdict.store.features import (
    FEATURE_SET,
    Aggregation,
    EntityKind,
    FeatureSpec,
    entity_id_of,
    evaluate_spec,
    events_in_window,
    validate_feature_set,
)
from verdict.store.leakage import (
    LeakageError,
    RetrievalLookup,
    ServedLookup,
    check_label_shift_invariance,
    check_point_in_time,
    training_rows_from,
)

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)

CARD_COUNT_1H = FeatureSpec(
    name="card_txn_count_1h",
    entity=EntityKind.CARD,
    aggregation=Aggregation.COUNT,
    window=dt.timedelta(hours=1),
    description="Transactions on this card in the last hour.",
)
CARD_AMOUNT_SUM_1H = FeatureSpec(
    name="card_amount_sum_1h",
    entity=EntityKind.CARD,
    aggregation=Aggregation.SUM,
    field="amount_cents",
    window=dt.timedelta(hours=1),
    description="Amount on this card in the last hour.",
)
DEVICE_DISTINCT_CARDS = FeatureSpec(
    name="device_distinct_cards_24h",
    entity=EntityKind.DEVICE,
    aggregation=Aggregation.DISTINCT_COUNT,
    field="card_id",
    window=dt.timedelta(hours=24),
    description="Cards seen on this device in a day. Card testing lives here.",
)

SPECS = [CARD_COUNT_1H, CARD_AMOUNT_SUM_1H, DEVICE_DISTINCT_CARDS]


def an_event(
    minutes: int, card: str = "card-1", device: str = "dev-1", amount: int = 1_000
) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{card}-{device}-{minutes}",
        event_time=START + dt.timedelta(minutes=minutes),
        card_id=card,
        device_id=device,
        merchant_id="mer-1",
        amount_cents=amount,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


@pytest.fixture
def events() -> list[TransactionEvent]:
    """A small log with a card-testing burst on one device in the middle."""
    ordinary = [an_event(minutes) for minutes in (0, 10, 20, 30, 90, 200)]
    burst = [
        an_event(100 + index, card=f"card-burst-{index}", device="dev-attacker", amount=120)
        for index in range(6)
    ]
    return sorted(ordinary + burst, key=lambda event: event.event_time)


# --- the correct serving implementation, and three broken ones --------------


def correct_served(events: list[TransactionEvent]) -> ServedLookup:
    """An incremental implementation, deliberately not the reference one.

    Calling `evaluate_spec` here would make the clean-path tests tautological:
    the harness compares the served value against `evaluate_spec`, so a served
    value that *is* `evaluate_spec` can never disagree with itself, and the
    test would pass on a harness that did nothing at all.

    So this keeps state per entity and walks the log in order, evicting as it
    goes, which is how the Bytewax dataflow will do it in week 3. It is a
    different execution of the same definition, and the fact that the two
    agree is the thing actually worth asserting.
    """
    ordered = sorted(events, key=lambda event: event.event_time)

    def lookup(spec: FeatureSpec, entity_id: str, as_of: dt.datetime) -> float:
        state: deque[TransactionEvent] = deque()
        for event in ordered:
            if event.event_time >= as_of:
                break  # a consumer at `as_of` has not seen this event yet
            if entity_id_of(event, spec.entity) != entity_id:
                continue
            state.append(event)
            if spec.window is not None:
                horizon = event.event_time - spec.window
                while state and state[0].event_time < horizon:
                    state.popleft()
        if spec.window is not None:
            horizon = as_of - spec.window
            while state and state[0].event_time < horizon:
                state.popleft()
        if not state:
            return -1.0
        return _reduce(spec, list(state), as_of)

    return lookup


def _reduce(spec: FeatureSpec, window: list[TransactionEvent], as_of: dt.datetime) -> float:
    """Reduce an already-windowed list, the way a stateful operator would."""
    match spec.aggregation:
        case Aggregation.COUNT:
            return float(len(window))
        case Aggregation.SECONDS_SINCE_LAST:
            return (as_of - window[-1].event_time).total_seconds()
        case Aggregation.DISTINCT_COUNT:
            return float(len({getattr(event, spec.field or "") for event in window}))
        case Aggregation.SUM:
            return float(sum(getattr(event, spec.field or "") for event in window))
        case Aggregation.MEAN:
            return float(sum(getattr(event, spec.field or "") for event in window)) / len(window)
        case Aggregation.MAX:
            return float(max(getattr(event, spec.field or "") for event in window))
        case Aggregation.MIN:
            return float(min(getattr(event, spec.field or "") for event in window))


def leaky_includes_current(events: list[TransactionEvent]) -> ServedLookup:
    """The classic: the window is `<=` where it should be `<`."""

    def lookup(spec: FeatureSpec, entity_id: str, as_of: dt.datetime) -> float:
        shifted = [
            event
            for event in events
            if entity_id_of(event, spec.entity) == entity_id and event.event_time <= as_of
        ]
        return evaluate_spec(spec, shifted, entity_id, as_of + dt.timedelta(microseconds=1))

    return lookup


def leaky_peeks_forward(events: list[TransactionEvent]) -> ServedLookup:
    """A window that reaches an hour into the future."""

    def lookup(spec: FeatureSpec, entity_id: str, as_of: dt.datetime) -> float:
        return evaluate_spec(spec, events, entity_id, as_of + dt.timedelta(hours=1))

    return lookup


# --- the checks come back clean on a correct implementation -----------------


def test_a_correct_implementation_is_clean(events: list[TransactionEvent]) -> None:
    report = check_point_in_time(SPECS, events, correct_served(events))
    assert report.clean, report.summary()
    assert report.features_checked == 3
    assert report.rows_checked > 0


def test_an_empty_feature_set_passes(events: list[TransactionEvent]) -> None:
    """Week 2's criterion: green over nothing, before any feature exists.

    This is not a formality. The test existing and passing now is what makes
    it impossible to add the first feature in week 3 without it.
    """
    report = check_point_in_time([], events, correct_served(events))
    assert report.clean
    assert report.features_checked == 0


def test_the_platform_s_own_feature_set_is_clean(events: list[TransactionEvent]) -> None:
    """Runs against whatever `FEATURE_SET` holds, which is nothing yet.

    It keeps working as week 3 fills it, which is the point of pointing it at
    the registry rather than at a fixture.
    """
    validate_feature_set(FEATURE_SET)
    report = check_point_in_time(FEATURE_SET, events, correct_served(events))
    report.raise_if_leaking()


# --- and red on each planted leak -------------------------------------------


def test_a_window_that_includes_the_current_event_is_caught(
    events: list[TransactionEvent],
) -> None:
    """The leak that flatters every offline metric and cannot be served."""
    report = check_point_in_time(SPECS, events, leaky_includes_current(events))
    assert not report.clean
    assert {violation.feature for violation in report.violations} == {spec.name for spec in SPECS}
    for violation in report.violations:
        assert violation.served > violation.reference


def test_a_window_that_peeks_forward_is_caught(events: list[TransactionEvent]) -> None:
    report = check_point_in_time(SPECS, events, leaky_peeks_forward(events))
    assert not report.clean
    assert any(violation.feature == "card_txn_count_1h" for violation in report.violations)


def test_the_report_names_the_feature_and_the_moment(
    events: list[TransactionEvent],
) -> None:
    """A failure has to say what to go and look at."""
    report = check_point_in_time(SPECS, events, leaky_includes_current(events))
    text = report.summary()
    assert "LEAKAGE" in text
    assert "card_txn_count_1h" in text
    violation = report.violations[0]
    assert violation.entity_id in str(violation)
    assert violation.as_of.isoformat() in str(violation)


def test_raise_if_leaking_raises_with_the_report(events: list[TransactionEvent]) -> None:
    report = check_point_in_time(SPECS, events, leaky_includes_current(events))
    with pytest.raises(LeakageError) as caught:
        report.raise_if_leaking()
    assert caught.value.report is report
    assert "LEAKAGE" in str(caught.value)


# --- label-shift invariance -------------------------------------------------


def correct_retrieval(events: list[TransactionEvent]) -> RetrievalLookup:
    """Ignores the label time entirely, which is the whole requirement."""

    def retrieve(
        spec: FeatureSpec, entity_id: str, event_time: dt.datetime, label_time: dt.datetime
    ) -> float:
        del label_time
        return evaluate_spec(spec, events, entity_id, event_time)

    return retrieve


def leaky_joins_on_label_time(events: list[TransactionEvent]) -> RetrievalLookup:
    """The second classic: the training join uses the label's timestamp."""

    def retrieve(
        spec: FeatureSpec, entity_id: str, event_time: dt.datetime, label_time: dt.datetime
    ) -> float:
        del event_time
        return evaluate_spec(spec, events, entity_id, label_time)

    return retrieve


def test_correct_retrieval_does_not_move_when_labels_move(
    events: list[TransactionEvent],
) -> None:
    rows = training_rows_from(events)
    report = check_label_shift_invariance(SPECS, rows, correct_retrieval(events))
    assert report.clean, report.summary()


def test_a_join_on_the_label_time_is_caught(events: list[TransactionEvent]) -> None:
    """Seven days of future, invisible to every offline metric."""
    rows = training_rows_from(events, label_delay=dt.timedelta(days=7))
    report = check_label_shift_invariance(
        SPECS, rows, leaky_joins_on_label_time(events), shift=dt.timedelta(days=6, hours=23)
    )
    assert not report.clean
    assert any(violation.check == "label-shift-invariance" for violation in report.violations)


def test_training_rows_carry_both_times_and_every_entity(
    events: list[TransactionEvent],
) -> None:
    rows = training_rows_from(events)
    assert len(rows) == len(events)
    row = rows[0]
    assert row.label_time - row.event_time == dt.timedelta(days=7)
    assert set(row.entity_ids) >= {"card", "device", "merchant"}
    assert "session" not in row.entity_ids  # card-present events have none


# --- the window convention itself -------------------------------------------


def test_the_window_is_half_open(events: list[TransactionEvent]) -> None:
    """`[as_of - window, as_of)`. Stated in the module, asserted here."""
    at_ten = START + dt.timedelta(minutes=10)
    selected = events_in_window(events, CARD_COUNT_1H, "card-1", at_ten)
    times = [event.event_time for event in selected]
    assert at_ten not in times
    assert START in times


def test_an_event_is_never_part_of_its_own_features(
    events: list[TransactionEvent],
) -> None:
    for event in events:
        window = events_in_window(events, CARD_COUNT_1H, event.card_id, event.event_time)
        assert event not in window


def test_an_empty_window_is_a_sentinel_not_a_zero() -> None:
    """Zero is a real count. "No history" is not the same claim."""
    value = evaluate_spec(CARD_COUNT_1H, [], "card-nobody", START)
    assert value == -1.0


def test_the_window_boundary_is_inclusive_at_the_far_end() -> None:
    """An event exactly one window old is in; one microsecond older is out."""
    as_of = START + dt.timedelta(hours=1)
    on_the_edge = [an_event(0)]
    assert evaluate_spec(CARD_COUNT_1H, on_the_edge, "card-1", as_of) == 1.0
    assert (
        evaluate_spec(CARD_COUNT_1H, on_the_edge, "card-1", as_of + dt.timedelta(microseconds=1))
        == -1.0
    )


# --- specification validation -----------------------------------------------


@pytest.mark.parametrize(
    ("aggregation", "field"),
    [
        (Aggregation.COUNT, "amount_cents"),
        (Aggregation.SUM, None),
        (Aggregation.SECONDS_SINCE_LAST, "amount_cents"),
        (Aggregation.DISTINCT_COUNT, None),
    ],
)
def test_an_incoherent_specification_is_refused(
    aggregation: Aggregation, field: str | None
) -> None:
    with pytest.raises(ValueError, match=".+"):
        FeatureSpec(name="x", entity=EntityKind.CARD, aggregation=aggregation, field=field)


def test_a_non_positive_window_is_refused() -> None:
    with pytest.raises(ValueError, match="window must be positive"):
        FeatureSpec(
            name="x",
            entity=EntityKind.CARD,
            aggregation=Aggregation.COUNT,
            window=dt.timedelta(0),
        )


def test_duplicate_feature_names_are_refused() -> None:
    with pytest.raises(ValueError, match="duplicate feature names"):
        validate_feature_set([CARD_COUNT_1H, CARD_COUNT_1H])


def test_aggregating_a_non_number_is_refused(events: list[TransactionEvent]) -> None:
    spec = FeatureSpec(
        name="nonsense",
        entity=EntityKind.CARD,
        aggregation=Aggregation.SUM,
        field="merchant_category",
    )
    with pytest.raises(ValueError, match="not a number"):
        evaluate_spec(spec, events, "card-1", START + dt.timedelta(hours=5))


def test_a_field_that_does_not_exist_is_refused(events: list[TransactionEvent]) -> None:
    spec = FeatureSpec(
        name="nonsense",
        entity=EntityKind.CARD,
        aggregation=Aggregation.SUM,
        field="definitely_not_a_field",
    )
    with pytest.raises(ValueError, match="no field"):
        evaluate_spec(spec, events, "card-1", START + dt.timedelta(hours=5))


# --- the aggregations mean what they say ------------------------------------


def test_the_aggregations_compute_what_they_claim(events: list[TransactionEvent]) -> None:
    as_of = START + dt.timedelta(minutes=35)
    assert evaluate_spec(CARD_COUNT_1H, events, "card-1", as_of) == 4.0
    assert evaluate_spec(CARD_AMOUNT_SUM_1H, events, "card-1", as_of) == 4_000.0
    mean = FeatureSpec(
        name="m",
        entity=EntityKind.CARD,
        aggregation=Aggregation.MEAN,
        field="amount_cents",
        window=dt.timedelta(hours=1),
    )
    assert evaluate_spec(mean, events, "card-1", as_of) == 1_000.0
    since = FeatureSpec(
        name="s", entity=EntityKind.CARD, aggregation=Aggregation.SECONDS_SINCE_LAST
    )
    assert evaluate_spec(since, events, "card-1", as_of) == 300.0


def test_distinct_count_sees_the_card_testing_burst(
    events: list[TransactionEvent],
) -> None:
    """The feature that exists to catch one device running many cards."""
    as_of = START + dt.timedelta(minutes=110)
    assert evaluate_spec(DEVICE_DISTINCT_CARDS, events, "dev-attacker", as_of) == 6.0
    assert evaluate_spec(DEVICE_DISTINCT_CARDS, events, "dev-1", as_of) == 1.0
