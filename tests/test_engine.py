"""The engine agrees with the definition, on a real replay.

This is the test the leakage work in week 2 was written for. It runs the
engine over generated traffic, records what it served, and checks every value
against the brute-force definition. It also holds down the three properties
that make that agreement structural rather than lucky: an event is served
before it is observed, an event at the same instant as another is not inside
its window, and an event that arrives late is refused rather than folded into
windows that have already passed it.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent
from verdict.features.engine import (
    FeatureEngine,
    LateEventError,
    StaleQueryError,
)
from verdict.features.sinks import DualSink, OfflineParquetSink
from verdict.features.unfixed import ObserveImmediatelyEngine
from verdict.features.verify import served_lookup_from_offline
from verdict.store.features import FEATURE_SET, NO_EVENTS, evaluate_spec
from verdict.store.leakage import check_point_in_time

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)
REFERENCE = Population(cards=300, devices=200, merchants=40)


def an_event(
    identifier: str,
    at: dt.datetime,
    *,
    card: str = "card-1",
    device: str = "dev-1",
    merchant: str = "mer-1",
    amount: int = 1_000,
) -> TransactionEvent:
    return TransactionEvent(
        event_id=identifier,
        event_time=at,
        card_id=card,
        device_id=device,
        merchant_id=merchant,
        amount_cents=amount,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


@pytest.fixture(scope="module")
def replay() -> list[TransactionEvent]:
    graph = EntityGraph.build(seed=11, population=REFERENCE)
    config = GeneratorConfig(
        seed=11, population=REFERENCE, events_per_second=50.0, target_fraud_share=0.05
    )
    return [record.event for record in Generator(config, graph).stream(limit=1_500)]


# --- the headline: the engine matches the definition ------------------------


def test_the_engine_agrees_with_the_definition_on_a_replay(
    replay: list[TransactionEvent], tmp_path: Path
) -> None:
    """Every feature, every row, against a recomputation from the raw log.

    The engine is checked through the offline store rather than directly,
    because the offline store is the record of what was actually served at
    the time. That is also what the live platform will check daily.
    """
    offline = OfflineParquetSink(tmp_path / "data")
    engine = FeatureEngine(FEATURE_SET)
    with DualSink(store=None, offline=offline, specs=FEATURE_SET, online=False) as sink:  # type: ignore[arg-type]
        for event in replay:
            sink.write(engine.process(event))
        engine.flush()

    served = served_lookup_from_offline(offline, FEATURE_SET)
    report = check_point_in_time(FEATURE_SET, replay, served, sample=replay[-200:])
    report.raise_if_leaking()
    assert report.features_checked == len(FEATURE_SET)
    assert report.rows_checked > 1_000


def test_every_feature_in_the_set_is_actually_produced(
    replay: list[TransactionEvent],
) -> None:
    """Every defined feature is actually computed.

    A feature that is defined but never produced would pass every other test
    in this file by being absent from the comparison.
    """
    engine = FeatureEngine(FEATURE_SET)
    produced: set[str] = set()
    for event in replay:
        for row in engine.process(event):
            produced.update(row.values)
    assert produced == {spec.name for spec in FEATURE_SET}


def test_the_features_separate_a_card_testing_burst(
    replay: list[TransactionEvent],
) -> None:
    """The entity-graph feature moves on a card-testing burst.

    It exists to make one device on many cards visible. If it does not move
    on the generator's own bursts, it is decoration.
    """
    engine = FeatureEngine(FEATURE_SET)
    highest = 0.0
    for event in replay:
        for row in engine.process(event):
            value = row.values.get("device_distinct_cards_1h", NO_EVENTS)
            highest = max(highest, value)
    assert highest >= 5.0


# --- the three structural properties ----------------------------------------


def test_an_event_is_never_inside_its_own_window() -> None:
    engine = FeatureEngine(FEATURE_SET)
    rows = engine.process(an_event("a", START))
    card = next(row for row in rows if row.kind.value == "card")
    assert card.values["card_txn_count_1h"] == NO_EVENTS


def test_two_events_at_the_same_instant_do_not_see_each_other() -> None:
    """The leak this engine actually shipped with for an afternoon.

    At a thousand events a second, two transactions sharing a timestamp is
    ordinary; on second-resolution data such as the public competition set it
    is almost universal. Observing the first immediately put it inside the
    second's window, which is `[t - w, t)` and excludes anything at `t`.
    """
    engine = FeatureEngine(FEATURE_SET)
    first = engine.process(an_event("a", START))
    second = engine.process(an_event("b", START))
    for rows in (first, second):
        card = next(row for row in rows if row.kind.value == "card")
        assert card.values["card_txn_count_1h"] == NO_EVENTS

    later = engine.process(an_event("c", START + dt.timedelta(minutes=5)))
    card = next(row for row in later if row.kind.value == "card")
    assert card.values["card_txn_count_1h"] == 2.0


def test_the_definition_agrees_about_the_same_instant() -> None:
    """The definition agrees about the same instant.

    The engine's behaviour here is not merely self-consistent; it is what the
    brute-force definition independently says.
    """
    events = [
        an_event("a", START),
        an_event("b", START),
        an_event("c", START + dt.timedelta(minutes=5)),
    ]
    spec = next(spec for spec in FEATURE_SET if spec.name == "card_txn_count_1h")
    engine = FeatureEngine(FEATURE_SET)
    served = [
        next(row.values[spec.name] for row in engine.process(event) if row.kind.value == "card")
        for event in events
    ]
    reference = [evaluate_spec(spec, events, "card-1", event.event_time) for event in events]
    assert served == reference


def test_a_late_event_is_refused_rather_than_folded_in() -> None:
    """A late event is refused rather than folded in.

    Folding it in would corrupt every window that has already passed it, and
    nothing downstream would ever notice.
    """
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START + dt.timedelta(minutes=10)))
    with pytest.raises(LateEventError) as caught:
        engine.process(an_event("b", START))
    assert caught.value.event.event_id == "b"
    assert "corrupt" in str(caught.value)


def test_the_engine_refuses_to_answer_for_the_past() -> None:
    """It keeps one evolving window per entity, not a history of them."""
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START))
    engine.process(an_event("b", START + dt.timedelta(hours=2)))
    spec = next(spec for spec in FEATURE_SET if spec.name == "card_txn_count_1h")
    with pytest.raises(StaleQueryError, match="offline store"):
        engine.lookup(spec, "card-1", START)


def test_lookup_answers_for_the_present() -> None:
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START))
    engine.process(an_event("b", START + dt.timedelta(minutes=5)))
    engine.flush()
    spec = next(spec for spec in FEATURE_SET if spec.name == "card_txn_count_1h")
    now = START + dt.timedelta(minutes=10)
    assert engine.lookup(spec, "card-1", now) == 2.0


def test_an_unknown_entity_reports_no_history_rather_than_failing() -> None:
    engine = FeatureEngine(FEATURE_SET)
    spec = next(spec for spec in FEATURE_SET if spec.name == "card_txn_count_1h")
    assert engine.lookup(spec, "card-never-seen", START) == NO_EVENTS


# --- the unfixed engine, kept on purpose ------------------------------------


def test_the_unfixed_engine_is_caught_at_second_resolution(tmp_path: Path) -> None:
    """The regression test for the fix, and the proof the check still bites.

    Timestamps are truncated to whole seconds, as the public competition data
    publishes them, which is the case where the leak is severe rather than
    rare.
    """
    events = [
        event.model_copy(update={"event_time": event.event_time.replace(microsecond=0)})
        for event in a_second_resolution_replay()
    ]

    for engine_type, expect_clean in ((FeatureEngine, True), (ObserveImmediatelyEngine, False)):
        offline = OfflineParquetSink(tmp_path / engine_type.__name__)
        engine = engine_type(FEATURE_SET)
        with DualSink(store=None, offline=offline, specs=FEATURE_SET, online=False) as sink:  # type: ignore[arg-type]
            for event in events:
                sink.write(engine.process(event))
            engine.flush()
        report = check_point_in_time(
            FEATURE_SET,
            events,
            served_lookup_from_offline(offline, FEATURE_SET),
            sample=events[-150:],
        )
        assert report.clean is expect_clean, report.summary()


def a_second_resolution_replay() -> list[TransactionEvent]:
    graph = EntityGraph.build(seed=3, population=REFERENCE)
    config = GeneratorConfig(
        seed=3, population=REFERENCE, events_per_second=200.0, target_fraud_share=0.05
    )
    return [record.event for record in Generator(config, graph).stream(limit=800)]


# --- state does not grow without bound --------------------------------------


def test_state_is_dropped_once_every_window_has_emptied() -> None:
    """Empty state is dropped.

    87 days at a thousand events a second would otherwise keep an aggregator
    for every card that ever transacted.
    """
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START))
    engine.flush()
    assert engine.tracked_entities > 0
    dropped = engine.prune(START + dt.timedelta(days=2))
    assert dropped > 0
    assert engine.tracked_entities == 0


def test_pruning_does_not_drop_state_that_is_still_in_window() -> None:
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START))
    engine.flush()
    engine.prune(START + dt.timedelta(minutes=5))
    assert engine.tracked_entities > 0


def test_a_sweep_keeps_an_entity_seen_again_and_drops_the_one_that_was_not() -> None:
    """The sweep goes by when an entity was last seen, not when it was first."""
    engine = FeatureEngine(FEATURE_SET)
    engine.process(an_event("a", START, card="card-1", device="dev-1", merchant="mer-1"))
    engine.process(
        an_event(
            "b", START + dt.timedelta(minutes=1), card="card-2", device="dev-2", merchant="mer-2"
        )
    )
    later = START + dt.timedelta(days=2)
    engine.process(an_event("c", later, card="card-1", device="dev-1", merchant="mer-1"))
    engine.flush()
    assert engine.prune(later + dt.timedelta(minutes=1)) == 3
    rows = engine.serve(an_event("d", later + dt.timedelta(minutes=2), card="card-1"))
    card = next(row for row in rows if row.kind.value == "card")
    assert any(value != NO_EVENTS for value in card.values.values())


def test_when_state_is_dropped_never_changes_a_feature(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sweeping after every event and never sweeping serve the same values.

    An emptied entity serves `NO_EVENTS` whether its state is held or gone,
    which is what lets the sweep be incremental and run whenever it likes.
    """
    events = [
        an_event(
            f"e{i}",
            START + dt.timedelta(hours=7 * i),
            card=f"card-{i % 3}",
            device=f"dev-{i % 5}",
            merchant=f"mer-{i % 2}",
            amount=100 * (i + 1),
        )
        for i in range(40)
    ]
    import verdict.features.engine as engine_module

    monkeypatch.setattr(engine_module, "PRUNE_EVERY", 10**9)
    kept = FeatureEngine(FEATURE_SET).run(events)
    monkeypatch.setattr(engine_module, "PRUNE_EVERY", 1)
    swept_engine = FeatureEngine(FEATURE_SET)
    swept = swept_engine.run(events)
    assert swept == kept
    assert swept_engine.tracked_entities < 3 + 5 + 2


def test_a_card_present_event_produces_no_session_row() -> None:
    """A card-present event produces no session row.

    Not every event has every entity, and a session that does not exist must
    not become a row keyed on the string "None".
    """
    engine = FeatureEngine(FEATURE_SET)
    rows = engine.process(an_event("a", START))
    assert {row.kind.value for row in rows} == {"card", "device", "merchant"}


def test_running_a_replay_flushes_at_the_end(replay: list[TransactionEvent]) -> None:
    engine = FeatureEngine(FEATURE_SET)
    engine.run(replay)
    spec = next(spec for spec in FEATURE_SET if spec.name == "card_txn_count_1h")
    last = replay[-1]
    assert engine.lookup(spec, last.card_id, last.event_time + dt.timedelta(seconds=1)) >= 1.0


def test_the_day_windows_agree_with_the_definition_across_hour_boundaries() -> None:
    """ADR 20: three days of sparse traffic, so buckets leave the day windows.

    Every feature the engine serves, on every event, against the definition
    recomputed from the raw events, including the hourly-resolved far edge.
    """
    import numpy as np

    from verdict.scoring.core import EngineFeatures
    from verdict.store.features import entity_id_of, evaluate_spec

    rng = np.random.default_rng(5)
    seconds = np.sort(rng.uniform(0, 3 * 86_400, size=400))
    events = [
        an_event(
            f"evt-{i}",
            START + dt.timedelta(seconds=float(at)),
            card=f"card-{rng.integers(0, 4)}",
            device=f"dev-{rng.integers(0, 3)}",
            merchant=f"mer-{rng.integers(0, 5)}",
            amount=int(rng.integers(100, 50_000)),
        )
        for i, at in enumerate(seconds)
    ]
    source = EngineFeatures(FeatureEngine(FEATURE_SET))
    for event in events:
        served = source.serve(event)
        for spec in FEATURE_SET:
            entity_id = entity_id_of(event, spec.entity)
            if entity_id is None:
                continue
            expected = evaluate_spec(spec, events, entity_id, event.event_time)
            assert served[spec.name] == pytest.approx(expected), (spec.name, event.event_id)
