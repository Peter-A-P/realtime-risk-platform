"""The scorer: every transaction decided once, in order, and never checkpointed early.

These run on the in-process stream, which the contract tests hold to the same
behaviour as Redpanda. The ones that matter most are the duplicate test,
which checks that a redelivered transaction reaches neither the decision topic
nor the feature engine, and the commit test, which checks that a failure to
write decisions leaves the transactions unacknowledged.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping

import pytest

from verdict.events.schema import (
    Action,
    DecisionEvent,
    EntryMode,
    MerchantCategory,
    ShadowEvent,
    TransactionEvent,
)
from verdict.features.engine import FeatureEngine
from verdict.scoring import loadtest
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import STAND_IN_VERSION, FixedModel, Model, StandInModel
from verdict.scoring.rules import DecisionRules
from verdict.scoring.timing import HOPS, HopSample, percentile, t_critical, t_interval
from verdict.store.features import NO_EVENTS, feature_names
from verdict.stream.base import StreamError
from verdict.stream.memory import MemoryBroker, MemoryStream

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)


def an_event(
    index: int, *, card: str = "card-1", seconds: float | None = None, amount: int = 2_500
) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{index}",
        event_time=START + dt.timedelta(seconds=index if seconds is None else seconds),
        card_id=card,
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=amount,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


def a_setup(partitions: int = 1) -> tuple[MemoryBroker, MemoryStream]:
    broker = MemoryBroker()
    broker.create_topic("transactions", partitions)
    broker.create_topic("decisions", 2)
    broker.create_topic("dead-letter", 1)
    return broker, broker.open()


def a_decider(ledger_size: int = 1_000_000) -> Decider:
    return Decider(
        features=EngineFeatures(FeatureEngine()),
        models=FixedModel(StandInModel()),
        ledger_size=ledger_size,
    )


def a_scorer(stream: MemoryStream, **kwargs: object) -> StreamScorer:
    decider = kwargs.pop("decider", None) or a_decider()
    return StreamScorer(stream, decider=decider, **kwargs)  # type: ignore[arg-type]


def send(stream: MemoryStream, events: Iterable[TransactionEvent]) -> None:
    for event in events:
        stream.produce("transactions", event.card_id, event.to_json().encode("utf-8"))


def decisions_on(broker: MemoryBroker) -> list[DecisionEvent]:
    reader = broker.open()
    out: list[DecisionEvent] = []
    while batch := reader.consume("decisions", "reader", max_records=1_000):
        out.extend(DecisionEvent.model_validate_json(record.value) for record in batch)
    return out


def drain(scorer: StreamScorer) -> None:
    while scorer.poll(max_records=7):
        pass


# --- the scorer -----------------------------------------------------------


def test_every_transaction_gets_exactly_one_decision() -> None:
    broker, stream = a_setup()
    events = [an_event(i, card=f"card-{i % 3}") for i in range(50)]
    send(stream, events)
    scorer = a_scorer(stream)
    drain(scorer)
    decisions = decisions_on(broker)
    assert sorted(d.event_id for d in decisions) == sorted(e.event_id for e in events)
    assert scorer.decider.stats.decided == 50
    assert all(d.model_version == STAND_IN_VERSION for d in decisions)


def test_a_redelivered_transaction_reaches_neither_the_decisions_nor_the_engine() -> None:
    """At least once upstream, exactly one decision and one observation downstream.

    If the duplicate reached the engine, the third event on the card would be
    served a count of three prior transactions in the hour instead of two.
    """
    broker, stream = a_setup()
    first, second, third = an_event(1), an_event(2), an_event(3)
    send(stream, [first, second, second, third])
    served: list[dict[str, float]] = []

    class Recording(EngineFeatures):
        def serve(self, event: TransactionEvent) -> dict[str, float]:
            values = super().serve(event)
            served.append(values)
            return values

    decider = Decider(features=Recording(FeatureEngine()), models=FixedModel(StandInModel()))
    scorer = StreamScorer(stream, decider=decider)
    drain(scorer)

    assert [d.event_id for d in decisions_on(broker)] == ["evt-1", "evt-2", "evt-3"]
    assert scorer.decider.stats.duplicates == 1
    assert [values["card_txn_count_1h"] for values in served] == [NO_EVENTS, 1.0, 2.0]


def test_decisions_are_not_checkpointed_ahead_of_being_written() -> None:
    """If the decisions cannot be flushed, the transactions must come back."""
    broker, stream = a_setup()
    send(stream, [an_event(i) for i in range(5)])

    class FailingFlush(MemoryStream):
        def flush(self, timeout_seconds: float = 10.0) -> None:
            raise StreamError("broker unavailable")

    failing = FailingFlush(broker)
    with pytest.raises(StreamError):
        a_scorer(failing).poll()
    failing.close()

    restarted = a_scorer(broker.open())
    drain(restarted)
    assert restarted.decider.stats.decided == 5


def test_a_restarted_scorer_resumes_after_its_last_committed_batch() -> None:
    broker, stream = a_setup()
    send(stream, [an_event(i) for i in range(10)])
    first = a_scorer(stream)
    assert first.poll(max_records=6) == 6
    stream.close()

    second = a_scorer(broker.open())
    drain(second)
    assert second.decider.stats.decided == 4


def test_a_failed_write_after_the_engine_saw_an_event_cannot_count_it_twice() -> None:
    """The ledger records an event when the engine sees it, not when its decision lands.

    If writing the decision fails and the scorer is asked about the same
    event again, it must refuse rather than serve it a second time: the first
    serve already put it in the windows.
    """
    broker, stream = a_setup()
    send(stream, [an_event(1), an_event(2)])

    class FailingProduce(MemoryStream):
        def produce(self, topic: str, key: str, value: bytes) -> None:
            if topic == "decisions":
                raise StreamError("broker unavailable")
            super().produce(topic, key, value)

    decider = a_decider()
    with pytest.raises(StreamError):
        StreamScorer(FailingProduce(broker), decider=decider).poll()

    assert decider.seen("evt-1")
    assert decider.decide(an_event(1), 0) is None
    third = decider.decide(an_event(3), 0)
    assert third is not None
    assert decider.stats.duplicates == 1


def test_the_ledger_is_bounded() -> None:
    broker, stream = a_setup()
    send(stream, [an_event(i) for i in range(20)])
    scorer = a_scorer(stream, decider=a_decider(ledger_size=5))
    drain(scorer)
    assert scorer.decider.remembered == 5


def test_every_decision_carries_a_complete_timing() -> None:
    broker, stream = a_setup()
    send(stream, [an_event(i) for i in range(10)])
    samples: list[HopSample] = []
    scorer = a_scorer(stream, on_decided=lambda event, decision, sample: samples.append(sample))
    drain(scorer)
    assert len(samples) == 10
    for sample in samples:
        assert min(sample.features_ns, sample.model_ns, sample.decision_ns, sample.persist_ns) >= 0
        assert sample.finished_ns >= sample.started_ns
    assert len(scorer.commits.commit_ns) == scorer.commits.batches
    assert (
        len(scorer.commits.flush_ns) == len(scorer.commits.checkpoint_ns) == scorer.commits.batches
    )


def test_the_feature_vector_has_no_gaps() -> None:
    """A feature whose entity is absent is the sentinel, not missing."""
    engine = FeatureEngine()
    event = an_event(1).model_copy(update={"device_id": None, "merchant_id": None})
    values = EngineFeatures(engine).serve(event)
    assert tuple(values) == feature_names()
    assert values["device_txn_count_1h"] == NO_EVENTS


def test_the_scorer_depends_only_on_the_stream_interface() -> None:
    """ADR 3: the scorer knows `Stream`, and nothing below it."""
    import ast
    from pathlib import Path

    import verdict.scoring.consumer as consumer

    tree = ast.parse(Path(consumer.__file__).read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "verdict.stream.base" in imported
    assert not {"verdict.stream.redpanda", "verdict.stream.memory", "confluent_kafka"} & imported


# --- the model and the rules ----------------------------------------------


def test_the_stand_in_is_a_model_and_scores_in_range() -> None:
    model = StandInModel()
    assert isinstance(model, Model)
    quiet = dict.fromkeys(feature_names(), NO_EVENTS)
    busy = quiet | {"card_txn_count_1h": 30.0, "device_distinct_cards_1h": 12.0}
    assert 0.0 <= model.score(quiet, an_event(1)) < model.score(busy, an_event(1)) <= 1.0


@pytest.mark.parametrize(
    ("score", "amount", "action", "rule"),
    [
        (0.95, 100, Action.DECLINE, "score-decline"),
        (0.90, 100, Action.DECLINE, "score-decline"),
        (0.60, 100, Action.REVIEW, "score-review"),
        (0.10, 900_000, Action.REVIEW, "large-amount-review"),
        (0.95, 900_000, Action.DECLINE, "score-decline"),
        (0.10, 100, Action.APPROVE, "default-approve"),
    ],
)
def test_rules_apply_in_order(score: float, amount: int, action: Action, rule: str) -> None:
    assert DecisionRules().decide(score, an_event(1, amount=amount)) == (action, rule)


@pytest.mark.parametrize(
    "kwargs",
    [{"review_at": 0.9, "decline_at": 0.5}, {"decline_at": 1.5}, {"review_amount_cents": 0}],
)
def test_incoherent_rules_are_refused(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="review"):
        DecisionRules(**kwargs)  # type: ignore[arg-type]


# --- the statistics -------------------------------------------------------


def test_percentile_interpolates_between_order_statistics() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 50) == pytest.approx(50.5)
    assert percentile(values, 99) == pytest.approx(99.01)
    assert percentile([7.0], 99) == 7.0


def test_the_t_interval_matches_a_hand_computation() -> None:
    """Five runs of 10, 12, 11, 13, 9: mean 11, s = 1.5811, t(4) = 2.776."""
    interval = t_interval([10, 12, 11, 13, 9])
    half = 2.776 * 1.58113883 / 5**0.5
    assert interval.mean == pytest.approx(11.0)
    assert interval.low == pytest.approx(11.0 - half, abs=1e-6)
    assert interval.high == pytest.approx(11.0 + half, abs=1e-6)


def test_a_single_run_has_no_interval() -> None:
    with pytest.raises(ValueError, match="two runs"):
        t_interval([1.0])


def test_unlisted_degrees_of_freedom_round_down_to_a_wider_interval() -> None:
    assert t_critical(22) == t_critical(20) > t_critical(25)


# --- the load test --------------------------------------------------------


def test_a_small_load_test_decides_everything_and_reports_every_hop() -> None:
    events = loadtest.generate_events(600, rate=2_000)
    backend = loadtest.memory_backend()
    results = [loadtest.run_once(backend, events, rate=2_000, warmup=100) for _ in range(2)]
    for result in results:
        assert result.sent == 600
        assert result.measured == 500
        assert set(result.hops) == set(HOPS)
        assert result.end_to_end["p50"] <= result.end_to_end["p99"]
        assert set(result.backlog) == {"early_p50", "late_p50"}
        assert result.producer == "the scorer's process"
        assert result.duplicates == 0
        assert result.set_aside == {}
    summary = loadtest.summarise(results)
    assert summary["end_to_end_ms"]["p99"]["runs"] == 2
    assert summary["backlog_ms"]["late_p50"]["runs"] == 2


def test_the_clock_the_load_test_joins_two_processes_on_is_one_clock() -> None:
    """The producer times its sends in its own process; the scorer times decisions here.

    The join is only meaningful if `perf_counter_ns` reads the same counter in
    both, which is a property of the host, not of Python. So it is measured
    rather than assumed: a child's reading has to fall between two of ours.
    """
    before = time.perf_counter_ns()
    child = subprocess.run(
        [sys.executable, "-c", "import time; print(time.perf_counter_ns())"],
        capture_output=True,
        text=True,
        check=True,
    )
    after = time.perf_counter_ns()
    assert before < int(child.stdout.strip()) < after


def dead_letters_on(broker: MemoryBroker) -> list[dict[str, str]]:
    reader = broker.open()
    out: list[dict[str, str]] = []
    while batch := reader.consume("dead-letter", "reader", max_records=1_000):
        out.extend(json.loads(record.value) for record in batch)
    return out


def test_a_transaction_that_arrives_out_of_time_order_is_set_aside_not_served() -> None:
    """Why the transaction topic has one partition: the engine will not guess.

    The late event is refused by the engine, goes to the dead-letter topic with
    the reason, and gets no decision; the scorer carries on with the next one.
    """
    broker, stream = a_setup()
    send(stream, [an_event(2), an_event(1), an_event(3)])
    scorer = a_scorer(stream)
    drain(scorer)
    assert [d.event_id for d in decisions_on(broker)] == ["evt-2", "evt-3"]
    [letter] = dead_letters_on(broker)
    assert letter["reason"] == "late"
    assert "evt-1" in letter["detail"]
    assert scorer.dead_letters == {"late": 1}


def test_one_malformed_record_is_set_aside_and_the_scorer_carries_on() -> None:
    """The fault this was built for: one bad message used to stop the scorer for good.

    It was never checkpointed, so every restart read it again and stopped
    again. Now it is set aside with its bytes untouched and checkpointed past.
    """
    broker, stream = a_setup()
    send(stream, [an_event(1)])
    stream.produce("transactions", "card-1", b"\xff{not json")
    send(stream, [an_event(2)])
    scorer = a_scorer(stream)
    drain(scorer)
    assert [d.event_id for d in decisions_on(broker)] == ["evt-1", "evt-2"]
    [letter] = dead_letters_on(broker)
    assert letter["reason"] == "undecodable"
    assert base64.b64decode(letter["value_base64"]) == b"\xff{not json"

    restarted = a_scorer(broker.open())
    assert restarted.poll() == 0


def test_a_record_from_a_newer_schema_is_set_aside_with_its_version() -> None:
    broker, stream = a_setup()
    stream.produce("transactions", "card-1", json.dumps({"schema_version": 3}).encode())
    send(stream, [an_event(1)])
    scorer = a_scorer(stream)
    drain(scorer)
    [letter] = dead_letters_on(broker)
    assert letter["reason"] == "unknown-schema-version"
    assert scorer.decider.stats.decided == 1


def test_a_run_of_records_that_cannot_be_decided_stops_the_scorer() -> None:
    """A run of them is a bad deployment upstream, and setting it all aside is worse."""
    from verdict.scoring.consumer import DeadLetterRunError

    broker, stream = a_setup()
    for _ in range(6):
        stream.produce("transactions", "card-1", json.dumps({"schema_version": 3}).encode())
    scorer = a_scorer(stream, max_consecutive_dead_letters=5)
    with pytest.raises(DeadLetterRunError, match="6 records in a row"):
        scorer.poll()


def test_a_good_record_resets_the_count_of_records_set_aside() -> None:
    broker, stream = a_setup()
    bad = json.dumps({"schema_version": 3}).encode()
    for index in range(1, 4):
        stream.produce("transactions", "card-1", bad)
        stream.produce("transactions", "card-1", bad)
        send(stream, [an_event(index)])
    scorer = a_scorer(stream, max_consecutive_dead_letters=2)
    drain(scorer)
    assert scorer.decider.stats.decided == 3
    assert scorer.dead_letters == {"unknown-schema-version": 6}


# --- shadow scoring -------------------------------------------------------


class Recorder:
    """A challenger that records what it was shown and scores a constant."""

    version = "challenger-test"

    def __init__(self, score: float = 0.95, *, fail: bool = False) -> None:
        """Set the constant score, or make every call raise."""
        self.seen: list[dict[str, float]] = []
        self._score = score
        self._fail = fail

    def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
        """Record the features, then score or raise."""
        del event
        if self._fail:
            raise RuntimeError("challenger broke")
        self.seen.append(dict(features))
        return self._score


def shadow_records_on(broker: MemoryBroker) -> list[ShadowEvent]:
    reader = broker.open()
    return [
        ShadowEvent.model_validate_json(record.value)
        for record in reader.consume("shadow", "reader", max_records=1_000)
    ]


def a_shadow_setup() -> tuple[MemoryBroker, MemoryStream]:
    broker, stream = a_setup()
    broker.create_topic("shadow", 2)
    return broker, stream


def test_a_shadow_changes_no_champion_decision_and_sees_the_same_features() -> None:
    events = [an_event(i, card=f"card-{i % 2}") for i in range(20)]

    plain_broker, plain_stream = a_shadow_setup()
    send(plain_stream, events)
    drain(a_scorer(plain_stream))

    broker, stream = a_shadow_setup()
    send(stream, events)
    challenger = Recorder(score=0.95)
    served: list[dict[str, float]] = []

    class Recording(EngineFeatures):
        def serve(self, event: TransactionEvent) -> dict[str, float]:
            values = super().serve(event)
            served.append(dict(values))
            return values

    decider = Decider(
        features=Recording(FeatureEngine()),
        models=FixedModel(StandInModel()),
        shadow=FixedModel(challenger),
    )
    drain(StreamScorer(stream, decider=decider))

    def essence(decisions: list[DecisionEvent]) -> list[tuple[str, float, Action, str]]:
        return sorted((d.event_id, d.score, d.action, d.rule) for d in decisions)

    assert essence(decisions_on(broker)) == essence(decisions_on(plain_broker))
    assert challenger.seen == served
    shadows = shadow_records_on(broker)
    assert len(shadows) == 20
    assert all(
        s.action is Action.DECLINE and s.champion_version == STAND_IN_VERSION for s in shadows
    )


def test_a_failing_shadow_is_counted_and_the_champion_still_decides() -> None:
    broker, stream = a_shadow_setup()
    send(stream, [an_event(i) for i in range(5)])
    decider = Decider(
        features=EngineFeatures(FeatureEngine()),
        models=FixedModel(StandInModel()),
        shadow=FixedModel(Recorder(fail=True)),
    )
    drain(StreamScorer(stream, decider=decider))
    assert len(decisions_on(broker)) == 5
    assert shadow_records_on(broker) == []
    assert decider.stats.shadow_failures == 5


def test_shadow_time_is_kept_out_of_the_champions_hops() -> None:
    import time as clock

    class Slow(Recorder):
        def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
            clock.sleep(0.02)
            return super().score(features, event)

    decider = Decider(
        features=EngineFeatures(FeatureEngine()),
        models=FixedModel(StandInModel()),
        shadow=FixedModel(Slow()),
    )
    started = clock.perf_counter_ns()
    outcome = decider.decide(an_event(1), started)
    assert outcome is not None
    assert outcome.shadow_ns >= 15_000_000
    assert outcome.model_ns + outcome.decision_ns < outcome.shadow_ns
