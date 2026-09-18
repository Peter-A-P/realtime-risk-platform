"""The scorer as a service: it stops between batches, and its metrics say what it did.

The loop is thin, so these are about the two properties a loop can break: a
stop request must never split a batch (decided but not checkpointed), and a
service that runs for months must not keep every timing it ever took.
"""

from __future__ import annotations

import datetime as dt
import threading

from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.observe.metrics import ScorerMetrics
from verdict.scoring import service
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.stream.memory import MemoryBroker, MemoryStream

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)


def an_event(index: int) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{index}",
        event_time=START + dt.timedelta(seconds=index),
        card_id=f"card-{index % 4}",
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=2_500 + index,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


def a_broker(events: int) -> MemoryBroker:
    broker = MemoryBroker()
    broker.create_topic("transactions", 1)
    broker.create_topic("decisions", 2)
    broker.create_topic("dead-letter", 1)
    stream = broker.open()
    for index in range(events):
        stream.produce("transactions", f"card-{index % 4}", an_event(index).to_json().encode())
    return broker


def a_scorer(stream: MemoryStream, metrics: ScorerMetrics | None = None) -> StreamScorer:
    return StreamScorer(
        stream,
        decider=Decider(
            features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel())
        ),
        on_decided=None if metrics is None else metrics.on_decided,
    )


class StopAfter(threading.Event):
    """A stop request that arrives after a set number of checks, mid-stream."""

    def __init__(self, checks: int) -> None:
        """Answer "not yet" this many times, then "stop".

        Args:
            checks: How many checks before the stop.
        """
        super().__init__()
        self.checks = checks

    def is_set(self) -> bool:
        """Count a check, and say whether the stop has arrived.

        Returns:
            Whether to stop.
        """
        self.checks -= 1
        return self.checks < 0


def test_a_stop_request_is_honoured_between_batches_never_inside_one() -> None:
    """Whatever was consumed before the stop is decided and checkpointed.

    The service is stopped after two polls of seven records. A second scorer on
    the same group must pick up exactly where the first stopped: nothing
    consumed and left unacknowledged, nothing acknowledged and left undecided.
    """
    broker = a_broker(30)
    first = a_scorer(broker.open())
    summary = service.run(first, stop=StopAfter(2), max_records=7, timeout_seconds=0.01)
    assert summary.polls == 2
    assert summary.records == 14
    assert summary.decided == 14

    second = a_scorer(broker.open())
    service.run(second, stop=StopAfter(5), max_records=7, timeout_seconds=0.01)
    assert second.decider.stats.decided == 16


def test_metrics_count_every_decision_and_drain_the_timing_lists() -> None:
    """A service must not keep every batch timing it ever took."""
    broker = a_broker(40)
    metrics = ScorerMetrics()
    scorer = a_scorer(broker.open(), metrics)
    service.run(scorer, stop=StopAfter(8), metrics=metrics, max_records=10, timeout_seconds=0.01)

    assert scorer.commits.batches == 4
    assert scorer.commits.flush_ns == []
    assert scorer.commits.checkpoint_ns == []
    decided = sum(
        sample.value
        for family in metrics.registry.collect()
        if family.name == "verdict_decisions"
        for sample in family.samples
        if sample.name == "verdict_decisions_total"
    )
    assert decided == 40
    flushes = metrics.registry.get_sample_value("verdict_commit_seconds_count", {"part": "flush"})
    assert flushes == 4
    batches = metrics.registry.get_sample_value("verdict_batch_records_sum")
    assert batches == 40


def test_metrics_count_what_was_set_aside_and_what_was_redelivered() -> None:
    broker = a_broker(3)
    stream = broker.open()
    stream.produce("transactions", "card-1", b"not a transaction")
    stream.produce("transactions", "card-0", an_event(0).to_json().encode())
    metrics = ScorerMetrics()
    scorer = a_scorer(broker.open(), metrics)
    service.run(scorer, stop=StopAfter(3), metrics=metrics, timeout_seconds=0.01)

    registry = metrics.registry
    assert registry.get_sample_value("verdict_dead_letters_total", {"reason": "undecodable"}) == 1
    assert registry.get_sample_value("verdict_duplicates_total") == 1
    assert registry.get_sample_value("verdict_last_decision_timestamp_seconds") is not None


def test_the_exposition_names_every_hop_the_scorer_can_time() -> None:
    broker = a_broker(5)
    metrics = ScorerMetrics()
    scorer = a_scorer(broker.open(), metrics)
    service.run(scorer, stop=StopAfter(2), metrics=metrics, timeout_seconds=0.01)
    text = metrics.exposition().decode()
    for hop in ("features", "model", "decision", "persist"):
        assert f'verdict_hop_seconds_count{{hop="{hop}"}} 5.0' in text


def test_two_scorers_do_not_share_counts() -> None:
    """Each has its own registry, never the process-global one."""
    first, second = ScorerMetrics(), ScorerMetrics()
    broker = a_broker(4)
    scorer = a_scorer(broker.open(), first)
    service.run(scorer, stop=StopAfter(2), metrics=first, timeout_seconds=0.01)
    assert second.registry.get_sample_value("verdict_duplicates_total") == 0
    assert first.registry is not second.registry
