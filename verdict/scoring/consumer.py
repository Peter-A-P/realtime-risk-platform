"""The scorer: a stream consumer, not an HTTP service.

For each transaction on the stream, in order: check it has not already been
decided, serve its features, score it, apply the rules, and hand the decision
to the stream. After each batch: wait for the decisions to be acknowledged,
then checkpoint the transactions. ADR 8 records why it is shaped this way;
three properties are worth stating where the code is.

**Duplicates are caught before the feature engine sees them.** Delivery is at
least once. A redelivered transaction that reached the engine would be counted
twice in every window it falls in, which is a feature bug that no model-side
idempotency could undo. So the ledger of decided event ids is consulted
first, and a duplicate is acknowledged and skipped without being served,
scored or observed.

**A checkpoint never runs ahead of a decision.** The transactions in a batch
are checkpointed only after `flush` has confirmed the batch's decisions are on
the stream. A crash in between means redelivery, not a transaction with no
decision.

**The engine needs its input in time order, so the transaction topic has one
partition.** The live stream is one Kinesis shard, which is totally ordered;
the local topic matches it. More partitions would interleave events and the
engine would refuse them as late, correctly. Scaling past one partition needs
a reorder buffer with a watermark, whose hold time is latency, and ADR 8
says what that costs before anyone builds it.

The ledger is in memory and bounded. It stops duplicates within a process's
lifetime; after a restart the engine's state is gone as well, which is the
larger problem and is the online store's to solve (rebuilt by replay, ADR 8).
"""

from __future__ import annotations

import datetime as dt
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from verdict.events.schema import DecisionEvent, TransactionEvent, decode_transaction
from verdict.features.engine import FeatureEngine
from verdict.scoring.model import Model
from verdict.scoring.rules import DecisionRules
from verdict.scoring.timing import HopSample
from verdict.store.features import NO_EVENTS
from verdict.stream.base import Stream

TRANSACTIONS_TOPIC: Final = "transactions"
DECISIONS_TOPIC: Final = "decisions"
SCORER_GROUP: Final = "scorer"

DEFAULT_LEDGER_SIZE: Final = 1_000_000
"""Decided event ids remembered for duplicate detection.

About twenty minutes at the live rate. A redelivery arrives within a batch or
two of the original, so this is generous; it is bounded because an 87-day
window would otherwise hold seven billion ids.
"""

Decided = Callable[[TransactionEvent, DecisionEvent, HopSample], None]
"""Called once per decision, after it is handed to the stream."""


class EngineFeatures:
    """Serves features from the feature engine, in the scorer's own process.

    The engine is the one computation of every feature (ADR 6), so serving
    from it directly is the same value the stores hold, without a network
    hop. The online store read is the other way to serve, and ADR 5 measured
    it separately; which one the live stack uses is ADR 8's decision.
    """

    def __init__(self, engine: FeatureEngine) -> None:
        """Wrap an engine.

        Args:
            engine: The engine, fresh or warmed by a replay.
        """
        self.engine = engine
        self._names = tuple(spec.name for spec in engine.specs)

    def serve(self, event: TransactionEvent) -> dict[str, float]:
        """Features for an event, and hold it back for the ones that follow.

        Args:
            event: The event.

        Returns:
            Every feature in the engine's set. A feature whose entity the
            event does not name carries `NO_EVENTS`, so the model sees a full
            vector with no gaps.
        """
        values = dict.fromkeys(self._names, NO_EVENTS)
        for row in self.engine.process(event):
            values.update(row.values)
        return values


@dataclass(slots=True)
class ScorerStats:
    """What a scorer has done.

    Attributes:
        decided: Transactions decided.
        duplicates: Transactions skipped as already decided.
        batches: Batches committed.
        commit_ns: Time spent in flush and checkpoint, one entry per batch.
    """

    decided: int = 0
    duplicates: int = 0
    batches: int = 0
    commit_ns: list[int] = field(default_factory=list)


class StreamScorer:
    """Consumes transactions and writes decisions."""

    def __init__(
        self,
        stream: Stream,
        *,
        features: EngineFeatures,
        model: Model,
        rules: DecisionRules | None = None,
        transactions_topic: str = TRANSACTIONS_TOPIC,
        decisions_topic: str = DECISIONS_TOPIC,
        group: str = SCORER_GROUP,
        ledger_size: int = DEFAULT_LEDGER_SIZE,
        on_decided: Decided | None = None,
    ) -> None:
        """Assemble the scorer.

        Args:
            stream: Where transactions come from and decisions go.
            features: The feature source.
            model: The model.
            rules: The rule set. Defaults to the placeholder thresholds.
            transactions_topic: The topic to consume.
            decisions_topic: The topic to write.
            group: The consumer group, where progress is checkpointed.
            ledger_size: How many decided event ids to remember.
            on_decided: Called for each decision, for measurement.
        """
        self.stream = stream
        self.features = features
        self.model = model
        self.rules = rules or DecisionRules()
        self.transactions_topic = transactions_topic
        self.decisions_topic = decisions_topic
        self.group = group
        self.ledger_size = ledger_size
        self.on_decided = on_decided
        self.stats = ScorerStats()
        self._ledger: OrderedDict[str, None] = OrderedDict()

    def poll(self, max_records: int = 500, timeout_seconds: float = 0.1) -> int:
        """Consume one batch, decide it, commit it.

        Args:
            max_records: The most transactions to take.
            timeout_seconds: How long to wait for any.

        Returns:
            How many records the batch held, duplicates included.
        """
        records = self.stream.consume(
            self.transactions_topic,
            self.group,
            max_records=max_records,
            timeout_seconds=timeout_seconds,
        )
        if not records:
            return 0
        for record in records:
            self._handle(record.value)
        committed = time.perf_counter_ns()
        self.stream.flush()
        self.stream.checkpoint(
            self.transactions_topic, self.group, [record.position for record in records]
        )
        self.stats.commit_ns.append(time.perf_counter_ns() - committed)
        self.stats.batches += 1
        return len(records)

    def _handle(self, raw: bytes) -> None:
        started = time.perf_counter_ns()
        event = decode_transaction(raw)
        if event.event_id in self._ledger:
            self.stats.duplicates += 1
            return

        served = self.features.serve(event)
        featured = time.perf_counter_ns()

        score = self.model.score(served, event)
        scored = time.perf_counter_ns()

        action, rule = self.rules.decide(score, event)
        decision = DecisionEvent(
            event_id=event.event_id,
            card_id=event.card_id,
            action=action,
            score=score,
            rule=rule,
            model_version=self.model.version,
            decided_at=dt.datetime.now(dt.UTC),
        )
        payload = decision.to_json().encode("utf-8")
        decided = time.perf_counter_ns()

        self.stream.produce(self.decisions_topic, event.card_id, payload)
        persisted = time.perf_counter_ns()

        self._remember(event.event_id)
        self.stats.decided += 1
        if self.on_decided is not None:
            self.on_decided(
                event,
                decision,
                HopSample(
                    started_ns=started,
                    features_ns=featured - started,
                    model_ns=scored - featured,
                    decision_ns=decided - scored,
                    persist_ns=persisted - decided,
                ),
            )

    def _remember(self, event_id: str) -> None:
        self._ledger[event_id] = None
        if len(self._ledger) > self.ledger_size:
            self._ledger.popitem(last=False)
