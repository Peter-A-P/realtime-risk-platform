"""The scorer: a stream consumer, not an HTTP service.

For each transaction on the stream, in order: check it has not already been
decided, serve its features, score it, apply the rules, and hand the decision
to the stream. After each batch: wait for the decisions to be acknowledged,
then checkpoint the transactions. ADR 8 records why it is shaped this way;
three properties are worth stating where the code is. The decision itself is
made by `core.Decider`, which the HTTP endpoint shares.

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

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from verdict.events.schema import DecisionEvent, TransactionEvent, decode_transaction
from verdict.scoring.core import Decider
from verdict.scoring.timing import HopSample
from verdict.stream.base import Stream

TRANSACTIONS_TOPIC: Final = "transactions"
DECISIONS_TOPIC: Final = "decisions"
SHADOW_TOPIC: Final = "shadow"
SCORER_GROUP: Final = "scorer"

Decided = Callable[[TransactionEvent, DecisionEvent, HopSample], None]
"""Called once per decision, after it is handed to the stream."""


@dataclass(slots=True)
class CommitStats:
    """Batches a scorer has committed.

    Attributes:
        batches: Batches committed.
        commit_ns: Time spent in flush and checkpoint together, per batch.
        flush_ns: Of that, time waiting for the decisions to be acknowledged.
        checkpoint_ns: Of that, time committing the transactions' offsets.

    The two are kept apart because they are different costs with different
    fixes: a flush waits for the broker to acknowledge writes this scorer
    made, and a checkpoint is a round trip to the group coordinator that
    could be made less often without losing a decision.
    """

    batches: int = 0
    commit_ns: list[int] = field(default_factory=list)
    flush_ns: list[int] = field(default_factory=list)
    checkpoint_ns: list[int] = field(default_factory=list)


class StreamScorer:
    """Consumes transactions and writes decisions."""

    def __init__(
        self,
        stream: Stream,
        *,
        decider: Decider,
        transactions_topic: str = TRANSACTIONS_TOPIC,
        decisions_topic: str = DECISIONS_TOPIC,
        shadow_topic: str = SHADOW_TOPIC,
        group: str = SCORER_GROUP,
        on_decided: Decided | None = None,
    ) -> None:
        """Assemble the scorer.

        Args:
            stream: Where transactions come from and decisions go.
            decider: What makes each decision.
            transactions_topic: The topic to consume.
            decisions_topic: The topic to write.
            shadow_topic: Where a shadow model's would-be decisions go.
            group: The consumer group, where progress is checkpointed.
            on_decided: Called for each decision, for measurement.
        """
        self.stream = stream
        self.decider = decider
        self.transactions_topic = transactions_topic
        self.decisions_topic = decisions_topic
        self.shadow_topic = shadow_topic
        self.group = group
        self.on_decided = on_decided
        self.commits = CommitStats()

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
        flushed = time.perf_counter_ns()
        self.stream.checkpoint(
            self.transactions_topic, self.group, [record.position for record in records]
        )
        done = time.perf_counter_ns()
        self.commits.commit_ns.append(done - committed)
        self.commits.flush_ns.append(flushed - committed)
        self.commits.checkpoint_ns.append(done - flushed)
        self.commits.batches += 1
        return len(records)

    def _handle(self, raw: bytes) -> None:
        started = time.perf_counter_ns()
        event = decode_transaction(raw)
        outcome = self.decider.decide(event, started)
        if outcome is None:
            return
        before_persist = time.perf_counter_ns()
        self.stream.produce(self.decisions_topic, event.card_id, outcome.payload)
        persisted = time.perf_counter_ns()
        if outcome.shadow_payload is not None:
            self.stream.produce(self.shadow_topic, event.card_id, outcome.shadow_payload)
        if self.on_decided is not None:
            self.on_decided(
                event,
                outcome.decision,
                HopSample(
                    started_ns=started,
                    features_ns=outcome.features_ns,
                    model_ns=outcome.model_ns,
                    decision_ns=outcome.decision_ns,
                    persist_ns=persisted - before_persist,
                ),
            )
