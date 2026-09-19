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

**A record that cannot be decided is set aside, not retried forever.** A
record that is not a transaction this build can read, or that arrives after
the stream has moved past its time, goes to the dead-letter topic with its
bytes untouched and the reason, and the scorer moves on. Before this, one such
record stopped the scorer, and because it was never checkpointed, every
restart read it again and stopped again: one malformed message was a
permanent outage. A dead letter is flushed before the checkpoint that passes
it, exactly as a decision is, so it cannot be lost. But a run of them is not
a bad record, it is a bad deployment (a producer that moved to a schema
version this scorer cannot read, say), and quietly setting the whole stream
aside would be worse than stopping. So after `max_consecutive_dead_letters`
in a row the scorer stops and says why. Anything else that goes wrong while
deciding is a bug, affects every record alike, and still stops the scorer at
once. ADR 8's addendum of 2026-09-18 records the choice, including what it
costs: a dead-lettered transaction gets no decision.

**Every decision is staged, with its features, before the checkpoint.** With
a history spool attached (`verdict/history`, ADR 18), each decision's row goes
to the spool as it is made and the spool is written before the decisions are
flushed, so a checkpointed transaction always has its row. A crash between the
two means the transaction comes again and is staged twice, which the day's
finalising removes.

The ledger is in memory and bounded. It stops duplicates within a process's
lifetime; after a restart the engine's state is gone as well, which is the
larger problem and is the online store's to solve (rebuilt by replay, ADR 8).
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from pydantic import ValidationError

from verdict.events.schema import (
    DecisionEvent,
    TransactionEvent,
    UnknownSchemaVersionError,
    decode_transaction,
)
from verdict.features.engine import LateEventError
from verdict.history.records import staged_row
from verdict.history.spool import SpoolWriter
from verdict.scoring.core import Decider
from verdict.scoring.timing import HopSample
from verdict.stream.base import Record, Stream, StreamError

TRANSACTIONS_TOPIC: Final = "transactions"
DECISIONS_TOPIC: Final = "decisions"
SHADOW_TOPIC: Final = "shadow"
DEAD_LETTER_TOPIC: Final = "dead-letter"

MAX_CONSECUTIVE_DEAD_LETTERS: Final = 50
"""How many records in a row may be set aside before the scorer stops.

A placeholder, chosen to be far more than any one bad message produces and
far fewer than a systemic fault would: at the live rate it is 50 ms of stream.
It is a guard, not a tuned threshold, and it says so when it trips.
"""

DETAIL_LIMIT: Final = 500
"""How much of an error's text a dead letter keeps."""
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

    def drain(self) -> tuple[list[int], list[int], list[int]]:
        """Hand over the timings recorded so far, and forget them.

        The lists grow by three entries a batch. That is what the load test
        wants, since it computes percentiles over a whole run; a service that
        ran for months would hold roughly a gigabyte a day at the live rate.
        The service drains them into its metrics after every poll
        (`observe/metrics.py`). `batches` keeps counting.

        Returns:
            Commit, flush and checkpoint times, in nanoseconds.
        """
        taken = (self.commit_ns, self.flush_ns, self.checkpoint_ns)
        self.commit_ns, self.flush_ns, self.checkpoint_ns = [], [], []
        return taken


class DeadLetterRunError(StreamError):
    """Raised when too many records in a row could not be decided."""


def dead_letter_payload(record: Record, reason: str, detail: str) -> bytes:
    """What goes on the dead-letter topic for a record that could not be decided.

    An operational record rather than part of the wire schema: it carries the
    original bytes verbatim, base64-encoded because they may not be text, so
    the record can be inspected and, once the fault is fixed, replayed.

    Args:
        record: The record as it came off the stream.
        reason: `undecodable`, `unknown-schema-version` or `late`.
        detail: The error's own text.

    Returns:
        JSON, UTF-8.
    """
    return json.dumps(
        {
            "reason": reason,
            "detail": detail[:DETAIL_LIMIT],
            "topic": record.topic,
            "partition": record.position.partition,
            "token": record.position.token,
            "key": record.key,
            "value_base64": base64.b64encode(record.value).decode("ascii"),
        },
        sort_keys=True,
    ).encode("utf-8")


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
        dead_letter_topic: str = DEAD_LETTER_TOPIC,
        group: str = SCORER_GROUP,
        on_decided: Decided | None = None,
        max_consecutive_dead_letters: int = MAX_CONSECUTIVE_DEAD_LETTERS,
        history: SpoolWriter | None = None,
    ) -> None:
        """Assemble the scorer.

        Args:
            stream: Where transactions come from and decisions go.
            decider: What makes each decision.
            transactions_topic: The topic to consume.
            decisions_topic: The topic to write.
            shadow_topic: Where a shadow model's would-be decisions go.
            dead_letter_topic: Where records that cannot be decided go.
            group: The consumer group, where progress is checkpointed.
            on_decided: Called for each decision, for measurement.
            max_consecutive_dead_letters: How many records in a row may be
                set aside before the scorer stops.
            history: Where each decision is staged with its features, if
                anywhere. The live scorer has one; the load test measures
                with and without.
        """
        self.stream = stream
        self.decider = decider
        self.transactions_topic = transactions_topic
        self.decisions_topic = decisions_topic
        self.shadow_topic = shadow_topic
        self.dead_letter_topic = dead_letter_topic
        self.max_consecutive_dead_letters = max_consecutive_dead_letters
        self.dead_letters: dict[str, int] = {}
        self._dead_in_a_row = 0
        self.group = group
        self.on_decided = on_decided
        self.commits = CommitStats()
        self.history = history
        self._latest_event: TransactionEvent | None = None

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
            self._handle(record)
        if self.history is not None:
            # Before the decisions are flushed, so before the checkpoint. Not
            # inside the commit timing: it is its own cost, and the load test
            # measures it by running with and without a spool.
            self.history.flush()
            if self._latest_event is not None:
                # Events come in time order, so every hour before the latest
                # event's is complete and can be closed for sealing.
                self.history.close_before(self._latest_event.event_time)
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

    def _handle(self, record: Record) -> None:
        started = time.perf_counter_ns()
        try:
            event = decode_transaction(record.value)
        except UnknownSchemaVersionError as error:
            # No version at all means not JSON, or not a versioned record:
            # garbage, not a producer on a newer schema, and the person reading
            # the dead-letter topic needs to tell those apart.
            reason = "undecodable" if error.found is None else "unknown-schema-version"
            self._set_aside(record, reason, str(error))
            return
        except (UnicodeDecodeError, ValidationError) as error:
            self._set_aside(record, "undecodable", str(error))
            return
        try:
            outcome = self.decider.decide(event, started)
        except LateEventError as error:
            self._set_aside(record, "late", str(error))
            return
        self._dead_in_a_row = 0
        if outcome is None:
            return
        before_persist = time.perf_counter_ns()
        self.stream.produce(self.decisions_topic, event.card_id, outcome.payload)
        persisted = time.perf_counter_ns()
        if outcome.shadow_payload is not None:
            self.stream.produce(self.shadow_topic, event.card_id, outcome.shadow_payload)
        if self.history is not None:
            self.history.append(
                event.event_time,
                staged_row(event, outcome.features, outcome.decision, outcome.shadow),
            )
            self._latest_event = event
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

    def _set_aside(self, record: Record, reason: str, detail: str) -> None:
        """Send a record to the dead-letter topic, or stop if too many have gone.

        Args:
            record: The record.
            reason: Why it could not be decided.
            detail: The error's text.

        Raises:
            DeadLetterRunError: If this makes too many in a row.
        """
        self._dead_in_a_row += 1
        if self._dead_in_a_row > self.max_consecutive_dead_letters:
            msg = (
                f"{self._dead_in_a_row} records in a row could not be decided, the last "
                f"{reason}: {detail[:200]}. That is a fault upstream, not a bad record; "
                "stopping rather than setting the stream aside"
            )
            raise DeadLetterRunError(msg)
        self.stream.produce(
            self.dead_letter_topic, record.key, dead_letter_payload(record, reason, detail)
        )
        self.dead_letters[reason] = self.dead_letters.get(reason, 0) + 1
