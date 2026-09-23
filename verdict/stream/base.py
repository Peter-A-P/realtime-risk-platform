"""The contract every stream implementation meets.

The scorer, the feature dataflow and the generator know this interface and
nothing below it (ADR 3). It is deliberately small: produce, consume,
checkpoint. Everything that differs between Kafka and Kinesis has to fit
behind those three, and the differences are real:

| | Kafka / Redpanda | Kinesis |
|---|---|---|
| Unit of order | partition | shard |
| Position | integer offset | sequence number, a large decimal string |
| Where progress is kept | the broker's group offsets | a lease table the consumer maintains |

So a `Position` is opaque: a partition or shard name and a token that the
implementation that issued it knows how to resume after. Callers pass
positions back and never do arithmetic on them.

## Delivery is at least once

`consume` returns records after the last checkpoint. A consumer that
processes records and dies before `checkpoint` sees them again when it comes
back, which is the at-least-once half of ADR 3. The other half is that every
decision downstream is idempotent by event id, which is the scorer's job,
not the stream's. Nothing here attempts exactly-once, and nothing should.

## Order is per key, not global

Records with the same key land in the same partition and come back in the
order they were produced. Records with different keys may interleave in any
order. The platform keys transactions by card, so a card's events stay in
order, which is what the feature engine needs; across cards the engine's
late-event refusal is what catches a violation of event time.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable


class StreamError(RuntimeError):
    """Raised when the stream cannot do what was asked of it."""


class UnknownTopicError(StreamError):
    """Raised on producing to or consuming from a topic that does not exist.

    Topics are created explicitly and auto-creation is off on the broker
    (`deploy/compose/docker-compose.yml`), so a mistyped topic is an error at
    once, not a new empty topic that a consumer waits on forever.
    """

    def __init__(self, topic: str) -> None:
        """Name the topic.

        Args:
            topic: The topic that does not exist.
        """
        super().__init__(f"no topic {topic!r}; topics are created explicitly, never on first use")
        self.topic = topic


@dataclass(frozen=True, slots=True, order=True)
class Position:
    """Where a record sits in its partition.

    Attributes:
        partition: The partition or shard, as a string.
        token: The implementation's own position marker. Opaque to callers.
    """

    partition: str
    token: str


@dataclass(frozen=True, slots=True)
class Record:
    """One record read from the stream.

    Attributes:
        topic: Where it came from.
        key: The key it was produced with.
        value: Its bytes, exactly as produced.
        position: Where it sits, for `checkpoint`.
    """

    topic: str
    key: str
    value: bytes
    position: Position


RIDE_OUT_SECONDS: Final = 600.0
"""How long a producer waits for a broker that has stopped answering.

Until 2026-09-22 a flush gave up after 10 s and raised, and every service
that produces stops on that error. For the scorer, stopping means a
restart with empty feature windows, every card served "no history" for up
to a day (ADR 8), to get past a broker that was only slow; freezing the
local broker inside a scorer batch for 15 s did exactly that
(`docs/failure-modes.md`, "The broker stops answering"). Nothing is decided
while the broker is away whether the scorer waits or restarts, so it waits.
Ten minutes is past any broker restart; longer than that is an outage a
person should look at, and the `ScorerStopped` alert says so at fifteen.
"""


@runtime_checkable
class Stream(Protocol):
    """A stream the platform can produce to and consume from."""

    def produce(self, topic: str, key: str, value: bytes) -> None:
        """Queue one record for a topic.

        Args:
            topic: The topic. Must exist.
            key: The partitioning key. Records sharing a key keep their order.
            value: The payload.

        Raises:
            UnknownTopicError: If the topic does not exist.
        """
        ...

    def flush(self, timeout_seconds: float = RIDE_OUT_SECONDS) -> None:
        """Wait until every queued record is durably on the stream.

        Args:
            timeout_seconds: How long to wait. Long by default: a broker
                that is only slow is waited for (`RIDE_OUT_SECONDS`).

        Raises:
            StreamError: If records are still undelivered when time runs out.
        """
        ...

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Read the next records for a consumer group.

        Returns records after the group's last checkpoint, and after anything
        this handle has already returned. It may return fewer than
        `max_records`, including none, when the stream has nothing more
        within the timeout.

        Args:
            topic: The topic. Must exist.
            group: The consumer group whose progress to follow.
            max_records: The most to return.
            timeout_seconds: How long to wait for anything at all.

        Returns:
            The records, in order within each partition.

        Raises:
            UnknownTopicError: If the topic does not exist.
        """
        ...

    def checkpoint(self, topic: str, group: str, positions: Iterable[Position]) -> None:
        """Record that a group has finished with everything up to these positions.

        A position covers its own record and every earlier one in the same
        partition. After a restart, `consume` for the group resumes with the
        record after the highest position checkpointed in each partition.

        Args:
            topic: The topic.
            group: The consumer group.
            positions: Positions of records the group has fully processed.
        """
        ...

    def close(self) -> None:
        """Release connections. Does not checkpoint anything."""
        ...
