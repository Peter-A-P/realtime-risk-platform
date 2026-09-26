"""The stream contract, in process.

For tests, for replays that do not need a broker, and as the reference the
broker implementations are held to: the contract tests run against this first
and against Redpanda second, and a disagreement between them is a bug in the
broker implementation or a gap in the contract.

It is shaped like Kafka on purpose. A `MemoryBroker` holds the topics and the
groups' checkpointed positions, as a broker does; a `MemoryStream` is one
client's handle on it, with its own read cursor that starts at the group's
checkpoint. Opening a second handle on the same broker is what "the consumer
restarted" means here, and it is how the tests show redelivery.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field

from verdict.stream.base import (
    RIDE_OUT_SECONDS,
    Position,
    PositionGoneError,
    Record,
    StreamError,
    UnknownTopicError,
)


def partition_for(key: str, partitions: int) -> int:
    """Choose a partition from a key, the same way on every run.

    Python's own `hash` is salted per process, which would scatter a card's
    events across partitions between runs.

    Args:
        key: The record key.
        partitions: How many partitions the topic has.

    Returns:
        The partition index.
    """
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partitions


@dataclass(slots=True)
class _Topic:
    partitions: list[list[tuple[str, bytes]]]


@dataclass(slots=True)
class MemoryBroker:
    """Topics and checkpoints, shared by every handle opened on it.

    Attributes:
        checkpoints: (topic, group, partition) to the next offset to read.
    """

    _topics: dict[str, _Topic] = field(default_factory=dict)
    checkpoints: dict[tuple[str, str, int], int] = field(default_factory=dict)
    kept_from: dict[tuple[str, int], int] = field(default_factory=dict)
    """(topic, partition) to the first offset still kept, as retention leaves it."""

    def expire(self, topic: str, partition: int, before: int) -> None:
        """Stop keeping a partition's records before an offset, as retention does.

        Only `reread` honours it; it is for testing a position that has gone.

        Args:
            topic: The topic.
            partition: The partition.
            before: The first offset still kept.
        """
        self.kept_from[(topic, partition)] = before

    def create_topic(self, name: str, partitions: int = 1) -> None:
        """Create a topic. Creating an existing one is an error, as on a broker.

        Args:
            name: The topic.
            partitions: How many partitions.

        Raises:
            StreamError: If it exists or the partition count is not positive.
        """
        if partitions < 1:
            msg = f"a topic needs at least one partition, got {partitions}"
            raise StreamError(msg)
        if name in self._topics:
            msg = f"topic {name!r} already exists"
            raise StreamError(msg)
        self._topics[name] = _Topic(partitions=[[] for _ in range(partitions)])

    def topic(self, name: str) -> _Topic:
        """Look a topic up.

        Args:
            name: The topic.

        Returns:
            The topic.

        Raises:
            UnknownTopicError: If there is no such topic.
        """
        found = self._topics.get(name)
        if found is None:
            raise UnknownTopicError(name)
        return found

    def open(self) -> MemoryStream:
        """Open a client handle.

        Returns:
            A fresh handle whose reads start at each group's checkpoint.
        """
        return MemoryStream(self)


class MemoryStream:
    """One client's handle on a `MemoryBroker`."""

    def __init__(self, broker: MemoryBroker) -> None:
        """Open the handle.

        Args:
            broker: The broker it reads and writes.
        """
        self.broker = broker
        self._cursors: dict[tuple[str, str, int], int] = {}
        self._closed = False

    def produce(self, topic: str, key: str, value: bytes) -> None:
        """Append a record to the key's partition.

        Args:
            topic: The topic.
            key: The partitioning key.
            value: The payload.
        """
        self._check_open()
        partitions = self.broker.topic(topic).partitions
        partitions[partition_for(key, len(partitions))].append((key, value))

    def flush(self, timeout_seconds: float = RIDE_OUT_SECONDS) -> None:
        """Nothing is queued in process, so there is nothing to wait for.

        Args:
            timeout_seconds: Unused.
        """
        del timeout_seconds
        self._check_open()

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Read the next records for a group, partition by partition.

        Args:
            topic: The topic.
            group: The consumer group.
            max_records: The most to return.
            timeout_seconds: Unused; an in-process read never waits.

        Returns:
            The records.
        """
        del timeout_seconds
        self._check_open()
        partitions = self.broker.topic(topic).partitions
        out: list[Record] = []
        for index, entries in enumerate(partitions):
            cursor_key = (topic, group, index)
            start = self._cursors.get(cursor_key, self.broker.checkpoints.get(cursor_key, 0))
            end = min(len(entries), start + max_records - len(out))
            for offset in range(start, end):
                key, value = entries[offset]
                out.append(Record(topic, key, value, Position(str(index), str(offset))))
            self._cursors[cursor_key] = end
            if len(out) >= max_records:
                break
        return out

    def checkpoint(self, topic: str, group: str, positions: Iterable[Position]) -> None:
        """Advance a group's checkpoint past these positions.

        Checkpoints only move forward. An older position arriving late, which
        happens when a batch is acknowledged out of order, does not rewind a
        group into redelivering what it already finished.

        Args:
            topic: The topic.
            group: The consumer group.
            positions: Positions the group has finished with.
        """
        self._check_open()
        self.broker.topic(topic)
        for position in positions:
            key = (topic, group, int(position.partition))
            following = int(position.token) + 1
            if following > self.broker.checkpoints.get(key, 0):
                self.broker.checkpoints[key] = following

    def committed(self, topic: str, group: str, partition: str) -> Position | None:
        """The last record a group checkpointed in one partition.

        Args:
            topic: The topic.
            group: The consumer group.
            partition: The partition.

        Returns:
            Its position, or None if the group has checkpointed nothing there.
        """
        self._check_open()
        self.broker.topic(topic)
        following = self.broker.checkpoints.get((topic, group, int(partition)), 0)
        return None if following == 0 else Position(partition, str(following - 1))

    def reread(
        self, topic: str, partition: str, after: Position | None, through: Position
    ) -> Iterator[Record]:
        """Read one partition again, between two positions, touching no group.

        Args:
            topic: The topic.
            partition: The partition.
            after: Start after this record; None for the partition's first.
            through: Stop after this record.

        Yields:
            The records.

        Raises:
            PositionGoneError: If the records after `after` have expired.
        """
        self._check_open()
        entries = self.broker.topic(topic).partitions[int(partition)]
        start = 0 if after is None else int(after.token) + 1
        if start < self.broker.kept_from.get((topic, int(partition)), 0):
            msg = f"{topic} partition {partition} no longer keeps offset {start}"
            raise PositionGoneError(msg)
        for offset in range(start, min(int(through.token) + 1, len(entries))):
            key, value = entries[offset]
            yield Record(topic, key, value, Position(partition, str(offset)))

    def close(self) -> None:
        """Close the handle. Uncheckpointed reads are forgotten, as on a real client."""
        self._closed = True

    def _check_open(self) -> None:
        if self._closed:
            msg = "this stream handle is closed"
            raise StreamError(msg)
