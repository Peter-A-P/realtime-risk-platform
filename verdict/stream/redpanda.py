"""The stream contract on Redpanda, over the Kafka protocol.

`deploy/compose/docker-compose.yml` runs the broker; this is the client. It
passes the same contract tests as `memory.py`, against the live broker, and
those tests skip rather than fail when no broker is listening, so the fast
suite still runs on a machine without Docker.

Three choices, each made for a measurement this platform publishes rather
than for convenience:

- **The producer is idempotent with `acks=all`.** A retried send cannot write
  a record twice. That does not make delivery exactly once end to end (a
  consumer can still see a record twice after a restart), but it keeps
  duplicates to the case the scorer already handles.
- **Consumers are assigned partitions; they do not join the group.**
  Subscribing starts a rebalance, which on Redpanda waits for the group's
  initial delay before handing out partitions, and that wait would land in
  the first latency measurement of every run. There is one scorer per group
  here, so there is nothing to balance. The group still exists: it is where
  checkpoints are committed and where a restarted consumer resumes from.
- **Retriable broker errors are retried, to a deadline.** A broker that has
  just started, or has just moved a group's coordinator, answers offset
  requests with `NOT_COORDINATOR` or `COORDINATOR_LOAD_IN_PROGRESS` for a few
  seconds. Kafka marks those retriable, and the first version of this client
  treated them as fatal; a CI run that started its tests a moment after the
  broker found it. Anything not marked retriable still fails at once.
- **Topic existence is checked once per topic, before the first send.**
  Auto-creation is off, and the Kafka protocol's own answer to a send to a
  missing topic is a delivery failure after the message timeout, which is
  a long time to learn about a typo.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Final, TypeVar

from verdict.stream.base import Position, Record, StreamError, UnknownTopicError

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from confluent_kafka import Consumer

DEFAULT_BOOTSTRAP: Final = "localhost:19092"
"""The host-facing listener the compose stack publishes."""

RETRY_DEADLINE_SECONDS: Final = 30.0
"""How long a retriable error is retried before it is reported."""

T = TypeVar("T")


def retrying(
    action: Callable[[], T],
    what: str,
    *,
    deadline_seconds: float = RETRY_DEADLINE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> T:
    """Run a broker call, retrying only errors the broker marks retriable.

    Args:
        action: The call.
        what: What it is doing, for the error message.
        deadline_seconds: How long to keep retrying.
        sleep: How to wait between attempts; tests pass a fake.
        clock: A monotonic clock; tests pass a fake.

    Returns:
        What the call returned.

    Raises:
        StreamError: If the error is not retriable, or the deadline passes.
    """
    from confluent_kafka import KafkaException

    deadline = clock() + deadline_seconds
    backoff = 0.1
    while True:
        try:
            return action()
        except KafkaException as error:
            detail = error.args[0] if error.args else None
            retriable = bool(getattr(detail, "retriable", lambda: False)())
            if not retriable or clock() + backoff > deadline:
                qualifier = "after retrying" if retriable else "and it is not retriable"
                msg = f"{what} failed {qualifier}: {detail}"
                raise StreamError(msg) from error
            sleep(backoff)
            backoff = min(backoff * 2, 2.0)


class RedpandaStream:
    """A `Stream` on a Kafka-protocol broker."""

    def __init__(
        self,
        bootstrap: str = DEFAULT_BOOTSTRAP,
        *,
        client_id: str = "verdict",
        linger_ms: int = 2,
    ) -> None:
        """Connect.

        Args:
            bootstrap: Broker address.
            client_id: How this client names itself to the broker.
            linger_ms: How long the producer waits to batch. Small, because
                the platform's latency budget starts counting at produce.
        """
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient

        self.bootstrap = bootstrap
        self._common: dict[str, Any] = {"bootstrap.servers": bootstrap, "client.id": client_id}
        self._producer = Producer(
            {
                **self._common,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": linger_ms,
            }
        )
        self._admin = AdminClient(self._common)
        self._partitions: dict[str, int] = {}
        self._consumers: dict[tuple[str, str], Consumer] = {}
        self._committed: dict[tuple[str, str, int], int] = {}
        self._delivery_errors: list[str] = []

    def partitions(self, topic: str, timeout_seconds: float = 10.0) -> int:
        """How many partitions a topic has, checking that it exists.

        Args:
            topic: The topic.
            timeout_seconds: How long to wait for the broker's metadata.

        Returns:
            The partition count.

        Raises:
            UnknownTopicError: If the broker has no such topic.
            StreamError: If the broker cannot be reached.
        """
        known = self._partitions.get(topic)
        if known is not None:
            return known
        from confluent_kafka import KafkaException

        try:
            metadata = self._admin.list_topics(topic=topic, timeout=timeout_seconds)
        except KafkaException as error:
            msg = f"cannot reach the broker at {self.bootstrap}: {error}"
            raise StreamError(msg) from error
        found = metadata.topics.get(topic)
        if found is None or found.error is not None or not found.partitions:
            raise UnknownTopicError(topic)
        self._partitions[topic] = len(found.partitions)
        return self._partitions[topic]

    def create_topic(self, topic: str, partitions: int, timeout_seconds: float = 15.0) -> None:
        """Create a topic, for load tests and test fixtures.

        The platform's own topics come from the compose stack's topics job;
        this is for topics that live as long as one run.

        Args:
            topic: The name.
            partitions: How many partitions.
            timeout_seconds: How long to wait for the broker.
        """
        from confluent_kafka.cimpl import NewTopic

        futures = self._admin.create_topics([NewTopic(topic, partitions, 1)])
        for future in futures.values():
            future.result(timeout=timeout_seconds)
        self._partitions[topic] = partitions

    def delete_topic(self, topic: str, timeout_seconds: float = 15.0) -> None:
        """Delete a topic created for one run.

        Args:
            topic: The name.
            timeout_seconds: How long to wait for the broker.
        """
        for future in self._admin.delete_topics([topic]).values():
            future.result(timeout=timeout_seconds)
        self._partitions.pop(topic, None)

    def produce(self, topic: str, key: str, value: bytes) -> None:
        """Queue one record.

        Args:
            topic: The topic.
            key: The partitioning key.
            value: The payload.
        """
        self.partitions(topic)
        while True:
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                # The local queue is full: serve delivery reports until it
                # drains, rather than dropping the record.
                self._producer.poll(0.05)
        self._producer.poll(0)

    def _on_delivery(self, error: object, message: object) -> None:
        del message
        if error is not None:
            self._delivery_errors.append(str(error))

    def flush(self, timeout_seconds: float = 10.0) -> None:
        """Wait for every queued record to be acknowledged.

        Args:
            timeout_seconds: How long to wait.

        Raises:
            StreamError: If records are undelivered, or any delivery failed.
        """
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            msg = f"{remaining} records still undelivered after {timeout_seconds}s"
            raise StreamError(msg)
        if self._delivery_errors:
            errors, self._delivery_errors = self._delivery_errors, []
            msg = f"{len(errors)} deliveries failed, first: {errors[0]}"
            raise StreamError(msg)

    def _consumer(self, topic: str, group: str) -> Consumer:
        """The consumer for a topic and group, assigned from the checkpoint.

        Args:
            topic: The topic.
            group: The consumer group.

        Returns:
            The consumer.
        """
        existing = self._consumers.get((topic, group))
        if existing is not None:
            return existing
        from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

        count = self.partitions(topic)
        consumer = Consumer(
            {
                **self._common,
                "group.id": group,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        wanted = [TopicPartition(topic, index) for index in range(count)]
        try:
            committed = retrying(
                lambda: consumer.committed(wanted, timeout=10),
                f"reading {group}'s committed offsets on {topic}",
            )
        except StreamError:
            consumer.close()
            raise
        assignment = [
            TopicPartition(topic, tp.partition, tp.offset if tp.offset >= 0 else OFFSET_BEGINNING)
            for tp in committed
        ]
        for tp in committed:
            if tp.offset >= 0:
                self._committed[(topic, group, tp.partition)] = tp.offset
        consumer.assign(assignment)
        self._consumers[(topic, group)] = consumer
        return consumer

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Read the next records for a group.

        Args:
            topic: The topic.
            group: The consumer group.
            max_records: The most to return.
            timeout_seconds: How long to wait for them.

        Returns:
            The records.

        Raises:
            StreamError: If the broker reports an error on a message.
        """
        consumer = self._consumer(topic, group)
        messages = consumer.consume(num_messages=max_records, timeout=timeout_seconds)
        out: list[Record] = []
        for message in messages:
            error = message.error()
            if error is not None:
                msg = f"error reading {topic}: {error}"
                raise StreamError(msg)
            raw_key = message.key()
            key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key or "")
            out.append(
                Record(
                    topic=topic,
                    key=key,
                    value=bytes(message.value() or b""),
                    position=Position(str(message.partition()), str(message.offset())),
                )
            )
        return out

    def checkpoint(self, topic: str, group: str, positions: Iterable[Position]) -> None:
        """Commit a group's progress, synchronously and only ever forwards.

        Args:
            topic: The topic.
            group: The consumer group.
            positions: Positions the group has finished with.
        """
        from confluent_kafka import TopicPartition

        highest: dict[int, int] = {}
        for position in positions:
            partition, following = int(position.partition), int(position.token) + 1
            highest[partition] = max(highest.get(partition, 0), following)
        offsets = [
            TopicPartition(topic, partition, following)
            for partition, following in sorted(highest.items())
            if following > self._committed.get((topic, group, partition), -1)
        ]
        if not offsets:
            return
        consumer = self._consumer(topic, group)
        retrying(
            lambda: consumer.commit(offsets=offsets, asynchronous=False),
            f"committing {group}'s offsets on {topic}",
        )
        for tp in offsets:
            self._committed[(topic, group, tp.partition)] = tp.offset

    def close(self) -> None:
        """Close consumers and let the producer finish what it was sending."""
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()
        self._producer.flush(5)


def broker_reachable(bootstrap: str = DEFAULT_BOOTSTRAP, timeout_seconds: float = 2.0) -> bool:
    """Whether a broker answers at an address, for tests that need one.

    Args:
        bootstrap: Broker address.
        timeout_seconds: How long to wait.

    Returns:
        True if the broker returned metadata.
    """
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    try:
        AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 1000}).list_topics(
            timeout=timeout_seconds
        )
    except KafkaException:
        return False
    return True
