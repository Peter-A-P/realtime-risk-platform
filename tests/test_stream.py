"""The stream contract, held against every implementation.

The same tests run against the in-process stream and against Redpanda. ADR 3
says the scorer and the dataflow know only the interface; that is only true if
the implementations agree, and these are what make them agree. Kinesis joins
the parameter list in week 7.

The Redpanda run needs the compose stack up. Without a broker it is skipped
with a reason rather than failed, so the fast suite runs anywhere; with one,
each test creates its own topics and deletes them afterwards.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest

from verdict.stream import Position, Record, Stream, StreamError, UnknownTopicError
from verdict.stream.memory import MemoryBroker, partition_for
from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream, broker_reachable, retrying

PARTITIONS = 3


@dataclass
class Backend:
    """A way to open handles on one shared stream, and a topic that exists on it."""

    name: str
    open: Callable[[], Stream]
    topic: str
    handles: list[Stream]

    def handle(self) -> Stream:
        """Open a handle, closed again when the test ends."""
        stream = self.open()
        self.handles.append(stream)
        return stream


def _memory() -> Iterator[Backend]:
    broker = MemoryBroker()
    topic = "transactions"
    broker.create_topic(topic, PARTITIONS)
    backend = Backend("memory", broker.open, topic, [])
    yield backend
    for stream in backend.handles:
        stream.close()


def _redpanda() -> Iterator[Backend]:
    if not broker_reachable():
        pytest.skip(f"no broker at {DEFAULT_BOOTSTRAP}; start deploy/compose to run these")
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    admin = AdminClient({"bootstrap.servers": DEFAULT_BOOTSTRAP})
    topic = f"test-{uuid.uuid4().hex[:12]}"
    for future in admin.create_topics([NewTopic(topic, PARTITIONS, 1)]).values():
        future.result(timeout=15)
    backend = Backend("redpanda", RedpandaStream, topic, [])
    yield backend
    for stream in backend.handles:
        stream.close()
    for future in admin.delete_topics([topic]).values():
        future.result(timeout=15)


@pytest.fixture(params=["memory", pytest.param("redpanda", marks=pytest.mark.broker)])
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    factory = _memory if request.param == "memory" else _redpanda
    yield from factory()


def drain(stream: Stream, topic: str, group: str, expected: int) -> list[Record]:
    """Read until `expected` records have arrived or the stream goes quiet."""
    out: list[Record] = []
    quiet = 0
    while len(out) < expected and quiet < 5:
        batch = stream.consume(topic, group, max_records=expected - len(out), timeout_seconds=1.0)
        out.extend(batch)
        quiet = 0 if batch else quiet + 1
    return out


def fresh_group() -> str:
    return f"group-{uuid.uuid4().hex[:8]}"


def produce_all(stream: Stream, topic: str, items: list[tuple[str, bytes]]) -> None:
    for key, value in items:
        stream.produce(topic, key, value)
    stream.flush()


# --- the contract ---------------------------------------------------------


def test_both_implementations_are_streams(backend: Backend) -> None:
    assert isinstance(backend.handle(), Stream)


def test_what_is_produced_is_consumed_byte_for_byte(backend: Backend) -> None:
    stream = backend.handle()
    items = [(f"card-{i % 5}", f'{{"n": {i}}}'.encode()) for i in range(40)]
    produce_all(stream, backend.topic, items)
    records = drain(stream, backend.topic, fresh_group(), len(items))
    assert sorted((r.key, r.value) for r in records) == sorted(items)


def test_records_sharing_a_key_come_back_in_the_order_produced(backend: Backend) -> None:
    """The feature engine needs a card's events in order; this is where that comes from."""
    stream = backend.handle()
    items = [(f"card-{i % 4}", str(i).encode()) for i in range(60)]
    produce_all(stream, backend.topic, items)
    records = drain(stream, backend.topic, fresh_group(), len(items))
    for key in {k for k, _ in items}:
        sent = [v for k, v in items if k == key]
        received = [r.value for r in records if r.key == key]
        assert received == sent
        assert len({r.position.partition for r in records if r.key == key}) == 1


def test_a_handle_does_not_return_the_same_record_twice(backend: Backend) -> None:
    stream = backend.handle()
    produce_all(stream, backend.topic, [("k", b"one"), ("k", b"two")])
    group = fresh_group()
    first = drain(stream, backend.topic, group, 2)
    again = stream.consume(backend.topic, group, timeout_seconds=1.0)
    assert len(first) == 2
    assert again == []


def test_records_already_readable_come_back_without_waiting_out_the_timeout(
    backend: Backend,
) -> None:
    """The timeout is how long to wait for anything, not for a full batch.

    Redpanda's client waits for the whole batch or the whole timeout, and at
    a live rate below the batch size that is the timeout every time: in the
    first dry run, 2026-09-21, no decision took under 50 ms.
    """
    stream = backend.handle()
    produce_all(stream, backend.topic, [("k", b"one"), ("k", b"two"), ("k", b"three")])
    group = fresh_group()
    assert len(stream.consume(backend.topic, group, max_records=1, timeout_seconds=10.0)) == 1
    started = time.monotonic()
    rest = stream.consume(backend.topic, group, max_records=500, timeout_seconds=5.0)
    assert [r.value for r in rest] == [b"two", b"three"]
    assert time.monotonic() - started < 1.0


def test_without_a_checkpoint_a_restarted_consumer_sees_everything_again(
    backend: Backend,
) -> None:
    """At least once: work not checkpointed is work redone."""
    producer = backend.handle()
    produce_all(producer, backend.topic, [(f"k{i}", str(i).encode()) for i in range(10)])
    group = fresh_group()
    before = backend.handle()
    assert len(drain(before, backend.topic, group, 10)) == 10
    before.close()
    after = backend.handle()
    assert len(drain(after, backend.topic, group, 10)) == 10


def test_a_restarted_consumer_resumes_after_its_checkpoint(backend: Backend) -> None:
    producer = backend.handle()
    items = [("card-a", str(i).encode()) for i in range(10)]
    produce_all(producer, backend.topic, items)
    group = fresh_group()

    before = backend.handle()
    records = drain(before, backend.topic, group, 10)
    before.checkpoint(backend.topic, group, [r.position for r in records[:6]])
    before.close()

    after = backend.handle()
    rest = drain(after, backend.topic, group, 4)
    assert [r.value for r in rest] == [str(i).encode() for i in range(6, 10)]
    assert after.consume(backend.topic, group, timeout_seconds=1.0) == []


def test_a_late_older_checkpoint_does_not_rewind_the_group(backend: Backend) -> None:
    producer = backend.handle()
    produce_all(producer, backend.topic, [("card-a", str(i).encode()) for i in range(8)])
    group = fresh_group()
    reader = backend.handle()
    records = drain(reader, backend.topic, group, 8)
    reader.checkpoint(backend.topic, group, [records[7].position])
    reader.checkpoint(backend.topic, group, [records[2].position])
    reader.close()
    assert backend.handle().consume(backend.topic, group, timeout_seconds=1.0) == []


def test_groups_keep_their_own_progress(backend: Backend) -> None:
    producer = backend.handle()
    produce_all(producer, backend.topic, [("k", b"x"), ("k", b"y")])
    scorer, auditor = fresh_group(), fresh_group()
    reader = backend.handle()
    records = drain(reader, backend.topic, scorer, 2)
    reader.checkpoint(backend.topic, scorer, [r.position for r in records])
    reader.close()
    assert len(drain(backend.handle(), backend.topic, auditor, 2)) == 2


def test_a_topic_that_does_not_exist_is_an_error_not_a_new_topic(backend: Backend) -> None:
    stream = backend.handle()
    missing = f"no-such-topic-{uuid.uuid4().hex[:8]}"
    with pytest.raises(UnknownTopicError):
        stream.produce(missing, "k", b"v")
    with pytest.raises(UnknownTopicError):
        stream.consume(missing, fresh_group())


# --- the in-process implementation's own behaviour ------------------------


def test_partitioning_is_stable_across_processes() -> None:
    """Salted `hash` would move a card between partitions from one run to the next."""
    assert partition_for("card-00000001", 4) == partition_for("card-00000001", 4)
    assert {partition_for(f"card-{i}", 4) for i in range(200)} == {0, 1, 2, 3}


def test_a_closed_memory_handle_refuses_work() -> None:
    broker = MemoryBroker()
    broker.create_topic("t")
    stream = broker.open()
    stream.close()
    with pytest.raises(StreamError):
        stream.produce("t", "k", b"v")


def test_the_memory_broker_refuses_a_duplicate_or_empty_topic() -> None:
    broker = MemoryBroker()
    broker.create_topic("t", 2)
    with pytest.raises(StreamError):
        broker.create_topic("t", 2)
    with pytest.raises(StreamError):
        broker.create_topic("u", 0)


def test_positions_order_within_a_partition() -> None:
    assert Position("0", "1") < Position("0", "2")


# --- the Redpanda client's retries, without a broker ------------------------


class FakeClock:
    """A monotonic clock that only moves when the code under test sleeps."""

    def __init__(self) -> None:
        """Start at zero."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def sleep(self, seconds: float) -> None:
        """Advance the clock instead of waiting."""
        self.sleeps.append(seconds)
        self.now += seconds

    def monotonic(self) -> float:
        """Read the clock."""
        return self.now


def a_kafka_error(code: int, *, retriable: bool) -> Exception:
    from confluent_kafka import KafkaError, KafkaException

    return KafkaException(KafkaError(code, "from the test", retriable=retriable))


def test_a_coordinator_still_loading_is_retried_until_it_answers() -> None:
    """The CI failure of 2026-09-15: NOT_COORDINATOR from a broker a moment old."""
    from confluent_kafka import KafkaError

    clock = FakeClock()
    attempts = 0

    def committed() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise a_kafka_error(KafkaError.NOT_COORDINATOR, retriable=True)
        return "offsets"

    result = retrying(committed, "reading offsets", sleep=clock.sleep, clock=clock.monotonic)
    assert result == "offsets"
    assert attempts == 3
    assert clock.sleeps == [0.1, 0.2]


def test_not_coordinator_is_retried_even_when_librdkafka_does_not_flag_it() -> None:
    """The CI failure after the first fix: the code is retriable, the flag was false."""
    from confluent_kafka import KafkaError

    clock = FakeClock()
    attempts = 0

    def committed() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise a_kafka_error(KafkaError.NOT_COORDINATOR, retriable=False)
        return "offsets"

    assert retrying(committed, "reading offsets", sleep=clock.sleep, clock=clock.monotonic) == (
        "offsets"
    )
    assert attempts == 2


def test_an_error_the_broker_does_not_mark_retriable_fails_at_once() -> None:
    from confluent_kafka import KafkaError

    clock = FakeClock()

    def denied() -> None:
        raise a_kafka_error(KafkaError.TOPIC_AUTHORIZATION_FAILED, retriable=False)

    with pytest.raises(StreamError, match="not retriable"):
        retrying(denied, "reading offsets", sleep=clock.sleep, clock=clock.monotonic)
    assert clock.sleeps == []


def test_a_retriable_error_that_never_clears_is_reported_at_the_deadline() -> None:
    from confluent_kafka import KafkaError

    clock = FakeClock()

    def forever() -> None:
        raise a_kafka_error(KafkaError.COORDINATOR_LOAD_IN_PROGRESS, retriable=True)

    with pytest.raises(StreamError, match="after retrying"):
        retrying(
            forever,
            "reading offsets",
            deadline_seconds=10.0,
            sleep=clock.sleep,
            clock=clock.monotonic,
        )
    assert sum(clock.sleeps) <= 10.0
    assert max(clock.sleeps) == 2.0


# --- the flush probe, which is a measuring instrument, not the platform -----


def test_the_probe_reports_the_two_paths_apart_rather_than_averaging_them() -> None:
    """A mean across connections would land where no connection ever is."""
    from verdict.stream.probe import ConnectionFlushes, summarise

    def a_connection(number: int, p50: float) -> ConnectionFlushes:
        return ConnectionFlushes(
            connection=number,
            p50=p50,
            p95=p50 * 1.1,
            fastest=p50 * 0.8,
            slowest=p50 * 1.5,
            path="slow" if p50 > 25.0 else "fast",
        )

    summary = summarise([a_connection(1, 6.0), a_connection(2, 47.0), a_connection(3, 48.0)])
    assert summary["connections"] == 3
    assert summary["by_path"]["fast"] == {"connections": 1, "median_flush_p50_ms": 6.0}
    assert summary["by_path"]["slow"] == {"connections": 2, "median_flush_p50_ms": 47.5}


def test_the_probe_refuses_to_summarise_nothing() -> None:
    from verdict.stream.probe import summarise

    with pytest.raises(ValueError, match="no connections"):
        summarise([])


@pytest.mark.broker
def test_the_flush_probe_measures_every_connection_it_opens() -> None:
    from verdict.stream.probe import flush_by_connection, summarise

    if not broker_reachable():
        pytest.skip(f"no broker at {DEFAULT_BOOTSTRAP}; start deploy/compose to run these")
    results = flush_by_connection(connections=2, batches=3, batch_size=5)
    assert [result.connection for result in results] == [1, 2]
    for result in results:
        assert 0.0 < result.fastest <= result.p50 <= result.slowest
        assert result.path in {"fast", "slow"}
    assert summarise(results)["connections"] == 2
