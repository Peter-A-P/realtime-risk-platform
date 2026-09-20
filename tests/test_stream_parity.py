"""The same replay through every stream gives the same features and decisions.

ADR 3's parity test. The in-process stream is held to a reference that uses
no stream at all, and so is Redpanda when a broker is running. Like the
leakage test, it is shown failing before it is trusted: a stream that
tampers with one payload is caught, and a stream that redelivers everything
is not a failure, because the scorer's ledger is what makes redelivery safe.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Sequence

import pytest

from verdict.events.schema import TransactionEvent
from verdict.scoring import loadtest
from verdict.stream.base import Record
from verdict.stream.memory import MemoryBroker, MemoryStream
from verdict.stream.parity import compare, reference, through
from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, broker_reachable

EVENTS = 400


@pytest.fixture(scope="module")
def replay() -> list[TransactionEvent]:
    # A low rate in event time so cards come round again inside their windows,
    # and the features have something to disagree about.
    return loadtest.generate_events(EVENTS, rate=5.0)


def a_memory_path(
    stream_type: type[MemoryStream] = MemoryStream,
) -> tuple[MemoryBroker, MemoryStream]:
    broker = MemoryBroker()
    broker.create_topic("transactions", 1)
    broker.create_topic("decisions", 4)
    broker.create_topic("dead-letter", 1)
    return broker, stream_type(broker)


def test_the_reference_serves_every_event_and_something_other_than_the_sentinel(
    replay: list[TransactionEvent],
) -> None:
    """A parity check over vectors of nothing but NO_EVENTS would prove nothing."""
    expected = reference(replay)
    assert len(expected.served) == EVENTS
    assert len(expected.decided) == EVENTS
    distinct = {value for vector in expected.served.values() for value in vector.values()}
    assert len(distinct) > 10


def test_the_in_process_stream_is_identical_to_no_stream(replay: list[TransactionEvent]) -> None:
    broker, stream = a_memory_path()
    path = through(
        "memory",
        stream,
        replay,
        transactions_topic="transactions",
        decisions_topic="decisions",
        reader=broker.open(),
    )
    report = compare(reference(replay), path)
    assert report.clean, report.summary()
    assert report.comparisons > EVENTS * 10


class Tampering(MemoryStream):
    """Adds a cent to the amount of one transaction, named by the test."""

    target_event_id = ""
    tampered = 0

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Deliver as usual, with one cent added to the transaction named."""
        records = list(
            super().consume(topic, group, max_records=max_records, timeout_seconds=timeout_seconds)
        )
        if topic != "transactions":
            return records
        for index, record in enumerate(records):
            event = TransactionEvent.model_validate_json(record.value)
            if event.event_id != Tampering.target_event_id:
                continue
            changed = event.model_copy(update={"amount_cents": event.amount_cents + 1})
            records[index] = dataclasses.replace(record, value=changed.to_json().encode())
            Tampering.tampered += 1
        return records


def seen_again(replay: Sequence[TransactionEvent]) -> int:
    """The first transaction whose card, device or merchant comes round again.

    A one cent change reaches a later transaction only through a window that
    holds it, so the fault has to be planted on an entity the replay visits
    twice. Which positions qualify is the generator's business and changes
    with it: this was a fixed index 199 until the scenarios were made harder
    (ADR 21), after which position 199 was a card, device and merchant never
    seen again, and the test failed for a reason that had nothing to do with
    parity. Finding the position keeps the assertion exactly as strong and
    stops it depending on the stream's luck.

    Args:
        replay: The transactions, in order.

    Returns:
        The index to tamper with.

    Raises:
        AssertionError: If no entity in the replay is visited twice, which
            would make the planted fault unobservable and the test a lie.
    """
    for index, event in enumerate(replay):
        if any(
            later.card_id == event.card_id
            or later.device_id == event.device_id
            or later.merchant_id == event.merchant_id
            for later in replay[index + 1 :]
        ):
            return index
    msg = "no transaction shares a card, device or merchant with a later one"
    raise AssertionError(msg)


def test_a_stream_that_changes_one_payload_is_caught(replay: list[TransactionEvent]) -> None:
    """The planted fault: one cent on one transaction, and the check must see it."""
    position = seen_again(replay)
    Tampering.target_event_id = replay[position].event_id
    Tampering.tampered = 0
    broker, stream = a_memory_path(Tampering)
    path = through(
        "tampering",
        stream,
        replay,
        transactions_topic="transactions",
        decisions_topic="decisions",
        reader=broker.open(),
    )
    assert Tampering.tampered == 1
    report = compare(reference(replay), path)
    assert not report.clean
    tampered = replay[position].event_id
    at_the_event = [d for d in report.disagreements if d.event_id == tampered]
    assert [d.what for d in at_the_event] == ["transaction"], report.summary()
    # And its effect: a later transaction whose window held the changed amount.
    later = {d.event_id for d in report.disagreements if d.what not in {"transaction", "decision"}}
    positions = {event.event_id: index for index, event in enumerate(replay)}
    assert later, report.summary()
    assert all(positions[event_id] > position for event_id in later)


class Redelivering(MemoryStream):
    """Delivers every transaction twice, as an at-least-once stream may."""

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Deliver every transaction, then deliver it again."""
        records = super().consume(
            topic, group, max_records=max_records, timeout_seconds=timeout_seconds
        )
        if topic != "transactions":
            return records
        return [copy for record in records for copy in (record, record)]


def test_redelivery_is_not_a_disagreement_because_the_ledger_stops_it(
    replay: list[TransactionEvent],
) -> None:
    broker, stream = a_memory_path(Redelivering)
    path = through(
        "redelivering",
        stream,
        replay,
        transactions_topic="transactions",
        decisions_topic="decisions",
        reader=broker.open(),
    )
    report = compare(reference(replay), path)
    assert report.clean, report.summary()
    assert path.duplicates == EVENTS


class Reordering(MemoryStream):
    """Swaps each pair of transactions in a batch."""

    def consume(
        self, topic: str, group: str, *, max_records: int = 500, timeout_seconds: float = 1.0
    ) -> Sequence[Record]:
        """Deliver each batch with neighbouring transactions swapped."""
        records = list(
            super().consume(topic, group, max_records=max_records, timeout_seconds=timeout_seconds)
        )
        if topic == "transactions":
            for index in range(0, len(records) - 1, 2):
                records[index], records[index + 1] = records[index + 1], records[index]
        return records


def test_a_stream_that_reorders_is_caught(replay: list[TransactionEvent]) -> None:
    """The engine refuses each late event, so it is set aside with no decision.

    Parity reports those as missing: a path that decided fewer transactions
    than the reference is not identical to it.
    """
    broker, stream = a_memory_path(Reordering)
    path = through(
        "reordering",
        stream,
        replay,
        transactions_topic="transactions",
        decisions_topic="decisions",
        reader=broker.open(),
    )
    report = compare(reference(replay), path)
    assert not report.clean
    assert {d.what for d in report.disagreements} >= {"missing"}


@pytest.mark.broker
def test_redpanda_is_identical_to_no_stream(replay: list[TransactionEvent]) -> None:
    if not broker_reachable():
        pytest.skip(f"no broker at {DEFAULT_BOOTSTRAP}; start deploy/compose to run these")
    from verdict.stream.redpanda import RedpandaStream

    admin = RedpandaStream()
    suffix = uuid.uuid4().hex[:8]
    transactions, decisions = f"parity-transactions-{suffix}", f"parity-decisions-{suffix}"
    admin.create_topic(transactions, 1)
    admin.create_topic(decisions, 4)
    dead = f"parity-dead-letter-{suffix}"
    admin.create_topic(dead, 1)
    stream, reader = RedpandaStream(), RedpandaStream()
    try:
        path = through(
            "redpanda",
            stream,
            replay,
            transactions_topic=transactions,
            decisions_topic=decisions,
            dead_letter_topic=dead,
            reader=reader,
        )
    finally:
        stream.close()
        reader.close()
        admin.delete_topic(transactions)
        admin.delete_topic(decisions)
        admin.delete_topic(dead)
        admin.close()
    report = compare(reference(replay), path)
    assert report.clean, report.summary()
