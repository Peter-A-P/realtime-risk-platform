"""The wire schema holds its shape, and refuses what it should refuse."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from verdict.events.schema import (
    SCHEMA_VERSION,
    GroundTruth,
    LabelEvent,
    SchemaFingerprint,
    TransactionEvent,
    UnknownSchemaVersionError,
    decode_transaction,
)

HASHES = json.loads(
    (Path(__file__).resolve().parents[1] / "docs" / "generator-hashes.json").read_text(
        encoding="utf-8"
    )
)


def an_event(**overrides: object) -> TransactionEvent:
    fields: dict[str, object] = {
        "event_id": "evt-1",
        "event_time": dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC),
        "card_id": "card-00000001",
        "device_id": "dev-00000001",
        "merchant_id": "mer-000001",
        "amount_cents": 4_250,
        "merchant_category": "grocery_pos",
        "entry_mode": "chip",
    }
    fields.update(overrides)
    return TransactionEvent(**fields)


def test_fingerprint_matches_the_committed_one() -> None:
    """A change to any field, type or constraint has to be deliberate.

    If this fails, the schema changed. Bump `SCHEMA_VERSION`, write the
    migration note, and update `docs/generator-hashes.json` in the same
    commit; do not simply paste the new hash in.
    """
    assert SchemaFingerprint.compute().sha256 == HASHES["schema_sha256"]


def test_round_trip_through_json() -> None:
    event = an_event()
    assert decode_transaction(event.to_json()) == event


def test_unknown_schema_version_is_refused() -> None:
    payload = json.loads(an_event().to_json())
    payload["schema_version"] = SCHEMA_VERSION + 1
    with pytest.raises(UnknownSchemaVersionError):
        decode_transaction(json.dumps(payload))


def test_a_record_without_a_version_is_refused() -> None:
    payload = json.loads(an_event().to_json())
    del payload["schema_version"]
    with pytest.raises(UnknownSchemaVersionError):
        decode_transaction(json.dumps(payload))


def test_unknown_fields_are_refused() -> None:
    """A producer that adds a field cannot have it silently dropped."""
    payload = json.loads(an_event().to_json())
    payload["risk_score"] = 0.9
    with pytest.raises(ValidationError):
        decode_transaction(json.dumps(payload))


def test_a_naive_timestamp_is_refused() -> None:
    with pytest.raises(ValidationError):
        an_event(event_time=dt.datetime(2027, 4, 5, 12, 0))


def test_a_non_utc_timestamp_is_refused() -> None:
    eastern = dt.timezone(dt.timedelta(hours=-4))
    with pytest.raises(ValidationError):
        an_event(event_time=dt.datetime(2027, 4, 5, 12, 0, tzinfo=eastern))


@pytest.mark.parametrize("amount", [0, -1, 100_000_001])
def test_impossible_amounts_are_refused(amount: int) -> None:
    with pytest.raises(ValidationError):
        an_event(amount_cents=amount)


def test_an_event_is_frozen() -> None:
    """Nothing mutates an event in flight; a decision is made about a value."""
    event = an_event()
    with pytest.raises(ValidationError):
        event.amount_cents = 1  # type: ignore[misc]


def test_a_transaction_carries_no_outcome() -> None:
    """The schema-level half of the leakage argument.

    A transaction with a label on it would make every downstream test moot,
    so the absence is asserted rather than assumed.
    """
    forbidden = {"is_fraud", "label", "label_time", "scenario", "regime", "recovered_cents"}
    assert forbidden.isdisjoint(TransactionEvent.model_fields)


def test_the_schema_is_at_version_two() -> None:
    """Version 2 is the one that lets an unknown entity be `None`."""
    assert SCHEMA_VERSION == 2
    assert json.loads(an_event().to_json())["schema_version"] == 2


@pytest.mark.parametrize("field", ["device_id", "merchant_id", "merchant_category"])
def test_an_unknown_entity_is_stated_not_defaulted(field: str) -> None:
    """A producer has to say an entity is absent; it cannot forget to send it.

    `None` is accepted and round-trips. Omitting the field is refused, so a
    producer that drops a column by mistake fails here instead of looking
    like a source that never had one.
    """
    absent = an_event(**{field: None})
    assert getattr(decode_transaction(absent.to_json()), field) is None

    payload = json.loads(an_event().to_json())
    del payload[field]
    with pytest.raises(ValidationError):
        decode_transaction(json.dumps(payload))


def test_a_card_is_never_optional() -> None:
    """Every card transaction has a card, even where the source cannot link it."""
    with pytest.raises(ValidationError):
        an_event(card_id=None)


def test_ground_truth_and_labels_are_separate_types() -> None:
    assert "scenario" in GroundTruth.model_fields
    assert "scenario" not in LabelEvent.model_fields
    assert "is_fraud" in LabelEvent.model_fields
