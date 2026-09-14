"""Wire schema for the transaction stream.

Everything that travels on the stream is one of the models in this module,
serialised as JSON. Three rules hold the schema together:

1. **It is versioned.** `SCHEMA_VERSION` is written into every record and
   `decode_transaction` refuses a record whose version it does not know,
   rather than reading a field that has since changed meaning.
2. **It is closed.** Every model forbids unknown fields, so a producer that
   starts sending something new cannot have it quietly ignored by a consumer.
3. **A transaction never carries its own label.** Whether an event turned out
   to be fraud arrives later as a separate `LabelEvent`, and what the
   generator knows in advance lives in `GroundTruth`, which never touches the
   transaction topic. Putting either on the event would be the first and
   largest leak the point-in-time test exists to catch.

Money is an integer count of cents. Floating-point dollars accumulate error in
exactly the aggregations this platform computes, and the review queue ranks by
expected loss in money.

## Version history

- **1** (week 1). Every transaction named a card, a device and a merchant.
- **2** (week 4, 2026-09-14). `device_id`, `merchant_id` and
  `merchant_category` may be `None`, and must be stated either way: none of
  them has a default. The real-data track has no merchant identifier at all
  and nothing that identifies a device (ADR 17), and a transaction whose
  entity is unknown now says so rather than carrying an identifier invented
  to fill the field. An invented merchant shared by every row would make each
  merchant-keyed feature a count of the whole data set. A version 1 reader
  would fail on a `None`, which is what makes this a breaking change. No
  version 1 record is kept anywhere that matters: the development logs are
  regenerated from a seed.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from enum import StrEnum
from typing import Annotated, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION: Final = 2
"""Major version of the wire schema. A breaking change increments this."""


class UnknownSchemaVersionError(ValueError):
    """Raised when a record carries a schema version this build cannot read."""

    def __init__(self, found: object) -> None:
        """Record the offending version.

        Args:
            found: The `schema_version` value read off the wire.
        """
        super().__init__(f"unknown schema version {found!r}, this build reads {SCHEMA_VERSION}")
        self.found = found


class EntryMode(StrEnum):
    """How the card was presented to the merchant.

    These are the public card-present and card-not-present modes the payment
    industry distinguishes, and the ones a fraud pattern actually turns on: a
    card-testing burst is card-not-present, a cloned-card run is magstripe.
    """

    CHIP = "chip"
    CONTACTLESS = "contactless"
    MAGSTRIPE = "magstripe"
    ECOMMERCE = "ecommerce"
    MANUAL = "manual"


class MerchantCategory(StrEnum):
    """Merchant category.

    This is the public category set published by the Sparkov synthetic
    transaction generator, recorded with its licence in `docs/data.md`. It is
    used so the synthetic track's category mix can be compared against a
    public reference rather than invented here.
    """

    ENTERTAINMENT = "entertainment"
    FOOD_DINING = "food_dining"
    GAS_TRANSPORT = "gas_transport"
    GROCERY_NET = "grocery_net"
    GROCERY_POS = "grocery_pos"
    HEALTH_FITNESS = "health_fitness"
    HOME = "home"
    KIDS_PETS = "kids_pets"
    MISC_NET = "misc_net"
    MISC_POS = "misc_pos"
    PERSONAL_CARE = "personal_care"
    SHOPPING_NET = "shopping_net"
    SHOPPING_POS = "shopping_pos"
    TRAVEL = "travel"


class FraudScenario(StrEnum):
    """The fraud patterns the generator can produce.

    All three are publicly documented card-fraud patterns; `NONE` marks a
    legitimate transaction. Sources are cited in `docs/adr/0002-two-tracks.md`.
    """

    NONE = "none"
    CARD_TESTING = "card_testing"
    ACCOUNT_TAKEOVER = "account_takeover"
    MERCHANT_COLLUSION = "merchant_collusion"


CountryCode = Annotated[str, Field(min_length=2, max_length=2, pattern=r"^[A-Z]{2}$")]
"""ISO 3166-1 alpha-2 country code."""

EntityId = Annotated[str, Field(min_length=1, max_length=64)]
"""Opaque identifier for a card, device, merchant or session.

Synthetic throughout. No real primary account number ever enters this system;
the real-data track is already tokenised by its publisher.
"""


def require_utc(value: dt.datetime) -> dt.datetime:
    """Reject naive or non-UTC timestamps.

    Every timestamp in this platform is timezone-aware UTC. A naive timestamp
    is the cheapest way to produce a feature that is right on the laptop and
    wrong in the live region, which is the class of error the leakage test
    exists to catch.

    Args:
        value: The timestamp to check.

    Returns:
        The same timestamp, guaranteed aware and in UTC.

    Raises:
        ValueError: If the timestamp is naive or not in UTC.
    """
    if value.tzinfo is None:
        msg = "timestamp must be timezone-aware UTC, got a naive datetime"
        raise ValueError(msg)
    if value.utcoffset() != dt.timedelta(0):
        msg = f"timestamp must be UTC, got offset {value.utcoffset()}"
        raise ValueError(msg)
    return value


class Record(BaseModel):
    """Base for every record on the wire: frozen, closed and strictly typed."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    def to_json(self) -> str:
        """Serialise to the JSON form that travels on the stream.

        Returns:
            A single-line JSON document.
        """
        return self.model_dump_json()


class TransactionEvent(Record):
    """One card transaction, as it appears on the stream before scoring.

    It carries no label, no score and no derived feature: only what an
    acquirer would know at the moment the transaction is presented.
    """

    schema_version: Literal[2] = SCHEMA_VERSION
    event_id: EntityId
    """Idempotency key. Decisions are keyed on it, so at-least-once delivery
    of the same event produces the same decision exactly once."""
    event_time: dt.datetime
    """When the transaction was presented, timezone-aware UTC."""
    card_id: EntityId
    device_id: EntityId | None
    """The device, or `None` when the source cannot identify one. No default:
    a producer states the absence rather than inheriting it."""
    merchant_id: EntityId | None
    """The merchant, or `None` when the source has no merchant identifier."""
    session_id: EntityId | None = None
    amount_cents: int = Field(gt=0, le=100_000_000)
    """Amount in cents. Integer, never a float: see the module docstring."""
    currency: Literal["USD"] = "USD"
    merchant_category: MerchantCategory | None
    """The merchant's category, or `None` when the merchant is unknown."""
    entry_mode: EntryMode
    card_country: CountryCode = "US"
    merchant_country: CountryCode = "US"
    is_recurring: bool = False

    @field_validator("event_time")
    @classmethod
    def _check_event_time(cls, value: dt.datetime) -> dt.datetime:
        return require_utc(value)

    @property
    def amount_dollars(self) -> float:
        """The amount in dollars, for display and reporting only.

        Returns:
            The amount as a float. Never use this in an aggregation.
        """
        return self.amount_cents / 100


class LabelEvent(Record):
    """The outcome of a transaction, arriving after a delay.

    Labels join to transactions by `event_id`. In life a chargeback takes days
    or weeks; the platform simulates a seven-day delay so that no training or
    promotion decision can use a label that would not yet have existed.
    """

    schema_version: Literal[2] = SCHEMA_VERSION
    event_id: EntityId
    label_time: dt.datetime
    """When the outcome became known. Always later than the event time."""
    is_fraud: bool
    recovered_cents: int = Field(default=0, ge=0)
    """How much of a fraudulent amount was recovered. Feeds expected loss."""

    @field_validator("label_time")
    @classmethod
    def _check_label_time(cls, value: dt.datetime) -> dt.datetime:
        return require_utc(value)


class GroundTruth(Record):
    """What the generator knows about an event it produced.

    This is generator-side only and is written to its own sink. It never
    appears on the transaction topic, and no feature, model or decision may
    read it. It exists so the synthetic track can be evaluated and so the
    sealed regime schedule can be graded after the fact.
    """

    schema_version: Literal[2] = SCHEMA_VERSION
    event_id: EntityId
    is_fraud: bool
    scenario: FraudScenario
    regime: str
    """Name of the regime in force when the event was generated."""


def decode_transaction(raw: str | bytes) -> TransactionEvent:
    """Decode one JSON record from the stream into a `TransactionEvent`.

    Args:
        raw: The JSON document as it came off the wire.

    Returns:
        The validated event.

    Raises:
        UnknownSchemaVersionError: If the record's schema version is not one
            this build knows how to read.
    """
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    version = _peek_schema_version(text)
    if version != SCHEMA_VERSION:
        raise UnknownSchemaVersionError(version)
    return TransactionEvent.model_validate_json(text)


def _peek_schema_version(text: str) -> object:
    """Read the schema version out of a record without validating the rest.

    Args:
        text: The JSON document.

    Returns:
        The `schema_version` value, or `None` if the record does not carry one.
    """
    try:
        parsed: object = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed.get("schema_version")
    return None


class SchemaFingerprint(Record):
    """A hash of the wire schema, so a silent change to it cannot pass CI.

    The fingerprint covers the JSON Schema of every record type. Changing a
    field name, type or constraint changes the fingerprint, which fails
    `tests/test_schema.py` until `SCHEMA_VERSION` and the committed
    fingerprint are both updated deliberately.
    """

    schema_version: Literal[2] = SCHEMA_VERSION
    sha256: str

    @classmethod
    def compute(cls) -> Self:
        """Compute the fingerprint of the current schema.

        Returns:
            The fingerprint of this build's wire schema.
        """
        payload = {
            "GroundTruth": GroundTruth.model_json_schema(),
            "LabelEvent": LabelEvent.model_json_schema(),
            "TransactionEvent": TransactionEvent.model_json_schema(),
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return cls(sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest())
