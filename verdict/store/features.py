"""What a feature is, and the one definition of how to compute it.

A feature here is a specification, not code: an entity to key on, a window to
look back over, an aggregation, and a field to aggregate. `evaluate_spec`
computes a specification directly from raw events by brute force.

**This is the definition, and the streaming dataflow is an optimisation of
it.** In week 3 the Bytewax dataflow computes these incrementally, keeping
state per entity instead of rescanning history. That is the same definition
executed differently, and `leakage.py` is what holds the two together: if the
incremental execution and the brute-force definition ever disagree, the
incremental one is wrong.

That is also the honest reading of this repository's rule that features are
computed once. The rule forbids a second *definition* of a feature for
training, which is what produces training-serving skew. A slow reference
evaluation that exists only to check the fast one is a test oracle, not a
second pipeline: nothing trains on it and nothing serves from it. ADR 7
records this reasoning.

## The window convention, stated once

A feature computed as of time `t` over window `w` uses events in the
half-open interval `[t - w, t)`. **Strictly before `t`, never at `t`.**

The event being scored is therefore never part of its own features. This is
the single most common point-in-time bug in fraud systems, because including
it is both easy and enormously flattering: a velocity count that includes the
current transaction separates fraud from legitimate traffic beautifully
offline and cannot be computed at all in production, where the decision has to
be made before the event exists anywhere.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from verdict.events.schema import TransactionEvent


class EntityKind(StrEnum):
    """What a feature is keyed on."""

    CARD = "card"
    DEVICE = "device"
    MERCHANT = "merchant"
    SESSION = "session"


class Aggregation(StrEnum):
    """How a feature reduces the events in its window to one number."""

    COUNT = "count"
    SUM = "sum"
    MEAN = "mean"
    MAX = "max"
    MIN = "min"
    DISTINCT_COUNT = "distinct_count"
    SECONDS_SINCE_LAST = "seconds_since_last"


NO_EVENTS: Final = -1.0
"""What a feature returns when its window holds nothing.

Not zero, and not a null. Zero is a real value that a count can legitimately
take, and a null propagates into the model as a decision someone has to make
later and usually makes wrongly. A negative sentinel is distinguishable from
every real value these aggregations produce, and the model learns it as the
category it is: this entity has no history in this window.
"""


def entity_id_of(event: TransactionEvent, kind: EntityKind) -> str | None:
    """Return the identifier a feature of this kind is keyed on.

    Args:
        event: The transaction.
        kind: Which entity.

    Returns:
        The identifier, or None when the event has no such entity, which
        happens for card-present transactions with no session.
    """
    match kind:
        case EntityKind.CARD:
            return event.card_id
        case EntityKind.DEVICE:
            return event.device_id
        case EntityKind.MERCHANT:
            return event.merchant_id
        case EntityKind.SESSION:
            return event.session_id


@dataclass(frozen=True, slots=True)
class FeatureSpec:
    """One feature, defined rather than implemented.

    Attributes:
        name: The feature's name, unique in the feature set. It is what the
            model, the store and the drift monitors all refer to.
        entity: What it is keyed on.
        aggregation: How the window is reduced.
        field: Which event field is aggregated. `None` for `COUNT` and
            `SECONDS_SINCE_LAST`, which need no field.
        window: How far back to look. `None` means unbounded: everything
            strictly before the as-of time.
        description: What it is for, in a sentence. It ends up in the feature
            registry, and a feature nobody can explain is a feature nobody
            should ship.
    """

    name: str
    entity: EntityKind
    aggregation: Aggregation
    field: str | None = None
    window: dt.timedelta | None = None
    description: str = ""

    def __post_init__(self) -> None:
        """Check the specification is coherent.

        Raises:
            ValueError: If a field is missing where one is needed, supplied
                where none is wanted, or the window is not positive.
        """
        needs_no_field = {Aggregation.COUNT, Aggregation.SECONDS_SINCE_LAST}
        if self.aggregation in needs_no_field and self.field is not None:
            msg = f"{self.name}: {self.aggregation} takes no field, got {self.field!r}"
            raise ValueError(msg)
        if self.aggregation not in needs_no_field and self.field is None:
            msg = f"{self.name}: {self.aggregation} needs a field to aggregate"
            raise ValueError(msg)
        if self.window is not None and self.window <= dt.timedelta(0):
            msg = f"{self.name}: window must be positive, got {self.window}"
            raise ValueError(msg)

    @property
    def window_seconds(self) -> float | None:
        """The window in seconds, for reporting and for the store's TTL.

        Returns:
            The window in seconds, or None if it is unbounded.
        """
        return None if self.window is None else self.window.total_seconds()


def events_in_window(
    events: Sequence[TransactionEvent],
    spec: FeatureSpec,
    entity_id: str,
    as_of: dt.datetime,
) -> list[TransactionEvent]:
    """Select the events a feature may see.

    The whole point-in-time argument lives in the comparison operators here:
    `event_time < as_of` and not `<=`, and `event_time >= as_of - window`.

    Args:
        events: Raw events, in any order.
        spec: The feature being computed.
        entity_id: The entity the feature is keyed on.
        as_of: The moment the feature describes.

    Returns:
        The eligible events, oldest first.
    """
    start = None if spec.window is None else as_of - spec.window
    selected = [
        event
        for event in events
        if entity_id_of(event, spec.entity) == entity_id
        and event.event_time < as_of
        and (start is None or event.event_time >= start)
    ]
    selected.sort(key=lambda event: event.event_time)
    return selected


def evaluate_spec(
    spec: FeatureSpec,
    events: Sequence[TransactionEvent],
    entity_id: str,
    as_of: dt.datetime,
) -> float:
    """Compute a feature the slow, obvious way.

    This is the reference. It scans every event it is given, which makes it
    far too slow to serve and exactly right to test against.

    Args:
        spec: The feature to compute.
        events: Raw events, in any order.
        entity_id: The entity to compute it for.
        as_of: The moment the feature describes.

    Returns:
        The feature's value, or `NO_EVENTS` if the window holds nothing.

    Raises:
        ValueError: If the specified field is not a number on the event.
    """
    window = events_in_window(events, spec, entity_id, as_of)
    if not window:
        return NO_EVENTS

    if spec.aggregation is Aggregation.COUNT:
        return float(len(window))

    if spec.aggregation is Aggregation.SECONDS_SINCE_LAST:
        return (as_of - window[-1].event_time).total_seconds()

    if spec.aggregation is Aggregation.DISTINCT_COUNT:
        return float(len({_field_value(event, spec.field) for event in window}))

    values = [_numeric_field(event, spec.field, spec.name) for event in window]
    # No fallback case, deliberately. The match covers every remaining
    # aggregation, and `mypy --strict` proves it: adding one to the enum
    # without handling it here fails the type check rather than raising at
    # runtime on whichever event happens to arrive first.
    match spec.aggregation:
        case Aggregation.SUM:
            return float(sum(values))
        case Aggregation.MEAN:
            return float(sum(values) / len(values))
        case Aggregation.MAX:
            return float(max(values))
        case Aggregation.MIN:
            return float(min(values))


def _field_value(event: TransactionEvent, field: str | None) -> object:
    """Read a field off an event by name.

    Args:
        event: The transaction.
        field: The field name.

    Returns:
        The value.

    Raises:
        ValueError: If the event has no such field.
    """
    if field is None or field not in type(event).model_fields:
        msg = f"no field {field!r} on a transaction event"
        raise ValueError(msg)
    return getattr(event, field)


def _numeric_field(event: TransactionEvent, field: str | None, name: str) -> float:
    """Read a numeric field off an event.

    Args:
        event: The transaction.
        field: The field name.
        name: The feature's name, for the error message.

    Returns:
        The value as a float.

    Raises:
        ValueError: If the field is not a number.
    """
    value = _field_value(event, field)
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = f"{name}: field {field!r} is {type(value).__name__}, not a number"
        raise ValueError(msg)
    return float(value)


_HOUR = dt.timedelta(hours=1)
_DAY = dt.timedelta(hours=24)
_SESSION = dt.timedelta(minutes=30)

FEATURE_SET: Final[tuple[FeatureSpec, ...]] = (
    # --- Card velocity. What this card has been doing lately. ---
    FeatureSpec(
        name="card_txn_count_1h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.COUNT,
        window=_HOUR,
        description="Transactions on this card in the last hour.",
    ),
    FeatureSpec(
        name="card_txn_count_24h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.COUNT,
        window=_DAY,
        description="Transactions on this card in the last day.",
    ),
    FeatureSpec(
        name="card_amount_sum_1h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.SUM,
        field="amount_cents",
        window=_HOUR,
        description="Money moved on this card in the last hour.",
    ),
    FeatureSpec(
        name="card_amount_mean_24h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.MEAN,
        field="amount_cents",
        window=_DAY,
        description=(
            "This card's usual ticket over a day. The model compares it with the "
            "amount on the event being scored, which is how a takeover's spending "
            "becomes unusual for this card rather than unusual in general."
        ),
    ),
    FeatureSpec(
        name="card_amount_max_24h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.MAX,
        field="amount_cents",
        window=_DAY,
        description="The largest amount on this card in the last day.",
    ),
    FeatureSpec(
        name="card_seconds_since_last",
        entity=EntityKind.CARD,
        aggregation=Aggregation.SECONDS_SINCE_LAST,
        window=_DAY,
        description=(
            "Silence before this transaction. A dormant card used twice in a "
            "minute is the shape of a takeover."
        ),
    ),
    FeatureSpec(
        name="card_distinct_merchants_24h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.DISTINCT_COUNT,
        field="merchant_id",
        window=_DAY,
        description="How many different merchants this card touched in a day.",
    ),
    # --- Device. The entity-graph view: one device, many cards. ---
    FeatureSpec(
        name="device_txn_count_1h",
        entity=EntityKind.DEVICE,
        aggregation=Aggregation.COUNT,
        window=_HOUR,
        description="Transactions from this device in the last hour.",
    ),
    FeatureSpec(
        name="device_distinct_cards_1h",
        entity=EntityKind.DEVICE,
        aggregation=Aggregation.DISTINCT_COUNT,
        field="card_id",
        window=_HOUR,
        description=(
            "Cards seen on this device in an hour. This is what a card-testing "
            "burst looks like from the outside, and it is invisible in any single "
            "transaction."
        ),
    ),
    FeatureSpec(
        name="device_distinct_cards_24h",
        entity=EntityKind.DEVICE,
        aggregation=Aggregation.DISTINCT_COUNT,
        field="card_id",
        window=_DAY,
        description="Cards seen on this device in a day.",
    ),
    FeatureSpec(
        name="device_amount_mean_1h",
        entity=EntityKind.DEVICE,
        aggregation=Aggregation.MEAN,
        field="amount_cents",
        window=_HOUR,
        description=(
            "Card testing runs small amounts, so a device with many cards and a "
            "low mean is a different thing from a busy shared family device."
        ),
    ),
    # --- Merchant. Where collusion shows up, over hours rather than seconds. ---
    FeatureSpec(
        name="merchant_txn_count_1h",
        entity=EntityKind.MERCHANT,
        aggregation=Aggregation.COUNT,
        window=_HOUR,
        description="Transactions at this merchant in the last hour.",
    ),
    FeatureSpec(
        name="merchant_distinct_cards_1h",
        entity=EntityKind.MERCHANT,
        aggregation=Aggregation.DISTINCT_COUNT,
        field="card_id",
        window=_HOUR,
        description="Cards seen at this merchant in an hour.",
    ),
    FeatureSpec(
        name="merchant_amount_mean_1h",
        entity=EntityKind.MERCHANT,
        aggregation=Aggregation.MEAN,
        field="amount_cents",
        window=_HOUR,
        description=(
            "A colluding merchant inflates its tickets, so its own mean moves "
            "while nothing about any single transaction looks wrong."
        ),
    ),
    # --- Session. Card-not-present activity, grouped as it happens. ---
    FeatureSpec(
        name="session_txn_count",
        entity=EntityKind.SESSION,
        aggregation=Aggregation.COUNT,
        window=_SESSION,
        description="Transactions in this session so far.",
    ),
    FeatureSpec(
        name="session_amount_sum",
        entity=EntityKind.SESSION,
        aggregation=Aggregation.SUM,
        field="amount_cents",
        window=_SESSION,
        description="Money moved in this session so far.",
    ),
)
"""The features this platform serves.

Sixteen, computed once by `verdict.features.engine` and served both online
and offline from that one computation. Each is keyed on an entity, because
none of the three fraud patterns in the generator is visible in a single
transaction: card testing is one device against many cards, a takeover is a
card behaving unlike itself, and collusion is one merchant's own distribution
moving.

They were added in week 3, after the leakage test in week 2, and every one of
them is checked by it on every run.
"""


def feature_names(specs: Iterable[FeatureSpec] = FEATURE_SET) -> tuple[str, ...]:
    """List the names in a feature set.

    Args:
        specs: The features. Defaults to the platform's own set.

    Returns:
        The names, in definition order.
    """
    return tuple(spec.name for spec in specs)


def validate_feature_set(specs: Sequence[FeatureSpec]) -> None:
    """Check a feature set is usable.

    Args:
        specs: The features.

    Raises:
        ValueError: If two features share a name.
    """
    names = [spec.name for spec in specs]
    duplicates = {name for name in names if names.count(name) > 1}
    if duplicates:
        msg = f"duplicate feature names: {sorted(duplicates)}"
        raise ValueError(msg)
