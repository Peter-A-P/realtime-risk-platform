"""The columns of what the platform keeps, and how a decision becomes a row.

A staged row is everything the platform knew about one transaction at the
moment it decided it: the transaction's own fields, the features it was
served (from the engine, the one computation of every feature, ADR 6), the
champion's decision, and the challenger's shadow decision if there was one.
It is written by the scorer in the same batch as the decision, so a feature
row exists for every decision that was checkpointed.

A history row is a staged row joined to its label, with the stratum and
weight the sample gave it (`sampling.py`).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

import pyarrow as pa

from verdict.events.schema import DecisionEvent, LabelEvent, ShadowEvent, TransactionEvent
from verdict.store.features import FEATURE_SET, FeatureSpec

_TIME: Final = pa.timestamp("us", tz="UTC")


def staged_schema(specs: Sequence[FeatureSpec] = FEATURE_SET) -> pa.Schema:
    """The columns of a staged decision.

    Args:
        specs: The features served, one float column each.

    Returns:
        The schema.
    """
    fields: list[pa.Field[Any]] = [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("card_id", pa.string(), nullable=False),
        pa.field("event_time", _TIME, nullable=False),
        pa.field("amount_cents", pa.int64(), nullable=False),
        pa.field("decided_at", _TIME, nullable=False),
        pa.field("champion_version", pa.string(), nullable=False),
        pa.field("champion_score", pa.float64(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("rule", pa.string(), nullable=False),
        pa.field("shadow_version", pa.string()),
        pa.field("shadow_score", pa.float64()),
        pa.field("shadow_action", pa.string()),
        *(pa.field(spec.name, pa.float64(), nullable=False) for spec in specs),
    ]
    return pa.schema(fields)


_LABEL_FIELDS: list[pa.Field[Any]] = [
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("label_time", _TIME, nullable=False),
    pa.field("is_fraud", pa.bool_(), nullable=False),
    pa.field("recovered_cents", pa.int64(), nullable=False),
]
LABEL_SCHEMA: Final = pa.schema(_LABEL_FIELDS)


def history_schema(specs: Sequence[FeatureSpec] = FEATURE_SET) -> pa.Schema:
    """The columns of a kept, labelled row.

    Args:
        specs: The features.

    Returns:
        The staged columns, then the label's, then the sample's.
    """
    fields: list[pa.Field[Any]] = [
        *staged_schema(specs),
        pa.field("label_time", _TIME, nullable=False),
        pa.field("is_fraud", pa.bool_(), nullable=False),
        pa.field("recovered_cents", pa.int64(), nullable=False),
        pa.field("stratum", pa.string(), nullable=False),
        pa.field("weight", pa.float64(), nullable=False),
    ]
    return pa.schema(fields)


def staged_row(
    event: TransactionEvent,
    features: Mapping[str, float],
    decision: DecisionEvent,
    shadow: ShadowEvent | None,
) -> dict[str, Any]:
    """One decision as a staged row.

    Args:
        event: The transaction.
        features: What the champion was served.
        decision: The champion's decision.
        shadow: The challenger's, if any.

    Returns:
        Column name to value.
    """
    row: dict[str, Any] = {
        "event_id": event.event_id,
        "card_id": event.card_id,
        "event_time": event.event_time,
        "amount_cents": event.amount_cents,
        "decided_at": decision.decided_at,
        "champion_version": decision.model_version,
        "champion_score": decision.score,
        "action": decision.action.value,
        "rule": decision.rule,
        "shadow_version": None if shadow is None else shadow.model_version,
        "shadow_score": None if shadow is None else shadow.score,
        "shadow_action": None if shadow is None else shadow.action.value,
    }
    row.update(features)
    return row


def label_row(label: LabelEvent) -> dict[str, Any]:
    """One label as a spooled row.

    Args:
        label: The label.

    Returns:
        Column name to value.
    """
    return {
        "event_id": label.event_id,
        "label_time": label.label_time,
        "is_fraud": label.is_fraud,
        "recovered_cents": label.recovered_cents,
    }
