"""Training sets that could have existed at the moment they claim to.

ADR 10 set four rules for labels; this module enforces the fourth:

> **Training at a cutoff uses only labels that had arrived by the cutoff.**

A model trained as of time T sees transactions from before T whose labels
had arrived by T. The last seven days of transactions before T have no
label yet, so they are **left out and counted**, never included as
legitimate: an unarrived label read as "not fraud" would teach the model that
last week's frauds were fine, and it would look like data rather than a bug.

Rows come from one of two places, both carrying the features exactly as the
engine served them (ADR 6, features computed once):

- **live history** (`verdict/history`, ADR 18), a weighted sample, whose
  weights every fit must use;
- **an offline replay** (`replay_table`), which runs the events through the
  same `EngineFeatures` the scorer uses, in the same order, so a training row
  holds the value the scorer would have served, not a recomputation of it.
  Every row has weight 1.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc

from verdict.events.schema import LabelEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.models.inputs import MODEL_INPUTS, matrix
from verdict.scoring.core import EngineFeatures
from verdict.store.features import FEATURE_SET, FeatureSpec

_TIME = pa.timestamp("us", tz="UTC")


def training_schema(specs: Sequence[FeatureSpec] = FEATURE_SET) -> pa.Schema:
    """The columns of a replayed training row.

    Args:
        specs: The features.

    Returns:
        Ids and times, the model's inputs, the label, and a weight.
    """
    fields: list[pa.Field[Any]] = [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_time", _TIME, nullable=False),
        pa.field("amount_cents", pa.int64(), nullable=False),
        *(pa.field(spec.name, pa.float64(), nullable=False) for spec in specs),
        pa.field("label_time", _TIME, nullable=False),
        pa.field("is_fraud", pa.bool_(), nullable=False),
        pa.field("weight", pa.float64(), nullable=False),
    ]
    return pa.schema(fields)


@dataclass(frozen=True, slots=True)
class Replayed:
    """An offline replay's training rows, and what it could not label.

    Attributes:
        table: One row per labelled event, in event order.
        unlabelled: Events with no label at all, left out.
    """

    table: pa.Table
    unlabelled: int


def replay_table(events: Iterable[TransactionEvent], labels: Mapping[str, LabelEvent]) -> Replayed:
    """Serve every event's features through the scorer's own engine path.

    Args:
        events: The events, in event-time order; the engine refuses a late one.
        labels: Each event's label, by event id.

    Returns:
        The rows, with weight 1, and the count of events without a label.
    """
    source = EngineFeatures(FeatureEngine())
    rows: list[dict[str, Any]] = []
    unlabelled = 0
    for event in events:
        served = source.serve(event)
        label = labels.get(event.event_id)
        if label is None:
            unlabelled += 1
            continue
        row: dict[str, Any] = {
            "event_id": event.event_id,
            "event_time": event.event_time,
            "amount_cents": event.amount_cents,
            "label_time": label.label_time,
            "is_fraud": label.is_fraud,
            "weight": 1.0,
        }
        row.update(served)
        rows.append(row)
    return Replayed(pa.Table.from_pylist(rows, schema=training_schema()), unlabelled)


@dataclass(frozen=True, slots=True)
class TrainingSet:
    """What a model is fitted on, and what was left out of it.

    Attributes:
        cutoff: The moment the set is as of.
        inputs: One row per transaction, columns in `MODEL_INPUTS` order.
        labels: True for fraud.
        weights: How many transactions each row stands for.
        event_ids: The rows' events, for tracing a prediction back.
        excluded_unarrived: Transactions before the cutoff whose label came
            after it: left out, not counted as legitimate.
        input_names: The column order, so it travels with the matrix.
    """

    cutoff: dt.datetime
    inputs: npt.NDArray[np.float64]
    labels: npt.NDArray[np.bool_]
    weights: npt.NDArray[np.float64]
    event_ids: tuple[str, ...]
    excluded_unarrived: int
    input_names: tuple[str, ...] = MODEL_INPUTS

    @property
    def frauds(self) -> int:
        """Labelled frauds, as rows (not weights).

        Returns:
            The count.
        """
        return int(self.labels.sum())


def at_cutoff(table: pa.Table, cutoff: dt.datetime) -> TrainingSet:
    """The training set as it could have been built at a moment.

    Args:
        table: Rows with `event_id`, `event_time`, `label_time`, `is_fraud`,
            the model's inputs, and optionally `weight` (1 if absent).
        cutoff: The moment. Timezone-aware.

    Returns:
        Rows whose transaction came before the cutoff and whose label had
        arrived by it.

    Raises:
        ValueError: If the cutoff is naive, or the table has no label time.
            Without a label time there is no telling which labels had
            arrived, and a set that cannot be checked is not built.
    """
    if cutoff.tzinfo is None:
        msg = "cutoff must be timezone-aware"
        raise ValueError(msg)
    if "label_time" not in table.column_names:
        msg = "a training table must carry label_time; rule 4 of ADR 10 is checked against it"
        raise ValueError(msg)
    moment = pa.scalar(cutoff, _TIME)
    before = pc.less(table["event_time"], moment)
    arrived = pc.less_equal(table["label_time"], moment)
    usable = table.filter(pc.and_(before, arrived))
    unarrived = pc.cast(pc.and_(before, pc.invert(arrived)), pa.int64())
    excluded = int(pc.sum(unarrived).as_py() or 0)
    weights = (
        usable["weight"].to_numpy(zero_copy_only=False).astype(np.float64)
        if "weight" in usable.column_names
        else np.ones(usable.num_rows, dtype=np.float64)
    )
    return TrainingSet(
        cutoff=cutoff,
        inputs=matrix(usable),
        labels=usable["is_fraud"].to_numpy(zero_copy_only=False).astype(np.bool_),
        weights=weights,
        event_ids=tuple(str(e) for e in usable["event_id"].to_pylist()),
        excluded_unarrived=excluded,
    )
