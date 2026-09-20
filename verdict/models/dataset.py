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
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Protocol

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from verdict.events.schema import LabelEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.history.sampling import draw
from verdict.models.inputs import MODEL_INPUTS, matrix, vector
from verdict.scoring.core import EngineFeatures
from verdict.scoring.model import BatchModel
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


def replay_table(
    events: Iterable[TransactionEvent],
    labels: Mapping[str, LabelEvent],
    *,
    engine: FeatureEngine | None = None,
) -> Replayed:
    """Serve every event's features through the scorer's own engine path.

    Args:
        events: The events, in event-time order; the engine refuses a late one.
        labels: Each event's label, by event id.
        engine: The engine to serve from. The platform's own by default; the
            leak measurement passes the unfixed one (`features/unfixed.py`).

    Returns:
        The rows, with weight 1, and the count of events without a label.
    """
    source = EngineFeatures(engine or FeatureEngine())
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


class Labelled(Protocol):
    """A transaction with its label: a generated or a mapped record."""

    @property
    def event(self) -> TransactionEvent:
        """The transaction."""
        ...

    @property
    def label(self) -> LabelEvent:
        """Its outcome."""
        ...


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """What a replay to Parquet served and kept.

    Attributes:
        path: The Parquet file.
        served: Events served through the engine, every one of them.
        kept: Rows written.
        frauds: Frauds among them, all kept.
        legit_rate: The share of legitimate rows kept, each weighted by its
            inverse.
    """

    path: Path
    served: int
    kept: int
    frauds: int
    legit_rate: float


SCORE_BATCH: Final = 20_000
"""Rows given to a model in one call by `serve_and_score`."""


def serve_and_score(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    engine: FeatureEngine | None = None,
    batch: int = SCORE_BATCH,
) -> Iterator[tuple[Labelled, Mapping[str, float], float]]:
    """Serve a stream through the engine and score it, in batches.

    Every event is served on its own, in order, so the windows are exactly
    what the scorer would have held. Only the model call is batched, because
    a per-row call over millions of transactions spends most of its time in
    ONNX Runtime's call overhead rather than in the model. The scores are
    therefore identical to the scorer's, and the run is not.

    Args:
        records: Transactions with their labels, in event-time order.
        model: The model to score with.
        engine: The engine to serve from; the platform's own by default.
        batch: Rows given to the model in one call.

    Yields:
        Each record with the features it was served and its score, in the
        order the records arrived.
    """
    source = EngineFeatures(engine or FeatureEngine())
    rows: list[list[float]] = []
    pending: list[tuple[Labelled, Mapping[str, float]]] = []

    def scored() -> Iterator[tuple[Labelled, Mapping[str, float], float]]:
        if not rows:
            return
        scores = model.score_matrix(np.asarray(rows, dtype=np.float64))
        for (record, features), score in zip(pending, scores, strict=True):
            yield record, features, float(score)
        rows.clear()
        pending.clear()

    for record in records:
        features = source.serve(record.event)
        rows.append(vector(features, record.event))
        pending.append((record, features))
        if len(rows) >= batch:
            yield from scored()
    yield from scored()


def replay_to_parquet(
    records: Iterable[Labelled],
    path: Path,
    *,
    engine: FeatureEngine | None = None,
    legit_rate: float = 1.0,
    batch: int = 100_000,
) -> ReplayReport:
    """Serve a long stream through the engine and write training rows as it goes.

    Every event is served, so every window is exactly what the scorer would
    have held; only the writing is sampled. Every fraud is kept, and a
    legitimate row is kept when the hash draw of its id falls under
    `legit_rate` (the same salted draw as ADR 18's sample), with weight
    `1 / legit_rate`, so estimates and fits that use the weight come out as
    on the full stream.

    Args:
        records: Transactions with their labels, in event-time order.
        path: Where to write the Parquet file.
        engine: The engine to serve from; the platform's own by default.
        legit_rate: The share of legitimate rows to keep, in (0, 1].
        batch: Rows per write.

    Returns:
        What was served and kept.

    Raises:
        ValueError: If the rate is not a probability that keeps something.
    """
    if not 0.0 < legit_rate <= 1.0:
        msg = f"legit_rate must be in (0, 1], got {legit_rate}"
        raise ValueError(msg)
    source = EngineFeatures(engine or FeatureEngine())
    schema = training_schema()
    path.parent.mkdir(parents=True, exist_ok=True)
    served = kept = frauds = 0
    rows: list[dict[str, Any]] = []
    with pq.ParquetWriter(path, schema, compression="zstd") as writer:
        for record in records:
            event, label = record.event, record.label
            features = source.serve(event)
            served += 1
            if not label.is_fraud and draw(event.event_id) >= legit_rate:
                continue
            row: dict[str, Any] = {
                "event_id": event.event_id,
                "event_time": event.event_time,
                "amount_cents": event.amount_cents,
                "label_time": label.label_time,
                "is_fraud": label.is_fraud,
                "weight": 1.0 if label.is_fraud else 1.0 / legit_rate,
            }
            row.update(features)
            rows.append(row)
            kept += 1
            frauds += int(label.is_fraud)
            if len(rows) >= batch:
                writer.write_table(pa.Table.from_pylist(rows, schema=schema))
                rows = []
        if rows:
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
    return ReplayReport(path=path, served=served, kept=kept, frauds=frauds, legit_rate=legit_rate)
