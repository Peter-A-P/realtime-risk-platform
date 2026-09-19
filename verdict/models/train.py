"""The champion: gradient-boosted trees, fitted once, exported to ONNX.

The model is timeboxed (ADR 1, CLAUDE.md): one well-tuned gradient-boosting
champion, not a search. The parameters below are conventional for tabular
fraud data and are fixed, not tuned against any test set; the number of
trees is chosen by early stopping on the last part of the training period,
in time order, never on the test period.

What this module guarantees rather than tunes:

- **The training set is as of a cutoff** (`dataset.at_cutoff`, ADR 10 rule
  4): labels that had not arrived are left out and counted.
- **Weights are used.** A sampled set (ADR 18, or `replay_to_parquet` with a
  legitimate rate below 1) is fitted with each row's weight, so the model
  sees the class balance of the stream, not of the sample.
- **Validation is later in time than training**, and the test is later
  still: the last `validation_share` of the training rows by event time for
  early stopping, and a separate period after the cutoff for the reported
  number (`split_by_time`).
- **The exported model scores what the booster scores.** `export_onnx`
  checks the ONNX file against the booster on rows the caller gives it and
  refuses to write one that disagrees.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc

from verdict.models.dataset import TrainingSet, at_cutoff
from verdict.models.inputs import MODEL_INPUTS

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    import xgboost as xgb

PARAMS: Final[dict[str, Any]] = {
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 1.0,
    "lambda": 1.0,
}
"""Fixed before any test number was seen. Conventional, not searched."""

MAX_ROUNDS: Final = 2_000
EARLY_STOPPING: Final = 50
VALIDATION_SHARE: Final = 0.15
PARITY_TOLERANCE: Final = 1e-4
"""How far an ONNX score may sit from the booster's. ONNX Runtime evaluates
trees in float32 thresholds, the booster in float64; beyond this is a bug."""


class ExportMismatchError(RuntimeError):
    """Raised when the exported model does not score what the booster scores."""


@dataclass(frozen=True, slots=True)
class Split:
    """A training set as of a cutoff, and the period after it to test on.

    Attributes:
        train: As of the cutoff: transactions before it whose labels had
            arrived by it.
        test: Transactions from the cutoff to the end, all labelled by the
            time they are evaluated.
        cutoff: The moment the model is as of.
    """

    train: TrainingSet
    test: TrainingSet
    cutoff: dt.datetime


def split_by_time(
    table: pa.Table, *, train_share: float = 0.7, wait_for_labels: bool = False
) -> Split:
    """Split a period in time: train before a point in it, test after.

    Two ways to be honest about labels, and each track uses one:

    - `wait_for_labels=False` (the real track's 182 days): the model is as of
      the split point, so its training set is `at_cutoff` there and the week
      before it, whose labels had not arrived, is left out and counted.
    - `wait_for_labels=True` (the synthetic track's ten days, where a week's
      wait would leave nothing to train on): the model is trained a label
      delay later, once every training transaction's label has arrived, on
      the transactions before the split point only. It still never sees a
      transaction from the test period, nor a label that had not arrived at
      the moment it was trained.

    Either way the test is every transaction from the split point on,
    evaluated once all its labels are in.

    Args:
        table: Replayed rows in event-time order, with label times.
        train_share: Where in the period the split falls.
        wait_for_labels: Train once the training rows' labels are all in.

    Returns:
        The split. Its `cutoff` is the moment the model is as of.

    Raises:
        ValueError: If the table is empty.
    """
    if table.num_rows == 0:
        msg = "no rows to split"
        raise ValueError(msg)
    times = table["event_time"]
    start = pc.min(times).as_py()
    end = pc.max(times).as_py()
    point = start + (end - start) * train_share
    moment = pa.scalar(point, times.type)
    labelled_by = pc.max(table["label_time"]).as_py()
    after = table.filter(pc.greater_equal(times, moment))
    if wait_for_labels:
        before = table.filter(pc.less(times, moment))
        trained_at = pc.max(before["label_time"]).as_py() + dt.timedelta(microseconds=1)
        train = at_cutoff(before, trained_at)
    else:
        trained_at = point
        train = at_cutoff(table, point)
    return Split(
        train=train,
        test=at_cutoff(after, labelled_by + dt.timedelta(microseconds=1)),
        cutoff=trained_at,
    )


@dataclass(slots=True)
class Fitted:
    """A fitted champion and how it was fitted.

    Attributes:
        booster: The XGBoost model.
        params: The parameters, as used.
        best_iteration: Trees kept after early stopping.
        train_rows: Rows fitted on.
        validation_rows: Rows early stopping watched.
        seconds: Wall time of the fit.
    """

    booster: xgb.Booster
    params: dict[str, Any] = field(default_factory=dict)
    best_iteration: int = 0
    train_rows: int = 0
    validation_rows: int = 0
    seconds: float = 0.0


def fit_champion(
    training: TrainingSet,
    *,
    validation_share: float = VALIDATION_SHARE,
    threads: int = 6,
    seed: int = 20270405,
) -> Fitted:
    """Fit the champion, early-stopping on the latest part of the training rows.

    Args:
        training: The training set, rows in event-time order.
        validation_share: The share of the latest rows watched for early
            stopping.
        threads: XGBoost threads.
        seed: For subsampling.

    Returns:
        The fitted model.

    Raises:
        ValueError: If there are too few frauds on either side of the split.
    """
    import time

    import xgboost as xgb

    rows = len(training.labels)
    split = int(rows * (1 - validation_share))
    fit_idx, val_idx = slice(0, split), slice(split, rows)
    if training.labels[fit_idx].sum() < 10 or training.labels[val_idx].sum() < 10:
        msg = "fewer than ten frauds on one side of the validation split"
        raise ValueError(msg)
    params = {**PARAMS, "nthread": threads, "seed": seed}
    fit = xgb.DMatrix(
        training.inputs[fit_idx], label=training.labels[fit_idx], weight=training.weights[fit_idx]
    )
    val = xgb.DMatrix(
        training.inputs[val_idx], label=training.labels[val_idx], weight=training.weights[val_idx]
    )
    started = time.perf_counter()
    booster = xgb.train(
        params,
        fit,
        num_boost_round=MAX_ROUNDS,
        evals=[(val, "validation")],
        early_stopping_rounds=EARLY_STOPPING,
        verbose_eval=False,
    )
    return Fitted(
        booster=booster,
        params=params,
        best_iteration=int(booster.best_iteration) + 1,
        train_rows=split,
        validation_rows=rows - split,
        seconds=time.perf_counter() - started,
    )


def booster_scores(fitted: Fitted, inputs: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
    """The booster's fraud probabilities, using only the trees early stopping kept.

    Args:
        fitted: The model.
        inputs: Rows in `MODEL_INPUTS` order.

    Returns:
        One probability per row.
    """
    import xgboost as xgb

    scores = fitted.booster.predict(xgb.DMatrix(inputs), iteration_range=(0, fitted.best_iteration))
    return np.asarray(scores, dtype=np.float64)


def export_onnx(fitted: Fitted, path: Path, check_rows: npt.NDArray[np.float64]) -> Path:
    """Write the champion as ONNX, having checked it scores as the booster does.

    Args:
        fitted: The model.
        path: Where to write it.
        check_rows: Rows to compare the two on; the test rows, typically.

    Returns:
        The path written.

    Raises:
        ExportMismatchError: If any score differs by more than
            `PARITY_TOLERANCE`. Nothing is written then.
    """
    import onnxmltools
    from onnxmltools.convert.common.data_types import FloatTensorType

    from verdict.scoring.onnx_model import OnnxModel

    trimmed = fitted.booster[: fitted.best_iteration]
    model = onnxmltools.convert_xgboost(
        trimmed,
        initial_types=[("inputs", FloatTensorType([None, len(MODEL_INPUTS)]))],
        target_opset=15,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(model.SerializeToString())
    exported = OnnxModel(temporary).score_matrix(check_rows)
    expected = booster_scores(fitted, check_rows)
    worst = float(np.max(np.abs(exported - expected))) if len(expected) else 0.0
    if worst > PARITY_TOLERANCE:
        temporary.unlink()
        msg = f"ONNX scores differ from the booster's by up to {worst:.2e}"
        raise ExportMismatchError(msg)
    temporary.replace(path)
    return path
