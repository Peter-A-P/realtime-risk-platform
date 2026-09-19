"""A trained model behind the scorer's `Model` interface, run by ONNX Runtime.

The champion is trained with XGBoost and exported to ONNX (`models/train.py`),
so the scorer needs only ONNX Runtime, not the training library, and the
same file scores in a test, in the load test and on the instance. The inputs
are built by `models.inputs.vector`, the one definition of their order.

The model's version is the first twelve hex digits of the file's SHA-256, so
a decision names exactly the bytes that made it.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from verdict.events.schema import TransactionEvent
from verdict.models.inputs import MODEL_INPUTS, vector


def model_version(path: Path, prefix: str = "champion") -> str:
    """A model's version: its role and the hash of its bytes.

    Args:
        path: The ONNX file.
        prefix: What the model is.

    Returns:
        For example `champion-3f2a9c1b0d4e`.
    """
    return f"{prefix}-{hashlib.sha256(path.read_bytes()).hexdigest()[:12]}"


class OnnxModel:
    """Scores one event from its served features, with ONNX Runtime."""

    def __init__(self, path: Path, *, prefix: str = "champion", threads: int = 1) -> None:
        """Load the model.

        Args:
            path: The ONNX file.
            prefix: What the model is, for its version.
            threads: Intra-op threads. One: the scorer is single-threaded,
                and a second thread on a two-core instance competes with the
                broker for a hop measured in microseconds.

        Raises:
            ValueError: If the model does not take `MODEL_INPUTS` columns.
        """
        import onnxruntime as ort

        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        self._session = ort.InferenceSession(
            str(path), sess_options=options, providers=["CPUExecutionProvider"]
        )
        inputs = self._session.get_inputs()
        width = inputs[0].shape[1]
        if len(inputs) != 1 or width != len(MODEL_INPUTS):
            msg = f"expected one input of {len(MODEL_INPUTS)} columns, got {width}"
            raise ValueError(msg)
        self._input = inputs[0].name
        outputs = [output.name for output in self._session.get_outputs()]
        self._probabilities = "probabilities" if "probabilities" in outputs else outputs[-1]
        self.version = model_version(path, prefix)

    def score_matrix(self, rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Fraud probabilities for many rows at once.

        Args:
            rows: One row per event, in `MODEL_INPUTS` order.

        Returns:
            The probability of fraud for each row.
        """
        result: list[Any] = self._session.run(
            [self._probabilities], {self._input: rows.astype(np.float32)}
        )
        return _fraud_column(result[0])

    def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
        """Score one event.

        Args:
            features: What the scorer served.
            event: The event.

        Returns:
            The probability of fraud, in [0, 1].
        """
        row = np.array([vector(features, event)], dtype=np.float32)
        result: list[Any] = self._session.run([self._probabilities], {self._input: row})
        return float(min(1.0, max(0.0, _fraud_column(result[0])[0])))


def _fraud_column(output: Any) -> npt.NDArray[np.float64]:  # noqa: ANN401 - runtime output
    """The fraud-class column of a probabilities output, array or list of maps."""
    if isinstance(output, list):
        return np.array([float(row[1]) for row in output], dtype=np.float64)
    array = np.asarray(output, dtype=np.float64)
    return array[:, 1] if array.ndim == 2 else array
