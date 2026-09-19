"""What a model is given: one ordered list of numbers, built one way.

A model trained on columns in one order and served them in another is wrong
in a way no metric shows until production. So the order is written down once,
here, and both sides use it: `vector` builds one row at decision time from
the features the scorer served and the event, and `matrix` builds the
training matrix from a table of rows the same features were recorded in
(staged history, or an offline replay through the same engine). A test holds
the two to the same numbers for the same decisions.

The inputs are the sixteen features and the amount. A feature with no
history carries the `NO_EVENTS` sentinel on both sides; the model learns
what it means, and nothing fills it in.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

import numpy as np
import numpy.typing as npt
import pyarrow as pa

from verdict.events.schema import TransactionEvent
from verdict.store.features import feature_names

MODEL_INPUTS: Final[tuple[str, ...]] = (*feature_names(), "amount_cents")
"""Every input, in the order a model receives them."""


class MissingInputError(KeyError):
    """Raised when a row or table lacks an input a model needs."""


def vector(features: Mapping[str, float], event: TransactionEvent) -> list[float]:
    """One decision's inputs, as the scorer would give them to a model.

    Args:
        features: What the scorer served.
        event: The transaction.

    Returns:
        The inputs in `MODEL_INPUTS` order.

    Raises:
        MissingInputError: If a feature was not served. The scorer serves
            every feature, with a sentinel where there is no history, so a gap
            is a bug upstream rather than something to fill here.
    """
    values: list[float] = []
    for name in MODEL_INPUTS:
        if name == "amount_cents":
            values.append(float(event.amount_cents))
        elif name in features:
            values.append(float(features[name]))
        else:
            raise MissingInputError(name)
    return values


def matrix(table: pa.Table) -> npt.NDArray[np.float64]:
    """A training matrix from rows that recorded the same inputs.

    Args:
        table: Staged or kept history, or an offline replay table.

    Returns:
        One row per table row, columns in `MODEL_INPUTS` order.

    Raises:
        MissingInputError: If the table lacks an input column.
    """
    missing = [name for name in MODEL_INPUTS if name not in table.column_names]
    if missing:
        raise MissingInputError(", ".join(missing))
    columns: list[Any] = [
        table[name].to_numpy(zero_copy_only=False).astype(np.float64) for name in MODEL_INPUTS
    ]
    if not columns[0].size:
        return np.empty((0, len(MODEL_INPUTS)), dtype=np.float64)
    return np.column_stack(columns)
