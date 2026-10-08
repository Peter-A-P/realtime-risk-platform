"""How good the live decisions were, from the kept history once labels are in.

The window's decisions are judged here against the labels that arrived a week
later: how much of the stream was fraud, how much of it each action caught,
and how much of what each action touched was fraud. Every figure is weighted
(ADR 18): the kept sample holds every reviewed or declined row and a fraction
of the approved ones, each with the inverse of its keeping rate as its
weight, so the weighted sums stand for the whole stream.

It was written on 2026-10-08, when the live window's first week declined 9.0%
of transactions and sent 7.9% to review, against a fraud share of about 3%
and the 1.9% the offline replay sent to review (README, ADR 22). Whether that
is a model scoring a population it was not trained on, or the fraud being
caught, is a question only the labels can answer.

Calibration is reported by score band, so a model that scores the live
population higher than the population it was trained on shows as a band
whose fraud rate is far below its scores.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

BANDS: Final[tuple[float, ...]] = (0.0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.0000001)
"""Score bands: the decision thresholds (0.5 review, 0.9 decline) are edges."""

COLUMNS: Final[tuple[str, ...]] = (
    "action",
    "rule",
    "champion_score",
    "shadow_score",
    "is_fraud",
    "amount_cents",
    "weight",
)
"""What the report reads from a kept day."""


class _Rows:
    """A kept day's columns as arrays, for weighted sums."""

    def __init__(self, table: pa.Table) -> None:
        self.weight = np.asarray(table["weight"].to_numpy(), dtype=np.float64)
        self.fraud = np.asarray(table["is_fraud"].to_numpy(zero_copy_only=False), dtype=bool)
        self.action = np.asarray(table["action"].to_pylist(), dtype=object)
        self.rule = np.asarray(table["rule"].to_pylist(), dtype=object)
        self.scores = {
            name: np.asarray(
                [np.nan if v is None else v for v in table[name].to_pylist()], dtype=np.float64
            )
            for name in ("champion_score", "shadow_score")
        }
        self.rows = table.num_rows

    def total(self, mask: NDArray[np.bool_] | None = None) -> float:
        return float(self.weight.sum() if mask is None else self.weight[mask].sum())


def _share(part: float, whole: float) -> float | None:
    return None if whole == 0 else part / whole


def _by_value(rows: _Rows, values: NDArray[np.object_]) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    total, frauds = rows.total(), rows.total(rows.fraud)
    for value in sorted({str(v) for v in values}):
        here = values == value
        weighted = rows.total(here)
        caught = rows.total(here & rows.fraud)
        out[value] = {
            "share_of_transactions": _share(weighted, total),
            "fraud_rate": _share(caught, weighted),
            "share_of_all_fraud": _share(caught, frauds),
        }
    return out


def _bands(rows: _Rows, name: str) -> list[dict[str, Any]]:
    scores = rows.scores[name]
    present = ~np.isnan(scores)
    whole = rows.total(present)
    out: list[dict[str, Any]] = []
    for low, high in itertools.pairwise(BANDS):
        here = present & (scores >= low) & (scores < high)
        weighted = rows.total(here)
        if weighted == 0:
            continue
        out.append(
            {
                "band": f"{low:.1f} to {min(high, 1.0):.1f}",
                "share_of_transactions": _share(weighted, whole),
                "mean_score": _share(float((scores[here] * rows.weight[here]).sum()), weighted),
                "fraud_rate": _share(rows.total(here & rows.fraud), weighted),
            }
        )
    return out


def decision_quality(table: pa.Table) -> dict[str, Any]:
    """Weighted decision quality over kept rows.

    Args:
        table: Kept rows with at least `COLUMNS`.

    Returns:
        The fraud share, each action's and rule's share, fraud rate and share
        of all fraud, and calibration by score band for the champion and the
        shadow model.
    """
    rows = _Rows(table)
    total, frauds = rows.total(), rows.total(rows.fraud)
    acted = rows.action != "approve"
    acted_frauds = rows.total(acted & rows.fraud)
    return {
        "transactions_weighted": total,
        "rows_kept": rows.rows,
        "fraud_share": _share(frauds, total),
        "acted_share": _share(rows.total(acted), total),
        "acted_precision": _share(acted_frauds, rows.total(acted)),
        "acted_recall": _share(acted_frauds, frauds),
        "by_action": _by_value(rows, rows.action),
        "by_rule": _by_value(rows, rows.rule),
        "champion_calibration": _bands(rows, "champion_score"),
        "shadow_calibration": _bands(rows, "shadow_score"),
    }


def report(days: Iterable[tuple[str, pa.Table]]) -> dict[str, Any]:
    """Decision quality for each kept day and for all of them together.

    Args:
        days: Each day's name and its kept rows.

    Returns:
        `{"days": {day: quality}, "all": quality}`.
    """
    named: Sequence[tuple[str, pa.Table]] = list(days)
    per_day: Mapping[str, dict[str, Any]] = {name: decision_quality(table) for name, table in named}
    together = pa.concat_tables([table for _, table in named]) if named else None
    return {
        "days": dict(per_day),
        "all": decision_quality(together) if together is not None else None,
    }
