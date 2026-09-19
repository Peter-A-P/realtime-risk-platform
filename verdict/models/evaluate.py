"""PR-AUC with its interval, the one way every model number here is reported.

Every number this project publishes carries a confidence interval and says
which track it came from (CLAUDE.md). A model's PR-AUC is average precision
(`promote.average_precision`, which reads weights), with a 95 percent
percentile bootstrap that resamples within each weight, so a weighted sample
(ADR 18) keeps its design in every resample, exactly as the promotion gate
does.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt

from verdict.models.promote import average_precision

RESAMPLES: Final = 1_000
SEED: Final = 20270405


@dataclass(frozen=True, slots=True)
class Interval:
    """A point estimate and its 95 percent bootstrap interval.

    Attributes:
        value: On all rows.
        low: 2.5th percentile of the resamples.
        high: 97.5th percentile.
        rows: Rows it was computed on.
        frauds: Frauds among them, as rows.
        resamples: Bootstrap resamples drawn.
    """

    value: float
    low: float
    high: float
    rows: int
    frauds: int
    resamples: int


def strata(weights: npt.NDArray[np.float64]) -> list[npt.NDArray[np.intp]]:
    """Row indices grouped by weight, for a bootstrap that keeps the design.

    Args:
        weights: One per row.

    Returns:
        One index array per distinct weight.
    """
    return [np.flatnonzero(weights == value) for value in np.unique(weights)]


def pr_auc(
    labels: npt.NDArray[np.bool_],
    scores: npt.NDArray[np.float64],
    weights: npt.NDArray[np.float64] | None = None,
    *,
    resamples: int = RESAMPLES,
    seed: int = SEED,
) -> Interval:
    """Weighted average precision with a stratified bootstrap interval.

    Args:
        labels: True for fraud.
        scores: The model's scores.
        weights: How many transactions each row stands for; all 1 if None.
        resamples: Bootstrap resamples.
        seed: Generator seed, so the interval is reproducible.

    Returns:
        The interval.
    """
    if weights is None:
        weights = np.ones(len(labels), dtype=np.float64)
    groups = strata(weights)
    rng = np.random.default_rng(seed)
    draws = np.empty(resamples)
    for draw in range(resamples):
        index = np.concatenate([g[rng.integers(0, len(g), size=len(g))] for g in groups])
        draws[draw] = average_precision(labels[index], scores[index], weights[index])
    return Interval(
        value=average_precision(labels, scores, weights),
        low=float(np.percentile(draws, 2.5)),
        high=float(np.percentile(draws, 97.5)),
        rows=len(labels),
        frauds=int(labels.sum()),
        resamples=resamples,
    )


def paired_difference(
    labels: npt.NDArray[np.bool_],
    first: npt.NDArray[np.float64],
    second: npt.NDArray[np.float64],
    weights: npt.NDArray[np.float64] | None = None,
    *,
    resamples: int = RESAMPLES,
    seed: int = SEED,
) -> Interval:
    """PR-AUC of `second` minus `first` on the same rows, with a paired interval.

    Args:
        labels: True for fraud.
        first: One set of scores.
        second: The other, for the same rows.
        weights: Row weights; all 1 if None.
        resamples: Bootstrap resamples.
        seed: Generator seed.

    Returns:
        The difference and its interval.
    """
    if weights is None:
        weights = np.ones(len(labels), dtype=np.float64)
    groups = strata(weights)
    rng = np.random.default_rng(seed)
    draws = np.empty(resamples)
    for draw in range(resamples):
        index = np.concatenate([g[rng.integers(0, len(g), size=len(g))] for g in groups])
        lab, w = labels[index], weights[index]
        draws[draw] = average_precision(lab, second[index], w) - average_precision(
            lab, first[index], w
        )
    return Interval(
        value=average_precision(labels, second, weights)
        - average_precision(labels, first, weights),
        low=float(np.percentile(draws, 2.5)),
        high=float(np.percentile(draws, 97.5)),
        rows=len(labels),
        frauds=int(labels.sum()),
        resamples=resamples,
    )
