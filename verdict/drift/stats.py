"""The population stability index and the two-sample Kolmogorov-Smirnov test.

Written here rather than taken from Evidently, as `PLAN.md` section 2.6 first
said (ADR 12 records why): each is a few lines of arithmetic, the tests below
hold both to hand-worked and statistical checks, and a monitor whose numbers
cannot be reproduced by reading its source is a poor thing to trigger
retraining with.

## PSI, and the sentinel

Bins are fixed from the reference window's quantiles, so a day is always
measured against the same yardstick. Features in this platform carry
`NO_EVENTS`, a negative sentinel meaning "this entity has no history in the
window". That is not a small number on the same scale as a count, so it gets
its own bin: a day on which far more cards arrive with no history has drifted,
even if the cards that do have history look exactly as before. Folding the
sentinel into the lowest quantile bin would hide that.

Empty bins are floored at a small share, `EPSILON`, before the logarithm. The
floor is stated because it bounds how large a PSI a single empty bin can
produce.

## KS, and what a p-value means at a thousand events a second

The statistic is the largest gap between the two empirical distribution
functions. The p-value uses the asymptotic Kolmogorov distribution with the
small-sample correction of Stephens (1970), as given in *Numerical Recipes*.
At the live rate a day holds eighty-six million events, and any difference at
all has a vanishing p-value, so `monitors.py` flags on the statistic's size
and uses the p-value only to refuse flags on days too small to support them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt

from verdict.store.features import NO_EVENTS

FloatArray = npt.NDArray[np.float64]

EPSILON: Final = 1e-4
"""The share an empty bin is floored at. One empty bin against a reference
share of ten percent contributes about 0.69 to the PSI."""

DEFAULT_BINS: Final = 10


@dataclass(frozen=True, slots=True)
class Bins:
    """A binning fixed from a reference window.

    Attributes:
        edges: Interior edges between value bins, strictly increasing. Values
            below the first edge fall in the first bin, above the last in the
            last.
        has_sentinel_bin: Whether `NO_EVENTS` is binned on its own. Always
            true for features; false for scores, which have no sentinel.
    """

    edges: tuple[float, ...]
    has_sentinel_bin: bool

    @property
    def count(self) -> int:
        """How many bins in all.

        Returns:
            Value bins plus the sentinel bin if there is one.
        """
        return len(self.edges) + 1 + int(self.has_sentinel_bin)


def fit_bins(reference: FloatArray, *, bins: int = DEFAULT_BINS, sentinel: bool = True) -> Bins:
    """Fix bin edges from a reference window's quantiles.

    Repeated quantiles collapse, which is what a count feature with few
    distinct values produces; it gets fewer, wider bins rather than empty
    ones.

    Args:
        reference: The reference values.
        bins: The number of value bins wanted.
        sentinel: Whether to give `NO_EVENTS` its own bin.

    Returns:
        The binning.
    """
    values = reference[reference != NO_EVENTS] if sentinel else reference
    if values.size == 0:
        return Bins(edges=(), has_sentinel_bin=sentinel)
    quantiles = np.quantile(values, np.linspace(0, 1, bins + 1)[1:-1])
    edges = tuple(float(edge) for edge in np.unique(quantiles))
    return Bins(edges=edges, has_sentinel_bin=sentinel)


def shares(values: FloatArray, bins: Bins) -> FloatArray:
    """The share of values in each bin.

    Args:
        values: The values.
        bins: The binning.

    Returns:
        One share per bin, summing to one; zeros if there are no values.
    """
    counts = np.zeros(bins.count)
    if values.size == 0:
        return counts
    if bins.has_sentinel_bin:
        is_sentinel = values == NO_EVENTS
        counts[-1] = np.count_nonzero(is_sentinel)
        values = values[~is_sentinel]
    index = np.searchsorted(np.asarray(bins.edges), values, side="right")
    counts[: len(bins.edges) + 1] += np.bincount(index, minlength=len(bins.edges) + 1)
    return counts / counts.sum()


def psi(reference: FloatArray, current: FloatArray, bins: Bins) -> float:
    """The population stability index of a window against its reference.

    Args:
        reference: The reference values the bins were fitted on.
        current: The window being judged.
        bins: The binning.

    Returns:
        The PSI, zero for identical distributions.
    """
    expected = np.maximum(shares(reference, bins), EPSILON)
    actual = np.maximum(shares(current, bins), EPSILON)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


@dataclass(frozen=True, slots=True)
class KSResult:
    """A two-sample Kolmogorov-Smirnov comparison.

    Attributes:
        statistic: The largest gap between the empirical distribution
            functions, between 0 and 1.
        p_value: Its asymptotic p-value.
    """

    statistic: float
    p_value: float


def kolmogorov_q(lam: float) -> float:
    """The Kolmogorov distribution's survival function, Q(lambda).

    Args:
        lam: The scaled statistic.

    Returns:
        Q(lambda), between 0 and 1.
    """
    if lam < 1e-3:
        return 1.0
    total = 0.0
    for k in range(1, 101):
        term = 2.0 * (-1) ** (k - 1) * math.exp(-2.0 * k * k * lam * lam)
        total += term
        if abs(term) < 1e-12:
            break
    return min(1.0, max(0.0, total))


def ks_two_sample(reference: FloatArray, current: FloatArray) -> KSResult:
    """Compare two samples' distributions.

    Args:
        reference: One sample.
        current: The other.

    Returns:
        The statistic and its p-value.

    Raises:
        ValueError: If either sample is empty.
    """
    if reference.size == 0 or current.size == 0:
        msg = "both samples need at least one value"
        raise ValueError(msg)
    a = np.sort(reference)
    b = np.sort(current)
    grid = np.concatenate([a, b])
    cdf_a = np.searchsorted(a, grid, side="right") / a.size
    cdf_b = np.searchsorted(b, grid, side="right") / b.size
    statistic = float(np.max(np.abs(cdf_a - cdf_b)))
    effective = math.sqrt(a.size * b.size / (a.size + b.size))
    p_value = kolmogorov_q((effective + 0.12 + 0.11 / effective) * statistic)
    return KSResult(statistic=statistic, p_value=p_value)
