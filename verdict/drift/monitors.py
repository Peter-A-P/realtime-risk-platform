"""A day of served features and scores, judged against a fixed reference.

## The reference is fixed

The reference window is the data the champion was trained on, and it changes
only when a new champion is promoted with a new training window. A rolling
reference (yesterday against the day before) would absorb a slow drift one
small step at a time and never report it, which is the shape the sealed
schedule's gradual regimes take.

## The thresholds are conventions, not tuned values

- **PSI at or above 0.25** is drift; 0.10 to 0.25 is reported as a moderate
  shift and triggers nothing. These are the credit-scoring conventions
  (Siddiqi 2006), chosen before any drift was seen here.
- **KS statistic at or above 0.10, with a p-value below 0.01.** The statistic
  decides; the p-value only stops a small day from flagging on noise.
- **At least 500 values** in a day's window for a quantity to be judged at all.

None of them was chosen by looking at the development schedule's regimes, and
none may be: the live schedule is sealed precisely so the monitors cannot be
tuned to it, and tuning them to the public development schedule would defeat
the same purpose from the other side. ADR 12 records the values and where they
come from.

## What is monitored

Every feature, and the champion's score. The score is watched because a shift
in inputs the model ignores does not matter, and a shift in the score does
even when no single feature moved much.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import numpy as np

from verdict.drift.stats import Bins, FloatArray, fit_bins, ks_two_sample, psi

PSI_DRIFT: Final = 0.25
PSI_MODERATE: Final = 0.10
KS_STATISTIC_DRIFT: Final = 0.10
KS_P_VALUE: Final = 0.01
MIN_VALUES: Final = 500

SCORE: Final = "score"
"""The name the champion's score is monitored under, beside the features."""


class Status(StrEnum):
    """What a day says about one quantity."""

    STABLE = "stable"
    MODERATE = "moderate"
    DRIFTED = "drifted"
    INSUFFICIENT = "insufficient"
    """Too few values to judge. Neither drift nor its absence."""


@dataclass(frozen=True, slots=True)
class QuantityResult:
    """One quantity on one day.

    Attributes:
        name: The feature, or `score`.
        values: Values in the day's window.
        psi: The population stability index against the reference.
        ks_statistic: The KS statistic against the reference.
        ks_p_value: Its p-value.
        status: The verdict.
    """

    name: str
    values: int
    psi: float
    ks_statistic: float
    ks_p_value: float
    status: Status


@dataclass(frozen=True, slots=True)
class DailyReport:
    """Every quantity on one day.

    Attributes:
        day: The day judged.
        results: One per quantity, in a stable order.
    """

    day: dt.date
    results: tuple[QuantityResult, ...]

    def drifted(self) -> frozenset[str]:
        """The quantities that drifted.

        Returns:
            Their names.
        """
        return frozenset(r.name for r in self.results if r.status is Status.DRIFTED)

    def result(self, name: str) -> QuantityResult:
        """One quantity's result.

        Args:
            name: The quantity.

        Returns:
            The result.

        Raises:
            KeyError: If the quantity was not monitored.
        """
        for result in self.results:
            if result.name == name:
                return result
        raise KeyError(name)


def columns(rows: Iterable[Mapping[str, float]]) -> dict[str, FloatArray]:
    """Turn served feature rows into one array per quantity.

    Args:
        rows: One mapping per decision: feature name to value, plus `score`.

    Returns:
        Name to values.
    """
    gathered: dict[str, list[float]] = {}
    for row in rows:
        for name, value in row.items():
            gathered.setdefault(name, []).append(float(value))
    return {name: np.asarray(values, dtype=np.float64) for name, values in gathered.items()}


class Reference:
    """The fixed reference window, with its binning fitted once."""

    def __init__(self, window: Mapping[str, FloatArray]) -> None:
        """Fit bins for every quantity in the reference window.

        Args:
            window: Name to reference values. The score has no sentinel; every
                feature does.
        """
        self.window = {
            name: np.asarray(values, dtype=np.float64) for name, values in window.items()
        }
        self.bins: dict[str, Bins] = {
            name: fit_bins(values, sentinel=name != SCORE) for name, values in self.window.items()
        }

    @property
    def names(self) -> tuple[str, ...]:
        """The quantities monitored, sorted.

        Returns:
            Their names.
        """
        return tuple(sorted(self.window))


def judge(name: str, reference: FloatArray, current: FloatArray, bins: Bins) -> QuantityResult:
    """Judge one quantity on one day.

    Args:
        name: The quantity.
        reference: Its reference values.
        current: The day's values.
        bins: Its binning.

    Returns:
        The result.
    """
    if current.size < MIN_VALUES:
        return QuantityResult(name, int(current.size), 0.0, 0.0, 1.0, Status.INSUFFICIENT)
    index = psi(reference, current, bins)
    ks = ks_two_sample(reference, current)
    if index >= PSI_DRIFT or (ks.statistic >= KS_STATISTIC_DRIFT and ks.p_value < KS_P_VALUE):
        status = Status.DRIFTED
    elif index >= PSI_MODERATE:
        status = Status.MODERATE
    else:
        status = Status.STABLE
    return QuantityResult(name, int(current.size), index, ks.statistic, ks.p_value, status)


def daily_report(
    day: dt.date, reference: Reference, window: Mapping[str, FloatArray]
) -> DailyReport:
    """Judge a day's window against the reference.

    Args:
        day: The day.
        reference: The fixed reference.
        window: Name to the day's values. A quantity the day lacks is judged
            on no values, which is insufficient, not stable.

    Returns:
        The report.
    """
    empty = np.empty(0, dtype=np.float64)
    return DailyReport(
        day=day,
        results=tuple(
            judge(name, reference.window[name], window.get(name, empty), reference.bins[name])
            for name in reference.names
        ),
    )


def reports_in_order(reports: Sequence[DailyReport]) -> list[DailyReport]:
    """Sort reports by day, refusing two for one day, which no run could be read from.

    Args:
        reports: The reports.

    Returns:
        The reports in day order.

    Raises:
        ValueError: If two reports share a day.
    """
    ordered = sorted(reports, key=lambda report: report.day)
    days = [report.day for report in ordered]
    if len(set(days)) != len(days):
        msg = "two reports for the same day"
        raise ValueError(msg)
    return ordered
