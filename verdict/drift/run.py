"""Running the drift monitors over a replayed stream, and what fires when.

The monitors (`monitors.py`) and the trigger (`trigger.py`) have been tested
against constructed windows since ADR 12 was written: given a shifted
distribution they report drift, and given two consecutive drifted days they
open a retraining request. What they had never been given is a stream. This
runs them over one the generator actually produced, whose regime schedule
moves the fraud rate, the amounts and the online share on days that are
written down, so the question is no longer whether the statistics work but
how long the platform takes to notice a shift and what it says about it.

**The reference is the champion's training window**, as `monitors.py`
requires: the days before the champion's cutoff, taken from this same
stream. Everything before the cutoff builds the reference, everything after
is judged against it, and no day is ever judged against yesterday.

**The windows are sampled, and the sample is drawn, not taken.** PSI and a
KS statistic need a distribution, not every row, and a day at this
configuration holds 1.7 million transactions. Each day keeps a hash-drawn
share of its transactions, the same salted draw ADR 18 samples history with,
so the sample is deterministic, uniform over the day, and the same
transactions feed every quantity. The monitors refuse to judge on fewer than
500 values; the sample is far above that, and the report carries the count
so a thin day is visible rather than silent.

**Nothing here tunes anything.** The thresholds come from ADR 12 and were
fixed before any drift was seen. The development schedule's regime days are
reported beside the firings only so the delay can be read; they are not an
input to any decision, and the live schedule is sealed so that this stays
true when it matters.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Final

import numpy as np

from verdict.drift.monitors import SCORE, DailyReport, Reference, Status, daily_report
from verdict.drift.stats import FloatArray
from verdict.drift.trigger import RetrainRequest, evaluate_trigger
from verdict.events.generator.regimes import RegimeSchedule
from verdict.history.sampling import draw
from verdict.models.dataset import Labelled, serve_and_score
from verdict.scoring.model import BatchModel

SAMPLE_RATE: Final = 0.03
"""Share of each day's transactions the monitors judge on. At 1.7 million a
day this is about 52,000 values per quantity, a hundred times the minimum a
day is judged on at all."""


class _Window:
    """One day's values, one list per quantity."""

    def __init__(self) -> None:
        """Start an empty window."""
        self.values: dict[str, list[float]] = {}

    def add(self, features: Mapping[str, float], score: float) -> None:
        """Keep one transaction's features and score.

        Args:
            features: What the scorer served.
            score: The champion's score.
        """
        for name, value in features.items():
            self.values.setdefault(name, []).append(float(value))
        self.values.setdefault(SCORE, []).append(score)

    def arrays(self) -> dict[str, FloatArray]:
        """The window as arrays.

        Returns:
            One array per quantity.
        """
        return {name: np.asarray(values, dtype=np.float64) for name, values in self.values.items()}

    def __bool__(self) -> bool:
        """Whether anything was kept.

        Returns:
            True if the window holds a transaction.
        """
        return bool(self.values)


def drift_reports(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    cutoff: dt.datetime,
    rate: float = SAMPLE_RATE,
    on_day: Callable[[DailyReport], None] | None = None,
) -> tuple[Reference, list[DailyReport]]:
    """Build the reference from before the cutoff and judge every day after it.

    This walks tens of millions of transactions and takes hours, so it reports
    each day as it is judged rather than only at the end. A run that shows
    nothing for four hours cannot be told from a run that has died, and the
    caller can keep what has been judged so far.

    Args:
        records: Transactions with their labels, in event-time order.
        model: The champion, which the score is taken from.
        cutoff: The champion's training cutoff. Before it the transactions
            build the reference; after it they are judged.
        rate: Share of transactions kept, by hash draw.
        on_day: Called with each day's report as it is judged.

    Returns:
        The reference and one report per judged day, in day order.

    Raises:
        ValueError: If the rate is not a probability that keeps something, or
            the stream ended before the cutoff, leaving nothing to judge.
    """
    if not 0.0 < rate <= 1.0:
        msg = f"rate must be in (0, 1], got {rate}"
        raise ValueError(msg)
    reference_window = _Window()
    reference: Reference | None = None
    reports: list[DailyReport] = []
    current = _Window()
    current_day: dt.date | None = None

    for record, features, score in serve_and_score(records, model=model):
        event = record.event
        if draw(event.event_id) >= rate:
            continue
        if event.event_time < cutoff:
            reference_window.add(features, score)
            continue
        if reference is None:
            reference = Reference(reference_window.arrays())
        day = event.event_time.date()
        if current_day is not None and day != current_day:
            judged = daily_report(current_day, reference, current.arrays())
            reports.append(judged)
            if on_day is not None:
                on_day(judged)
            current = _Window()
        current_day = day
        current.add(features, score)

    if reference is None:
        msg = "the stream ended before the cutoff, so no day was judged"
        raise ValueError(msg)
    if current_day is not None and current:
        judged = daily_report(current_day, reference, current.arrays())
        reports.append(judged)
        if on_day is not None:
            on_day(judged)
    return reference, reports


def judge_against(
    records: Iterable[Labelled],
    reference: Reference,
    *,
    model: BatchModel,
    judge_from: dt.datetime,
    rate: float = SAMPLE_RATE,
    on_day: Callable[[DailyReport], None] | None = None,
) -> list[DailyReport]:
    """Judge every day of a stream from a moment on against a reference built elsewhere.

    `drift_reports` builds its reference from the stream's own first days.
    This takes one already built, so a different stream (another seed, or
    the same traffic with a daily cycle, ADR 29) can be held to the
    champion's own training window. The days before `judge_from` are served
    and scored, so the windows are full, and not judged.

    Args:
        records: Transactions with their labels, in event-time order.
        reference: The fixed reference.
        model: The champion.
        judge_from: The first moment judged; give it a day's midnight.
        rate: Share of transactions kept, by hash draw.
        on_day: Called with each day's report as it is judged.

    Returns:
        One report per judged day, in order.
    """
    reports: list[DailyReport] = []
    current = _Window()
    current_day: dt.date | None = None
    for record, features, score in serve_and_score(records, model=model):
        event = record.event
        if event.event_time < judge_from or draw(event.event_id) >= rate:
            continue
        day = event.event_time.date()
        if current_day is not None and day != current_day:
            reports.append(daily_report(current_day, reference, current.arrays()))
            if on_day is not None:
                on_day(reports[-1])
            current = _Window()
        current_day = day
        current.add(features, score)
    if current_day is not None and current:
        reports.append(daily_report(current_day, reference, current.arrays()))
        if on_day is not None:
            on_day(reports[-1])
    return reports


def first_trigger(reports: list[DailyReport]) -> tuple[RetrainRequest | None, int]:
    """Walk the days in order and stop at the first retraining request.

    The trigger is asked once per day, as the scheduled job would ask it,
    rather than being given every day at once: a request that would have
    opened on day three is not the same finding as one that opens on day
    twenty.

    Args:
        reports: The daily reports, in day order.

    Returns:
        The first request and how many days had been judged when it opened,
        or None and the number of days judged.
    """
    for index in range(1, len(reports) + 1):
        request = evaluate_trigger(reports[:index], request_open=False)
        if request is not None:
            return request, index
    return None, len(reports)


def _rendered(report: DailyReport) -> dict[str, Any]:
    """One day's report as JSON can carry it.

    `dataclasses.asdict` leaves a `date` and a `StrEnum` in place, which
    `json.dumps` refuses, and it refuses it after the run rather than before.

    Args:
        report: The day's report.

    Returns:
        The report with every value a JSON type.
    """
    return {
        "day": report.day.isoformat(),
        "results": [
            {
                "name": result.name,
                "values": result.values,
                "psi": round(result.psi, 4),
                "ks_statistic": round(result.ks_statistic, 4),
                "ks_p_value": float(result.ks_p_value),
                "status": str(result.status),
            }
            for result in report.results
        ],
    }


def run_report(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    cutoff: dt.datetime,
    schedule: RegimeSchedule,
    start_time: dt.datetime,
    rate: float = SAMPLE_RATE,
    on_day: Callable[[DailyReport], None] | None = None,
) -> dict[str, Any]:
    """Run the monitors over a stream and report what fired, and when.

    Args:
        records: Transactions with their labels, in event-time order.
        model: The champion.
        cutoff: The champion's training cutoff.
        schedule: The regime schedule the stream was generated with, for the
            days its regimes begin. Reported, never used to decide.
        start_time: The stream's first moment, which the regime offsets are
            measured from.
        rate: Share of transactions kept, by hash draw.
        on_day: Called with each day's report as it is judged.

    Returns:
        The report.
    """
    reference, reports = drift_reports(
        records, model=model, cutoff=cutoff, rate=rate, on_day=on_day
    )
    request, days_to_trigger = first_trigger(reports)
    return {
        "track": "synthetic, offline replay",
        "model": model.version,
        "cutoff": cutoff.isoformat(),
        "sample_rate": rate,
        "reference": {
            "quantities": list(reference.names),
            "values": int(reference.window[SCORE].size),
        },
        "days_judged": len(reports),
        "regime_days": [
            {
                "name": regime.name,
                "starts_on": (start_time + dt.timedelta(days=regime.starts_after_days))
                .date()
                .isoformat(),
            }
            for regime in schedule.regimes
        ],
        "days": [
            {
                "day": report.day.isoformat(),
                "drifted": sorted(report.drifted()),
                "moderate": sorted(r.name for r in report.results if r.status is Status.MODERATE),
                "insufficient": sorted(
                    r.name for r in report.results if r.status is Status.INSUFFICIENT
                ),
                "values": max(r.values for r in report.results),
            }
            for report in reports
        ],
        "first_request": None
        if request is None
        else {
            "opened_on": request.opened_on.isoformat(),
            "after_days_judged": days_to_trigger,
            "quantities": list(request.quantities),
            "markdown": request.to_markdown(),
            "evidence": [_rendered(report) for report in request.evidence],
        },
    }
