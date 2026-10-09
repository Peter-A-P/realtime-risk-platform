"""When drift is enough to ask for a retrained candidate.

`PLAN.md` section 2.6: two consecutive days above threshold open a retraining
job, the job trains a candidate and runs it in shadow, and it opens a pull
request with the evidence. Merging the pull request is the approval. Nothing
retrains itself into production.

This module decides the first step and writes its evidence. The job that
trains and the pull request that carries the candidate's shadow verdict are
week 6's, once a model exists to retrain.

Three rules, each stated because the obvious alternative is wrong:

- **The same quantity, two consecutive calendar days.** Drift in the amount
  on Monday and in a device count on Tuesday is two single days of noise, not
  a sustained shift. And "consecutive" means calendar days: a day with no
  report between two drifted days breaks the run, because nobody saw it.
- **A day too small to judge is not a clean day and not a drifted one.** It
  breaks a run, because the second day of evidence is missing, but it does not
  count as recovery either.
- **No new request while one is open.** A shift that persists for a week is
  one request, not five; the open request's candidate is already answering it.
- **A request closes only when it has been answered.** That is, when a
  candidate beats the incumbent, or when the drift has stopped for as long as
  it took to start: two consecutive calendar days on which every quantity it
  named was judged and none drifted. A candidate that lost does not close it.
  The first candidate a request can build is fitted before the shifted days'
  labels arrive (ADR 24), so it usually loses; closing the request on that
  would leave the stream drifted and the previous rule would then keep the
  platform silent about it for good.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from typing import Final

from verdict.drift.monitors import DailyReport, Status, reports_in_order

CONSECUTIVE_DAYS: Final = 2


@dataclass(frozen=True, slots=True)
class RetrainRequest:
    """A request for a retrained candidate, with its evidence.

    Most requests are opened by the monitors, on a run of drift. One may also
    be opened by a person, with a reason and no drift (ADR 32): on 2026-10-08
    the champion, trained on the scaled synthetic population, was found to
    barely separate fraud on the full live one from the window's first day,
    which no drift monitor judging against the live window's own first days
    could see. A request opened by hand goes through the same candidate, pull
    request, shadow window and gate, and closes only when a candidate beats
    the incumbent.

    Attributes:
        opened_on: The day the run of drift completed, or the day a person
            opened it.
        quantities: The quantities that drifted on every day of the run;
            empty for a request opened by hand.
        evidence: The reports for the days of the run; empty for a request
            opened by hand.
        reason: Why a person opened it; None for a request the monitors
            opened.
    """

    opened_on: dt.date
    quantities: tuple[str, ...]
    evidence: tuple[DailyReport, ...]
    reason: str | None = None

    @property
    def by_hand(self) -> bool:
        """Whether a person opened it, rather than a run of drift."""
        return self.reason is not None

    def to_markdown(self) -> str:
        """The request's evidence, for the retraining pull request.

        Returns:
            Markdown, plain punctuation.
        """
        if self.reason is not None:
            return (
                f"### Requested by hand on {self.opened_on.isoformat()}, not by drift\n\n"
                f"{self.reason}\n\n"
                "A retrained candidate is requested. It runs in shadow and is promoted only "
                "through the promotion gate and a merged pull request.\n"
            )
        lines = [
            f"### Drift on {len(self.evidence)} consecutive days, to {self.opened_on.isoformat()}",
            "",
            "A retrained candidate is requested. It runs in shadow and is promoted only "
            "through the promotion gate and a merged pull request.",
            "",
            "| Quantity | Day | Values | PSI | KS statistic | KS p-value | Status |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
        for name in self.quantities:
            for report in self.evidence:
                result = report.result(name)
                lines.append(
                    f"| {name} | {report.day.isoformat()} | {result.values:,} | "
                    f"{result.psi:.3f} | {result.ks_statistic:.3f} | {result.ks_p_value:.2g} | "
                    f"{result.status} |"
                )
        return "\n".join(lines) + "\n"


def evaluate_trigger(
    reports: Sequence[DailyReport], *, request_open: bool
) -> RetrainRequest | None:
    """Decide whether the latest reports complete a run of drift.

    Args:
        reports: Daily reports, in any order, the latest last by day.
        request_open: Whether a retraining request is already open.

    Returns:
        A request, or None.
    """
    if request_open or len(reports) < CONSECUTIVE_DAYS:
        return None
    run = reports_in_order(reports)[-CONSECUTIVE_DAYS:]
    for earlier, later in pairwise(run):
        if later.day - earlier.day != dt.timedelta(days=1):
            return None
    # A quantity too small to judge on either day is never in `drifted()`, so
    # the intersection already treats an insufficient day as breaking the run.
    persistent = frozenset.intersection(*(report.drifted() for report in run))
    if not persistent:
        return None
    return RetrainRequest(
        opened_on=run[-1].day,
        quantities=tuple(sorted(persistent)),
        evidence=tuple(run),
    )


class Resolution(StrEnum):
    """What became of a request."""

    ANSWERED = "answered"
    """A candidate beat the incumbent."""

    DRIFT_ENDED = "drift-ended"
    """Two consecutive days on which every quantity it named was judged stable."""

    STILL_OPEN = "still-open"
    """Neither. It is asked again when more labelled history has arrived."""


def resolve(
    request: RetrainRequest,
    *,
    candidate_beat_incumbent: bool,
    since: Sequence[DailyReport] = (),
) -> Resolution:
    """Decide whether a request may close.

    Args:
        request: The open request.
        candidate_beat_incumbent: Whether its latest candidate beat the
            incumbent, by an interval that excludes zero.
        since: Daily reports after the request opened.

    Returns:
        The resolution.
    """
    if candidate_beat_incumbent:
        return Resolution.ANSWERED
    if request.by_hand:
        # No drift opened it, so no end of drift can close it.
        return Resolution.STILL_OPEN
    later = [report for report in since if report.day > request.opened_on]
    if len(later) < CONSECUTIVE_DAYS:
        return Resolution.STILL_OPEN
    run = reports_in_order(later)[-CONSECUTIVE_DAYS:]
    for earlier, next_day in pairwise(run):
        if next_day.day - earlier.day != dt.timedelta(days=1):
            return Resolution.STILL_OPEN
    for report in run:
        for name in request.quantities:
            # Too small to judge is not a clean day (the second rule above).
            if report.result(name).status in (Status.DRIFTED, Status.INSUFFICIENT):
                return Resolution.STILL_OPEN
    return Resolution.DRIFT_ENDED
