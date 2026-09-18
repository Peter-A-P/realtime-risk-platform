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
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import Final

from verdict.drift.monitors import DailyReport, reports_in_order

CONSECUTIVE_DAYS: Final = 2


@dataclass(frozen=True, slots=True)
class RetrainRequest:
    """A request for a retrained candidate, with its evidence.

    Attributes:
        opened_on: The day the run of drift completed.
        quantities: The quantities that drifted on every day of the run.
        evidence: The reports for the days of the run.
    """

    opened_on: dt.date
    quantities: tuple[str, ...]
    evidence: tuple[DailyReport, ...]

    def to_markdown(self) -> str:
        """The request's evidence, for the retraining pull request.

        Returns:
            Markdown, plain punctuation.
        """
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
