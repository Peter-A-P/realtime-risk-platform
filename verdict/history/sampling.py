"""Which decided transactions the platform keeps, and what each one stands for.

At 1,000 events a second the live window is about five billion transactions,
and no disk this project can afford holds them (ADR 14, ADR 18). So history is
a sample, and a sample is only honest if every kept row carries the weight
that makes totals come out right: a row kept with probability `p` stands for
`1 / p` rows. Every estimate the platform publishes from history (PR-AUC and
decision cost at promotion, training) reads the weight.

Three strata, decided once the label has arrived:

- **acted**: the champion reviewed or declined. Kept, all of them. These are
  the rows the review queue works on and the rows a false decline costs, so
  the queue evaluation (ADR 13) reads them exactly, with no weight at all.
- **fraud**: approved, and fraud. Kept at `fraud` (a tenth by default).
- **legit**: approved, and not fraud. Kept at `legit` (a hundredth).

Choosing by outcome is case-control sampling, and it is unbiased for any
total or ratio of totals as long as the weights are used, which the tests in
`tests/test_history.py` check against the full data on a replay.

**The draw is a hash of the event id, not a random number.** The same event is
kept or dropped on every run, on every machine, after every restart: a replay
reproduces the sample exactly, a duplicate delivery cannot be kept twice by
being drawn twice, and the choice cannot depend on anything but the id and
the stratum. The salt fixes the hash to this purpose, so an event's draw here
is unrelated to any other hash of its id.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from verdict.events.schema import Action

SALT: Final = b"verdict/history/sample/v1"


class Stratum(StrEnum):
    """Which rule decided whether a row is kept."""

    ACTED = "acted"
    FRAUD = "fraud"
    LEGIT = "legit"


@dataclass(frozen=True, slots=True)
class SampleRates:
    """The probability a row in each stratum is kept.

    Attributes:
        acted: Reviewed or declined by the champion. 1.0: the queue needs all
            of them.
        fraud: Approved frauds.
        legit: Approved legitimate transactions.
    """

    acted: float = 1.0
    fraud: float = 0.10
    legit: float = 0.01

    def __post_init__(self) -> None:
        """Check every rate is a probability that keeps something.

        Raises:
            ValueError: If a rate is not in (0, 1]. A rate of zero would keep
                a stratum's rows with an infinite weight, which is no sample.
        """
        for name in ("acted", "fraud", "legit"):
            rate = getattr(self, name)
            if not 0.0 < rate <= 1.0:
                msg = f"{name} rate must be in (0, 1], got {rate}"
                raise ValueError(msg)

    def rate(self, stratum: Stratum) -> float:
        """The keep probability for a stratum.

        Args:
            stratum: The stratum.

        Returns:
            Its rate.
        """
        rate: float = getattr(self, stratum.value)
        return rate


def stratum_of(action: Action, is_fraud: bool) -> Stratum:
    """The stratum a labelled decision falls in.

    Args:
        action: What the champion decided.
        is_fraud: The label.

    Returns:
        The stratum.
    """
    if action is not Action.APPROVE:
        return Stratum.ACTED
    return Stratum.FRAUD if is_fraud else Stratum.LEGIT


def draw(event_id: str) -> float:
    """A number in [0, 1) fixed by the event id.

    Args:
        event_id: The event.

    Returns:
        The draw. Uniform over events, for any ids that are distinct.
    """
    digest = hashlib.sha256(SALT + event_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def could_be_kept(event_id: str, action: Action, rates: SampleRates) -> bool:
    """Whether a decision might be kept, before its label is known.

    Acted rows are kept whatever the label; an approved row is kept only if its
    draw falls under the rate of whichever stratum its label puts it in, so
    under the larger of the two it might be. What is not kept is never read
    by anything that reads history, so work done on a row only for history
    (scoring it in shadow, ADR 11's addendum of 2026-09-27) can be spared on
    the rest.

    Args:
        event_id: The event.
        action: What the champion decided.
        rates: The keep probabilities.

    Returns:
        False only for a row that will certainly be dropped.
    """
    if action is not Action.APPROVE:
        return True
    return draw(event_id) < max(rates.fraud, rates.legit)


def keep(event_id: str, stratum: Stratum, rates: SampleRates) -> float | None:
    """Whether to keep a row, and its weight if so.

    Args:
        event_id: The event.
        stratum: Its stratum.
        rates: The keep probabilities.

    Returns:
        The weight, `1 / rate`, if the row is kept; None if it is dropped.
    """
    rate = rates.rate(stratum)
    return 1.0 / rate if draw(event_id) < rate else None
