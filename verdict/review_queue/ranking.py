"""Ranking the review queue by expected loss, and measuring what that buys.

`PLAN.md` section 2.7: expected loss is the probability of fraud times the
amount times one minus the expected recovery, less the cost of the review. At a
fixed analyst capacity, the evaluation replays labelled transactions and
reports money caught per analyst-hour under expected-loss ranking against
score ranking. A high-probability twelve-dollar fraud is not worth an
analyst's time; a medium-probability forty-thousand-dollar one is.

## The simulation

One day at a time, because an analyst team works a day and a queue that
carried every transaction for six months would be measuring a backlog, not a
policy.

- Transactions that the decision rules send to review arrive at their event
  times.
- The simulation steps an hour at a time. At the end of each hour, the team
  reviews up to its capacity: the highest-priority items waiting, including
  that hour's arrivals. Priority is the policy under test.
- An item waiting longer than `max_wait` leaves the queue unreviewed. Card
  fraud is acted on within hours or the money has gone; an item reviewed a day
  late catches nothing, so it is not counted as caught. Waiting is measured
  at the hour's end, so `max_wait` below an hour would expire everything
  before any review, and is refused.
- A reviewed fraud catches its amount less what would have been recovered
  anyway. A reviewed legitimate transaction catches nothing. Every review
  costs the same analyst time whatever it finds.

Money caught per analyst-hour is the day's caught money over the day's
analyst-hours. Both policies see the same arrivals, the same capacity and the
same labels, so the comparison is paired by day, and its interval is a
bootstrap over days.

## What the prices do and do not change

At a fixed capacity, ranking by expected loss orders the queue exactly as
probability times amount does. The recovery rate scales every item's saving by
the same factor, and the review cost subtracts the same number from each, so
neither moves one item past another. The prices matter elsewhere: in how much
money a policy is reported to catch, and, in week 6's rules, in whether a
transaction is worth sending to review at all, which is where a negative
expected loss means no. A test holds the ranking to that equivalence so
nobody tunes the prices believing they reorder the queue.

## What the probability is

The score, treated as a probability. That is only honest if the score is
calibrated, and the stand-in model's is not; week 5's champion is calibrated
on held-out data before this evaluation is published, and the result table
says which model's scores it used. Ranking by score is unaffected by
calibration; ranking by expected loss is not, which is part of what the
comparison shows.
"""

from __future__ import annotations

import datetime as dt
import heapq
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np

HOUR: Final = dt.timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class Costs:
    """The prices the queue is judged at. Stated, never implied.

    Attributes:
        review_cost_cents: Analyst time for one review.
        recovery_rate: Share of a fraud's amount recovered without review, by
            chargeback. Catching a fraud saves the rest.
    """

    review_cost_cents: int = 500
    recovery_rate: float = 0.30

    def __post_init__(self) -> None:
        """Check the prices make sense.

        Raises:
            ValueError: If the review cost is negative or the recovery rate is
                outside [0, 1].
        """
        if self.review_cost_cents < 0:
            msg = "review_cost_cents cannot be negative"
            raise ValueError(msg)
        if not 0.0 <= self.recovery_rate <= 1.0:
            msg = f"recovery_rate must be in [0, 1], got {self.recovery_rate}"
            raise ValueError(msg)


def expected_loss(probability: float, amount_cents: int, costs: Costs) -> float:
    """What reviewing one transaction is expected to save, in cents.

    Args:
        probability: The probability the transaction is fraud.
        amount_cents: Its amount.
        costs: The prices.

    Returns:
        Expected saving less the cost of the review. Negative when a review
        is expected to cost more than it saves.
    """
    return probability * amount_cents * (1.0 - costs.recovery_rate) - costs.review_cost_cents


@dataclass(frozen=True, slots=True)
class QueueItem:
    """A transaction waiting for review.

    Attributes:
        event_id: The transaction.
        arrived_at: When it entered the queue.
        score: The model's score, read as a probability.
        amount_cents: The amount.
        is_fraud: Its label. Read only when an item is reviewed, to count what
            the review caught; never by a policy.
    """

    event_id: str
    arrived_at: dt.datetime
    score: float
    amount_cents: int
    is_fraud: bool


Policy = Callable[[QueueItem, Costs], float]
"""A priority: higher is reviewed first. Must not read `is_fraud`."""


def by_score(item: QueueItem, costs: Costs) -> float:
    """Rank by how likely the model thinks fraud is.

    Args:
        item: The item.
        costs: Unused; score ranking ignores money.

    Returns:
        The score.
    """
    del costs
    return item.score


def by_expected_loss(item: QueueItem, costs: Costs) -> float:
    """Rank by what a review is expected to save.

    Args:
        item: The item.
        costs: The prices.

    Returns:
        The expected loss.
    """
    return expected_loss(item.score, item.amount_cents, costs)


@dataclass(frozen=True, slots=True)
class DayResult:
    """One simulated day under one policy.

    Attributes:
        reviewed: Items reviewed.
        caught_cents: Money saved by reviews that found fraud.
        expired: Items that waited too long and left unreviewed.
        analyst_hours: Hours of analyst time the day provided.
    """

    reviewed: int
    caught_cents: float
    expired: int
    analyst_hours: float

    @property
    def caught_per_analyst_hour(self) -> float:
        """Money caught per analyst-hour, in cents.

        Returns:
            The rate.
        """
        return self.caught_cents / self.analyst_hours if self.analyst_hours else 0.0


def simulate_day(
    items: Sequence[QueueItem],
    policy: Policy,
    *,
    costs: Costs,
    analysts: int,
    reviews_per_analyst_hour: int,
    max_wait: dt.timedelta,
    hours: int = 24,
) -> DayResult:
    """Run one day of the queue under one policy.

    Args:
        items: The day's arrivals, in any order.
        policy: How to prioritise.
        costs: The prices.
        analysts: Analysts on shift.
        reviews_per_analyst_hour: Reviews one analyst completes in an hour.
        max_wait: How long an item may wait before it is worth nothing.
        hours: Length of the shift, from the first arrival's hour.

    Returns:
        The day's result.

    Raises:
        ValueError: If the team has no capacity, or `max_wait` is shorter
            than the simulation's one-hour step.
    """
    capacity = analysts * reviews_per_analyst_hour
    if capacity <= 0:
        msg = "a team needs at least one review an hour"
        raise ValueError(msg)
    if max_wait < HOUR:
        msg = f"max_wait must be at least an hour, the simulation's step; got {max_wait}"
        raise ValueError(msg)
    if not items:
        return DayResult(reviewed=0, caught_cents=0.0, expired=0, analyst_hours=analysts * hours)

    ordered = sorted(items, key=lambda item: item.arrived_at)
    start = ordered[0].arrived_at.replace(minute=0, second=0, microsecond=0)
    waiting: list[tuple[float, int, QueueItem]] = []
    next_arrival = 0
    reviewed = expired = 0
    caught = 0.0

    for hour in range(hours):
        end = start + (hour + 1) * HOUR
        while next_arrival < len(ordered) and ordered[next_arrival].arrived_at < end:
            item = ordered[next_arrival]
            heapq.heappush(waiting, (-policy(item, costs), next_arrival, item))
            next_arrival += 1

        done = 0
        survivors: list[tuple[float, int, QueueItem]] = []
        while waiting and done < capacity:
            _, _, item = heapq.heappop(waiting)
            if end - item.arrived_at > max_wait:
                expired += 1
                continue
            done += 1
            reviewed += 1
            if item.is_fraud:
                caught += item.amount_cents * (1.0 - costs.recovery_rate)
        for entry in waiting:
            if end - entry[2].arrived_at > max_wait:
                expired += 1
            else:
                survivors.append(entry)
        heapq.heapify(survivors)
        waiting = survivors

    expired += len(waiting) + (len(ordered) - next_arrival)
    return DayResult(
        reviewed=reviewed,
        caught_cents=caught,
        expired=expired,
        analyst_hours=float(analysts * hours),
    )


@dataclass(frozen=True, slots=True)
class Comparison:
    """Expected-loss ranking against score ranking, over many days.

    Attributes:
        days: Days simulated.
        score_per_hour: Mean money caught per analyst-hour under score
            ranking, in cents.
        loss_per_hour: The same under expected-loss ranking.
        difference: Expected-loss minus score, mean over days.
        low: Lower bound of the 95 percent bootstrap interval over days.
        high: Upper bound.
        resamples: Bootstrap resamples.
        seed: Generator seed.
    """

    days: int
    score_per_hour: float
    loss_per_hour: float
    difference: float
    low: float
    high: float
    resamples: int
    seed: int


def compare_policies(
    days: Sequence[Sequence[QueueItem]],
    *,
    costs: Costs,
    analysts: int,
    reviews_per_analyst_hour: int,
    max_wait: dt.timedelta = dt.timedelta(hours=4),
    resamples: int = 2_000,
    seed: int = 20270405,
) -> Comparison:
    """Simulate both policies on the same days and compare them.

    Args:
        days: Each day's queue arrivals.
        costs: The prices.
        analysts: Analysts on shift.
        reviews_per_analyst_hour: Reviews per analyst per hour.
        max_wait: How long an item stays worth reviewing.
        resamples: Bootstrap resamples over days.
        seed: Generator seed.

    Returns:
        The comparison.

    Raises:
        ValueError: If fewer than two days are given.
    """
    if len(days) < 2:
        msg = "an interval over days needs at least two days"
        raise ValueError(msg)

    def run(policy: Policy) -> np.ndarray:
        return np.array(
            [
                simulate_day(
                    day,
                    policy,
                    costs=costs,
                    analysts=analysts,
                    reviews_per_analyst_hour=reviews_per_analyst_hour,
                    max_wait=max_wait,
                ).caught_per_analyst_hour
                for day in days
            ]
        )

    score = run(by_score)
    loss = run(by_expected_loss)
    paired = loss - score
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, len(days), size=(resamples, len(days)))
    means = paired[draws].mean(axis=1)
    return Comparison(
        days=len(days),
        score_per_hour=float(score.mean()),
        loss_per_hour=float(loss.mean()),
        difference=float(paired.mean()),
        low=float(np.percentile(means, 2.5)),
        high=float(np.percentile(means, 97.5)),
        resamples=resamples,
        seed=seed,
    )
