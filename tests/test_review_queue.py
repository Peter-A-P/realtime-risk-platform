"""The review queue: ranked by money, simulated honestly, compared with an interval.

The plan's own example opens the file: a high-probability twelve-dollar fraud
and a medium-probability forty-thousand-dollar one, ranked the two ways. The
rest pins the simulation (capacity, expiry, what counts as caught), the rule
that no policy reads the label, and the comparison's interval.
"""

from __future__ import annotations

import dataclasses
import datetime as dt

import numpy as np
import pytest

from verdict.review_queue.ranking import (
    Costs,
    QueueItem,
    by_expected_loss,
    by_score,
    compare_policies,
    expected_loss,
    simulate_day,
)

MIDNIGHT = dt.datetime(2027, 4, 5, tzinfo=dt.UTC)
COSTS = Costs(review_cost_cents=500, recovery_rate=0.30)


def an_item(
    index: int,
    *,
    score: float,
    amount: int,
    fraud: bool = True,
    minutes: float = 0.0,
    day: int = 0,
) -> QueueItem:
    return QueueItem(
        event_id=f"evt-{day}-{index}",
        arrived_at=MIDNIGHT + dt.timedelta(days=day, minutes=minutes),
        score=score,
        amount_cents=amount,
        is_fraud=fraud,
    )


def test_the_plans_example_ranks_the_way_the_plan_says() -> None:
    """A $12 fraud at 0.95 against a $40,000 one at 0.30."""
    small = an_item(1, score=0.95, amount=1_200)
    large = an_item(2, score=0.30, amount=4_000_000)
    assert by_score(small, COSTS) > by_score(large, COSTS)
    assert by_expected_loss(large, COSTS) > by_expected_loss(small, COSTS)


def test_expected_loss_is_the_plans_formula() -> None:
    assert expected_loss(0.5, 100_000, COSTS) == pytest.approx(0.5 * 100_000 * 0.7 - 500)


def test_prices_never_reorder_the_queue() -> None:
    """At fixed capacity, expected-loss ranking is probability times amount."""
    rng = np.random.default_rng(1)
    items = [
        an_item(i, score=float(rng.random()), amount=int(rng.integers(100, 10_000_000)))
        for i in range(300)
    ]

    def order(costs: Costs) -> list[str]:
        return [i.event_id for i in sorted(items, key=lambda i: -by_expected_loss(i, costs))]

    reference = [i.event_id for i in sorted(items, key=lambda i: -(i.score * i.amount_cents))]
    for costs in (Costs(0, 0.0), Costs(500, 0.3), Costs(50_000, 0.9)):
        assert order(costs) == reference


def test_no_policy_can_see_the_label() -> None:
    item = an_item(1, score=0.4, amount=250_000, fraud=True)
    twin = dataclasses.replace(item, is_fraud=False)
    for policy in (by_score, by_expected_loss):
        assert policy(item, COSTS) == policy(twin, COSTS)


def test_capacity_is_respected_and_the_best_items_go_first() -> None:
    items = [an_item(i, score=0.1 * i, amount=10_000, minutes=i) for i in range(10)]
    result = simulate_day(
        items,
        by_score,
        costs=COSTS,
        analysts=1,
        reviews_per_analyst_hour=3,
        max_wait=dt.timedelta(hours=2),
        hours=1,
    )
    assert result.reviewed == 3
    assert result.expired == 7
    assert result.caught_cents == pytest.approx(3 * 10_000 * 0.7)


def test_a_wait_limit_shorter_than_the_step_is_refused() -> None:
    with pytest.raises(ValueError, match="at least an hour"):
        simulate_day(
            [an_item(1, score=0.5, amount=100)],
            by_score,
            costs=COSTS,
            analysts=1,
            reviews_per_analyst_hour=1,
            max_wait=dt.timedelta(minutes=30),
        )


def test_a_reviewed_legitimate_transaction_catches_nothing() -> None:
    items = [an_item(1, score=0.9, amount=50_000, fraud=False)]
    result = simulate_day(
        items,
        by_score,
        costs=COSTS,
        analysts=1,
        reviews_per_analyst_hour=5,
        max_wait=dt.timedelta(hours=4),
    )
    assert result.reviewed == 1
    assert result.caught_cents == 0.0


def test_an_item_left_waiting_too_long_is_worth_nothing() -> None:
    """Capacity for one an hour, and a higher-priority arrival every hour after the first.

    The 0.9 item is reviewed at the end of hour 0. The 0.8 item is outranked by
    each new arrival, and at the end of hour 4 it has waited more than the four
    hours allowed, so it leaves unreviewed. Every other item is reviewed.
    """
    items = [
        an_item(1, score=0.9, amount=10_000, minutes=0),
        an_item(2, score=0.8, amount=10_000, minutes=1),
    ]
    busy = [an_item(10 + h, score=0.99, amount=10_000, minutes=60 * h + 2) for h in range(1, 6)]
    result = simulate_day(
        items + busy,
        by_score,
        costs=COSTS,
        analysts=1,
        reviews_per_analyst_hour=1,
        max_wait=dt.timedelta(hours=4),
        hours=6,
    )
    assert result.expired == 1
    assert result.reviewed == 6


def test_a_team_with_no_capacity_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one review"):
        simulate_day(
            [],
            by_score,
            costs=COSTS,
            analysts=0,
            reviews_per_analyst_hour=10,
            max_wait=dt.timedelta(hours=1),
        )


@pytest.mark.parametrize(("review", "recovery"), [(-1, 0.3), (500, 1.5), (500, -0.1)])
def test_impossible_prices_are_refused(review: int, recovery: float) -> None:
    with pytest.raises(ValueError, match="review_cost_cents|recovery_rate"):
        Costs(review_cost_cents=review, recovery_rate=recovery)


def synthetic_days(count: int, *, vary_amounts: bool, seed: int = 2) -> list[list[QueueItem]]:
    """Days where fraud is more likely at high scores and amounts vary widely."""
    rng = np.random.default_rng(seed)
    days: list[list[QueueItem]] = []
    for day in range(count):
        n = 400
        scores = rng.random(n)
        frauds = rng.random(n) < scores * 0.3
        amounts = (
            np.round(rng.lognormal(9.5, 1.6, n)).clip(100, 20_000_000)
            if vary_amounts
            else np.full(n, 20_000.0)
        )
        minutes = np.sort(rng.random(n) * 24 * 60)
        days.append(
            [
                an_item(
                    i,
                    score=float(scores[i]),
                    amount=int(amounts[i]),
                    fraud=bool(frauds[i]),
                    minutes=float(minutes[i]),
                    day=day,
                )
                for i in range(n)
            ]
        )
    return days


def test_expected_loss_catches_more_money_when_amounts_differ() -> None:
    comparison = compare_policies(
        synthetic_days(20, vary_amounts=True),
        costs=COSTS,
        analysts=2,
        reviews_per_analyst_hour=4,
        resamples=500,
    )
    assert comparison.loss_per_hour > comparison.score_per_hour
    assert comparison.low > 0


def test_with_equal_amounts_the_two_rankings_are_the_same_policy() -> None:
    comparison = compare_policies(
        synthetic_days(10, vary_amounts=False),
        costs=COSTS,
        analysts=2,
        reviews_per_analyst_hour=4,
        resamples=200,
    )
    assert comparison.difference == 0.0
    assert comparison.low == comparison.high == 0.0


def test_one_day_is_not_an_interval() -> None:
    with pytest.raises(ValueError, match="two days"):
        compare_policies(
            synthetic_days(1, vary_amounts=True),
            costs=COSTS,
            analysts=1,
            reviews_per_analyst_hour=1,
        )
