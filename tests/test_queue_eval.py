"""The bridge from a replayed stream to a review queue, and what it must not do.

The ranking itself is tested in `test_review_queue.py`. These hold the
evaluation that feeds it: only what the rules send to review reaches the
queue, the arrivals are grouped by the day they arrived, and a run too short
to carry an interval is refused rather than reported.
"""

from __future__ import annotations

import datetime as dt
import itertools

import numpy as np
import numpy.typing as npt
import pytest

from verdict.events.generator.driver import GeneratedRecord
from verdict.events.schema import Action
from verdict.models.champion import SYNTHETIC, synthetic_records
from verdict.review_queue.evaluate import by_day, evaluate_queue, review_items
from verdict.review_queue.ranking import QueueItem
from verdict.scoring.rules import DecisionRules


class FixedScores:
    """A model that returns scores from a list, in order."""

    def __init__(self, scores: list[float]) -> None:
        """Hold the scores.

        Args:
            scores: One per row it will be asked about, in order.
        """
        self._scores = scores
        self._at = 0

    @property
    def version(self) -> str:
        """The model's identifier.

        Returns:
            A fixed name.
        """
        return "fixed-scores"

    def score_matrix(self, rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """The next scores.

        Args:
            rows: One row per event.

        Returns:
            One score per row.
        """
        taken = self._scores[self._at : self._at + len(rows)]
        self._at += len(rows)
        return np.array(taken, dtype=np.float64)


def a_short_stream(count: int) -> list[GeneratedRecord]:
    """The first `count` generated records.

    Args:
        count: How many.

    Returns:
        The records.
    """
    return list(itertools.islice(synthetic_records(1.0, SYNTHETIC), count))


def test_only_the_transactions_the_rules_send_to_review_reach_the_queue() -> None:
    """A queue holding approvals or declines would measure the wrong thing."""
    records = a_short_stream(30)
    rules = DecisionRules()
    # Below review, inside review, and above decline, in turn.
    scores = [0.1, 0.6, 0.95] * 10
    items, served = review_items(
        records,
        model=FixedScores(scores),
        rules=rules,
        batch=len(records),
    )
    assert served == 30
    queued = {item.event_id for item in items}
    expected = {
        record.event.event_id
        for record, score in zip(records, scores, strict=True)
        if rules.decide(score, record.event)[0] is Action.REVIEW
    }
    assert queued == expected
    assert all(rules.review_at <= item.score < rules.decline_at for item in items)


def test_a_large_amount_reaches_the_queue_on_its_own() -> None:
    """The amount rule is part of what the queue holds, not only the score."""
    records = a_short_stream(40)
    rules = DecisionRules(review_amount_cents=1)
    items, _ = review_items(
        records,
        model=FixedScores([0.0] * len(records)),
        rules=rules,
        batch=len(records),
    )
    assert len(items) == len(records)


def test_arrivals_are_grouped_by_the_day_they_arrived() -> None:
    start = dt.datetime(2027, 1, 1, 23, 30, tzinfo=dt.UTC)
    items = [
        QueueItem(f"evt-{index}", start + dt.timedelta(hours=index), 0.6, 1_000, False)
        for index in range(4)
    ]
    days = by_day(items)
    assert [len(day) for day in days] == [1, 3]
    assert days[0][0].event_id == "evt-0"


def test_a_stream_too_short_for_an_interval_is_refused() -> None:
    """One day gives a number with no interval, which this project does not publish."""
    records = a_short_stream(200)
    with pytest.raises(ValueError, match="two days"):
        evaluate_queue(records, model=FixedScores([0.6] * len(records)))
