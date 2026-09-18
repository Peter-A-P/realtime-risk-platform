"""Turning a score into an action.

Three actions and three rules, evaluated in order, the first that matches
deciding. The thresholds are placeholders for week 4, chosen only so each
action occurs; they are not tuned and no claim rests on them. Week 6 replaces
the review threshold with the expected-loss ranking of `PLAN.md` section 2.7
and ADR 13, which is where a threshold stops being a number and becomes a cost.

Rules are data rather than code so that a decision can name the rule that
made it, and so the rollback flag in week 5 can switch a rule set as easily
as a model.
"""

from __future__ import annotations

from dataclasses import dataclass

from verdict.events.schema import Action, TransactionEvent


@dataclass(frozen=True, slots=True)
class DecisionRules:
    """The rule set.

    Attributes:
        decline_at: Scores at or above this are declined.
        review_at: Scores at or above this, below `decline_at`, go to review.
        review_amount_cents: Amounts at or above this go to review whatever
            the score, unless declined.
    """

    decline_at: float = 0.90
    review_at: float = 0.50
    review_amount_cents: int = 500_000

    def __post_init__(self) -> None:
        """Check the thresholds are ordered.

        Raises:
            ValueError: If review is not below decline, or either is outside
                [0, 1], or the amount is not positive.
        """
        if not 0.0 <= self.review_at < self.decline_at <= 1.0:
            msg = f"need 0 <= review_at < decline_at <= 1, got {self.review_at}, {self.decline_at}"
            raise ValueError(msg)
        if self.review_amount_cents <= 0:
            msg = "review_amount_cents must be positive"
            raise ValueError(msg)

    def decide(self, score: float, event: TransactionEvent) -> tuple[Action, str]:
        """Choose an action.

        Args:
            score: The model's score.
            event: The event.

        Returns:
            The action and the name of the rule that chose it.
        """
        if score >= self.decline_at:
            return Action.DECLINE, "score-decline"
        if score >= self.review_at:
            return Action.REVIEW, "score-review"
        if event.amount_cents >= self.review_amount_cents:
            return Action.REVIEW, "large-amount-review"
        return Action.APPROVE, "default-approve"
