"""Whether a challenger has earned promotion, and the evidence either way.

`PLAN.md` section 2.5: the challenger scores every event in shadow, and it may
be promoted only on the labelled shadow window, with non-inferior PR-AUC and
expected loss shown with their intervals, latency within budget, and a human
approval. This module is everything in that sentence except the human. It
returns a verdict and the table a pull request carries; it never touches the
champion pointer. ADR 11 records the choices.

## What is compared

Every row is one transaction the champion decided and the challenger scored
in shadow, joined to its label. Two quantities per model:

- **PR-AUC**, as average precision: at each distinct score threshold, the
  precision reached, weighted by the recall gained. Computed here rather than
  imported, because this project otherwise has no need of scikit-learn, and a
  test holds the implementation to hand-worked cases.
- **Decision cost**, in cents: the fraud each model's decisions would have let
  through, plus the cost of the reviews they would have ordered, plus the
  business lost to legitimate transactions they would have declined, under
  the same rules. That is the expected-loss comparison the plan asks for, made
  concrete as the loss the decisions would actually have realised on labelled
  rows. Lower is better. Week 6 replaces the flat review threshold with the
  expected-loss queue; the comparison stays the same shape.

## How the intervals are made

A paired bootstrap: each resample draws row indices once and computes both
models' metrics on the same rows, so the interval is on the difference, and a
row that is hard for both models does not widen it. Percentile intervals at
95 percent, from a seeded generator so the evidence is reproducible.

## When it refuses

- **Labels that had not arrived.** A row counts only if its label time is at
  or before the moment of evaluation. Otherwise promotion would be judged on
  outcomes nobody had yet, which is the leakage test's rule applied to the
  gate.
- **Too little evidence.** Fewer labelled frauds than the minimum, and no
  interval is computed at all: a PR-AUC from a dozen frauds is noise with a
  confidence interval drawn on it.
- **An interval crossing its margin.** Non-inferiority means the worst
  plausible difference is still within the margin, not that the point
  estimate looks better.
- **Latency over budget.** The challenger's shadow p99 against the scorer's
  model hop budget.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

import numpy as np
import numpy.typing as npt

from verdict.scoring.rules import DecisionRules

DEFAULT_RESAMPLES: Final = 1_000
MIN_FRAUDS: Final = 50
"""Labelled frauds below which no verdict is attempted.

Fifty frauds put the standard error of a recall estimate near 0.07 at a recall
of 0.5, which is about as wide an interval as a promotion decision can use.
"""


@dataclass(frozen=True, slots=True)
class ShadowRow:
    """One labelled transaction, as both models saw it.

    Attributes:
        event_id: The transaction.
        is_fraud: Its outcome.
        label_time: When the outcome became known.
        amount_cents: The amount at stake.
        champion_score: The champion's score.
        challenger_score: The challenger's score, from the shadow topic.
    """

    event_id: str
    is_fraud: bool
    label_time: dt.datetime
    amount_cents: int
    champion_score: float
    challenger_score: float


@dataclass(frozen=True, slots=True)
class Margins:
    """How much worse a challenger may plausibly be and still be promoted.

    Attributes:
        pr_auc: The largest tolerated drop in PR-AUC, at the lower end of the
            interval.
        cost_fraction: The largest tolerated rise in decision cost, at the
            upper end of the interval, as a fraction of the champion's cost.
        review_cost_cents: What one review costs, for decision cost.
        false_decline_fraction: The share of a declined legitimate
            transaction's amount counted as lost. A placeholder like the
            review cost, and week 6's queue evaluation replaces both with
            stated assumptions; what matters here is that it is not zero,
            because at zero a model that declines everything costs nothing.
        model_p99_budget_ms: The model hop's budget at p99 (ADR 9).
    """

    pr_auc: float = 0.01
    cost_fraction: float = 0.02
    review_cost_cents: int = 500
    false_decline_fraction: float = 0.10
    model_p99_budget_ms: float = 3.0


@dataclass(frozen=True, slots=True)
class Difference:
    """Challenger minus champion, with its 95 percent paired bootstrap interval.

    Attributes:
        champion: The champion's value on all rows.
        challenger: The challenger's value on all rows.
        difference: Challenger minus champion.
        low: Lower bound of the interval on the difference.
        high: Upper bound.
    """

    champion: float
    challenger: float
    difference: float
    low: float
    high: float


@dataclass(frozen=True, slots=True)
class Verdict:
    """What the gate concluded.

    Attributes:
        eligible: Whether every condition for promotion holds. Eligible is not
            promoted: a person merges the pull request, or does not.
        reasons: Every condition that failed, in words. Empty when eligible.
        rows: Labelled rows used.
        frauds: Labelled frauds among them.
        excluded_unlabelled: Rows left out because their label had not arrived.
        pr_auc: The PR-AUC comparison, if evidence sufficed.
        cost_cents: The decision cost comparison, if evidence sufficed.
        challenger_p99_ms: The challenger's shadow latency at p99.
        resamples: Bootstrap resamples drawn.
        seed: The generator seed, so the table can be reproduced.
        margins: The margins it was judged against.
    """

    eligible: bool
    reasons: tuple[str, ...]
    rows: int
    frauds: int
    excluded_unlabelled: int
    pr_auc: Difference | None
    cost_cents: Difference | None
    challenger_p99_ms: float
    resamples: int
    seed: int
    margins: Margins = field(default_factory=Margins)

    def to_markdown(self, *, champion: str, challenger: str) -> str:
        """The evidence table for the promotion pull request.

        Args:
            champion: The champion's version.
            challenger: The challenger's version.

        Returns:
            Markdown, plain punctuation.
        """
        lines = [
            f"### Shadow evidence: {challenger} against {champion}",
            "",
            f"Verdict: **{'eligible for promotion' if self.eligible else 'refused'}**. "
            "Eligible is not promoted; merging this pull request is the approval.",
            "",
            f"Labelled rows: {self.rows:,}, of which frauds: {self.frauds:,}. "
            f"Rows excluded because their label had not arrived: {self.excluded_unlabelled:,}.",
            "",
        ]
        if self.pr_auc is not None and self.cost_cents is not None:
            lines += [
                "| Measure | Champion | Challenger | Difference (95% CI) | Margin |",
                "|---|---:|---:|---|---|",
                (
                    f"| PR-AUC | {self.pr_auc.champion:.4f} | {self.pr_auc.challenger:.4f} | "
                    f"{self.pr_auc.difference:+.4f} ({self.pr_auc.low:+.4f} to "
                    f"{self.pr_auc.high:+.4f}) | lower bound at least "
                    f"{-self.margins.pr_auc:+.4f} |"
                ),
                (
                    f"| Decision cost, $ | {self.cost_cents.champion / 100:,.2f} | "
                    f"{self.cost_cents.challenger / 100:,.2f} | "
                    f"{self.cost_cents.difference / 100:+,.2f} ("
                    f"{self.cost_cents.low / 100:+,.2f} to {self.cost_cents.high / 100:+,.2f}) | "
                    f"upper bound at most {self.margins.cost_fraction:.0%} of champion |"
                ),
                (
                    f"| Challenger model hop p99, ms | | {self.challenger_p99_ms:.3f} | | "
                    f"at most {self.margins.model_p99_budget_ms:.1f} |"
                ),
                "",
            ]
        lines.append(
            f"Paired bootstrap, {self.resamples:,} resamples, seed {self.seed}, "
            "percentile intervals."
        )
        if self.reasons:
            lines += ["", "Refused because:", *(f"- {reason}" for reason in self.reasons)]
        return "\n".join(lines) + "\n"


FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]


def average_precision(labels: BoolArray, scores: FloatArray) -> float:
    """PR-AUC as average precision, over distinct score thresholds.

    Tied scores are one threshold: the rows sharing a score are admitted
    together, so a model cannot gain from the order ties happen to be listed
    in.

    Args:
        labels: True for fraud.
        scores: Higher means more likely fraud.

    Returns:
        The average precision, or 0.0 when there are no frauds, since there is
        no recall to gain.
    """
    positives = int(labels.sum())
    if positives == 0:
        return 0.0
    order = np.argsort(-scores, kind="mergesort")
    sorted_scores = scores[order]
    sorted_labels = labels[order]
    true_positives = np.cumsum(sorted_labels)
    seen = np.arange(1, len(sorted_scores) + 1)
    last_of_tie = np.r_[np.diff(sorted_scores) != 0, True]
    tp = true_positives[last_of_tie]
    precision = tp / seen[last_of_tie]
    recall = tp / positives
    recall_gained = np.diff(np.r_[0.0, recall])
    return float(np.sum(recall_gained * precision))


def decision_cost(
    labels: BoolArray,
    amounts: FloatArray,
    scores: FloatArray,
    rules: DecisionRules,
    review_cost: float,
    false_decline_fraction: float,
) -> float:
    """The cost the decisions from these scores would have realised, in cents.

    An approved fraud costs its amount. A reviewed transaction costs one
    review, and a reviewed fraud is taken to be caught. A declined fraud costs
    nothing. A declined legitimate transaction costs `false_decline_fraction`
    of its amount, the business lost. The masks restate `DecisionRules.decide`
    in vectorised form, and a test holds the two to the same action on every
    row.

    Args:
        labels: True for fraud.
        amounts: Amounts in cents.
        scores: The model's scores.
        rules: The rules both models are judged under.
        review_cost: Cents per review.
        false_decline_fraction: Share of a declined legitimate amount lost.

    Returns:
        The total cost in cents.
    """
    decline = scores >= rules.decline_at
    review = ~decline & ((scores >= rules.review_at) | (amounts >= rules.review_amount_cents))
    approve = ~decline & ~review
    missed = np.sum(amounts[approve & labels])
    reviews = review_cost * np.count_nonzero(review)
    lost_business = false_decline_fraction * np.sum(amounts[decline & ~labels])
    return float(missed + reviews + lost_business)


def evaluate(
    rows: Sequence[ShadowRow],
    *,
    as_of: dt.datetime,
    challenger_p99_ms: float,
    margins: Margins | None = None,
    rules: DecisionRules | None = None,
    resamples: int = DEFAULT_RESAMPLES,
    seed: int = 20270405,
    min_frauds: int = MIN_FRAUDS,
) -> Verdict:
    """Judge a challenger against the champion on the labelled shadow window.

    Args:
        rows: Shadow rows with labels.
        as_of: The moment of evaluation. Rows labelled after it are excluded.
        challenger_p99_ms: The challenger's shadow p99, from the scorer.
        margins: The non-inferiority margins.
        rules: The rules both models are judged under.
        resamples: Bootstrap resamples.
        seed: Generator seed.
        min_frauds: The fewest labelled frauds a verdict may rest on.

    Returns:
        The verdict, with its reasons.
    """
    margins = margins or Margins()
    rules = rules or DecisionRules()
    usable = [row for row in rows if row.label_time <= as_of]
    excluded = len(rows) - len(usable)
    labels = np.array([row.is_fraud for row in usable], dtype=np.bool_)
    frauds = int(labels.sum())
    reasons: list[str] = []

    if challenger_p99_ms > margins.model_p99_budget_ms:
        reasons.append(
            f"challenger model hop p99 {challenger_p99_ms:.3f} ms exceeds the "
            f"{margins.model_p99_budget_ms:.1f} ms budget"
        )
    if frauds < min_frauds:
        reasons.append(
            f"{frauds} labelled frauds is fewer than the {min_frauds} a verdict may rest on"
        )
        return Verdict(
            eligible=False,
            reasons=tuple(reasons),
            rows=len(usable),
            frauds=frauds,
            excluded_unlabelled=excluded,
            pr_auc=None,
            cost_cents=None,
            challenger_p99_ms=challenger_p99_ms,
            resamples=0,
            seed=seed,
            margins=margins,
        )

    amounts = np.array([row.amount_cents for row in usable], dtype=np.float64)
    champion = np.array([row.champion_score for row in usable], dtype=np.float64)
    challenger = np.array([row.challenger_score for row in usable], dtype=np.float64)
    review_cost = float(margins.review_cost_cents)
    lost = margins.false_decline_fraction

    def both(index: npt.NDArray[np.intp]) -> tuple[float, float, float, float]:
        lab, amt = labels[index], amounts[index]
        return (
            average_precision(lab, champion[index]),
            average_precision(lab, challenger[index]),
            decision_cost(lab, amt, champion[index], rules, review_cost, lost),
            decision_cost(lab, amt, challenger[index], rules, review_cost, lost),
        )

    everything = np.arange(len(usable))
    ap_champion, ap_challenger, cost_champion, cost_challenger = both(everything)
    rng = np.random.default_rng(seed)
    ap_diffs = np.empty(resamples)
    cost_diffs = np.empty(resamples)
    for draw in range(resamples):
        index = rng.integers(0, len(usable), size=len(usable))
        a, b, c, d = both(index)
        ap_diffs[draw] = b - a
        cost_diffs[draw] = d - c

    pr_auc = Difference(
        champion=ap_champion,
        challenger=ap_challenger,
        difference=ap_challenger - ap_champion,
        low=float(np.percentile(ap_diffs, 2.5)),
        high=float(np.percentile(ap_diffs, 97.5)),
    )
    cost = Difference(
        champion=cost_champion,
        challenger=cost_challenger,
        difference=cost_challenger - cost_champion,
        low=float(np.percentile(cost_diffs, 2.5)),
        high=float(np.percentile(cost_diffs, 97.5)),
    )
    if pr_auc.low < -margins.pr_auc:
        reasons.append(
            f"PR-AUC could be {-pr_auc.low:.4f} lower, beyond the {margins.pr_auc:.4f} margin"
        )
    allowed = margins.cost_fraction * cost_champion
    if cost.high > allowed:
        reasons.append(
            f"decision cost could be ${cost.high / 100:,.2f} higher, beyond the "
            f"${allowed / 100:,.2f} margin ({margins.cost_fraction:.0%} of the champion's)"
        )
    return Verdict(
        eligible=not reasons,
        reasons=tuple(reasons),
        rows=len(usable),
        frauds=frauds,
        excluded_unlabelled=excluded,
        pr_auc=pr_auc,
        cost_cents=cost,
        challenger_p99_ms=challenger_p99_ms,
        resamples=resamples,
        seed=seed,
        margins=margins,
    )
