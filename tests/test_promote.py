"""The promotion gate refuses when it should, and only then.

`PLAN.md` section 4 names "the promotion function refuses when the interval
crosses the margin" as a test that matters. It is here, with the refusals the
gate needs besides: too few frauds, labels that had not yet arrived, latency
over budget, and the challenger that games a cost measure by declining
everything. The data is synthetic and small, so the suite stays fast.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pytest

from verdict.events.schema import Action, EntryMode, MerchantCategory, TransactionEvent
from verdict.models.promote import (
    Margins,
    ShadowRow,
    average_precision,
    decision_cost,
    evaluate,
)
from verdict.scoring.rules import DecisionRules

AS_OF = dt.datetime(2027, 5, 1, tzinfo=dt.UTC)
LABELLED = AS_OF - dt.timedelta(days=1)
RESAMPLES = 200


def a_window(
    n: int = 4_000, fraud_rate: float = 0.05, seed: int = 7
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Labels, amounts, and an informative champion score."""
    rng = np.random.default_rng(seed)
    labels = rng.random(n) < fraud_rate
    amounts = np.round(rng.lognormal(8.0, 1.0, n)).clip(100, 5_000_000)
    champion = np.where(labels, rng.beta(5, 2, n), rng.beta(2, 5, n))
    return labels, amounts, champion


def rows_from(
    labels: np.ndarray,
    amounts: np.ndarray,
    champion: np.ndarray,
    challenger: np.ndarray,
    label_time: dt.datetime = LABELLED,
) -> list[ShadowRow]:
    return [
        ShadowRow(
            event_id=f"evt-{i}",
            is_fraud=bool(labels[i]),
            label_time=label_time,
            amount_cents=int(amounts[i]),
            champion_score=float(champion[i]),
            challenger_score=float(challenger[i]),
        )
        for i in range(len(labels))
    ]


# --- the measures ---------------------------------------------------------


def test_average_precision_matches_a_hand_worked_case() -> None:
    """Frauds at ranks 1 and 3: precision 1 at recall 0.5, then 2/3 at recall 1."""
    labels = np.array([True, False, True, False])
    scores = np.array([0.9, 0.8, 0.7, 0.1])
    assert average_precision(labels, scores) == pytest.approx(0.5 * 1 + 0.5 * 2 / 3)


def test_a_perfect_ranking_scores_one_and_no_ranking_scores_the_base_rate() -> None:
    labels = np.array([True, True, False, False, False])
    assert average_precision(labels, np.array([0.9, 0.8, 0.3, 0.2, 0.1])) == pytest.approx(1.0)
    assert average_precision(labels, np.full(5, 0.5)) == pytest.approx(0.4)


def test_the_order_of_tied_rows_cannot_change_the_answer() -> None:
    labels = np.array([True, False, False, True, False, False])
    scores = np.array([0.7, 0.7, 0.7, 0.2, 0.2, 0.1])
    reordered = [5, 2, 1, 4, 0, 3]
    assert average_precision(labels, scores) == pytest.approx(
        average_precision(labels[reordered], scores[reordered])
    )


def test_no_frauds_means_no_precision_to_average() -> None:
    assert average_precision(np.zeros(3, dtype=bool), np.array([0.1, 0.2, 0.3])) == 0.0


def test_the_vectorised_cost_takes_the_same_action_as_the_rules_on_every_row() -> None:
    """`decision_cost` restates `DecisionRules.decide`; this holds them together."""
    rules = DecisionRules()
    rng = np.random.default_rng(3)
    scores = rng.random(500)
    amounts = rng.choice(np.array([100.0, 2_500.0, 499_999.0, 500_000.0, 900_000.0]), 500)
    labels = rng.random(500) < 0.3
    expected = 0.0
    for score, amount, fraud in zip(scores, amounts, labels, strict=True):
        event = TransactionEvent(
            event_id="e",
            event_time=AS_OF,
            card_id="c",
            device_id=None,
            merchant_id=None,
            amount_cents=int(amount),
            merchant_category=MerchantCategory.MISC_NET,
            entry_mode=EntryMode.ECOMMERCE,
        )
        action, _ = rules.decide(float(score), event)
        if action is Action.REVIEW:
            expected += 500
        elif action is Action.APPROVE and fraud:
            expected += amount
        elif action is Action.DECLINE and not fraud:
            expected += 0.10 * amount
    assert decision_cost(labels, amounts, scores, rules, 500.0, 0.10) == pytest.approx(expected)


# --- the verdicts ---------------------------------------------------------


def test_an_identical_challenger_is_eligible() -> None:
    labels, amounts, champion = a_window()
    verdict = evaluate(
        rows_from(labels, amounts, champion, champion),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
    )
    assert verdict.eligible, verdict.reasons
    assert verdict.pr_auc is not None
    assert verdict.pr_auc.low == verdict.pr_auc.high == 0.0


def test_a_clearly_better_challenger_is_eligible() -> None:
    labels, amounts, champion = a_window()
    better = np.where(labels, np.maximum(champion, 0.97), champion * 0.5)
    verdict = evaluate(
        rows_from(labels, amounts, champion, better),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
    )
    assert verdict.eligible, verdict.reasons
    assert verdict.pr_auc is not None
    assert verdict.pr_auc.low > 0


def test_a_worse_challenger_is_refused_because_its_interval_crosses_the_margin() -> None:
    """The test the plan names."""
    labels, amounts, champion = a_window()
    noise = np.random.default_rng(11).random(len(labels))
    worse = 0.5 * champion + 0.5 * noise
    verdict = evaluate(
        rows_from(labels, amounts, champion, worse),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
    )
    assert not verdict.eligible
    assert any("PR-AUC could be" in reason for reason in verdict.reasons)
    assert verdict.pr_auc is not None
    assert verdict.pr_auc.low < -Margins().pr_auc


def test_a_point_estimate_inside_the_margin_is_not_enough() -> None:
    """Non-inferiority is about the worst plausible difference, not the average one.

    On a window of about sixty frauds, a challenger 0.03 below the champion sits
    inside a 0.05 margin by its point estimate, and its interval reaches past
    the margin. It must be refused.
    """
    labels, amounts, champion = a_window(n=1_200, fraud_rate=0.05, seed=5)
    jitter = np.random.default_rng(13).normal(0, 0.04, len(labels))
    close = np.clip(champion + jitter, 0, 1)
    verdict = evaluate(
        rows_from(labels, amounts, champion, close),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
        margins=Margins(pr_auc=0.05),
    )
    assert verdict.pr_auc is not None
    assert -0.05 < verdict.pr_auc.difference < 0
    assert verdict.pr_auc.low < -0.05
    assert not verdict.eligible
    assert any("PR-AUC could be" in reason for reason in verdict.reasons)


def test_declining_everything_does_not_buy_a_low_cost() -> None:
    """At a zero price for false declines, this challenger would cost nothing."""
    labels, amounts, champion = a_window()
    everything = np.full(len(labels), 0.99)
    verdict = evaluate(
        rows_from(labels, amounts, champion, everything),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
    )
    assert not verdict.eligible
    assert any("decision cost could be" in reason for reason in verdict.reasons)


def test_too_few_frauds_gets_no_verdict_and_no_interval() -> None:
    labels, amounts, champion = a_window(n=600, fraud_rate=0.03)
    verdict = evaluate(
        rows_from(labels, amounts, champion, champion),
        as_of=AS_OF,
        challenger_p99_ms=0.5,
        resamples=RESAMPLES,
    )
    assert not verdict.eligible
    assert verdict.pr_auc is None
    assert verdict.resamples == 0
    assert any("fewer than the 50" in reason for reason in verdict.reasons)


def test_labels_that_had_not_arrived_are_not_evidence() -> None:
    """The leakage rule, applied to the gate.

    The late-labelled rows are exactly the ones on which the challenger is
    perfect. Counting them would make it look better than anything known at
    the moment of evaluation could show.
    """
    labels, amounts, champion = a_window()
    known = rows_from(labels, amounts, champion, champion)
    late_labels = np.ones(300, dtype=bool)
    late = rows_from(
        late_labels,
        np.full(300, 10_000.0),
        np.full(300, 0.1),
        np.full(300, 0.99),
        label_time=AS_OF + dt.timedelta(seconds=1),
    )
    verdict = evaluate(known + late, as_of=AS_OF, challenger_p99_ms=0.5, resamples=RESAMPLES)
    assert verdict.excluded_unlabelled == 300
    assert verdict.rows == len(known)
    assert verdict.frauds == int(labels.sum())
    assert verdict.pr_auc is not None
    assert verdict.pr_auc.difference == 0.0


def test_a_challenger_over_the_latency_budget_is_refused() -> None:
    labels, amounts, champion = a_window()
    verdict = evaluate(
        rows_from(labels, amounts, champion, champion),
        as_of=AS_OF,
        challenger_p99_ms=7.5,
        resamples=RESAMPLES,
    )
    assert not verdict.eligible
    assert verdict.reasons == ("challenger model hop p99 7.500 ms exceeds the 3.0 ms budget",)


def test_the_evidence_is_reproducible_and_readable() -> None:
    labels, amounts, champion = a_window()
    rows = rows_from(labels, amounts, champion, np.clip(champion * 0.9 + 0.05, 0, 1))
    first = evaluate(rows, as_of=AS_OF, challenger_p99_ms=0.5, resamples=RESAMPLES, seed=1)
    again = evaluate(rows, as_of=AS_OF, challenger_p99_ms=0.5, resamples=RESAMPLES, seed=1)
    assert first == again
    table = first.to_markdown(champion="xgb-1", challenger="ftt-1")
    assert "| PR-AUC |" in table
    assert "merging this pull request is the approval" in table
    assert "seed 1" in table
    typographic = {
        chr(code) for code in (0x2012, 0x2013, 0x2014, 0x2015, 0x2018, 0x2019, 0x201C, 0x201D)
    }
    assert typographic.isdisjoint(table)
