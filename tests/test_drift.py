"""Drift monitors that notice a planted shift, and a trigger that waits for it to persist.

The shift planted here is the one the development schedule is built around: the
amount distribution moves while the fraud rate holds still. A monitor that
watched only the fraud rate, or only the score, would miss it. The data is
synthetic numpy, not the generator, so the suite stays fast; the thresholds
are the published conventions in `monitors.py`, not values fitted to anything
here.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pytest

from verdict.drift.monitors import (
    MIN_VALUES,
    SCORE,
    DailyReport,
    QuantityResult,
    Reference,
    Status,
    columns,
    daily_report,
)
from verdict.drift.stats import (
    EPSILON,
    fit_bins,
    kolmogorov_q,
    ks_two_sample,
    psi,
    shares,
)
from verdict.drift.trigger import evaluate_trigger
from verdict.store.features import NO_EVENTS

MONDAY = dt.date(2027, 4, 5)


# --- the statistics -------------------------------------------------------


def test_identical_distributions_have_no_drift() -> None:
    values = np.random.default_rng(1).normal(0, 1, 5_000)
    bins = fit_bins(values, sentinel=False)
    assert psi(values, values, bins) == pytest.approx(0.0)
    result = ks_two_sample(values, values)
    assert result.statistic == 0.0
    assert result.p_value == pytest.approx(1.0)


def test_psi_matches_a_hand_computation() -> None:
    """Two bins split at 5; shares move from 50/50 to 80/20."""
    reference = np.array([1.0] * 50 + [9.0] * 50)
    current = np.array([1.0] * 80 + [9.0] * 20)
    bins = fit_bins(reference, bins=2, sentinel=False)
    expected = (0.8 - 0.5) * np.log(0.8 / 0.5) + (0.2 - 0.5) * np.log(0.2 / 0.5)
    assert psi(reference, current, bins) == pytest.approx(expected)


def test_an_empty_bin_is_floored_not_infinite() -> None:
    reference = np.array([1.0] * 50 + [9.0] * 50)
    current = np.array([1.0] * 100)
    bins = fit_bins(reference, bins=2, sentinel=False)
    value = psi(reference, current, bins)
    assert np.isfinite(value)
    expected = (1.0 - 0.5) * np.log(1.0 / 0.5) + (EPSILON - 0.5) * np.log(EPSILON / 0.5)
    assert value == pytest.approx(expected)


def test_more_entities_with_no_history_is_drift_even_when_the_rest_is_unchanged() -> None:
    """The sentinel has its own bin, so this shift cannot hide in the lowest quantile."""
    rng = np.random.default_rng(2)
    history = rng.poisson(3, 4_000).astype(float)
    reference = np.concatenate([history, np.full(1_000, NO_EVENTS)])
    current = np.concatenate([history, np.full(6_000, NO_EVENTS)])
    bins = fit_bins(reference)
    assert shares(current, bins)[-1] == pytest.approx(0.6)
    assert psi(reference, current, bins) > 0.25


def test_a_count_feature_with_few_values_gets_fewer_bins_not_empty_ones() -> None:
    reference = np.array([0.0, 1.0, 1.0, 2.0] * 250)
    bins = fit_bins(reference, bins=10)
    assert len(bins.edges) == len(set(bins.edges)) <= 3
    assert shares(reference, bins).sum() == pytest.approx(1.0)


def test_the_ks_statistic_matches_a_hand_computation() -> None:
    """Empirical CDFs of {1,2,3,4} and {3,4,5,6} differ by at most 0.5, at 2."""
    result = ks_two_sample(np.array([1.0, 2.0, 3.0, 4.0]), np.array([3.0, 4.0, 5.0, 6.0]))
    assert result.statistic == pytest.approx(0.5)


def test_the_kolmogorov_tail_matches_published_values() -> None:
    """Q(1.36) is about 0.049 and Q(1.63) about 0.010: the familiar 5 and 1 percent points."""
    assert kolmogorov_q(1.36) == pytest.approx(0.049, abs=0.002)
    assert kolmogorov_q(1.63) == pytest.approx(0.010, abs=0.001)
    assert kolmogorov_q(0.0) == 1.0


def test_ks_p_values_are_calibrated_under_no_drift() -> None:
    """Two samples from one distribution reject at 5 percent about 5 percent of the time."""
    rng = np.random.default_rng(3)
    trials = 400
    rejections = sum(
        ks_two_sample(rng.normal(0, 1, 400), rng.normal(0, 1, 400)).p_value < 0.05
        for _ in range(trials)
    )
    assert 0.02 < rejections / trials < 0.09


def test_ks_refuses_an_empty_sample() -> None:
    with pytest.raises(ValueError, match="at least one value"):
        ks_two_sample(np.array([]), np.array([1.0]))


# --- the monitors ---------------------------------------------------------


def a_day(
    rng: np.random.Generator,
    *,
    n: int = 5_000,
    amount_scale: float = 1.0,
    fraud_rate: float = 0.03,
) -> dict[str, np.ndarray]:
    """One day of served values: an amount, a velocity count, a fraud-rate proxy, a score."""
    amounts = rng.lognormal(8.0, 1.0, n) * amount_scale
    counts = rng.poisson(2.0, n).astype(float)
    counts[rng.random(n) < 0.2] = NO_EVENTS
    frauds = (rng.random(n) < fraud_rate).astype(float)
    score = np.clip(rng.beta(2, 30, n) + frauds * 0.3, 0, 1)
    return {
        "card_amount_mean_24h": amounts,
        "card_txn_count_1h": counts,
        "fraud_flag_proxy": frauds,
        SCORE: score,
    }


def reference_and_days(
    shifts: list[float], *, n: int = 5_000
) -> tuple[Reference, list[DailyReport]]:
    rng = np.random.default_rng(7)
    reference = Reference(a_day(rng, n=20_000))
    reports = [
        daily_report(MONDAY + dt.timedelta(days=i), reference, a_day(rng, n=n, amount_scale=scale))
        for i, scale in enumerate(shifts)
    ]
    return reference, reports


def test_an_unshifted_day_is_stable_on_every_quantity() -> None:
    _, reports = reference_and_days([1.0])
    assert reports[0].drifted() == frozenset()
    assert all(r.status in {Status.STABLE, Status.MODERATE} for r in reports[0].results)


def test_an_amount_shift_is_caught_while_the_fraud_rate_holds_still() -> None:
    """The development schedule's hard case: the money moves, the fraud rate does not."""
    _, reports = reference_and_days([2.5])
    report = reports[0]
    assert "card_amount_mean_24h" in report.drifted()
    assert report.result("fraud_flag_proxy").status is Status.STABLE
    assert report.result("card_txn_count_1h").status is Status.STABLE


def test_a_day_too_small_to_judge_is_insufficient_not_stable() -> None:
    _, reports = reference_and_days([2.5], n=MIN_VALUES - 1)
    assert all(r.status is Status.INSUFFICIENT for r in reports[0].results)
    assert reports[0].drifted() == frozenset()


def test_a_quantity_missing_from_a_day_is_insufficient() -> None:
    rng = np.random.default_rng(9)
    reference = Reference(a_day(rng))
    day = a_day(rng)
    del day[SCORE]
    report = daily_report(MONDAY, reference, day)
    assert report.result(SCORE).status is Status.INSUFFICIENT


def test_columns_gathers_served_rows_by_name() -> None:
    gathered = columns([{"a": 1.0, SCORE: 0.2}, {"a": 3.0, SCORE: 0.4}])
    assert gathered["a"].tolist() == [1.0, 3.0]
    assert gathered[SCORE].tolist() == [0.2, 0.4]


# --- the trigger ----------------------------------------------------------


def test_one_day_of_drift_opens_nothing() -> None:
    _, reports = reference_and_days([1.0, 2.5])
    assert evaluate_trigger(reports, request_open=False) is None


def test_two_consecutive_days_of_the_same_drift_open_a_request() -> None:
    _, reports = reference_and_days([1.0, 2.5, 2.5])
    request = evaluate_trigger(reports, request_open=False)
    assert request is not None
    assert "card_amount_mean_24h" in request.quantities
    assert "fraud_flag_proxy" not in request.quantities
    assert request.opened_on == MONDAY + dt.timedelta(days=2)
    table = request.to_markdown()
    assert "| card_amount_mean_24h |" in table
    assert "merged pull request" in table


def test_drift_that_recovers_in_between_opens_nothing() -> None:
    _, reports = reference_and_days([2.5, 1.0, 2.5])
    assert evaluate_trigger(reports, request_open=False) is None


def test_a_missing_day_breaks_the_run() -> None:
    _, reports = reference_and_days([2.5, 2.5])
    later = DailyReport(day=reports[1].day + dt.timedelta(days=1), results=reports[1].results)
    assert evaluate_trigger([reports[0], later], request_open=False) is None


def test_a_day_too_small_to_judge_breaks_the_run() -> None:
    rng = np.random.default_rng(7)
    reference = Reference(a_day(rng, n=20_000))
    drifted = daily_report(MONDAY, reference, a_day(rng, amount_scale=2.5))
    thin = daily_report(
        MONDAY + dt.timedelta(days=1), reference, a_day(rng, n=100, amount_scale=2.5)
    )
    assert evaluate_trigger([drifted, thin], request_open=False) is None


def test_different_quantities_on_consecutive_days_are_not_a_run() -> None:
    _, reports = reference_and_days([2.5, 2.5])
    only_amount = frozenset({"card_amount_mean_24h"})
    first = DailyReport(
        day=reports[0].day,
        results=tuple(
            r if r.name in only_amount else _as_status(r, Status.STABLE) for r in reports[0].results
        ),
    )
    second = DailyReport(
        day=reports[1].day,
        results=tuple(
            _as_status(r, Status.DRIFTED)
            if r.name == "card_txn_count_1h"
            else _as_status(r, Status.STABLE)
            for r in reports[1].results
        ),
    )
    assert evaluate_trigger([first, second], request_open=False) is None


def test_no_second_request_while_one_is_open() -> None:
    _, reports = reference_and_days([2.5, 2.5])
    assert evaluate_trigger(reports, request_open=True) is None


def test_two_reports_for_one_day_are_refused() -> None:
    _, reports = reference_and_days([2.5])
    with pytest.raises(ValueError, match="same day"):
        evaluate_trigger([reports[0], reports[0]], request_open=False)


def _as_status(result: QuantityResult, status: Status) -> QuantityResult:
    import dataclasses

    return dataclasses.replace(result, status=status)
