"""A retraining request closes only when it has been answered.

The trigger refuses to open a second request while one is open, so closing a
request too early is not a small mistake: the stream stays drifted and the
platform says nothing more about it. ADR 24 found the case that makes this
likely, a first candidate fitted before the shifted days' labels arrive,
which loses. These hold the rule for when a request may close.
"""

from __future__ import annotations

import datetime as dt

from verdict.drift.monitors import DailyReport, QuantityResult, Status
from verdict.drift.trigger import Resolution, RetrainRequest, resolve

OPENED = dt.date(2027, 1, 16)
QUANTITY = "merchant_txn_count_1h"


def a_day(day: dt.date, status: Status, other: Status = Status.STABLE) -> DailyReport:
    """One day's report, with the request's quantity in a given state.

    Args:
        day: The day.
        status: The request's quantity's status that day.
        other: Another quantity's status, which the request did not name.

    Returns:
        The report.
    """
    values = 400 if status is Status.INSUFFICIENT else 52_000
    return DailyReport(
        day=day,
        results=(
            QuantityResult(QUANTITY, values, 0.4, 0.2, 1e-9, status),
            QuantityResult("amount_cents", 52_000, 0.3, 0.2, 1e-9, other),
        ),
    )


def a_request() -> RetrainRequest:
    """The request, opened on the second of two drifted days.

    Returns:
        The request.
    """
    evidence = (
        a_day(OPENED - dt.timedelta(days=1), Status.DRIFTED),
        a_day(OPENED, Status.DRIFTED),
    )
    return RetrainRequest(opened_on=OPENED, quantities=(QUANTITY,), evidence=evidence)


def after(days: int) -> dt.date:
    """A day after the request opened.

    Args:
        days: How many days after.

    Returns:
        The date.
    """
    return OPENED + dt.timedelta(days=days)


def test_a_candidate_that_beats_the_incumbent_answers_the_request() -> None:
    assert resolve(a_request(), candidate_beat_incumbent=True) is Resolution.ANSWERED


def test_a_losing_candidate_leaves_the_request_open() -> None:
    """The case ADR 24 measured: built before the labels, it lost, and the drift is still there."""
    since = [a_day(after(1), Status.DRIFTED), a_day(after(2), Status.DRIFTED)]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.STILL_OPEN
    )


def test_two_consecutive_clean_days_end_the_drift() -> None:
    since = [a_day(after(1), Status.STABLE), a_day(after(2), Status.MODERATE)]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.DRIFT_ENDED
    )


def test_one_clean_day_is_not_the_end_of_a_drift() -> None:
    since = [a_day(after(1), Status.DRIFTED), a_day(after(2), Status.STABLE)]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.STILL_OPEN
    )


def test_a_day_too_small_to_judge_is_not_a_clean_day() -> None:
    """Nobody saw that day, so it cannot be evidence the drift stopped."""
    since = [a_day(after(1), Status.STABLE), a_day(after(2), Status.INSUFFICIENT)]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.STILL_OPEN
    )


def test_clean_days_with_a_gap_between_them_are_not_a_run() -> None:
    since = [a_day(after(1), Status.STABLE), a_day(after(3), Status.STABLE)]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.STILL_OPEN
    )


def test_drift_in_a_quantity_the_request_did_not_name_does_not_keep_it_open() -> None:
    """A request answers the shift it was opened for; a new one opens a new request."""
    since = [
        a_day(after(1), Status.STABLE, other=Status.DRIFTED),
        a_day(after(2), Status.STABLE, other=Status.DRIFTED),
    ]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.DRIFT_ENDED
    )


def test_days_before_the_request_opened_are_not_evidence_of_recovery() -> None:
    since = [
        a_day(OPENED - dt.timedelta(days=3), Status.STABLE),
        a_day(OPENED - dt.timedelta(days=2), Status.STABLE),
    ]
    assert resolve(a_request(), candidate_beat_incumbent=False, since=since) is (
        Resolution.STILL_OPEN
    )
