"""Running the monitors over a stream: the reference, the days, and the firing.

The statistics are tested in `test_drift.py` and the trigger's rule in the
same place. These hold what `drift.run` adds: everything before the cutoff
builds the reference and nothing after it does, each day after is judged on
its own, and the trigger is asked day by day so that the day a request would
have opened is the day reported.

The serving path is stubbed here on purpose. It is tested where it lives,
and running thousands of events through the engine would make this a slow
test of something it is not testing.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import cast

import numpy as np
import numpy.typing as npt
import pytest

from verdict.drift import run as drift_run
from verdict.drift.monitors import SCORE, Status
from verdict.drift.run import drift_reports, first_trigger, run_report
from verdict.events.generator.regimes import DEV_SCHEDULE
from verdict.models.dataset import Labelled

_Row = tuple["_Record", dict[str, float], float]

START = dt.datetime(2027, 1, 1, tzinfo=dt.UTC)
CUTOFF = START + dt.timedelta(days=3)
PER_DAY = 600


@dataclass(frozen=True)
class _Event:
    event_id: str
    event_time: dt.datetime


@dataclass(frozen=True)
class _Record:
    event: _Event


class _NoModel:
    """Never called: the serving path is stubbed."""

    @property
    def version(self) -> str:
        return "stub-0"

    def score_matrix(self, rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        raise AssertionError("the stub should have replaced serving")


def _stream(shift_from: int, days: int = 6) -> list[_Row]:
    """Days of rows, with the amount shifted from `shift_from` onwards.

    Args:
        shift_from: The first day index whose amounts are shifted.
        days: How many days to build.

    Returns:
        One tuple per row, as `serve_and_score` yields them.
    """
    rng = np.random.default_rng(7)
    rows: list[_Row] = []
    for day in range(days):
        shifted = day >= shift_from
        for index in range(PER_DAY):
            at = START + dt.timedelta(days=day, seconds=index * 10)
            amount = float(rng.normal(120.0 if shifted else 20.0, 3.0))
            features = {"amount_cents": amount, "card_txn_count_1h": float(rng.integers(0, 5))}
            score = float(rng.uniform(0.6, 0.9) if shifted else rng.uniform(0.0, 0.2))
            rows.append((_Record(_Event(f"evt-{day}-{index}", at)), features, score))
    return rows


def _as_records(rows: list[_Row]) -> Iterable[Labelled]:
    """Hand the stub its rows under the signature the real function has.

    The stub yields them straight back, so what travels here is the stub's
    contract rather than the engine's; the cast says so in one place instead
    of at every call.

    Args:
        rows: The rows the stub will yield.

    Returns:
        The same rows.
    """
    return cast("Iterable[Labelled]", rows)


@pytest.fixture
def stubbed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace serving with rows that carry a known shift."""

    def fake(
        records: Iterable[_Row], **_: object
    ) -> Iterator[tuple[_Record, Mapping[str, float], float]]:
        yield from records

    monkeypatch.setattr(drift_run, "serve_and_score", fake)


def test_the_reference_is_built_before_the_cutoff_and_every_later_day_is_judged(
    stubbed: None,
) -> None:
    reference, reports = drift_reports(
        _as_records(_stream(shift_from=99)), model=_NoModel(), cutoff=CUTOFF, rate=1.0
    )
    assert reference.window[SCORE].size == 3 * PER_DAY
    assert [report.day for report in reports] == [
        (CUTOFF + dt.timedelta(days=offset)).date() for offset in range(3)
    ]
    assert all(not report.drifted() for report in reports)


def test_a_shift_after_the_cutoff_is_reported_as_drift(stubbed: None) -> None:
    _, reports = drift_reports(
        _as_records(_stream(shift_from=4)), model=_NoModel(), cutoff=CUTOFF, rate=1.0
    )
    assert not reports[0].drifted()
    assert "amount_cents" in reports[1].drifted()
    assert reports[1].result(SCORE).status is Status.DRIFTED


def test_the_trigger_reports_the_day_it_would_have_opened_not_the_last(stubbed: None) -> None:
    """A request that opens on the second drifted day is not a finding about the sixth."""
    _, reports = drift_reports(
        _as_records(_stream(shift_from=4)), model=_NoModel(), cutoff=CUTOFF, rate=1.0
    )
    request, judged = first_trigger(reports)
    assert request is not None
    assert judged == 3
    assert request.opened_on == reports[2].day
    assert "amount_cents" in request.quantities


def test_a_stream_that_ends_before_the_cutoff_is_refused(stubbed: None) -> None:
    with pytest.raises(ValueError, match="before the cutoff"):
        drift_reports(
            _as_records(_stream(shift_from=99, days=2)), model=_NoModel(), cutoff=CUTOFF, rate=1.0
        )


def test_a_rate_that_keeps_nothing_is_refused(stubbed: None) -> None:
    with pytest.raises(ValueError, match="rate must be"):
        drift_reports(
            _as_records(_stream(shift_from=99)), model=_NoModel(), cutoff=CUTOFF, rate=0.0
        )


def test_the_report_is_json_a_file_can_hold(stubbed: None) -> None:
    """The run costs hours, so it must not fail at the last line.

    The first version put `dataclasses.asdict` of a day's report into the
    output, which leaves a `date` and a `StrEnum` in place. `json.dumps`
    refuses both, and refused them after a fifty-day run had finished.
    """
    report = run_report(
        _as_records(_stream(shift_from=4)),
        model=_NoModel(),
        cutoff=CUTOFF,
        schedule=DEV_SCHEDULE,
        start_time=START,
        rate=1.0,
    )
    written = json.dumps(report)
    assert report["first_request"] is not None
    assert "amount_cents" in written
