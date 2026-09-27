"""The drift monitors on the live window, day by day, from staged history (ADR 28).

The monitors, thresholds and trigger were tested on replays (ADR 12, ADR 23).
These hold what is new: which days are judged and when, that a day the scorer
spent on thin features is never read as drift, that the state survives a
restart without judging a day twice, and that the reference is the one the
champion was fitted on.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq

from verdict.drift import live
from verdict.drift.monitors import SCORE, Reference, Status
from verdict.drift.trigger import Resolution
from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.history.compact import HistoryPaths
from verdict.history.records import staged_schema
from verdict.history.sampling import draw
from verdict.store.features import FEATURE_SET

NAMES = [spec.name for spec in FEATURE_SET]
WINDOW = dt.datetime(2026, 10, 1, 15, 0, tzinfo=dt.UTC)
"""The window starts mid-afternoon, so its first whole day is the next."""
FIRST = dt.date(2026, 10, 2)


def a_reference(seed: int = 1) -> Reference:
    rng = np.random.default_rng(seed)
    return Reference({name: rng.normal(10.0, 2.0, 4_000) for name in [*NAMES, SCORE]})


def stage_day(
    paths: HistoryPaths, day: dt.date, *, shift: float = 0.0, rows: int = 2_400, seed: int = 0
) -> None:
    """Seal a day of staged decisions, 100 an hour, drawn like the reference or shifted."""
    rng = np.random.default_rng(seed + day.toordinal())
    schema = staged_schema()
    paths.staged.mkdir(parents=True, exist_ok=True)
    per_hour = rows // 24
    for hour in range(24):
        at = dt.datetime(day.year, day.month, day.day, hour, tzinfo=dt.UTC)
        columns: dict[str, Any] = {
            "event_id": [f"{day}-{hour}-{i}" for i in range(per_hour)],
            "card_id": ["card"] * per_hour,
            "event_time": [at] * per_hour,
            "amount_cents": [1_000] * per_hour,
            "decided_at": [at] * per_hour,
            "champion_version": ["champion-x"] * per_hour,
            "champion_score": rng.normal(10.0 + shift, 2.0, per_hour),
            "action": ["approve"] * per_hour,
            "rule": ["model"] * per_hour,
            "shadow_version": [None] * per_hour,
            "shadow_score": [None] * per_hour,
            "shadow_action": [None] * per_hour,
        }
        for name in NAMES:
            columns[name] = rng.normal(10.0 + shift, 2.0, per_hour)
        table = pa.table(columns).cast(schema)
        pq.write_table(table, paths.staged / f"{day.isoformat()}T{hour:02d}.parquet")


def after(day: dt.date, hours: float = 4) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC) + dt.timedelta(
        days=1, hours=hours
    )


def a_pass(
    paths: HistoryPaths,
    state: live.DriftState,
    reference: Reference,
    now: dt.datetime,
    starts: list[live.Start] | None = None,
) -> live.Pass:
    return live.watch_once(
        paths, state, reference, since=WINDOW, starts=starts or [], now=now, rate=1.0
    )


def test_a_day_is_judged_once_it_is_over_settled_and_sealed(tmp_path: Path) -> None:
    paths, state, reference = (
        HistoryPaths(tmp_path / "h"),
        live.DriftState(tmp_path / "d"),
        (a_reference()),
    )
    stage_day(paths, FIRST)
    assert a_pass(paths, state, reference, after(FIRST, hours=1)).judged == []
    (paths.staged / f"{FIRST.isoformat()}T23").mkdir()  # an hour still being written
    assert a_pass(paths, state, reference, after(FIRST)).judged == []
    (paths.staged / f"{FIRST.isoformat()}T23").rmdir()
    assert a_pass(paths, state, reference, after(FIRST)).judged == [(FIRST, False)]
    assert a_pass(paths, state, reference, after(FIRST, hours=9)).judged == []
    assert state.judged() == {FIRST}


def test_nothing_before_the_windows_first_whole_day_is_judged(tmp_path: Path) -> None:
    paths, state = HistoryPaths(tmp_path / "h"), live.DriftState(tmp_path / "d")
    stage_day(paths, FIRST - dt.timedelta(days=1))
    stage_day(paths, FIRST)
    judged = a_pass(paths, state, a_reference(), after(FIRST)).judged
    assert [day for day, _ in judged] == [FIRST]
    assert live.first_whole_day(dt.datetime(2026, 10, 1, tzinfo=dt.UTC)) == dt.date(2026, 10, 1)


def test_two_drifted_days_open_a_request_that_survives_a_restart(tmp_path: Path) -> None:
    paths, reference = HistoryPaths(tmp_path / "h"), a_reference()
    days = [FIRST + dt.timedelta(days=n) for n in range(3)]
    stage_day(paths, days[0])
    stage_day(paths, days[1], shift=3.0)
    stage_day(paths, days[2], shift=3.0)
    state = live.DriftState(tmp_path / "d")
    result = a_pass(paths, state, reference, after(days[2]))
    assert result.opened is not None
    assert result.opened.opened_on == days[2]
    assert set(result.opened.quantities) == {*NAMES, SCORE}
    restarted = live.DriftState(tmp_path / "d")
    request = restarted.open_request()
    assert request is not None
    assert request.opened_on == days[2]
    assert a_pass(paths, restarted, reference, after(days[2], hours=8)).opened is None


def test_a_request_closes_when_the_drift_has_ended_and_the_run_does_not_reopen_it(
    tmp_path: Path,
) -> None:
    paths, state, reference = (
        HistoryPaths(tmp_path / "h"),
        live.DriftState(tmp_path / "d"),
        a_reference(),
    )
    shifts = [3.0, 3.0, 0.0, 0.0]
    days = [FIRST + dt.timedelta(days=n) for n in range(len(shifts))]
    for day, shift in zip(days, shifts, strict=True):
        stage_day(paths, day, shift=shift)
    first = a_pass(paths, state, reference, after(days[1]))
    assert first.opened is not None
    second = a_pass(paths, state, reference, after(days[3]))
    assert second.closed is Resolution.DRIFT_ENDED
    assert state.open_request() is None
    assert state.last_closed_on() == days[3]
    assert a_pass(paths, state, reference, after(days[3], hours=9)).opened is None


def test_an_answered_request_closes(tmp_path: Path) -> None:
    paths, state, reference = (
        HistoryPaths(tmp_path / "h"),
        live.DriftState(tmp_path / "d"),
        a_reference(),
    )
    days = [FIRST, FIRST + dt.timedelta(days=1)]
    for day in days:
        stage_day(paths, day, shift=3.0)
    assert a_pass(paths, state, reference, after(days[1])).opened is not None
    closed = live.watch_once(
        paths, state, reference, since=WINDOW, starts=[], now=after(days[1], 6), answered=True
    )
    assert closed.closed is Resolution.ANSWERED


def test_a_day_the_scorer_spent_on_thin_features_is_never_read_as_drift(tmp_path: Path) -> None:
    """A cold start makes every card count too low: that is the platform, not the stream."""
    paths, state, reference = (
        HistoryPaths(tmp_path / "h"),
        live.DriftState(tmp_path / "d"),
        a_reference(),
    )
    days = [FIRST, FIRST + dt.timedelta(days=1)]
    for day in days:
        stage_day(paths, day, shift=3.0)
    cold = live.Start(dt.datetime(2026, 10, 2, 22, 0, tzinfo=dt.UTC), restored=False)
    result = a_pass(paths, state, reference, after(days[1]), starts=[cold])
    assert result.judged == [(days[0], True), (days[1], True)]
    assert result.opened is None
    assert all(r.status is Status.INSUFFICIENT for r in state.reports()[0].results)


def test_a_restored_start_leaves_a_day_to_be_judged(tmp_path: Path) -> None:
    starts = [live.Start(dt.datetime(2026, 10, 2, 4, tzinfo=dt.UTC), restored=True)]
    assert not live.cold_during(FIRST, starts)
    late_cold = [live.Start(dt.datetime(2026, 9, 30, 22, 30, tzinfo=dt.UTC), restored=False)]
    assert not live.cold_during(FIRST, late_cold)  # over 25 hours before the day
    within = [live.Start(dt.datetime(2026, 10, 1, 1, 30, tzinfo=dt.UTC), restored=False)]
    assert live.cold_during(FIRST, within)


def test_the_scorers_starts_are_read_as_it_writes_them(tmp_path: Path) -> None:
    path = tmp_path / "engine" / live.STARTS_FILE
    at = dt.datetime(2026, 10, 2, 4, tzinfo=dt.UTC)
    live.record_start(path, at=at, restored=False, detail="no snapshot")
    live.record_start(path, at=at + dt.timedelta(hours=5), restored=True, detail="restored")
    assert live.read_starts(path) == [
        live.Start(at, restored=False),
        live.Start(at + dt.timedelta(hours=5), restored=True),
    ]
    assert live.read_starts(tmp_path / "absent.jsonl") == []


def test_a_days_values_are_the_same_hash_draw_the_replay_used(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    stage_day(paths, FIRST, rows=24_000)
    window = live.day_window(paths, FIRST, rate=0.03)
    table = pq.read_table(sorted(paths.staged.glob("*.parquet")))
    drawn = [draw(str(i)) < 0.03 for i in table["event_id"].to_pylist()]
    expected = np.asarray(table["champion_score"].to_numpy())[np.asarray(drawn)]
    np.testing.assert_array_equal(np.sort(window[SCORE]), np.sort(expected))
    assert 500 < window[SCORE].size < 1_000


def test_a_reference_is_kept_and_read_back_with_what_built_it(tmp_path: Path) -> None:
    reference = a_reference()
    path = tmp_path / live.REFERENCE_FILE
    live.save_reference(reference, path, meta={"champion": "champion-x"})
    back, meta = live.load_reference(path)
    assert meta == {"champion": "champion-x"}
    assert back.names == reference.names
    for name in reference.names:
        np.testing.assert_array_equal(back.window[name], reference.window[name])


class Zeros:
    """A batch model that scores every row zero."""

    version = "zeros"

    def score_matrix(self, rows: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Zero for every row."""
        return np.zeros(len(rows), dtype=np.float64)

    def score(self, features: Mapping[str, float], event: object) -> float:
        """Zero."""
        return 0.0


def test_the_reference_is_the_stream_before_the_cutoff_and_nothing_after() -> None:
    population = Population(cards=100, devices=80, merchants=20)
    config = GeneratorConfig(seed=3, population=population, events_per_second=20.0)
    records = list(
        Generator(config, EntityGraph.build(seed=3, population=population)).stream(limit=600)
    )
    cutoff = records[400].event.event_time
    reference = live.build_reference(records, model=Zeros(), cutoff=cutoff, rate=1.0)
    before = sum(1 for record in records if record.event.event_time < cutoff)
    assert reference.window[SCORE].size == before
    assert set(reference.names) == {*NAMES, SCORE}
