"""The drift monitors on the live window, one finished day at a time (ADR 28).

`run.py` judges a replayed stream in one pass. On the live stack there is no
replay: the scorer stages every decision with the features it was served and
the champion's score (ADR 18), and this reads those staged rows back, a day
at a time, once the day is over and its hours are sealed. The monitors,
thresholds and trigger are the same objects the replay used (ADR 12, ADR 23);
what is new is only where the values come from and where the state lives.

**The reference is rebuilt, not shipped.** It is the champion's training
window, as the replay's was: the first seven days of the synthetic stream the
champion was fitted on, served through the engine and scored by the champion,
with the same hash draw. That is deterministic, so the instance builds it
once from the code in its image (`build_reference`) and keeps it on the data
volume, and anyone can rebuild the same arrays. It is tens of megabytes, too
large to commit.

**A day the scorer spent on thin features is not judged.** A scorer that
starts with empty feature windows serves counts that are too low until the
day-long windows refill (ADR 27 makes that rare; it is not impossible). Those
days would read as drift in every card feature, and they would be the
platform's own doing, not the stream's. So the scorer records every start and
whether it restored its state, and a day within 25 hours of a cold start (a
day-long window at hourly resolution holds up to 25) is reported as too thin
to judge. The trigger already treats such a day as breaking a run and not as
recovery, which is what it is.

**State is files on the data volume**, so a spot replacement neither forgets
a judged day nor judges one twice: one report per day, and the open request,
if any.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, cast

import numpy as np
import pyarrow.parquet as pq

from verdict.drift.monitors import (
    SCORE,
    DailyReport,
    QuantityResult,
    Reference,
    Status,
    daily_report,
)
from verdict.drift.run import SAMPLE_RATE
from verdict.drift.stats import FloatArray
from verdict.drift.trigger import Resolution, RetrainRequest, evaluate_trigger, resolve
from verdict.history.compact import HistoryPaths
from verdict.history.sampling import draw
from verdict.models.dataset import Labelled, serve_and_score
from verdict.scoring.model import BatchModel
from verdict.store.features import FEATURE_SET

REFERENCE_FILE: Final = "reference.npz"
REFERENCE_DAYS: Final = 7.0
"""The champion's training window, in days from the synthetic stream's start:
the cutoff ADR 23's replay used."""

COLD_REACH: Final = dt.timedelta(hours=25)
"""How long features stay thin after a cold start: a day-long window at
hourly resolution holds up to 25 hours (ADR 20)."""

SETTLE: Final = dt.timedelta(hours=3)
"""How long after a day ends before it is judged: its last hour is sealed
minutes after it closes (ADR 18), and this leaves room for a replacement."""

STARTS_FILE: Final = "starts.jsonl"
"""The scorer's record of its starts, beside its saved state (ADR 27)."""

_QUANTITIES: Final = tuple(spec.name for spec in FEATURE_SET)
_META: Final = "__meta__"


# --- the reference ------------------------------------------------------------


def build_reference(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    cutoff: dt.datetime,
    rate: float = SAMPLE_RATE,
) -> Reference:
    """The champion's training window, served, scored and drawn as the replay drew it.

    Args:
        records: The synthetic stream the champion was fitted on, in order.
        model: The champion.
        cutoff: The end of its training window.
        rate: Share of transactions kept, by hash draw.

    Returns:
        The reference.
    """
    kept: dict[str, list[float]] = {}
    for record, features, score in serve_and_score(_before(records, cutoff), model=model):
        if draw(record.event.event_id) >= rate:
            continue
        for name, value in features.items():
            kept.setdefault(name, []).append(float(value))
        kept.setdefault(SCORE, []).append(score)
    if not kept:
        msg = "nothing before the cutoff to build a reference from"
        raise ValueError(msg)
    return Reference({name: np.asarray(values, dtype=np.float64) for name, values in kept.items()})


def _before(records: Iterable[Labelled], cutoff: dt.datetime) -> Iterator[Labelled]:
    for record in records:
        if record.event.event_time >= cutoff:
            return
        yield record


def save_reference(reference: Reference, path: Path, *, meta: dict[str, object]) -> None:
    """Keep a reference on disk, with what it was built from.

    Args:
        reference: The reference.
        path: Where.
        meta: What built it, kept beside the arrays.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".partial.npz")
    arrays: dict[str, Any] = {name: reference.window[name] for name in reference.names}
    arrays[_META] = np.asarray(json.dumps(meta, sort_keys=True, default=str))
    np.savez_compressed(partial, **arrays)
    partial.replace(path)


def load_reference(path: Path) -> tuple[Reference, dict[str, Any]]:
    """Read a kept reference back.

    Args:
        path: Where it was saved.

    Returns:
        The reference, and what built it.
    """
    with np.load(path, allow_pickle=False) as saved:
        meta: dict[str, Any] = json.loads(str(saved[_META]))
        window = {name: saved[name] for name in saved.files if name != _META}
    return Reference(window), meta


# --- the scorer's starts ----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Start:
    """One start of the scorer.

    Attributes:
        at: When.
        restored: Whether it started from saved feature state.
    """

    at: dt.datetime
    restored: bool


def record_start(path: Path, *, at: dt.datetime, restored: bool, detail: str) -> None:
    """Append a start to the scorer's record. Called by `verdict score`.

    Args:
        path: The record.
        at: When the scorer started deciding.
        restored: Whether it restored its feature state.
        detail: What it said about it, for a person.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"at": at.isoformat(), "restored": restored, "detail": detail})
    with path.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def read_starts(path: Path) -> list[Start]:
    """Every start recorded, in order.

    Args:
        path: The record; absent means none recorded.

    Returns:
        The starts.
    """
    if not path.exists():
        return []
    starts = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            item = json.loads(line)
            starts.append(Start(dt.datetime.fromisoformat(item["at"]), bool(item["restored"])))
    return starts


def cold_during(day: dt.date, starts: Iterable[Start]) -> bool:
    """Whether the scorer served thin features at any time on a day.

    Args:
        day: The day.
        starts: The scorer's starts.

    Returns:
        True if a cold start fell within `COLD_REACH` before the day, or on it.
    """
    begins = _midnight(day)
    ends = begins + dt.timedelta(days=1)
    return any(not start.restored and begins - COLD_REACH < start.at < ends for start in starts)


# --- one day's values -------------------------------------------------------------


def _midnight(day: dt.date) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC)


def _hour_key(day: dt.date, hour: int) -> str:
    return f"{day.isoformat()}T{hour:02d}"


def day_ready(paths: HistoryPaths, day: dt.date, now: dt.datetime) -> bool:
    """Whether a day's staged decisions are all sealed and can be read.

    An hour with no decisions at all (the scorer down for all of it) has no
    file, and is simply absent; an hour still being written is a directory.

    Args:
        paths: The history root.
        day: The day.
        now: The time.

    Returns:
        True once the day is over, settled, and none of its hours is open.
    """
    if now < _midnight(day) + dt.timedelta(days=1) + SETTLE:
        return False
    return not any((paths.staged / _hour_key(day, hour)).is_dir() for hour in range(24))


def day_window(
    paths: HistoryPaths, day: dt.date, *, rate: float = SAMPLE_RATE
) -> dict[str, FloatArray]:
    """The drawn share of a day's staged decisions, one array per quantity.

    Read a batch at a time: a live hour is millions of rows, and only the
    drawn few percent are kept.

    Args:
        paths: The history root.
        day: The day.
        rate: Share kept, by the same hash draw the replay used.

    Returns:
        Each feature, and the champion's score as `score`.
    """
    kept: dict[str, list[FloatArray]] = {name: [] for name in (*_QUANTITIES, SCORE)}
    columns = ["event_id", "champion_score", *_QUANTITIES]
    for hour in range(24):
        path = paths.staged / f"{_hour_key(day, hour)}.parquet"
        if not path.exists():
            continue
        for batch in pq.ParquetFile(path).iter_batches(batch_size=200_000, columns=columns):
            ids = cast("list[str]", batch.column("event_id").to_pylist())
            mask = np.array([draw(event_id) < rate for event_id in ids], dtype=np.bool_)
            if not mask.any():
                continue
            for name in _QUANTITIES:
                values = batch.column(name).to_numpy(zero_copy_only=False)
                kept[name].append(np.asarray(values, dtype=np.float64)[mask])
            scores = batch.column("champion_score").to_numpy(zero_copy_only=False)
            kept[SCORE].append(np.asarray(scores, dtype=np.float64)[mask])
    return {
        name: np.concatenate(parts) if parts else np.empty(0, dtype=np.float64)
        for name, parts in kept.items()
    }


def thin_day(day: dt.date, reference: Reference) -> DailyReport:
    """A day reported as too thin to judge in every quantity.

    Args:
        day: The day.
        reference: For the quantities' names.

    Returns:
        The report.
    """
    return DailyReport(
        day=day,
        results=tuple(
            QuantityResult(name, 0, 0.0, 0.0, 1.0, Status.INSUFFICIENT) for name in reference.names
        ),
    )


# --- reports and requests as files ------------------------------------------------


def report_to_json(report: DailyReport, *, cold: bool) -> dict[str, Any]:
    """A day's report as JSON can carry it.

    Args:
        report: The report.
        cold: Whether it was left unjudged for a cold start.

    Returns:
        The report.
    """
    return {
        "day": report.day.isoformat(),
        "cold_engine": cold,
        "drifted": sorted(report.drifted()),
        "results": [
            {
                "name": result.name,
                "values": result.values,
                "psi": round(result.psi, 4),
                "ks_statistic": round(result.ks_statistic, 4),
                "ks_p_value": float(result.ks_p_value),
                "status": str(result.status),
            }
            for result in report.results
        ],
    }


def report_from_json(item: dict[str, Any]) -> DailyReport:
    """Read a day's report back.

    Args:
        item: What `report_to_json` wrote.

    Returns:
        The report.
    """
    return DailyReport(
        day=dt.date.fromisoformat(item["day"]),
        results=tuple(
            QuantityResult(
                name=result["name"],
                values=result["values"],
                psi=result["psi"],
                ks_statistic=result["ks_statistic"],
                ks_p_value=result["ks_p_value"],
                status=Status(result["status"]),
            )
            for result in item["results"]
        ),
    )


def request_to_json(request: RetrainRequest) -> dict[str, Any]:
    """A request as JSON.

    Args:
        request: The request.

    Returns:
        It, with its evidence.
    """
    return {
        "opened_on": request.opened_on.isoformat(),
        "quantities": list(request.quantities),
        "evidence": [report_to_json(report, cold=False) for report in request.evidence],
    }


def request_from_json(item: dict[str, Any]) -> RetrainRequest:
    """Read a request back.

    Args:
        item: What `request_to_json` wrote.

    Returns:
        The request.
    """
    return RetrainRequest(
        opened_on=dt.date.fromisoformat(item["opened_on"]),
        quantities=tuple(item["quantities"]),
        evidence=tuple(report_from_json(report) for report in item["evidence"]),
    )


@dataclass(slots=True)
class DriftState:
    """What the live monitors have judged and asked for, on the data volume.

    Attributes:
        directory: Where it lives.
    """

    directory: Path

    @property
    def days(self) -> Path:
        """One report per judged day."""
        return self.directory / "days"

    @property
    def request_file(self) -> Path:
        """The open request, if any."""
        return self.directory / "request.json"

    @property
    def log_file(self) -> Path:
        """Every request opened or closed, one line each."""
        return self.directory / "requests.jsonl"

    def judged(self) -> set[dt.date]:
        """The days already judged.

        Returns:
            Their dates.
        """
        if not self.days.is_dir():
            return set()
        return {dt.date.fromisoformat(path.stem) for path in self.days.glob("*.json")}

    def save_day(self, report: DailyReport, *, cold: bool) -> None:
        """Keep a day's report.

        Args:
            report: The report.
            cold: Whether it was left unjudged for a cold start.
        """
        self.days.mkdir(parents=True, exist_ok=True)
        _write_json(self.days / f"{report.day.isoformat()}.json", report_to_json(report, cold=cold))

    def reports(self) -> list[DailyReport]:
        """Every day's report, in day order.

        Returns:
            The reports.
        """
        if not self.days.is_dir():
            return []
        return sorted(
            (
                report_from_json(json.loads(path.read_text(encoding="utf-8")))
                for path in self.days.glob("*.json")
            ),
            key=lambda report: report.day,
        )

    def open_request(self) -> RetrainRequest | None:
        """The request that is open, if any.

        Returns:
            It, or None.
        """
        if not self.request_file.exists():
            return None
        return request_from_json(json.loads(self.request_file.read_text(encoding="utf-8")))

    def last_closed_on(self) -> dt.date | None:
        """The last day a request closed on, so the same run cannot reopen it.

        Returns:
            The day, or None if none has closed.
        """
        closed = [item for item in self._log() if item["event"] == "closed"]
        return dt.date.fromisoformat(closed[-1]["on"]) if closed else None

    def open(self, request: RetrainRequest, *, at: dt.datetime) -> None:
        """Open a request.

        Args:
            request: The request.
            at: When.
        """
        _write_json(self.request_file, request_to_json(request))
        self._append({"event": "opened", "at": at.isoformat(), **request_to_json(request)})

    def close(self, resolution: Resolution, *, on: dt.date, at: dt.datetime) -> None:
        """Close the open request.

        Args:
            resolution: Why.
            on: The last day judged when it closed.
            at: When.
        """
        request = self.open_request()
        if request is None:
            return
        self._append(
            {
                "event": "closed",
                "at": at.isoformat(),
                "on": on.isoformat(),
                "opened_on": request.opened_on.isoformat(),
                "resolution": str(resolution),
            }
        )
        self.request_file.unlink()

    def _log(self) -> list[dict[str, Any]]:
        if not self.log_file.exists():
            return []
        return [
            json.loads(line)
            for line in self.log_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def _append(self, item: dict[str, Any]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as file:
            file.write(json.dumps(item, sort_keys=True) + "\n")


def _write_json(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".partial")
    partial.write_text(json.dumps(item, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    partial.replace(path)


# --- one pass -------------------------------------------------------------------


@dataclass(slots=True)
class Pass:
    """What one pass of the live monitors did.

    Attributes:
        judged: Days judged, with whether each was left thin for a cold start.
        opened: The request opened, if one was.
        closed: How the open request closed, if it did.
    """

    judged: list[tuple[dt.date, bool]] = field(default_factory=list)
    opened: RetrainRequest | None = None
    closed: Resolution | None = None


def first_whole_day(since: dt.datetime) -> dt.date:
    """The first day wholly inside the window.

    Args:
        since: The window's start.

    Returns:
        That day, or the next if the window began after its midnight.
    """
    day = since.astimezone(dt.UTC).date()
    return day if _midnight(day) == since else day + dt.timedelta(days=1)


def watch_once(
    paths: HistoryPaths,
    state: DriftState,
    reference: Reference,
    *,
    since: dt.datetime,
    starts: list[Start],
    now: dt.datetime,
    answered: bool = False,
    rate: float = SAMPLE_RATE,
) -> Pass:
    """Judge every finished day not yet judged, then ask the trigger.

    Args:
        paths: The history root.
        state: The monitors' state.
        reference: The fixed reference.
        since: The live window's start; nothing before its first whole day
            is judged.
        starts: The scorer's starts.
        now: The time.
        answered: Whether the open request's latest candidate beat the
            incumbent, from the retraining job's record.
        rate: Share of each day judged.

    Returns:
        What the pass did.
    """
    result = Pass()
    judged = state.judged()
    day = first_whole_day(since)
    while _midnight(day) + dt.timedelta(days=1) <= now:
        if day not in judged:
            if not day_ready(paths, day, now):
                break
            cold = cold_during(day, starts)
            report = (
                thin_day(day, reference)
                if cold
                else daily_report(day, reference, day_window(paths, day, rate=rate))
            )
            state.save_day(report, cold=cold)
            result.judged.append((day, cold))
        day += dt.timedelta(days=1)

    reports = state.reports()
    if not reports:
        return result
    latest = reports[-1].day
    request = state.open_request()
    if request is not None:
        resolution = resolve(request, candidate_beat_incumbent=answered, since=reports)
        if resolution is not Resolution.STILL_OPEN:
            state.close(resolution, on=latest, at=now)
            result.closed = resolution
        return result
    closed_on = state.last_closed_on()
    if closed_on is not None:
        reports = [report for report in reports if report.day > closed_on]
    opened = evaluate_trigger(reports, request_open=False)
    if opened is not None:
        state.open(opened, at=now)
        result.opened = opened
    return result
