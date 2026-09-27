"""The live window's drift, retraining and promotion gate, as one job on the instance (ADR 28).

Every pass, in order:

1. **Judge** every day that has finished and been sealed since the last pass,
   and ask the trigger (`drift/live.py`).
2. **Fit a candidate** if a request is open and one is due
   (`models/live.candidate_due`), and open a pull request that adds it and
   makes it the shadow model. Merging it is the approval to run it in
   shadow; a person then rolls the image.
3. **Ask the gate** about the current shadow model once it has a week of
   finalised shadow scores, and open a pull request with the verdict when it
   is worth telling (`models/live.gate_due`). Merging an eligible one is the
   approval to promote; a person then moves the pointer with `verdict flag
   set`.

Nothing here moves the champion pointer, writes a flag, merges, or deploys.
The job runs at the lowest CPU priority with XGBoost on one thread, beside a
scorer that owns one core of four.

Model files are only ever added. A candidate ships under its own name, so no
file the pointer might name is ever replaced, and a rollback can always find
what it rolls back to.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from prometheus_client import CollectorRegistry, Counter, Gauge

from verdict.drift import live as drift_live
from verdict.drift.monitors import Reference
from verdict.drift.trigger import RetrainRequest
from verdict.history.compact import HistoryPaths
from verdict.live.github import FileChange, GitHub
from verdict.models import live as models_live

COMPOSE_PATH: Final = "deploy/live/compose.yml"
ARTIFACTS_PATH: Final = "verdict/models/artifacts"
RECORDS_PATH: Final = "docs/live"
_SHADOW_FLAG: Final = re.compile(r"^(\s*- --shadow=)([A-Za-z0-9_.-]+)\s*$", re.MULTILINE)


class JobMetrics:
    """What the job reports, on a registry of its own."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Create the metrics.

        Args:
            registry: Where to register them. A new one if None.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self.passes = Counter(
            "verdict_models_job_passes",
            "Passes of the live drift, retraining and gate job, by how each ended.",
            ("outcome",),
            registry=self.registry,
        )
        self.last_pass = Gauge(
            "verdict_models_job_last_pass_timestamp_seconds",
            "Wall-clock time the last pass finished, however it ended.",
            registry=self.registry,
        )
        self.days_judged = Gauge(
            "verdict_drift_days_judged",
            "Days of the live window the drift monitors have judged.",
            registry=self.registry,
        )
        self.request_open = Gauge(
            "verdict_drift_request_open",
            "1 while a retraining request is open.",
            registry=self.registry,
        )
        self.pull_requests = Counter(
            "verdict_models_pull_requests",
            "Pull requests the job opened, by kind (candidate or verdict).",
            ("kind",),
            registry=self.registry,
        )
        for outcome in ("ok", "failed"):
            self.passes.labels(outcome)
        for kind in ("candidate", "verdict"):
            self.pull_requests.labels(kind)


@dataclass(slots=True)
class Job:
    """Everything a pass needs.

    Attributes:
        paths: The history root.
        state_dir: Where the job keeps its state.
        reference: The drift monitors' fixed reference.
        since: The live window's start.
        starts_file: The scorer's record of its starts.
        champion_path: The incumbent champion's ONNX file.
        shadow_path: The shadow model's ONNX file. None to take it from
            history: the model the scorer ran on the latest finalised day.
        github: Where pull requests go; None to write them locally only.
        clock: The time.
        fit: Fits a candidate; the retraining job's `retrain` by default.
        gate: Runs the promotion gate on shadow rows.
    """

    paths: HistoryPaths
    state_dir: Path
    reference: Reference
    since: dt.datetime
    starts_file: Path
    champion_path: Path
    shadow_path: Path | None
    github: GitHub | None
    clock: Callable[[], dt.datetime] = field(default=lambda: dt.datetime.now(dt.UTC))
    fit: Callable[..., dict[str, Any]] | None = None
    gate: Callable[..., Any] | None = None

    @property
    def drift_state(self) -> drift_live.DriftState:
        """The monitors' state."""
        return drift_live.DriftState(self.state_dir / "drift")

    @property
    def models_state(self) -> models_live.ModelsState:
        """What has been fitted and told."""
        return models_live.ModelsState(self.state_dir / "models.json")


def run_pass(job: Job) -> dict[str, Any]:
    """One pass: judge, fit if due, gate if due.

    Args:
        job: The job.

    Returns:
        What happened, for the log.
    """
    now = job.clock()
    said: dict[str, Any] = {"at": now.isoformat()}
    open_request = job.drift_state.open_request()
    answered = open_request is not None and any(
        c["beats_incumbent"] for c in job.models_state.candidates_for(open_request.opened_on)
    )
    judged = drift_live.watch_once(
        job.paths,
        job.drift_state,
        job.reference,
        since=job.since,
        starts=drift_live.read_starts(job.starts_file),
        now=now,
        answered=answered,
    )
    said["judged"] = [(day.isoformat(), cold) for day, cold in judged.judged]
    if judged.opened is not None:
        said["opened"] = judged.opened.opened_on.isoformat()
    if judged.closed is not None:
        said["closed"] = str(judged.closed)

    finalised = [day for day in models_live.finalised_days(job.paths) if day >= job.since.date()]
    request = job.drift_state.open_request()
    if request is not None:
        days = models_live.candidate_due(
            job.models_state, opened_on=request.opened_on, finalised=finalised
        )
        if days is not None:
            said["candidate"] = _candidate(job, request, days, now)
    shadow = job.shadow_path or _shadow_from_history(job.paths, finalised)
    if shadow is not None:
        verdict = _verdict(job, shadow, finalised, now)
        if verdict is not None:
            said["verdict"] = verdict
    return said


def _shadow_from_history(paths: HistoryPaths, finalised: list[dt.date]) -> Path | None:
    from verdict.scoring.registry import path_of

    version = models_live.current_shadow(paths, finalised)
    if version is None:
        return None
    try:
        return path_of(version)
    except FileNotFoundError:
        return None  # a model this image does not ship cannot be timed or judged


def _candidate(
    job: Job, request: RetrainRequest, days: list[dt.date], now: dt.datetime
) -> dict[str, Any]:
    from verdict.models.retrain import pull_request_body, retrain
    from verdict.scoring.onnx_model import model_version

    number = len(job.models_state.candidates_for(request.opened_on)) + 1
    stem = f"candidate-{request.opened_on.isoformat()}-{number}"
    work = job.state_dir / "work" / stem
    work.mkdir(parents=True, exist_ok=True)
    table, share = models_live.training_table(job.paths, days)
    import pyarrow.parquet as pq

    table_path = work / "table.parquet"
    pq.write_table(table, table_path)
    candidate_path = work / f"{stem}.onnx"
    fit = job.fit or retrain
    report = fit(
        request,
        track="synthetic",
        table_path=table_path,
        champion_path=job.champion_path,
        candidate_path=candidate_path,
        threads=1,
    )
    report["track"] = "synthetic live, finalised history"
    report["fitted_on_days"] = [days[0].isoformat(), days[-1].isoformat()]
    report["table_share_kept"] = share
    body = pull_request_body(report, request) + _after_merging_candidate(stem)
    version = model_version(candidate_path, stem)
    record = {
        "opened_on": request.opened_on.isoformat(),
        "stem": stem,
        "version": version,
        "last_day": days[-1].isoformat(),
        "beats_incumbent": bool(report["beats_incumbent"]),
        "at": now.isoformat(),
        "pull_request": None,
    }
    files = [
        FileChange(f"{ARTIFACTS_PATH}/{stem}.onnx", candidate_path.read_bytes()),
        FileChange(f"{RECORDS_PATH}/{stem}.json", _json_bytes(report)),
        FileChange(f"{RECORDS_PATH}/{stem}.md", body.encode("utf-8")),
    ]
    (work / "pull-request.md").write_text(body, encoding="utf-8")
    if job.github is not None:
        compose = job.github.read_file(COMPOSE_PATH).decode("utf-8")
        files.append(FileChange(COMPOSE_PATH, shadow_to(compose, stem).encode("utf-8")))
        record["pull_request"] = job.github.open_pull_request(
            branch=f"live/{stem}",
            title=f"Retraining candidate {version}, for the drift of {request.opened_on}",
            body=body,
            message=f"Add {stem}, fitted on the live window, as the shadow model",
            files=files,
        )
    job.models_state.add("candidates", record)
    return record


def _verdict(
    job: Job, shadow_path: Path, finalised: list[dt.date], now: dt.datetime
) -> dict[str, Any] | None:
    from verdict.models.champion import _model_hop
    from verdict.models.inputs import matrix
    from verdict.models.promote import evaluate
    from verdict.scoring.onnx_model import model_version

    version = model_version(shadow_path, shadow_path.stem)
    days_scored = models_live.shadow_days(job.paths, finalised, shadow_version=version)
    if len(days_scored) < models_live.SHADOW_DAYS:
        return None
    rows, share = models_live.shadow_rows(job.paths, days_scored, shadow_version=version)
    sample, _ = models_live.training_table(job.paths, days_scored[-1:])
    hop = _model_hop(shadow_path, matrix(sample)[:5_000])
    gate = job.gate or evaluate
    verdict = gate(rows, as_of=now, challenger_p99_ms=hop["p99"])
    if not models_live.gate_due(
        job.models_state,
        shadow_version=version,
        days_scored=days_scored,
        eligible_now=verdict.eligible,
    ):
        return None
    champion = model_version(job.champion_path)
    body = verdict.to_markdown(champion=champion, challenger=version) + _after_merging_verdict(
        version, eligible=verdict.eligible
    )
    stem = f"verdict-{version}-{now.date().isoformat()}"
    evidence = {
        "track": "synthetic live, finalised history",
        "shadow_version": version,
        "champion_version": champion,
        "days": [day.isoformat() for day in days_scored],
        "rows_share_kept": share,
        "eligible": verdict.eligible,
        "reasons": list(verdict.reasons),
        "rows": verdict.rows,
        "frauds": verdict.frauds,
        "challenger_p99_ms": verdict.challenger_p99_ms,
        "markdown": body,
    }
    record = {
        "shadow_version": version,
        "eligible": verdict.eligible,
        "at": now.isoformat(),
        "pull_request": None,
    }
    if job.github is not None:
        title = (
            f"Promote {version}: the gate finds it eligible"
            if verdict.eligible
            else f"Shadow verdict on {version}: refused"
        )
        record["pull_request"] = job.github.open_pull_request(
            branch=f"live/{stem}",
            title=title,
            body=body,
            message=f"Record the promotion gate's verdict on {version}",
            files=[
                FileChange(f"{RECORDS_PATH}/{stem}.json", _json_bytes(evidence)),
                FileChange(f"{RECORDS_PATH}/{stem}.md", body.encode("utf-8")),
            ],
        )
    job.models_state.add("verdicts", record)
    return record


def shadow_to(compose: str, stem: str) -> str:
    """The live compose file with the scorer's shadow model changed.

    Args:
        compose: The file as it is on `main`.
        stem: The new shadow model's file stem.

    Returns:
        The file with its one `--shadow=` line changed.

    Raises:
        ValueError: If the file has no such line, or more than one.
    """
    found = _SHADOW_FLAG.findall(compose)
    if len(found) != 1:
        msg = f"expected one --shadow= line in the compose file, found {len(found)}"
        raise ValueError(msg)
    return _SHADOW_FLAG.sub(lambda match: f"{match.group(1)}{stem}", compose)


def _after_merging_candidate(stem: str) -> str:
    return (
        "\n### After merging\n\n"
        f"The scorer runs `{stem}` in shadow from the next image roll, on the decisions "
        "history could keep (ADR 11's addendum). Build and push the image and roll it "
        "as `docs/STATE.md` describes. Nothing moves the champion; the gate's own pull "
        "request does that, a week of labelled shadow scores later.\n"
    )


def _after_merging_verdict(version: str, *, eligible: bool) -> str:
    if not eligible:
        return (
            "\n### After merging\n\n"
            "Nothing changes: merging records the verdict. The champion stays, and the "
            "shadow model keeps scoring until a new candidate replaces it.\n"
        )
    return (
        "\n### After merging\n\n"
        f"Move the pointer on the instance: `verdict flag set {version}`. The previous "
        "champion's file stays in the image, so `verdict flag rollback` returns to it in "
        "one event.\n"
    )


def _json_bytes(item: dict[str, Any]) -> bytes:
    return (json.dumps(item, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")
