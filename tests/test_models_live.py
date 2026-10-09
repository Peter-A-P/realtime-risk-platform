"""Retraining and the promotion gate on the live window, and the pull requests they open (ADR 28).

What must hold: the tables a live job reads are an unbiased weighted sample
of history however they are thinned; a candidate is fitted only when there
is something new to fit on; a verdict is told once, and again only if it
turns eligible; the pull requests carry what they say; and nothing ever
touches the champion pointer or replaces a model file the pointer might name.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from verdict.drift import live as drift_live
from verdict.drift.monitors import SCORE, Reference
from verdict.drift.trigger import RetrainRequest
from verdict.history.compact import HistoryPaths
from verdict.history.records import history_schema
from verdict.live import models_job
from verdict.live.github import FileChange, GitHub
from verdict.models import live
from verdict.models.dataset import training_schema
from verdict.scoring.onnx_model import model_version
from verdict.scoring.registry import ARTIFACTS
from verdict.store.features import FEATURE_SET

NAMES = [spec.name for spec in FEATURE_SET]
DAY = dt.date(2026, 10, 2)
LIVE_COMPOSE = Path(__file__).resolve().parents[1] / "deploy" / "live" / "compose.yml"


def keep_day(
    paths: HistoryPaths,
    day: dt.date,
    *,
    frauds: int,
    legit: int,
    shadow_version: str | None = None,
    seed: int = 0,
) -> None:
    """A finalised day of kept rows: some frauds, some legitimate, each weighted."""
    rng = np.random.default_rng(seed + day.toordinal())
    rows = frauds + legit
    at = dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC)
    times = [at + dt.timedelta(seconds=int(s)) for s in np.sort(rng.integers(0, 86_400, rows))]
    is_fraud = np.array([True] * frauds + [False] * legit)
    rng.shuffle(is_fraud)
    columns: dict[str, Any] = {
        "event_id": [f"{day}-{i}" for i in range(rows)],
        "card_id": ["card"] * rows,
        "event_time": times,
        "amount_cents": rng.integers(100, 100_000, rows),
        "decided_at": times,
        "champion_version": ["champion-x"] * rows,
        "champion_score": rng.random(rows),
        "action": ["approve"] * rows,
        "rule": ["model"] * rows,
        "shadow_version": [shadow_version] * rows,
        "shadow_score": rng.random(rows) if shadow_version else [None] * rows,
        "shadow_action": ["approve" if shadow_version else None] * rows,
        "label_time": [t + dt.timedelta(days=7) for t in times],
        "is_fraud": is_fraud,
        "recovered_cents": [0] * rows,
        "stratum": ["fraud" if f else "legit" for f in is_fraud],
        "weight": np.where(is_fraud, 10.0, 100.0),
    }
    for name in NAMES:
        columns[name] = rng.random(rows)
    paths.kept.mkdir(parents=True, exist_ok=True)
    schema = history_schema()
    table = pa.table({name: columns[name] for name in schema.names}).cast(schema)
    pq.write_table(table, paths.kept_file(day))


# --- tables ----------------------------------------------------------------------


def test_a_table_under_its_targets_keeps_every_row_and_weight(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    keep_day(paths, DAY, frauds=30, legit=200)
    table, share = live.training_table(paths, [DAY])
    assert table.num_rows == 230
    assert share == {"fraud": 1.0, "legit": 1.0}
    assert table.schema.names == training_schema().names
    times = pc.cast(table["event_time"], pa.int64()).to_numpy()
    assert (np.diff(times) >= 0).all()


def test_a_thinned_table_still_stands_for_the_same_totals(tmp_path: Path) -> None:
    """Thinning is case-control sampling again: the weights must carry it."""
    paths = HistoryPaths(tmp_path)
    days = [DAY, DAY + dt.timedelta(days=1)]
    for day in days:
        keep_day(paths, day, frauds=2_000, legit=20_000)
    table, share = live.bounded_table(paths, days, columns=[], targets={True: 1_000, False: 4_000})
    assert share == {"fraud": 0.25, "legit": 0.1}
    fraud = table["is_fraud"].to_numpy(zero_copy_only=False)
    weight = table["weight"].to_numpy(zero_copy_only=False)
    assert weight[fraud].sum() == pytest.approx(4_000 * 10.0, rel=0.1)
    assert weight[~fraud].sum() == pytest.approx(40_000 * 100.0, rel=0.1)
    assert 800 < fraud.sum() < 1_200


def test_the_gate_reads_only_what_the_shadow_model_scored(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    keep_day(paths, DAY, frauds=20, legit=100, shadow_version="candidate-a")
    keep_day(paths, DAY + dt.timedelta(days=1), frauds=20, legit=100, shadow_version="other")
    days = [DAY, DAY + dt.timedelta(days=1)]
    rows, _ = live.shadow_rows(paths, days, shadow_version="candidate-a")
    assert len(rows) == 120
    assert live.shadow_days(paths, days, shadow_version="candidate-a") == [DAY]


# --- when ------------------------------------------------------------------------


def test_a_candidate_is_fitted_only_when_there_is_something_new_to_fit_on(
    tmp_path: Path,
) -> None:
    state = live.ModelsState(tmp_path / "models.json")
    opened = DAY
    few = [DAY + dt.timedelta(days=n) for n in range(live.MIN_DAYS - 1)]
    assert live.candidate_due(state, opened_on=opened, finalised=few) is None
    enough = [DAY + dt.timedelta(days=n) for n in range(live.MIN_DAYS)]
    assert live.candidate_due(state, opened_on=opened, finalised=enough) == enough
    state.add("candidates", {"opened_on": opened.isoformat(), "last_day": enough[-1].isoformat()})
    more = [*enough, *(enough[-1] + dt.timedelta(days=n + 1) for n in range(live.NEW_DAYS - 1))]
    assert live.candidate_due(state, opened_on=opened, finalised=more) is None
    most = [*more, more[-1] + dt.timedelta(days=1)]
    assert live.candidate_due(state, opened_on=opened, finalised=most) == most[-live.FIT_DAYS :]


def test_a_verdict_is_told_once_and_again_only_if_it_turns_eligible(tmp_path: Path) -> None:
    state = live.ModelsState(tmp_path / "models.json")
    week = [DAY + dt.timedelta(days=n) for n in range(live.SHADOW_DAYS)]
    assert not live.gate_due(state, shadow_version="c", days_scored=week[:-1], eligible_now=False)
    assert live.gate_due(state, shadow_version="c", days_scored=week, eligible_now=False)
    state.add("verdicts", {"shadow_version": "c", "eligible": False})
    assert not live.gate_due(state, shadow_version="c", days_scored=week, eligible_now=False)
    assert live.gate_due(state, shadow_version="c", days_scored=week, eligible_now=True)
    state.add("verdicts", {"shadow_version": "c", "eligible": True})
    assert not live.gate_due(state, shadow_version="c", days_scored=week, eligible_now=True)


def test_a_candidate_becomes_the_shadow_model_by_one_line_of_the_compose_file() -> None:
    compose = LIVE_COMPOSE.read_text(encoding="utf-8")
    changed = models_job.shadow_to(compose, "candidate-2026-10-12-1")
    assert "      - --shadow=candidate-2026-10-12-1\n" in changed
    # Whatever the shadow model is on main, that one line is all that changes.
    assert models_job.shadow_to(changed, "x") == models_job.shadow_to(compose, "x")
    assert len(changed.splitlines()) == len(compose.splitlines())
    with pytest.raises(ValueError, match="one --shadow= line"):
        models_job.shadow_to("services: {}\n", "candidate")


# --- the pull request ---------------------------------------------------------------


class FakeGitHub:
    """GitHub's REST API as far as a pull request needs it, recording every call."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        """A repository whose `main` holds these files."""
        self.calls: list[tuple[str, str, dict[str, Any] | None]] = []
        self.files = files or {}
        self.blobs: dict[str, bytes] = {}
        self.trees: list[dict[str, Any]] = []
        self.pulls: list[dict[str, Any]] = []

    def __call__(self, method: str, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        """Answer one request."""
        self.calls.append((method, path, body))
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": "main-sha"}}
        if "/git/commits/" in path:
            return {"tree": {"sha": "main-tree"}}
        if "/contents/" in path:
            name = path.split("/contents/")[1].split("?")[0]
            return {"content": base64.b64encode(self.files[name]).decode()}
        if path.endswith("/git/blobs"):
            assert body is not None
            sha = f"blob-{len(self.blobs)}"
            self.blobs[sha] = base64.b64decode(body["content"])
            return {"sha": sha}
        if path.endswith("/git/trees"):
            assert body is not None
            self.trees.append(body)
            return {"sha": "new-tree"}
        if path.endswith("/git/commits"):
            return {"sha": "new-commit"}
        if path.endswith("/git/refs"):
            return {}
        if path.endswith("/pulls"):
            assert body is not None
            self.pulls.append(body)
            return {"html_url": f"https://github.com/pull/{len(self.pulls)}"}
        raise AssertionError(f"unexpected {method} {path}")

    def committed(self) -> dict[str, bytes]:
        """The files the last commit carried, by path."""
        return {entry["path"]: self.blobs[entry["sha"]] for entry in self.trees[-1]["tree"]}


def test_a_pull_request_is_a_branch_from_main_and_nothing_is_pushed_to_main() -> None:
    fake = FakeGitHub()
    address = GitHub(fake).open_pull_request(
        branch="live/x",
        title="t",
        body="b",
        message="m",
        files=[FileChange("docs/live/x.md", b"hello")],
    )
    assert address == "https://github.com/pull/1"
    refs = [body for method, path, body in fake.calls if path.endswith("/git/refs")]
    assert refs == [{"ref": "refs/heads/live/x", "sha": "new-commit"}]
    assert fake.pulls == [{"title": "t", "head": "live/x", "base": "main", "body": "b"}]
    assert fake.trees[-1]["base_tree"] == "main-tree"
    assert not any("merge" in path for _, path, _ in fake.calls)


def a_report(beats: bool) -> dict[str, Any]:
    """What `retrain` returns, as far as the pull request body reads it."""
    interval = {"value": 0.05 if beats else -0.01, "low": 0.02 if beats else -0.03, "high": 0.08}
    return {
        "comparison": {
            "champion": "champion-x",
            "challenger": "candidate-y",
            "challenger_minus_champion": interval,
            "champion_pr_auc": {"value": 0.5},
            "challenger_pr_auc": {"value": 0.55 if beats else 0.49},
            "test": {"rows": 1_000},
            "model_hop_ms": {"p50": 0.1, "p99": 0.2},
        },
        "cutoff": "2026-10-10T00:00:00+00:00",
        "train": {"rows": 5_000, "frauds": 300, "excluded_unarrived_labels": 0},
        "fit": {"trees": 120},
        "drifted_days_in_training": {
            "covers_the_drift": beats,
            "first_drifted_day": "2026-10-03",
            "last_day_trained_on": "2026-10-04" if beats else "2026-10-02",
            "days_short": 0 if beats else 1,
        },
        "request": "answered" if beats else "still-open",
        "beats_incumbent": beats,
        "promoted": False,
    }


def test_a_pass_fits_a_candidate_for_an_open_request_and_opens_its_pull_request(
    tmp_path: Path,
) -> None:
    paths = HistoryPaths(tmp_path / "history")
    for n in range(live.MIN_DAYS):
        keep_day(paths, DAY + dt.timedelta(days=n), frauds=40, legit=200)
    rng = np.random.default_rng(0)
    reference = Reference({name: rng.normal(size=1_000) for name in [*NAMES, SCORE]})
    state_dir = tmp_path / "state"
    request_day = DAY + dt.timedelta(days=1)
    drift_live.DriftState(state_dir / "drift").open(
        RetrainRequest(request_day, ("score",), ()),
        at=dt.datetime(2026, 10, 4, tzinfo=dt.UTC),
    )
    fitted: list[dict[str, Any]] = []

    def fit(request: object, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        fitted.append(kwargs)
        kwargs["candidate_path"].write_bytes(b"an onnx file")
        return a_report(beats=False)

    compose = LIVE_COMPOSE.read_bytes()
    fake = FakeGitHub({models_job.COMPOSE_PATH: compose})
    pointer = tmp_path / "flags" / "champion.json"
    job = models_job.Job(
        paths=paths,
        state_dir=state_dir,
        reference=reference,
        since=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        starts_file=tmp_path / "starts.jsonl",
        champion_path=ARTIFACTS / "champion.onnx",
        shadow_path=None,
        github=GitHub(fake),
        clock=lambda: dt.datetime(2026, 10, 20, 12, tzinfo=dt.UTC),
        fit=fit,
    )
    said = models_job.run_pass(job)
    assert fitted
    assert fitted[0]["threads"] == 1
    stem = f"candidate-{request_day.isoformat()}-1"
    assert said["candidate"]["stem"] == stem
    assert said["candidate"]["pull_request"] == "https://github.com/pull/1"
    committed = fake.committed()
    assert committed[f"verdict/models/artifacts/{stem}.onnx"] == b"an onnx file"
    assert f"--shadow={stem}" in committed[models_job.COMPOSE_PATH].decode()
    assert json.loads(committed[f"docs/live/{stem}.json"])["promoted"] is False
    assert "does not promote anything" in fake.pulls[0]["body"]
    # Model files are only ever added: nothing the pointer might name is replaced.
    assert not any(
        path.startswith("verdict/models/artifacts/") and not path.endswith(f"{stem}.onnx")
        for path in committed
    )
    assert not pointer.exists()
    # Nothing new to fit on: the next pass fits nothing.
    assert "candidate" not in models_job.run_pass(job)


def test_the_gate_opens_one_pull_request_with_its_verdict(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path / "history")
    shadow = ARTIFACTS / "challenger.onnx"
    version = model_version(shadow, "challenger")
    week = [DAY + dt.timedelta(days=n) for n in range(live.SHADOW_DAYS)]
    for day in week:
        keep_day(paths, day, frauds=10, legit=50, shadow_version=version)
    rng = np.random.default_rng(0)
    reference = Reference({name: rng.normal(size=1_000) for name in [*NAMES, SCORE]})

    class Refused:
        eligible = False
        reasons = ("too few frauds",)
        rows, frauds, challenger_p99_ms = 420, 70, 0.3

        def to_markdown(self, *, champion: str, challenger: str) -> str:
            return f"### Shadow evidence: {challenger} against {champion}\n"

    seen: list[int] = []

    def gate(rows: list[object], **kwargs: object) -> Refused:
        seen.append(len(rows))
        return Refused()

    fake = FakeGitHub()
    job = models_job.Job(
        paths=paths,
        state_dir=tmp_path / "state",
        reference=reference,
        since=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        starts_file=tmp_path / "starts.jsonl",
        champion_path=ARTIFACTS / "champion.onnx",
        shadow_path=shadow,
        github=GitHub(fake),
        clock=lambda: dt.datetime(2026, 10, 30, tzinfo=dt.UTC),
        gate=gate,
    )
    said = models_job.run_pass(job)
    assert seen == [len(week) * 60]
    assert said["verdict"]["eligible"] is False
    assert fake.pulls[0]["title"] == f"Shadow verdict on {version}: refused"
    assert "Nothing changes" in fake.pulls[0]["body"]
    assert not any(path.startswith("verdict/") for path in fake.committed())
    assert "verdict" not in models_job.run_pass(job)
    assert len(fake.pulls) == 1


def test_a_pass_with_no_reference_yet_judges_nothing_and_still_runs(tmp_path: Path) -> None:
    job = models_job.Job(
        paths=HistoryPaths(tmp_path / "history"),
        state_dir=tmp_path / "state",
        reference=None,
        since=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        starts_file=tmp_path / "starts.jsonl",
        champion_path=ARTIFACTS / "champion.onnx",
        shadow_path=None,
        github=None,
        clock=lambda: dt.datetime(2026, 10, 3, tzinfo=dt.UTC),
    )
    assert models_job.run_pass(job)["judged"] == "no reference yet"


def test_a_request_opened_by_hand_fits_a_candidate_whose_pull_request_says_so(
    tmp_path: Path,
) -> None:
    """ADR 32: the same path as drift, with the reason where the drift evidence would be."""
    from typer.testing import CliRunner

    from verdict.cli import app

    paths = HistoryPaths(tmp_path / "history")
    for n in range(live.MIN_DAYS):
        keep_day(paths, DAY + dt.timedelta(days=n), frauds=40, legit=200)
    state_dir = tmp_path / "state"
    reason = "The champion was trained on another population."
    runner = CliRunner()
    opened = runner.invoke(app, ["models-request", f"--state={state_dir}", f"--reason={reason}"])
    assert opened.exit_code == 0, opened.output
    again = runner.invoke(app, ["models-request", f"--state={state_dir}", "--reason=again"])
    assert again.exit_code == 1

    def fit(request: object, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        kwargs["candidate_path"].write_bytes(b"an onnx file")
        report = a_report(beats=True)
        report["drifted_days_in_training"] = {
            "covers_the_drift": None,
            "first_drifted_day": None,
            "last_day_trained_on": "2026-10-04",
            "days_short": None,
        }
        return report

    fake = FakeGitHub({models_job.COMPOSE_PATH: LIVE_COMPOSE.read_bytes()})
    job = models_job.Job(
        paths=paths,
        state_dir=state_dir,
        reference=None,
        since=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
        starts_file=tmp_path / "starts.jsonl",
        champion_path=ARTIFACTS / "champion.onnx",
        shadow_path=None,
        github=GitHub(fake),
        clock=lambda: dt.datetime(2026, 10, 20, 12, tzinfo=dt.UTC),
        fit=fit,
    )
    said = models_job.run_pass(job)
    assert said["candidate"]["pull_request"] == "https://github.com/pull/1"
    pull = fake.pulls[0]
    assert "requested by hand" in pull["title"]
    assert reason in pull["body"]
    assert "not by drift" in pull["body"]
    assert "does not promote anything" in pull["body"]


def test_a_candidate_for_a_request_opened_by_hand_has_no_drift_to_reach() -> None:
    from verdict.models.retrain import _drift_coverage

    request = RetrainRequest(DAY, (), (), reason="by hand")
    coverage = _drift_coverage(
        request,
        dt.datetime(2026, 10, 9, tzinfo=dt.UTC),
        dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC),
    )
    assert coverage["covers_the_drift"] is None
    assert coverage["first_drifted_day"] is None
