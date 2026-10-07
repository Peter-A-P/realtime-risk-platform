"""Alerts reach Peter, and each says something true (ADR 26).

The tests that matter most:

- **every rule reads a metric the platform exports**, as the dashboard's
  queries do: a rule over a renamed metric never fires, and nothing else
  would notice until the day it should have;
- **the rules fire when they should and not before**, run through
  Prometheus's own rule tester on series shaped like the failures they are
  for (skipped where Docker is not running);
- **one message when an alert starts, one when it ends**, not one a minute,
  and a restart of the relay neither repeats nor forgets one;
- **a failed poll of Prometheus is not a resolution**: only ten in a row
  are, and then as an alert of their own;
- **the compactor's metrics are true while its runs fail**, because they
  come from the spools, not from the runs.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml

from verdict.events.schema import LabelEvent
from verdict.history import spool
from verdict.history.compact import HistoryPaths, final_after
from verdict.history.compactor import (
    CompactorMetrics,
    Outcome,
    Step,
    compact_command,
    run_forever,
    run_with_limit,
    unsealed_age,
)
from verdict.history.labels import CollectorMetrics
from verdict.history.records import LABEL_SCHEMA, label_row
from verdict.live.feed import Feed, FeedMetrics
from verdict.live.models_job import JobMetrics
from verdict.observe.alerts import REMIND_EVERY, UNREACHABLE, UNREACHABLE_AFTER, Relay, firing
from verdict.observe.metrics import ScorerMetrics

ROOT = Path(__file__).resolve().parents[1]
LIVE_COMPOSE = ROOT / "deploy" / "live" / "compose.yml"
BOOT = ROOT / "deploy" / "terraform" / "boot.sh.tftpl"
FAILURE_MODES = ROOT / "docs" / "failure-modes.md"
T0 = dt.datetime(2027, 4, 20, 12, tzinfo=dt.UTC)
_SUFFIXES = {"counter": ("_total",), "histogram": ("_bucket", "_sum", "_count"), "gauge": ("",)}


def _compose() -> dict[str, Any]:
    loaded = yaml.safe_load(LIVE_COMPOSE.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _rules() -> list[dict[str, Any]]:
    content = _compose()["configs"]["alert-rules"]["content"]
    groups = yaml.safe_load(content.replace("$$", "$"))["groups"]
    return [rule for group in groups for rule in group["rules"]]


def _exported(tmp_path: Path) -> set[str]:
    registries = [
        ScorerMetrics().registry,
        FeedMetrics(Feed.LABELS).registry,
        CollectorMetrics().registry,
        CompactorMetrics(HistoryPaths(tmp_path)).registry,
        JobMetrics().registry,
    ]
    names: set[str] = set()
    for registry in registries:
        for family in registry.collect():
            names |= {family.name + suffix for suffix in _SUFFIXES.get(family.type, ("",))}
    return names


# --- the rules ------------------------------------------------------------


def test_every_rule_reads_a_metric_the_platform_exports(tmp_path: Path) -> None:
    wanted = {name for rule in _rules() for name in re.findall(r"verdict_[a-z_]+", rule["expr"])}
    assert wanted, "the rules should read the platform's metrics"
    assert wanted <= _exported(tmp_path), wanted - _exported(tmp_path)


def test_every_alert_says_what_it_means_and_where_to_look() -> None:
    """The message carries the summary and names a section of the runbook."""
    runbook = FAILURE_MODES.read_text(encoding="utf-8")
    for rule in _rules():
        assert rule["annotations"]["summary"], rule["alert"]
        assert f"### {rule['alert']}" in runbook, rule["alert"]
    assert f"### {UNREACHABLE}" in runbook


def test_prometheus_scrapes_what_the_rules_read() -> None:
    compose = _compose()
    scrape = yaml.safe_load(compose["configs"]["prometheus"]["content"])
    targets = {
        job["job_name"]: target
        for job in scrape["scrape_configs"]
        for group in job["static_configs"]
        for target in group["targets"]
    }
    assert targets["labels"] == "labels:9111"
    assert targets["compactor"] == "compactor:9112"
    assert "--metrics-port=9111" in compose["services"]["labels"]["command"]
    assert "--metrics-port=9112" in compose["services"]["compactor"]["command"]
    assert scrape["rule_files"] == ["/etc/prometheus/rules.yml"]


def test_only_the_host_sends_and_it_sends_as_the_instance() -> None:
    """The alerts container writes files; the boot script's loop publishes them."""
    alerts = _compose()["services"]["alerts"]
    assert "network_mode" not in alerts
    assert alerts["volumes"] == ["/data/alerts:/data/alerts"]
    boot = BOOT.read_text(encoding="utf-8")
    assert "aws sns publish" in boot
    assert '"${alert_topic_arn}"' in boot
    assert re.search(r"chown 10001:10001 .*/data/alerts/outbox", boot)


_RULE_TESTS = """\
rule_files: [rules.yml]
evaluation_interval: 1m
tests:
  # Labels flowing; the collector keeps up, then stops at 01:00.
  - interval: 1m
    input_series:
      - series: 'verdict_feed_latest_due_timestamp_seconds{feed="labels"}'
        values: '0+60x300'
      - series: 'verdict_labels_latest_label_timestamp_seconds'
        values: '0+60x60 3600x240'
    alert_rule_test:
      - eval_time: 2h
        alertname: LabelCollectorBehind
        exp_alerts: []
      - eval_time: 3h20m
        alertname: LabelCollectorBehind
        exp_alerts:
          - exp_annotations:
              summary: The label collector is over two hours behind the labels feed.
  # Before the labels start, both are zero: nothing is behind.
  - interval: 1m
    input_series:
      - series: 'verdict_feed_latest_due_timestamp_seconds{feed="labels"}'
        values: '0x300'
      - series: 'verdict_labels_latest_label_timestamp_seconds'
        values: '0x300'
    alert_rule_test:
      - eval_time: 4h
        alertname: LabelCollectorBehind
        exp_alerts: []
  # A day ready to finalise for 50 minutes is a run in progress; for 70 is not.
  - interval: 1m
    input_series:
      - series: 'verdict_history_finalisable_days'
        values: '1x120'
    alert_rule_test:
      - eval_time: 50m
        alertname: DayNotFinalised
        exp_alerts: []
      - eval_time: 70m
        alertname: DayNotFinalised
        exp_alerts:
          - exp_annotations:
              summary: A day has been ready to finalise for over an hour and is not final.
  # One finalise stopped at its limit at 01:00 is told, and stops being news
  # three hours on; ok runs and the seal step never are.
  - interval: 1m
    input_series:
      - series: 'verdict_history_compact_runs_total{step="finalise", outcome="timeout"}'
        values: '0x60 1x300'
      - series: 'verdict_history_compact_runs_total{step="seal", outcome="ok"}'
        values: '0+1x360'
    alert_rule_test:
      - eval_time: 59m
        alertname: CompactionTimedOut
        exp_alerts: []
      - eval_time: 90m
        alertname: CompactionTimedOut
        exp_alerts:
          - exp_labels:
              step: finalise
              outcome: timeout
            exp_annotations:
              summary: A compaction run went past its time limit and was stopped.
      - eval_time: 4h30m
        alertname: CompactionTimedOut
        exp_alerts: []
  # A finalise with a two-hour limit: at its limit it is being stopped, not
  # stuck; a quarter of an hour past it, it is stuck. 2026-10-07's run, which
  # had no limit, would have been told at 02:15 instead of never.
  - interval: 1m
    input_series:
      - series: 'verdict_history_compact_run_seconds{step="finalise"}'
        values: '0+60x300'
      - series: 'verdict_history_compact_run_limit_seconds{step="finalise"}'
        values: '7200x300'
      - series: 'verdict_history_compact_run_seconds{step="seal"}'
        values: '0x300'
      - series: 'verdict_history_compact_run_limit_seconds{step="seal"}'
        values: '1800x300'
    alert_rule_test:
      - eval_time: 2h
        alertname: CompactionStuck
        exp_alerts: []
      - eval_time: 2h16m
        alertname: CompactionStuck
        exp_alerts:
          - exp_labels:
              step: finalise
            exp_annotations:
              summary: A compaction run is over its time limit and has not ended.
  # Saved at 10 minutes and never again: quiet for the first hour, not after.
  # Before the first save the gauge is zero, which the 45 minutes rides out.
  - interval: 1m
    input_series:
      - series: 'verdict_engine_snapshot_timestamp_seconds'
        values: '0x9 600x200'
    alert_rule_test:
      - eval_time: 9m
        alertname: EngineSnapshotStale
        exp_alerts: []
      - eval_time: 100m
        alertname: EngineSnapshotStale
        exp_alerts: []
      - eval_time: 2h
        alertname: EngineSnapshotStale
        exp_alerts:
          - exp_annotations:
              summary: The scorer's feature state has not been saved whole for over an hour.
  # A drift request is news the hour it opens, not every hour it stays open.
  - interval: 1m
    input_series:
      - series: 'verdict_drift_request_open'
        values: '0x120 1x300'
    alert_rule_test:
      - eval_time: 119m
        alertname: DriftRequestOpened
        exp_alerts: []
      - eval_time: 150m
        alertname: DriftRequestOpened
        exp_alerts:
          - exp_annotations:
              summary: The drift monitors opened a retraining request.
      - eval_time: 200m
        alertname: DriftRequestOpened
        exp_alerts: []
  # One pull request opened at 01:00 is told, and stops being news by 03:30.
  - interval: 1m
    input_series:
      - series: 'verdict_models_pull_requests_total{kind="candidate"}'
        values: '0x60 1x240'
    alert_rule_test:
      - eval_time: 90m
        alertname: ModelPullRequestOpened
        exp_alerts:
          - exp_labels:
              kind: candidate
            exp_annotations:
              summary: The live models job opened a pull request for a person to read.
      - eval_time: 210m
        alertname: ModelPullRequestOpened
        exp_alerts: []
  # A spot replacement's four minutes down is not an alert.
  - interval: 1m
    input_series:
      - series: 'up{job="scorer", instance="scorer:9108"}'
        values: '1x10 0x4 1x20'
    alert_rule_test:
      - eval_time: 15m
        alertname: TargetDown
        exp_alerts: []
"""


@pytest.mark.skipif(
    shutil.which("docker") is None and shutil.which("promtool") is None,
    reason="needs promtool, or Docker to run it",
)
def test_the_rules_fire_when_they_should_and_not_before(tmp_path: Path) -> None:
    content = _compose()["configs"]["alert-rules"]["content"].replace("$$", "$")
    (tmp_path / "rules.yml").write_text(content, encoding="utf-8")
    (tmp_path / "tests.yml").write_text(_RULE_TESTS, encoding="utf-8")
    local = shutil.which("promtool")
    if local is not None:
        # The same tester, installed: what a runner without Docker uses.
        found = subprocess.run(
            [local, "test", "rules", "tests.yml"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        assert found.returncode == 0, found.stdout + found.stderr
        return
    # promtool runs as the image's own user, nobody; pytest makes its
    # directories readable by their owner only, which on a Linux runner is
    # someone else, so the directory and files are opened up to be read.
    tmp_path.chmod(0o755)
    for written in tmp_path.iterdir():
        written.chmod(0o644)
    image = _compose()["services"]["prometheus"]["image"]
    try:
        result = subprocess.run(
            [
                "docker", "run", "--rm", "-v", f"{tmp_path}:/w", "-w", "/w",
                "--entrypoint", "promtool", image, "test", "rules", "tests.yml",
            ],
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )  # fmt: skip
    except subprocess.TimeoutExpired:
        pytest.skip("Docker did not answer")
    if "Cannot connect to the Docker daemon" in result.stderr or "error during connect" in (
        result.stderr
    ):
        pytest.skip("Docker is installed but not running")
    assert result.returncode == 0, result.stdout + result.stderr


# --- the relay ------------------------------------------------------------


class FakePrometheus:
    """Prometheus's alerts API, with alerts set by the test."""

    def __init__(self) -> None:
        """Nothing firing, and answering."""
        self.alerts: list[dict[str, Any]] = []
        self.down = False

    def fire(self, name: str, state: str = "firing", **labels: str) -> None:
        """Make an alert active."""
        self.alerts.append(
            {
                "labels": {"alertname": name, **labels},
                "annotations": {"summary": f"{name} means something"},
                "state": state,
            }
        )

    def read(self) -> Mapping[str, Any]:
        """One poll, as the relay makes it."""
        if self.down:
            raise OSError("connection refused")
        return {"status": "success", "data": {"alerts": list(self.alerts)}}


class Clock:
    """A clock the test moves."""

    def __init__(self) -> None:
        """At T0."""
        self.at = T0

    def now(self) -> dt.datetime:
        """The time."""
        return self.at


def _relay(tmp_path: Path, prometheus: FakePrometheus, clock: Clock) -> Relay:
    return Relay(tmp_path / "outbox", tmp_path / "told.json", read=prometheus.read, now=clock.now)


def _messages(tmp_path: Path) -> list[dict[str, str]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((tmp_path / "outbox").glob("*.json"))
    ]


def test_an_alert_is_told_once_when_it_starts_and_once_when_it_ends(tmp_path: Path) -> None:
    prometheus, clock = FakePrometheus(), Clock()
    relay = _relay(tmp_path, prometheus, clock)
    prometheus.fire("HistoryUnsealed")
    assert relay.poll() == ["verdict: HistoryUnsealed firing"]
    for _ in range(5):
        clock.at += dt.timedelta(minutes=1)
        assert relay.poll() == []
    prometheus.alerts.clear()
    assert relay.poll() == ["verdict: HistoryUnsealed resolved"]
    assert relay.poll() == []
    first, last = _messages(tmp_path)
    assert first["Subject"] == "verdict: HistoryUnsealed firing"
    assert "HistoryUnsealed means something" in first["Message"]
    assert "docs/failure-modes.md" in first["Message"]
    assert last["Subject"] == "verdict: HistoryUnsealed resolved"


def test_an_alert_that_keeps_firing_is_repeated_now_and_then(tmp_path: Path) -> None:
    prometheus, clock = FakePrometheus(), Clock()
    relay = _relay(tmp_path, prometheus, clock)
    prometheus.fire("ScorerStopped")
    relay.poll()
    clock.at += REMIND_EVERY - dt.timedelta(minutes=1)
    assert relay.poll() == []
    clock.at += dt.timedelta(minutes=1)
    assert relay.poll() == ["verdict: ScorerStopped still firing"]


def test_a_restarted_relay_neither_repeats_nor_forgets(tmp_path: Path) -> None:
    """A spot replacement restarts the relay; its state is on the data volume."""
    prometheus, clock = FakePrometheus(), Clock()
    prometheus.fire("DayNotFinalised")
    _relay(tmp_path, prometheus, clock).poll()
    again = _relay(tmp_path, prometheus, clock)
    assert again.poll() == []
    prometheus.alerts.clear()
    assert again.poll() == ["verdict: DayNotFinalised resolved"]


def test_the_same_alert_on_two_targets_is_two_alerts(tmp_path: Path) -> None:
    prometheus, clock = FakePrometheus(), Clock()
    prometheus.fire("TargetDown", job="labels")
    prometheus.fire("TargetDown", job="compactor")
    assert len(_relay(tmp_path, prometheus, clock).poll()) == 2
    assert {m["Message"].count("job: ") for m in _messages(tmp_path)} == {1}


def test_a_pending_alert_is_not_told() -> None:
    prometheus = FakePrometheus()
    prometheus.fire("TargetDown", state="pending")
    assert firing(prometheus.read()) == []


def test_a_failed_poll_is_not_a_resolution_but_ten_are_an_alert(tmp_path: Path) -> None:
    prometheus, clock = FakePrometheus(), Clock()
    relay = _relay(tmp_path, prometheus, clock)
    prometheus.fire("LabelCollectorBehind")
    relay.poll()
    prometheus.down = True
    for _ in range(UNREACHABLE_AFTER - 1):
        assert relay.poll() == []
    # Only the blindness is news; what was firing is not called resolved.
    assert relay.poll() == [f"verdict: {UNREACHABLE} firing"]
    prometheus.down = False
    assert relay.poll() == [f"verdict: {UNREACHABLE} resolved"]


def test_a_subject_fits_what_sns_allows(tmp_path: Path) -> None:
    prometheus, clock = FakePrometheus(), Clock()
    prometheus.fire("A" * 200)
    _relay(tmp_path, prometheus, clock).poll()
    (message,) = _messages(tmp_path)
    assert len(message["Subject"]) == 100


# --- the compactor's own metrics ------------------------------------------


def _metric(metrics: CompactorMetrics, name: str, **labels: str) -> float:
    value = metrics.registry.get_sample_value(name, labels)
    assert value is not None, name
    return value


def test_the_oldest_unsealed_hour_is_measured_from_its_end(tmp_path: Path) -> None:
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    for hour in (9, 10, 12):
        at = T0.replace(hour=hour)
        writer.append(at, label_row(LabelEvent(event_id=f"e{hour}", label_time=at, is_fraud=False)))
    writer.close()
    assert spool.seal(tmp_path, "2027-04-20T09", LABEL_SCHEMA)
    now = T0.replace(hour=12, minute=30)
    # 09 is sealed; 10 ended at 11:00; 12 is still being written.
    assert unsealed_age(tmp_path, now) == 90 * 60
    assert unsealed_age(tmp_path, T0.replace(hour=10, minute=30)) == 0.0
    assert unsealed_age(tmp_path / "missing", now) == 0.0


def test_the_spool_metrics_are_read_when_scraped(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    clock = Clock()
    metrics = CompactorMetrics(paths, now=clock.now)
    assert _metric(metrics, "verdict_history_unsealed_age_seconds", spool="staged") == 0.0
    assert _metric(metrics, "verdict_history_finalisable_days") == 0.0
    day = T0.date() - dt.timedelta(days=9)
    hour = paths.staged / f"{day.isoformat()}T05"
    hour.mkdir(parents=True)
    clock.at = final_after(day) + dt.timedelta(minutes=1)
    # Its labels are all in, but an hour it reads is not sealed yet.
    assert _metric(metrics, "verdict_history_finalisable_days") == 0.0
    assert _metric(metrics, "verdict_history_unsealed_age_seconds", spool="staged") > 0
    hour.rmdir()
    (paths.staged / f"{day.isoformat()}T05.parquet").touch()
    assert _metric(metrics, "verdict_history_finalisable_days") == 1.0
    assert _metric(metrics, "verdict_history_unsealed_age_seconds", spool="staged") == 0.0


def _runs(metrics: CompactorMetrics, step: Step, outcome: Outcome) -> float:
    return _metric(
        metrics, "verdict_history_compact_runs_total", step=step.value, outcome=outcome.value
    )


def test_every_run_is_counted_by_its_step_and_how_it_ended(tmp_path: Path) -> None:
    metrics = CompactorMetrics(HistoryPaths(tmp_path))
    stop = threading.Event()
    codes = iter([0, 137, None, 0, 1])
    limits: list[float] = []

    def run(argv: Sequence[str], timeout_seconds: float) -> int | None:
        limits.append(timeout_seconds)
        code = next(codes)
        if code == 1:
            stop.set()
        return code

    run_forever(
        Step.FINALISE,
        ["compact"],
        metrics,
        every_seconds=0,
        timeout_seconds=60,
        stop=stop,
        run=run,
    )
    assert limits == [60] * 5
    assert _runs(metrics, Step.FINALISE, Outcome.OK) == 2
    assert _runs(metrics, Step.FINALISE, Outcome.FAILED) == 2
    assert _runs(metrics, Step.FINALISE, Outcome.TIMEOUT) == 1
    # The other step's series exist from the start, at zero, so a first
    # timeout is an increase Prometheus can see.
    for outcome in Outcome:
        assert _runs(metrics, Step.SEAL, outcome) == 0


def test_the_run_in_hand_is_timed_and_its_limit_published(tmp_path: Path) -> None:
    """A run that cannot be stopped still shows, as a run older than its limit."""
    now = [100.0]
    metrics = CompactorMetrics(
        HistoryPaths(tmp_path),
        limits={Step.SEAL: 30.0, Step.FINALISE: 600.0},
        clock=lambda: now[0],
    )
    seconds = "verdict_history_compact_run_seconds"
    assert _metric(metrics, seconds, step="finalise") == 0.0
    assert _metric(metrics, "verdict_history_compact_run_limit_seconds", step="seal") == 30.0
    assert _metric(metrics, "verdict_history_compact_run_limit_seconds", step="finalise") == 600.0
    metrics.started(Step.FINALISE)
    now[0] = 400.0
    assert _metric(metrics, seconds, step="finalise") == 300.0
    now[0] = 900.0
    assert _metric(metrics, seconds, step="finalise") == 800.0
    assert _metric(metrics, seconds, step="seal") == 0.0
    metrics.ended(Step.FINALISE, Outcome.TIMEOUT)
    assert _metric(metrics, seconds, step="finalise") == 0.0


def test_a_run_past_its_limit_is_stopped() -> None:
    sleeper = [sys.executable, "-c", "import time; time.sleep(30)"]
    started = time.monotonic()
    assert run_with_limit(sleeper, 0.5) is None
    assert time.monotonic() - started < 10
    assert run_with_limit([sys.executable, "-c", "raise SystemExit(3)"], 30) == 3


def test_sealing_and_finalising_are_separate_runs(tmp_path: Path) -> None:
    seal = compact_command(tmp_path, 4, Step.SEAL)
    finalise = compact_command(tmp_path, 4, Step.FINALISE)
    assert seal[-1] == "--no-finalise"
    assert finalise[-1] == "--no-seal"
    assert seal[:-1] == finalise[:-1]


def test_a_stuck_finalise_does_not_hold_sealing_back(tmp_path: Path) -> None:
    """2026-10-07: one finalise ran for hours and no hour was sealed meanwhile."""
    metrics = CompactorMetrics(HistoryPaths(tmp_path))
    stop = threading.Event()
    release = threading.Event()
    sealed = threading.Event()
    seal_runs = 0

    def finalise(argv: Sequence[str], timeout_seconds: float) -> int | None:
        release.wait(10)
        return 0

    def seal(argv: Sequence[str], timeout_seconds: float) -> int | None:
        nonlocal seal_runs
        seal_runs += 1
        if seal_runs == 3:
            sealed.set()
        return 0

    loops = [
        threading.Thread(
            target=run_forever,
            args=(step, [step.value], metrics),
            kwargs={"every_seconds": 0, "timeout_seconds": 60, "stop": stop, "run": run},
        )
        for step, run in ((Step.FINALISE, finalise), (Step.SEAL, seal))
    ]
    for loop in loops:
        loop.start()
    try:
        assert sealed.wait(5), "sealing waited on the finalise"
        assert _runs(metrics, Step.FINALISE, Outcome.OK) == 0
    finally:
        stop.set()
        release.set()
        for loop in loops:
            loop.join(10)
    assert _runs(metrics, Step.FINALISE, Outcome.OK) == 1


def test_a_relay_whose_state_was_left_empty_starts_and_tells(tmp_path: Path) -> None:
    """2026-09-29: a hard stop left told.json empty and the relay crash-looped."""
    prometheus, clock = FakePrometheus(), Clock()
    (tmp_path / "told.json").write_text("", encoding="utf-8")
    prometheus.fire("ScorerStopped")
    assert _relay(tmp_path, prometheus, clock).poll() == ["verdict: ScorerStopped firing"]
    assert json.loads((tmp_path / "told.json").read_text(encoding="utf-8"))
