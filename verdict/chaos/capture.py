"""Chaos runs whose raw signals are kept: logs, metrics, and the label apart.

`faults.run_fault` reports what a fault did. A capture keeps what an operator
would have seen while it happened, for a project that learns failure causes
from symptoms (project 10 in the same portfolio, 2026-10-17): the scorer's
process output and the broker's container log, line by line; every metric the
scorer and the broker export, from a Prometheus scraping both every second;
and, in a file of its own, which fault it was. Nothing is tuned to make a
fault easier to tell apart: the platform logs little, and that is the signal.

Layout, one directory per run under the campaign's root:

    <run_id>/manifest.json         the ground truth, and the harness's report
    <run_id>/logs/harness.jsonl    the scorer's process: stdout and stderr
    <run_id>/logs/broker.jsonl     the broker container's log
    <run_id>/prometheus/series.json    query_range over the run, every series
    <run_id>/prometheus/metadata.json  each metric's type and HELP text
    <run_id>/prometheus/scrape.json    the scrape config and its targets

There are no traces: the platform has no OpenTelemetry, and adding it would
be a change to its design rather than a capture of it.

It runs against the local stack (`deploy/compose`), never the live one.
"""

from __future__ import annotations

import datetime as dt
import json
import platform
import random
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Final

PROMETHEUS_IMAGE: Final = "prom/prometheus:v3.14.0"
PROMETHEUS_CONTAINER: Final = "verdict-chaos-prometheus"
PROMETHEUS_PORT: Final = 19090
SCRAPE_SECONDS: Final = 1
SCORER_METRICS_PORT: Final = 9177
BROKER_CONTAINER: Final = "verdict-redpanda"
WARM_UP_SECONDS: Final = 20.0
"""Before the harness starts: the broker's metrics with nothing happening."""
COOL_DOWN_SECONDS: Final = 20.0
"""After it ends, so recovery is in the window."""

TARGETED: Final = {
    "none": [],
    "pause": ["broker"],
    "throttle": ["broker"],
    "scorer-stall": ["scorer"],
    "duplicates": ["producer"],
    "out-of-order": ["producer"],
    "poison": ["producer"],
    "schema-change": ["producer"],
    "clock-skew": ["producer"],
}
"""Which component each fault is done to."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """One run to make.

    Attributes:
        fault: One of `faults.FAULTS`.
        seconds: How long the fault lasts.
        intensity: A data fault's intensity, or None.
        mid_batch: For a broker fault, begin it inside a scorer batch.
    """

    fault: str
    seconds: float
    intensity: float | None = None
    mid_batch: bool = True


def plan(repeats: int = 5, controls: int = 2) -> list[Scenario]:
    """Every fault, each repeated with its timing or intensity varied, and the controls.

    Args:
        repeats: Runs per fault.
        controls: Runs with nothing done.

    Returns:
        The runs, in the order they will be made: shuffled, so that a slow
        drift in the machine is not confounded with one fault.
    """
    seconds = [15.0, 30.0, 45.0, 60.0, 20.0, 40.0, 50.0, 25.0]
    shares = [0.002, 0.01, 0.05, 0.2, 0.5, 0.03, 0.1, 0.005]
    vary: dict[str, list[Scenario]] = {
        "pause": [Scenario("pause", s, mid_batch=i % 2 == 0) for i, s in enumerate(seconds)],
        "throttle": [Scenario("throttle", s, mid_batch=i % 2 == 0) for i, s in enumerate(seconds)],
        "scorer-stall": [Scenario("scorer-stall", s) for s in seconds],
        "duplicates": [
            Scenario("duplicates", s, intensity=q)
            for s, q in zip(seconds, [1.0, 0.5, 0.2, 0.05, 0.8, 0.3, 0.1, 0.6], strict=True)
        ],
        "out-of-order": [
            Scenario("out-of-order", s, intensity=b)
            for s, b in zip(seconds, [0.5, 1.0, 2.0, 5.0, 0.2, 3.0, 10.0, 0.1], strict=True)
        ],
        "poison": [
            Scenario("poison", s, intensity=q) for s, q in zip(seconds, shares, strict=True)
        ],
        "schema-change": [
            Scenario("schema-change", s, intensity=q) for s, q in zip(seconds, shares, strict=True)
        ],
        "clock-skew": [
            Scenario("clock-skew", s, intensity=k)
            for s, k in zip(
                seconds, [60.0, 300.0, 3600.0, -300.0, 86400.0, -60.0, 900.0, 30.0], strict=True
            )
        ],
    }
    runs = [scenario for options in vary.values() for scenario in options[:repeats]]
    runs += [Scenario("none", 60.0) for _ in range(controls)]
    random.Random(20271017).shuffle(runs)
    return runs


# --- the run's signals -----------------------------------------------------------------

_LIBRDKAFKA = re.compile(r"^%(\d)\|[\d.]+\|([A-Z_]+)\|([^|]+)\|\s*(.*)$")
_BROKER = re.compile(r"^(TRACE|DEBUG|INFO|WARN|ERROR)\s+\S+\s+\S+\s+(.*)$")
"""A broker line: its level, its own date and time (the capture keeps Docker's), the rest."""
_SYSLOG: Final = {0: "CRITICAL", 1: "CRITICAL", 2: "CRITICAL", 3: "ERROR", 4: "WARNING"}


@dataclass(slots=True)
class _Lines:
    """Lines read from a process, each stamped when it arrived."""

    stamped: list[tuple[str, str, str]] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def read(self, pipe: IO[str], stream: str) -> None:
        for line in pipe:
            now = dt.datetime.now(dt.UTC).isoformat(timespec="microseconds")
            with self.lock:
                self.stamped.append((now, stream, line.rstrip("\n")))


def harness_records(lines: list[tuple[str, str, str]]) -> Iterator[dict[str, Any]]:
    """The scorer process's output as log records, a traceback kept whole.

    Args:
        lines: (UTC time, `stdout` or `stderr`, text), in arrival order.

    Yields:
        One record a line, or one per traceback: `ts`, `service`, `severity`,
        `message`, the client and facility for a broker-client line, and the
        exception's type and full text for a traceback.
    """
    held: list[tuple[str, str]] = []

    def flush() -> Iterator[dict[str, Any]]:
        if not held:
            return
        text = "\n".join(line for _, line in held)
        last = held[-1][1]
        kind = last.split(":", 1)[0].strip() if ":" in last else last.strip()
        yield {
            "ts": held[0][0],
            "service": "scorer",
            "severity": "ERROR",
            "message": last.strip(),
            "exception_type": kind,
            "traceback": text,
        }
        held.clear()

    for ts, stream, line in lines:
        if held:
            held.append((ts, line))
            if line and not line.startswith((" ", "\t", "Traceback")):
                yield from flush()
            continue
        if line.startswith("Traceback (most recent call last):"):
            held.append((ts, line))
            continue
        match = _LIBRDKAFKA.match(line)
        if match:
            level, facility, client, message = match.groups()
            role = "producer" if "producer" in client else "consumer"
            yield {
                "ts": ts,
                "service": f"scorer-kafka-{role}",
                "severity": _SYSLOG.get(int(level), "INFO" if int(level) < 7 else "DEBUG"),
                "message": message,
                "client": client,
                "facility": facility,
            }
            continue
        if line.strip():
            yield {
                "ts": ts,
                "service": "scorer",
                "severity": "ERROR" if stream == "stderr" else "INFO",
                "message": line,
            }
    yield from flush()


def broker_records(text: str) -> Iterator[dict[str, Any]]:
    """The broker container's log, from `docker logs --timestamps`, as log records.

    Args:
        text: The log.

    Yields:
        One record a line: `ts`, `service`, `severity`, `message`.
    """
    for line in text.splitlines():
        stamp, _, rest = line.partition(" ")
        if not rest:
            continue
        match = _BROKER.match(rest)
        severity, message = (match.group(1), match.group(2)) if match else ("INFO", rest)
        yield {
            "ts": stamp,
            "service": "broker",
            "severity": {"WARN": "WARNING"}.get(severity, severity),
            "message": message,
        }


def _write_jsonl(path: Path, records: Iterator[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as out:
        for record in records:
            out.write(json.dumps(record, sort_keys=True) + "\n")
            count += 1
    return count


# --- Prometheus --------------------------------------------------------------------------


def prometheus_config() -> str:
    """The scrape config: the scorer's metrics and the broker's, every second.

    Returns:
        YAML.
    """
    return (
        "global:\n"
        f"  scrape_interval: {SCRAPE_SECONDS}s\n"
        f"  scrape_timeout: {SCRAPE_SECONDS}s\n"
        "scrape_configs:\n"
        "  - job_name: scorer\n"
        "    static_configs:\n"
        f"      - targets: ['host.docker.internal:{SCORER_METRICS_PORT}']\n"
        "  - job_name: broker\n"
        "    metrics_path: /public_metrics\n"
        "    static_configs:\n"
        "      - targets: ['host.docker.internal:9644']\n"
    )


def start_prometheus(root: Path) -> str:
    """Start a Prometheus for the campaign, fresh, scraping the local stack.

    Args:
        root: The campaign's directory; its config is written there.

    Returns:
        Its HTTP address.
    """
    config = root / "prometheus.yml"
    config.write_text(prometheus_config(), encoding="utf-8")
    subprocess.run(["docker", "rm", "-f", PROMETHEUS_CONTAINER], capture_output=True, check=False)
    subprocess.run(
        [
            "docker", "run", "-d", "--name", PROMETHEUS_CONTAINER,
            "-p", f"{PROMETHEUS_PORT}:9090",
            "--add-host", "host.docker.internal:host-gateway",
            "-v", f"{config.resolve()}:/etc/prometheus/prometheus.yml:ro",
            PROMETHEUS_IMAGE,
            "--config.file=/etc/prometheus/prometheus.yml",
            "--storage.tsdb.retention.time=2d",
        ],
        check=True, capture_output=True,
    )  # fmt: skip
    address = f"http://localhost:{PROMETHEUS_PORT}"
    for _ in range(60):
        try:
            _get(address, "/-/ready")
            return address
        except OSError:
            time.sleep(1)
    msg = "the campaign's Prometheus did not become ready"
    raise RuntimeError(msg)


def _get(address: str, path: str, **params: str) -> Any:  # noqa: ANN401 - JSON
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    with urllib.request.urlopen(f"{address}{path}{query}", timeout=60) as response:
        body = response.read()
    return json.loads(body) if body[:1] in (b"{", b"[") else body.decode()


def dump_prometheus(address: str, out: Path, start: dt.datetime, end: dt.datetime) -> int:
    """Every series Prometheus holds over a window, at the scrape interval.

    Args:
        address: Prometheus.
        out: The run's `prometheus/` directory.
        start: The window's start.
        end: Its end.

    Returns:
        How many series were written.
    """
    out.mkdir(parents=True, exist_ok=True)
    window = {"start": f"{start.timestamp():.3f}", "end": f"{end.timestamp():.3f}"}
    names = _get(address, "/api/v1/label/__name__/values", **window)["data"]
    series: dict[str, Any] = {}
    count = 0
    for name in names:
        got = _get(address, "/api/v1/query_range", query=name, step=f"{SCRAPE_SECONDS}s", **window)
        series[name] = got["data"]["result"]
        count += len(series[name])
    (out / "series.json").write_text(json.dumps(series) + "\n", encoding="utf-8")
    metadata = _get(address, "/api/v1/metadata")["data"]
    (out / "metadata.json").write_text(json.dumps(metadata, indent=1) + "\n", encoding="utf-8")
    scrape = {
        "scrape_interval_seconds": SCRAPE_SECONDS,
        "step_seconds": SCRAPE_SECONDS,
        "window_utc": [start.isoformat(), end.isoformat()],
        "config": prometheus_config(),
        "targets": _get(address, "/api/v1/targets")["data"]["activeTargets"],
        "alerts": "none: the local Prometheus evaluates no alert rules",
    }
    (out / "scrape.json").write_text(json.dumps(scrape, indent=1) + "\n", encoding="utf-8")
    return count


# --- one run, and a campaign ---------------------------------------------------------


def _commit() -> str:
    done = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def run_one(
    scenario: Scenario, root: Path, prometheus: str, *, rate: float = 1_000.0, count: int = 90_000
) -> Path:
    """Make one run and keep its signals.

    Args:
        scenario: The run.
        root: The campaign's directory.
        prometheus: The campaign's Prometheus.
        rate: Transactions a second.
        count: Transactions in all.

    Returns:
        The run's directory.
    """
    run_id = f"{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    run_dir = root / run_id
    report_path = run_dir / "harness-report.json"
    run_dir.mkdir(parents=True)
    window_start = dt.datetime.now(dt.UTC)
    time.sleep(WARM_UP_SECONDS)
    command = [
        sys.executable, "-m", "verdict.cli", "chaos", "run",
        f"--fault={scenario.fault}", f"--seconds={scenario.seconds}",
        f"--rate={rate}", f"--count={count}",
        f"--metrics-port={SCORER_METRICS_PORT}", f"--out={report_path}", "--quiet",
        "--mid-batch" if scenario.mid_batch else "--no-mid-batch",
    ]  # fmt: skip
    if scenario.intensity is not None:
        command.append(f"--intensity={scenario.intensity}")
    import psutil

    load_before = psutil.cpu_percent(interval=1.0)
    lines = _Lines()
    started = dt.datetime.now(dt.UTC)
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8"
    )
    assert process.stdout is not None
    assert process.stderr is not None
    readers = [
        threading.Thread(target=lines.read, args=(process.stdout, "stdout")),
        threading.Thread(target=lines.read, args=(process.stderr, "stderr")),
    ]
    for reader in readers:
        reader.start()
    code = process.wait()
    for reader in readers:
        reader.join()
    finished = dt.datetime.now(dt.UTC)
    load_after = psutil.cpu_percent(interval=1.0)
    time.sleep(COOL_DOWN_SECONDS)
    window_end = dt.datetime.now(dt.UTC)

    harness = _write_jsonl(run_dir / "logs" / "harness.jsonl", harness_records(lines.stamped))
    broker_log = subprocess.run(
        [
            "docker", "logs", "--timestamps",
            "--since", window_start.isoformat(), "--until", window_end.isoformat(),
            BROKER_CONTAINER,
        ],
        capture_output=True, text=True, encoding="utf-8", check=False,
    )  # fmt: skip
    broker = _write_jsonl(
        run_dir / "logs" / "broker.jsonl",
        broker_records(broker_log.stdout + broker_log.stderr),
    )
    series = dump_prometheus(prometheus, run_dir / "prometheus", window_start, window_end)

    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else None
    report_path.unlink(missing_ok=True)
    manifest = {
        "run_id": run_id,
        "scenario": scenario.fault,
        "fault": {
            "name": scenario.fault,
            "seconds": scenario.seconds,
            "intensity": scenario.intensity,
            "mid_batch": scenario.mid_batch if scenario.fault in ("pause", "throttle") else None,
            "rate_per_second": rate,
            "transactions": count,
        },
        # The control injects nothing; its harness still times an empty window.
        "injection_start_utc": None
        if scenario.fault == "none"
        else report and report.get("fault_began_utc"),
        "injection_end_utc": None
        if scenario.fault == "none"
        else report and report.get("fault_ended_utc"),
        "components_targeted": TARGETED[scenario.fault],
        "window_utc": {
            "start": window_start.isoformat(),
            "harness_started": started.isoformat(),
            "harness_finished": finished.isoformat(),
            "end": window_end.isoformat(),
        },
        "harness_exit_code": code,
        "commit": _commit(),
        "host": f"{platform.node()}: {platform.system()} {platform.release()} "
        f"{platform.machine()}, Python {platform.python_version()}; broker in Docker "
        f"({BROKER_CONTAINER}), scorer in the harness process",
        # The machine is shared with other projects' jobs; a slow run may be theirs.
        "host_cpu_percent": {"before": load_before, "after": load_after},
        "signals": {"log_records": {"harness": harness, "broker": broker}, "series": series},
        "harness_report": report,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n", encoding="utf-8")
    return run_dir


def campaign(
    root: Path, *, repeats: int = 5, controls: int = 2, only: list[str] | None = None
) -> list[Path]:
    """Make every planned run and keep each one's signals.

    Args:
        root: Where the runs go.
        repeats: Runs per fault.
        controls: Control runs.
        only: Limit to these faults, for a trial.

    Returns:
        The runs' directories.
    """
    root.mkdir(parents=True, exist_ok=True)
    prometheus = start_prometheus(root)
    runs = [s for s in plan(repeats, controls) if only is None or s.fault in only]
    done = []
    for number, scenario in enumerate(runs, start=1):
        print(f"run {number} of {len(runs)}", flush=True)
        done.append(run_one(scenario, root, prometheus))
    return done
