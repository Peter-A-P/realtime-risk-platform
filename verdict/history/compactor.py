"""The compactor as a service: runs every few minutes, and what it reports.

`verdict history compact` seals, finalises, or both, once, and exits, and
each run is its own process on purpose: a process that exits gives its
memory back before the next (`compact.seal_closed`). Until 2026-09-22 a
shell loop ran it, which left nothing that could say the compactor had
stopped working. On the dry run's first night it was killed by the kernel
every run for hours, and the only sign was the instance's memory.

On 2026-10-07 the live window's first finalise started at about 06:00Z and
had not ended four hours later. Sealing ran in the same process, before
finalising, and the parent waited on each run with no limit, so sealing
stopped with it; and a run that never ends is never counted as failed, so
`CompactionFailing` could not fire. Since then (ADR 18's second addendum):

- **Two loops, one per step** (`Step`), side by side, each still one process
  per run. A slow or stuck finalise no longer holds sealing back.
  `compact.is_sealed_for` keeps them off each other's hours.
- **Every run has a time limit.** A run past it is killed and counted as a
  timeout, and the loop goes on.
- **How long the current run has taken is a metric**, so a run that cannot
  even be killed (blocked in the kernel) is still seen.

The parent serves:

- `verdict_history_unsealed_age_seconds{spool}`: how long ago the oldest
  unsealed hour ended. A few minutes when all is well (an hour is sealed ten
  minutes after it ends, by a run every five); hours when runs fail or do
  not happen, at about a gigabyte an hour of unsealed staged rows.
- `verdict_history_finalisable_days`: days whose labels are all in, whose
  hours are all sealed, and which are not final yet. Zero when all is well;
  a day that stays here is finalising runs failing or not ending.
- `verdict_history_compact_runs_total{step, outcome}`: runs that exited
  cleanly (`ok`), that exited otherwise, including a run the kernel killed
  (`failed`), and that were stopped at their time limit (`timeout`).
- `verdict_history_compact_run_seconds{step}`: how long the run in hand has
  taken; zero between runs.
- `verdict_history_compact_run_limit_seconds{step}`: each step's time limit,
  so the alert on a run that outlives its limit reads it rather than
  repeating it.

The spool metrics are computed when Prometheus scrapes, from directory
listings, so they are true even while a run is failing.
"""

from __future__ import annotations

import datetime as dt
import enum
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge

from verdict.history import spool
from verdict.history.compact import HistoryPaths, finalisable

Now = Callable[[], dt.datetime]

Run = Callable[[Sequence[str], float], int | None]
"""Runs a command under a time limit: its exit code, or None if it was stopped at the limit."""


class Step(enum.StrEnum):
    """What one compaction run does."""

    SEAL = "seal"
    FINALISE = "finalise"


class Outcome(enum.StrEnum):
    """How one compaction run ended."""

    OK = "ok"
    FAILED = "failed"
    TIMEOUT = "timeout"


DEFAULT_LIMITS: Mapping[Step, float] = {Step.SEAL: 1800.0, Step.FINALISE: 7200.0}
"""Seconds a run may take. Sealing four live hours takes a few minutes and
finalising a live day was estimated at well under an hour (ADR 18's
addenda), so each limit is several times what a healthy run needs."""


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def unsealed_age(directory: Path, now: dt.datetime) -> float:
    """Seconds since the oldest unsealed hour ended; 0 if none has.

    The hour being written is never counted: it has not ended.

    Args:
        directory: A spool's directory.
        now: The current time.

    Returns:
        The age in seconds.
    """
    if not directory.exists():
        return 0.0
    unsealed = sorted(p.name for p in directory.iterdir() if p.is_dir())
    for key in unsealed:
        ended = spool.hour_start(key) + dt.timedelta(hours=1)
        if ended <= now:
            return (now - ended).total_seconds()
    return 0.0


def _age_of(directory: Path, now: Now) -> Callable[[], float]:
    """A spool's `unsealed_age`, read when Prometheus scrapes."""
    return lambda: unsealed_age(directory, now())


class CompactorMetrics:
    """What the compactor reports, on a registry of its own."""

    def __init__(
        self,
        paths: HistoryPaths,
        *,
        now: Now = _utc_now,
        limits: Mapping[Step, float] = DEFAULT_LIMITS,
        clock: Callable[[], float] = time.monotonic,
        registry: CollectorRegistry | None = None,
    ) -> None:
        """Create the metrics.

        Args:
            paths: The history directories the spool metrics read.
            now: The wall clock, replaceable in tests.
            limits: Each step's time limit, in seconds.
            clock: The clock runs are timed by, replaceable in tests.
            registry: Where to register them. A new one if None.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self._clock = clock
        self._started: dict[Step, float] = {}
        self._lock = threading.Lock()
        age = Gauge(
            "verdict_history_unsealed_age_seconds",
            "Seconds since the oldest unsealed hour of a spool ended; 0 if none has.",
            ("spool",),
            registry=self.registry,
        )
        for name, directory in (("staged", paths.staged), ("labels", paths.labels)):
            age.labels(name).set_function(_age_of(directory, now))
        Gauge(
            "verdict_history_finalisable_days",
            "Days whose labels are all in and hours all sealed, and which are not final yet.",
            registry=self.registry,
        ).set_function(lambda: float(len(list(finalisable(paths, now())))))
        self._runs = Counter(
            "verdict_history_compact_runs",
            "Compaction runs, by step and by how the process ended.",
            ("step", "outcome"),
            registry=self.registry,
        )
        running = Gauge(
            "verdict_history_compact_run_seconds",
            "Seconds the run of a step in hand has taken; 0 between runs.",
            ("step",),
            registry=self.registry,
        )
        limit = Gauge(
            "verdict_history_compact_run_limit_seconds",
            "Seconds a run of a step may take before it is stopped.",
            ("step",),
            registry=self.registry,
        )
        for step in Step:
            # Every series exists from the start, at zero, so that the first
            # timeout is an increase Prometheus can see.
            for outcome in Outcome:
                self._runs.labels(step.value, outcome.value)
            running.labels(step.value).set_function(self._elapsed_of(step))
            limit.labels(step.value).set(limits[step])

    def _elapsed_of(self, step: Step) -> Callable[[], float]:
        def elapsed() -> float:
            with self._lock:
                started = self._started.get(step)
            return 0.0 if started is None else self._clock() - started

        return elapsed

    def started(self, step: Step) -> None:
        """Mark a run of a step as begun."""
        with self._lock:
            self._started[step] = self._clock()

    def ended(self, step: Step, outcome: Outcome) -> None:
        """Count how a run of a step ended, and mark it over."""
        with self._lock:
            self._started.pop(step, None)
        self._runs.labels(step.value, outcome.value).inc()


def compact_command(root: Path, seal_limit: int, step: Step) -> list[str]:
    """The command for one run of a step, in a process of its own.

    Args:
        root: The history root.
        seal_limit: The most hours one seal run seals.
        step: What the run does.

    Returns:
        The argument list.
    """
    only = "--no-finalise" if step is Step.SEAL else "--no-seal"
    return [
        sys.executable,
        "-m",
        "verdict.cli",
        "history",
        "compact",
        f"--root={root}",
        f"--seal-limit={seal_limit}",
        only,
    ]


def run_with_limit(argv: Sequence[str], timeout_seconds: float) -> int | None:
    """Run a command; kill it if it outlives the limit.

    `subprocess.run` kills the child at the limit and then waits for it. A
    child blocked in the kernel cannot die until the kernel lets it go, so
    that wait can itself be long; `verdict_history_compact_run_seconds`
    keeps rising through it, which is what `CompactionStuck` watches.

    Args:
        argv: The command.
        timeout_seconds: The limit.

    Returns:
        The exit code, or None if the run was stopped at the limit.
    """
    try:
        return subprocess.run(argv, check=False, timeout=timeout_seconds).returncode
    except subprocess.TimeoutExpired:
        return None


def run_forever(
    step: Step,
    command: Sequence[str],
    metrics: CompactorMetrics,
    *,
    every_seconds: float,
    timeout_seconds: float,
    stop: threading.Event,
    run: Run | None = None,
) -> None:
    """Run the command, count how it ended, wait, and again, until stopped.

    Args:
        step: What the command does, for the metrics.
        command: One run.
        metrics: Where outcomes and the run in hand are reported.
        every_seconds: The wait between the end of one run and the next.
        timeout_seconds: How long a run may take before it is stopped.
        stop: Set to stop after the run in hand.
        run: Runs a command under a limit; `run_with_limit` if None.
    """
    execute = run or run_with_limit
    while not stop.is_set():
        metrics.started(step)
        code = execute(command, timeout_seconds)
        if code is None:
            outcome = Outcome.TIMEOUT
        elif code == 0:
            outcome = Outcome.OK
        else:
            outcome = Outcome.FAILED
        metrics.ended(step, outcome)
        stop.wait(every_seconds)
