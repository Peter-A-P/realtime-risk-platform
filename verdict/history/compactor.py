"""The compactor as a service: a run every few minutes, and what it reports.

`verdict history compact` seals and finalises once and exits, and each run
is its own process on purpose: a process that exits gives its memory back
before the next (`compact.seal_closed`). Until 2026-09-22 a shell loop ran
it, which left nothing that could say the compactor had stopped working. On
the dry run's first night it was killed by the kernel every run for hours,
and the only sign was the instance's memory.

This keeps the one-process-per-run shape and adds a parent that outlives the
runs and serves metrics about them and about the spools themselves:

- `verdict_history_unsealed_age_seconds{spool}`: how long ago the oldest
  unsealed hour ended. A few minutes when all is well (an hour is sealed ten
  minutes after it ends, by a run every five); hours when runs fail or do
  not happen, at about a gigabyte an hour of unsealed staged rows.
- `verdict_history_finalisable_days`: days whose labels are all in and which
  are not final yet. Zero when all is well; a day that stays here is a
  finalising run failing every time.
- `verdict_history_compact_runs_total{outcome}`: runs that exited cleanly and
  runs that did not, which includes a run the kernel killed.

The spool metrics are computed when Prometheus scrapes, from directory
listings, so they are true even while a run is failing.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge

from verdict.history import spool
from verdict.history.compact import HistoryPaths, finalisable

Now = Callable[[], dt.datetime]


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
        self, paths: HistoryPaths, *, now: Now = _utc_now, registry: CollectorRegistry | None = None
    ) -> None:
        """Create the metrics.

        Args:
            paths: The history directories the spool metrics read.
            now: The clock, replaceable in tests.
            registry: Where to register them. A new one if None.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
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
            "Days whose labels are all in and which are not final yet.",
            registry=self.registry,
        ).set_function(lambda: float(len(list(finalisable(paths, now())))))
        runs = Counter(
            "verdict_history_compact_runs",
            "Compaction runs, by whether the process exited cleanly.",
            ("outcome",),
            registry=self.registry,
        )
        self.succeeded = runs.labels("ok")
        self.failed = runs.labels("failed")


def compact_command(root: Path, seal_limit: int) -> list[str]:
    """The command for one compaction run, in a process of its own.

    Args:
        root: The history root.
        seal_limit: The most hours one run seals.

    Returns:
        The argument list.
    """
    return [
        sys.executable,
        "-m",
        "verdict.cli",
        "history",
        "compact",
        f"--root={root}",
        f"--seal-limit={seal_limit}",
    ]


def run_forever(
    command: Sequence[str],
    metrics: CompactorMetrics,
    *,
    every_seconds: float,
    stop: threading.Event,
    run: Callable[[Sequence[str]], int] | None = None,
) -> None:
    """Run the command, count how it ended, wait, and again, until stopped.

    Args:
        command: One run.
        metrics: Where outcomes are counted.
        every_seconds: The wait between the end of one run and the next.
        stop: Set to stop after the run in hand.
        run: Runs a command and returns its exit code; a subprocess if None.
    """
    execute = run or (lambda argv: subprocess.run(argv, check=False).returncode)
    while not stop.is_set():
        if execute(command) == 0:
            metrics.succeeded.inc()
        else:
            metrics.failed.inc()
        stop.wait(every_seconds)
