"""The live feeds: transactions as their time comes, and labels a week later.

Two processes run the same generator configuration (ADR 15):

- the **transaction feed** sends each transaction to `transactions` when the
  wall clock reaches its event time;
- the **label feed** runs the same stream from the same start and sends each
  record's label to `labels` when the wall clock reaches its label time,
  seven days later (ADR 10).

Holding a week of labels in memory is not possible at the live rate (about
600 million), and a second run of a deterministic generator costs a few
percent of a core, so the label feed regenerates rather than remembers.

**Neither keeps ground truth.** The generator is deterministic, so after the
secret is revealed anyone can regenerate every record, scenario and regime.
Storing it live would be ten gigabytes a day of something derivable.

**A feed survives a spot replacement by its snapshot.** After each flush,
at most every `snapshot_every`, the feed writes the generator's whole state
to the data volume, atomically. A replacement restores it and carries on
from the first record not yet acknowledged. Records sent after the last
snapshot and before the interruption are sent again: at least once, like
everything else (ADR 8), and the scorer and history are idempotent by event
id. If the feed was down for a while, the records whose time passed are sent
as fast as the generator allows until it has caught up; none are skipped,
because a skipped record would still have a label a week later, and the
sealed schedule is graded on the stream as generated.
"""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from prometheus_client import CollectorRegistry, Counter, Gauge

from verdict.events.generator.driver import (
    GeneratedRecord,
    Generator,
    GeneratorConfig,
    GeneratorRun,
)
from verdict.events.generator.regimes import (
    DEV_SCHEDULE,
    RegimeSchedule,
    SealedCommitment,
    derive_schedule,
)
from verdict.stream.base import Stream

SNAPSHOT_EVERY: Final = dt.timedelta(seconds=30)
"""How often a feed saves its place. At the live rate, at most about 30,000
records are sent twice after an interruption."""

MAX_BATCH: Final = 5_000
"""The most records sent between flushes, so catching up still snapshots."""

MAX_IDLE_SLEEP: Final = 0.05
"""The longest a feed sleeps before looking at the clock again, in seconds."""

FRESH_START_TOLERANCE: Final = dt.timedelta(hours=1)
"""How far into a window a feed may start with no snapshot.

Starting fresh on day thirty would replay thirty days as fast as possible,
which is hours of backlog and never what was meant. Past this, starting
fresh needs `from_start=True`, said on purpose.
"""


class Feed(StrEnum):
    """Which half of the stream a feed sends."""

    TRANSACTIONS = "transactions"
    LABELS = "labels"


class ScheduleMismatchError(ValueError):
    """Raised when a sealed schedule's secret does not match its commitment."""


class NoSnapshotError(RuntimeError):
    """Raised on a fresh start well into a window, which would replay days."""


def live_schedule(
    mode: str, *, secret: str | None = None, commitment: str | None = None
) -> RegimeSchedule:
    """The regime schedule a live feed runs.

    Args:
        mode: `dev` for the public development schedule (a dry run), or
            `sealed` for the live window's.
        secret: The sealed secret, for `sealed`. Never logged.
        commitment: The committed hashes as JSON (`docs/sealed-schedule.json`),
            for `sealed`.

    Returns:
        The schedule.

    Raises:
        ScheduleMismatchError: If `sealed` is asked for without both, or the
            secret, the derived schedule or `regimes.py` does not match the
            commitment. A feed on an unverified schedule would grade the drift
            monitors against something nobody committed to.
        ValueError: On an unknown mode.
    """
    if mode == "dev":
        return DEV_SCHEDULE
    if mode != "sealed":
        msg = f"schedule must be dev or sealed, got {mode!r}"
        raise ValueError(msg)
    if not secret or not commitment:
        msg = "a sealed schedule needs both the secret and the commitment"
        raise ScheduleMismatchError(msg)
    sealed = SealedCommitment.model_validate_json(commitment)
    if not sealed.verify(secret):
        msg = "the secret, the derived schedule or regimes.py does not match the commitment"
        raise ScheduleMismatchError(msg)
    return derive_schedule(secret, window_days=sealed.window_days, name=sealed.schedule_name)


class SnapshotStore:
    """One feed's saved place, on the data volume, replaced atomically."""

    def __init__(self, directory: Path, feed: Feed) -> None:
        """Name the store.

        Args:
            directory: Where the feed keeps its state.
            feed: Which feed.
        """
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{feed.value}.snapshot"

    def load(self) -> bytes | None:
        """The last saved place, if any.

        Returns:
            The snapshot, or None.
        """
        return self.path.read_bytes() if self.path.exists() else None

    def save(self, snapshot: bytes) -> None:
        """Save a place, so that a crash mid-write leaves the previous one.

        Args:
            snapshot: What `GeneratorRun.snapshot` returned.
        """
        temporary = self.path.with_suffix(".tmp")
        temporary.write_bytes(snapshot)
        temporary.replace(self.path)


class FeedMetrics:
    """What a feed reports, on a registry of its own."""

    def __init__(self, feed: Feed, registry: CollectorRegistry | None = None) -> None:
        """Create the metrics.

        Args:
            feed: Which feed, as a label.
            registry: Where to register them.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self.sent = Counter(
            "verdict_feed_records",
            "Records a live feed has sent.",
            ("feed",),
            registry=self.registry,
        ).labels(feed.value)
        self.lag = Gauge(
            "verdict_feed_lag_seconds",
            "How far behind its records' due time a live feed is sending.",
            ("feed",),
            registry=self.registry,
        ).labels(feed.value)
        self.snapshots = Counter(
            "verdict_feed_snapshots",
            "Places a live feed has saved.",
            ("feed",),
            registry=self.registry,
        ).labels(feed.value)


@dataclass(frozen=True, slots=True)
class Clock:
    """The feed's sense of time, replaceable in tests.

    Attributes:
        now: The current time, timezone-aware UTC.
        sleep: Wait this many seconds.
    """

    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC)
    sleep: Callable[[float], None] = time.sleep


class LiveFeed:
    """Sends one half of a generator run in real time, and saves its place."""

    def __init__(
        self,
        run: GeneratorRun,
        stream: Stream,
        feed: Feed,
        store: SnapshotStore,
        *,
        clock: Clock | None = None,
        metrics: FeedMetrics | None = None,
        snapshot_every: dt.timedelta = SNAPSHOT_EVERY,
    ) -> None:
        """Assemble the feed.

        Args:
            run: The generator run, fresh or restored.
            stream: Where records go.
            feed: Transactions or labels.
            store: Where the place is saved.
            clock: The time source.
            metrics: Where counts go.
            snapshot_every: How often to save the place.
        """
        self.run = run
        self.stream = stream
        self.feed = feed
        self.store = store
        self.clock = clock or Clock()
        self.metrics = metrics or FeedMetrics(feed)
        self.snapshot_every = snapshot_every
        self._last_snapshot: dt.datetime | None = None

    def due(self, record: GeneratedRecord) -> dt.datetime:
        """When a record is to be sent.

        Args:
            record: The record.

        Returns:
            Its event time for transactions, its label time for labels.
        """
        return (
            record.event.event_time if self.feed is Feed.TRANSACTIONS else record.label.label_time
        )

    def _send(self, record: GeneratedRecord) -> None:
        if self.feed is Feed.TRANSACTIONS:
            event = record.event
            self.stream.produce("transactions", event.card_id, event.to_json().encode("utf-8"))
        else:
            label = record.label
            self.stream.produce("labels", label.event_id, label.to_json().encode("utf-8"))

    def step(self) -> int:
        """Send every record now due, up to `MAX_BATCH`; flush; maybe save.

        Returns:
            How many records were sent.
        """
        now = self.clock.now()
        sent = 0
        last_due: dt.datetime | None = None
        while sent < MAX_BATCH:
            upcoming = self.run.peek()
            due = self.due(upcoming)
            if due > now:
                break
            self._send(upcoming)
            next(self.run)
            last_due = due
            sent += 1
        if sent:
            self.stream.flush()
            self.metrics.sent.inc(sent)
            if last_due is not None:
                self.metrics.lag.set(max(0.0, (now - last_due).total_seconds()))
        if self._last_snapshot is None or now - self._last_snapshot >= self.snapshot_every:
            self.save()
        return sent

    def save(self) -> None:
        """Save the place: everything before the next record has been flushed."""
        self.store.save(self.run.snapshot())
        self._last_snapshot = self.clock.now()
        self.metrics.snapshots.inc()

    def run_until(self, stop: Callable[[], bool]) -> None:
        """Send in real time until asked to stop, then save the place.

        Args:
            stop: Checked between steps.
        """
        try:
            while not stop():
                if self.step():
                    continue
                wait = (self.due(self.run.peek()) - self.clock.now()).total_seconds()
                self.clock.sleep(min(max(wait, 0.0), MAX_IDLE_SLEEP))
        finally:
            self.stream.flush()
            self.save()


def open_run(
    config: GeneratorConfig,
    store: SnapshotStore,
    *,
    now: dt.datetime,
    from_start: bool = False,
) -> GeneratorRun:
    """The run a feed continues: its saved place, or the window's start.

    Args:
        config: The generator configuration, identical across restarts.
        store: Where the feed saved its place.
        now: The current time.
        from_start: Start fresh even well into the window.

    Returns:
        The run.

    Raises:
        NoSnapshotError: If there is no saved place and the window started
            more than `FRESH_START_TOLERANCE` ago, unless `from_start`.
    """
    generator = Generator(config)
    saved = store.load()
    if saved is not None:
        return generator.resume(saved)
    if not from_start and now - config.start_time > FRESH_START_TOLERANCE:
        msg = (
            f"no saved place in {store.path.parent}, and the window started at "
            f"{config.start_time.isoformat()}: starting fresh would replay it all. "
            "Pass from_start to mean it."
        )
        raise NoSnapshotError(msg)
    return GeneratorRun(generator)
