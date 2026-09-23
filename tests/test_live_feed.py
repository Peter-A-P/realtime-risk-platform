"""The live feeds: a generator that resumes exactly, played in real time.

What matters, in order:

- **a resumed run is the same stream.** Snapshotted at any point, including
  mid-step with records built and not yet handed out, and restored under the
  same configuration, a run produces byte for byte what the original would
  have produced next. Otherwise the stream after a spot replacement is not
  the stream the sealed schedule was committed to.
- **a snapshot will not continue a different stream**, and a sealed schedule
  will not run on a secret that does not match its commitment.
- **the feeds send each record at its time**: a transaction at its event
  time, its label seven days later, nothing early, nothing skipped after an
  interruption, and nothing replayed from the window's start by accident.
"""

from __future__ import annotations

import datetime as dt
import itertools
import json
import sys
import time
from pathlib import Path

import pytest

from verdict.events.generator.driver import (
    Generator,
    GeneratorConfig,
    GeneratorRun,
    SnapshotMismatchError,
)
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import SealedCommitment
from verdict.events.schema import LabelEvent, TransactionEvent
from verdict.live.feed import (
    MAX_BATCH,
    Clock,
    Feed,
    LiveFeed,
    NoSnapshotError,
    ScheduleMismatchError,
    SnapshotStore,
    live_schedule,
    open_run,
)
from verdict.stream.memory import MemoryBroker

START = dt.datetime(2027, 4, 5, tzinfo=dt.UTC)
SMALL = Population(cards=2_000, devices=1_500, merchants=100)
CONFIG = GeneratorConfig(population=SMALL, events_per_second=50.0, start_time=START)


@pytest.fixture(scope="module")
def graph() -> EntityGraph:
    return EntityGraph.build(CONFIG.seed, SMALL)


def _wire(run: object, n: int) -> list[str]:
    assert isinstance(run, GeneratorRun)
    return [
        record.event.to_json() + record.label.to_json() + record.truth.to_json()
        for record in itertools.islice(run, n)
    ]


# --- resuming ---------------------------------------------------------------


@pytest.mark.parametrize("at", [0, 1, 7, 333, 2_500])
def test_a_resumed_run_continues_the_same_stream_byte_for_byte(graph: EntityGraph, at: int) -> None:
    whole = _wire(GeneratorRun(Generator(CONFIG, graph=graph)), at + 3_000)
    first = GeneratorRun(Generator(CONFIG, graph=graph))
    head = _wire(first, at)
    snapshot = first.snapshot()
    resumed = Generator(CONFIG, graph=graph).resume(snapshot)
    assert resumed.emitted == at
    assert head + _wire(resumed, 3_000) == whole


def test_a_snapshot_taken_while_records_wait_keeps_them(graph: EntityGraph) -> None:
    """Peeking builds a step; the snapshot must carry the built records."""
    run = GeneratorRun(Generator(CONFIG, graph=graph))
    _wire(run, 50)
    waiting = run.peek()
    resumed = Generator(CONFIG, graph=graph).resume(run.snapshot())
    assert next(resumed).event == waiting.event


def test_the_stream_is_the_stream_it_always_was(graph: EntityGraph) -> None:
    """`stream` with a limit is a slice of the resumable run."""
    generator = Generator(CONFIG, graph=graph)
    assert [r.event for r in generator.stream(limit=500)] == [
        r.event for r in itertools.islice(GeneratorRun(generator), 500)
    ]


def test_a_snapshot_will_not_continue_a_different_stream(graph: EntityGraph) -> None:
    run = GeneratorRun(Generator(CONFIG, graph=graph))
    _wire(run, 10)
    other = GeneratorConfig(population=SMALL, events_per_second=60.0, start_time=START)
    with pytest.raises(SnapshotMismatchError):
        Generator(other, graph=graph).resume(run.snapshot())


# --- the sealed schedule ------------------------------------------------------


def test_a_sealed_schedule_runs_only_on_the_secret_it_was_sealed_with() -> None:
    commitment = SealedCommitment.seal(
        "correct horse", window_days=60, name="live", now=START
    ).to_document()
    schedule = live_schedule("sealed", secret="correct horse", commitment=commitment)
    assert schedule.name == "live"
    with pytest.raises(ScheduleMismatchError):
        live_schedule("sealed", secret="wrong horse", commitment=commitment)
    with pytest.raises(ScheduleMismatchError):
        live_schedule("sealed", secret=None, commitment=commitment)


# --- the feeds ------------------------------------------------------------------


class FakeClock:
    """A clock that moves only when told to, or when a feed sleeps."""

    def __init__(self, at: dt.datetime) -> None:
        """Start the clock.

        Args:
            at: The starting time.
        """
        self.at = at

    def now(self) -> dt.datetime:
        """The current fake time.

        Returns:
            The time.
        """
        return self.at

    def sleep(self, seconds: float) -> None:
        """Move the clock on.

        Args:
            seconds: How far.
        """
        self.at += dt.timedelta(seconds=seconds)


def _broker() -> MemoryBroker:
    broker = MemoryBroker()
    broker.create_topic("transactions", 1)
    broker.create_topic("labels", 1)
    return broker


def _read(broker: MemoryBroker, topic: str) -> list[bytes]:
    reader = broker.open()
    out: list[bytes] = []
    while batch := reader.consume(topic, "reader", max_records=10_000):
        out.extend(record.value for record in batch)
    return out


def _feed(
    broker: MemoryBroker, graph: EntityGraph, feed: Feed, tmp: Path, clock: FakeClock
) -> LiveFeed:
    store = SnapshotStore(tmp, feed)
    saved = store.load()
    generator = Generator(CONFIG, graph=graph)
    run = generator.resume(saved) if saved else GeneratorRun(generator)
    return LiveFeed(run, broker.open(), feed, store, clock=Clock(now=clock.now, sleep=clock.sleep))


def test_a_transaction_is_sent_when_its_time_comes_and_not_before(
    graph: EntityGraph, tmp_path: Path
) -> None:
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(seconds=10))
    feed = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    feed.step()
    sent = [TransactionEvent.model_validate_json(v) for v in _read(broker, "transactions")]
    assert sent
    assert all(event.event_time <= clock.at for event in sent)
    assert feed.due(feed.run.peek()) > clock.at


def test_a_label_is_sent_a_week_after_its_transaction(graph: EntityGraph, tmp_path: Path) -> None:
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(days=6, hours=23))
    feed = _feed(broker, graph, Feed.LABELS, tmp_path, clock)
    assert feed.step() == 0
    clock.at = START + dt.timedelta(days=7, seconds=10)
    feed.step()
    labels = [LabelEvent.model_validate_json(v) for v in _read(broker, "labels")]
    assert labels
    assert all(label.label_time <= clock.at for label in labels)
    assert min(label.label_time for label in labels) >= START + dt.timedelta(days=7)
    # What the label collector's lag is measured against (docs/failure-modes.md).
    latest = max(label.label_time for label in labels)
    assert feed.metrics.latest_due._value.get() == latest.timestamp()


def test_after_an_interruption_nothing_is_skipped(graph: EntityGraph, tmp_path: Path) -> None:
    """Lose the process, restart from the snapshot, catch up: nothing missing.

    Every transaction due by the end is on the topic, in order once
    duplicates are removed, and the ones sent after the last snapshot are on
    it twice: at least once, never at most once.
    """
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(seconds=30))
    first = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    first.snapshot_every = dt.timedelta(seconds=10)
    for _ in range(4):
        first.step()
        clock.sleep(10)
    first.snapshot_every = dt.timedelta(hours=1)
    first.step()  # sent after the last snapshot: these are sent again
    clock.sleep(60)  # the process is gone for a minute
    second = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    while second.step():
        pass

    sent = [TransactionEvent.model_validate_json(v) for v in _read(broker, "transactions")]
    expected = [r.event for r in Generator(CONFIG, graph=graph).stream(limit=len(sent) + 1_000)]
    expected = [e for e in expected if e.event_time <= clock.at]
    unique = list(dict.fromkeys(e.event_id for e in sent))
    assert unique == [e.event_id for e in expected]
    assert len(sent) > len(unique)


def test_a_fresh_start_deep_into_a_window_is_refused(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, Feed.TRANSACTIONS)
    late = START + dt.timedelta(days=3)
    with pytest.raises(NoSnapshotError):
        open_run(CONFIG, store, now=late)
    run = open_run(CONFIG, store, now=late, from_start=True)
    assert run.emitted == 0


def test_a_saved_place_is_replaced_whole(graph: EntityGraph, tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, Feed.LABELS)
    run = GeneratorRun(Generator(CONFIG, graph=graph))
    _wire(run, 5)
    store.save(run.snapshot())
    assert not store.path.with_suffix(".tmp").exists()
    assert Generator(CONFIG, graph=graph).resume(store.load() or b"").emitted == 5
    assert json.dumps(sorted(p.name for p in tmp_path.iterdir())) == '["labels.snapshot"]'


class SlowStore(SnapshotStore):
    """A store whose writes take as long as pickling the live run does."""

    def save(self, snapshot: bytes) -> None:
        """Write, slowly.

        Args:
            snapshot: The snapshot.
        """
        time.sleep(1.5)
        super().save(snapshot)


@pytest.mark.skipif(sys.platform == "win32", reason="background saves fork; Linux only")
def test_a_background_save_does_not_hold_the_stream_and_saves_the_same_place(
    graph: EntityGraph, tmp_path: Path
) -> None:
    """In the first dry run each save held the stream up by about 3.3 s, twice a minute."""
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(seconds=10))
    run = GeneratorRun(Generator(CONFIG, graph=graph))
    store = SlowStore(tmp_path, Feed.TRANSACTIONS)
    feed = LiveFeed(
        run,
        broker.open(),
        Feed.TRANSACTIONS,
        store,
        clock=Clock(now=clock.now, sleep=clock.sleep),
        save_in_background=True,
    )
    started = time.monotonic()
    feed.step()
    assert time.monotonic() - started < 1.0
    at_the_save = run.emitted
    clock.at += dt.timedelta(seconds=5)
    feed.step()
    assert run.emitted > at_the_save

    feed._wait_for_background_save(block=True)
    saved = store.load()
    assert saved is not None
    resumed = Generator(CONFIG, graph=graph).resume(saved)
    assert resumed.emitted == at_the_save
    assert feed.metrics.snapshots._value.get() == 1


def test_without_fork_a_feed_saves_in_the_foreground(graph: EntityGraph, tmp_path: Path) -> None:
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(seconds=10))
    feed = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    assert not feed.save_in_background
    feed.step()
    assert SnapshotStore(tmp_path, Feed.TRANSACTIONS).load() is not None


def test_a_feed_with_nothing_sent_since_its_last_save_does_not_save_again(
    graph: EntityGraph, tmp_path: Path
) -> None:
    """The labels feed sends nothing for a week; each save costs seconds of CPU."""
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(days=1))
    feed = _feed(broker, graph, Feed.LABELS, tmp_path, clock)
    assert feed.step() == 0
    for _ in range(5):
        clock.at += dt.timedelta(minutes=1)
        assert feed.step() == 0
    assert feed.metrics.snapshots._value.get() == 1
    clock.at = START + dt.timedelta(days=7, seconds=10)
    assert feed.step() > 0
    assert feed.metrics.snapshots._value.get() == 2


# --- the host's clock jumps (docs/failure-modes.md, clock skew) -------------


def test_a_clock_that_jumps_forward_is_caught_up_in_order_and_nothing_skipped(
    graph: EntityGraph, tmp_path: Path
) -> None:
    """An hour's jump makes an hour's records due at once.

    They go out `MAX_BATCH` at a time, flushed between, in event order, with
    nothing skipped or doubled; the lag metric says how far behind the feed
    is until it has caught up. The stream's event times do not move: only
    when they are sent does.
    """
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(seconds=10))
    feed = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    feed.step()
    clock.at += dt.timedelta(hours=1)
    steps = []
    while sent := feed.step():
        steps.append(sent)
        assert sent <= MAX_BATCH
        if len(steps) == 1:
            assert feed.metrics.lag._value.get() > 3_000
    assert len(steps) > 1, "an hour at this rate is more than one batch"
    assert feed.metrics.lag._value.get() < 1
    sent_events = [TransactionEvent.model_validate_json(v) for v in _read(broker, "transactions")]
    expected = [r.event for r in Generator(CONFIG, graph=graph).stream(limit=len(sent_events))]
    assert [e.event_id for e in sent_events] == [e.event_id for e in expected]
    assert all(e.event_time <= clock.at for e in sent_events)


def test_a_clock_that_jumps_back_sends_nothing_again_and_waits(
    graph: EntityGraph, tmp_path: Path
) -> None:
    """A step back in time sends nothing until the clock passes where it was.

    Nothing already sent is sent again: the feed's place is in the stream, not
    the clock. The stream is quiet for as long as the jump, which the scorer
    and the dashboard see as no traffic, and `ScorerStopped` fires if it is
    over fifteen minutes.
    """
    broker = _broker()
    clock = FakeClock(START + dt.timedelta(minutes=5))
    feed = _feed(broker, graph, Feed.TRANSACTIONS, tmp_path, clock)
    while feed.step():
        pass
    clock.at -= dt.timedelta(minutes=2)
    for _ in range(3):
        assert feed.step() == 0
        clock.at += dt.timedelta(seconds=30)
    clock.at = START + dt.timedelta(minutes=6)
    while feed.step():
        pass
    sent = [TransactionEvent.model_validate_json(v) for v in _read(broker, "transactions")]
    assert len(sent) > 0
    ids = [e.event_id for e in sent]
    assert len(ids) == len(set(ids)), "nothing sent twice"
