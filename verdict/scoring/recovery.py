"""Saving the scorer's feature state as it runs, and starting from it (ADR 27).

A scorer that starts with an empty engine serves every card "no history"
until its windows refill, which for the day-long ones is a day. On the dry
run the instance was replaced eleven times in three days, eight of them in
one day, so without this the scorer would almost never have been on full
history. This keeps a copy of the engine's state on the data volume, and a
replacement starts from it and replays only what came after.

**The state is saved a slice at a time, on the scorer's own thread, between
batches.** The engine is millions of small Python objects, and there is no
cheap way to copy them whole. Pickling them in one go takes about 13 s per
gigabyte of engine (measured 2026-09-26), which would hold every decision
behind it for a minute and a half at the live size. A forked child would not
hold the scorer, but reading an object in CPython writes its reference count,
so the child copies every page it pickles, and on a 16 GB instance with a
7.5 GB engine that is the kernel's out-of-memory killer. So each step pickles
entities for at most `SLICE_SECONDS` and then gives the thread back, and a
whole pass takes some minutes while the scorer goes on deciding.

**Each slice says which records it holds, and that is what makes the pass
consistent.** A pass begins at a stream position, `after`: the last record
before the events the engine is still holding back. Every slice records how
many records after that the scorer had handled when it was taken, and which
events were still held back then. Restoring, the replay reads every record
after `after` and, for each entity, folds in only the events its slice had
not already taken. An entity with no slice (new since the pass began, or
dropped by pruning before its turn) takes every event after `after`, which
is its whole history or, for one pruned, history that had all expired.
`tests/test_recovery.py` holds a restored scorer to the features an
uninterrupted one serves, crashing it at every point of a pass.

**It fails towards starting cold, never towards wrong state.** No snapshot, a
snapshot written by other feature code (`fingerprint`), one cut short, or one
older than the topic keeps (a day, ADR 18): each means the replay cannot be
exact, and the scorer starts with an empty engine, as it did before, and says
why.
"""

from __future__ import annotations

import datetime as dt
import gc
import hashlib
import os
import pickle
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final

from pydantic import ValidationError

from verdict.events.schema import UnknownSchemaVersionError, decode_transaction
from verdict.features import aggregators as aggregators_module
from verdict.features import engine as engine_module
from verdict.features.engine import FeatureEngine, LateEventError
from verdict.store.features import EntityKind, entity_id_of
from verdict.stream.base import Position, PositionGoneError, Rereadable

if TYPE_CHECKING:  # pragma: no cover - types only
    from verdict.scoring.consumer import StreamScorer

SNAPSHOT_NAME: Final = "engine.snapshot"
"""The last complete pass, in the snapshot directory. A pass in progress is
written beside it with `.partial` added and renamed over it when complete."""

FORMAT: Final = 1
"""The file's own layout. Part of the fingerprint."""

SLICE_SECONDS: Final = 0.002
"""The longest one step pickles for before giving the thread back.

Every decision behind a step waits for it, so it is well inside the 50 ms
budget, where the live p99 was 22 ms on the dry run."""

GAP_SECONDS: Final = 0.008
"""The least time between two steps: with `SLICE_SECONDS`, a pass takes at
most a fifth of the scorer's thread however often it polls."""

EVERY_SECONDS: Final = 900.0
"""How often a pass starts. A restore replays from the start of the last
complete pass, so at most about this plus one pass's length of records."""

LEDGER_SAVED: Final = 40_000
"""Decided event ids saved with each pass, so a record sent twice across the
save is still recognised as a redelivery. The feeds resend at most 30 s after a restart
(ADR 15), which is 30,000 at the live rate."""


def fingerprint() -> str:
    """What must match for saved state to be read back into this code.

    The file layout, and the source of the engine and its aggregators: state
    pickled by one version of an aggregator is not state for another, and a
    new image that changes either starts cold once rather than serving
    features from objects it did not define.

    Returns:
        A hex digest.
    """
    digest = hashlib.sha256(f"format {FORMAT}\n".encode())
    for module in (engine_module, aggregators_module):
        digest.update(Path(module.__file__ or "").read_bytes())
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class Header:
    """The start of a pass.

    Attributes:
        fingerprint: `fingerprint()` of the code that wrote it.
        topic: The topic the state was built from.
        partition: Its partition; the engine needs one, in order.
        after: The last record before the pass's first; None if the pass
            starts at the partition's beginning.
        ledger: Event ids decided up to `after`, oldest first.
        started_at: Wall-clock time the pass began.
    """

    fingerprint: str
    topic: str
    partition: str
    after: Position | None
    ledger: list[str]
    started_at: dt.datetime


@dataclass(frozen=True, slots=True)
class Slice:
    """Some entities' state, and what it holds.

    Attributes:
        records: Records after `after` the scorer had handled when this was
            taken; every one of them is in these entities' state unless it
            is in `pending`.
        pending: Events served but still held back by the engine then.
        entities: (kind, entity id, pickled aggregators).
    """

    records: int
    pending: frozenset[str]
    entities: list[tuple[EntityKind, str, bytes]]


@dataclass(frozen=True, slots=True)
class Trailer:
    """The end of a complete pass.

    Attributes:
        entities: Entities saved.
        reached: The latest event time the engine had taken at the end.
        finished_at: Wall-clock time the pass ended.
    """

    entities: int
    reached: dt.datetime | None
    finished_at: dt.datetime


@dataclass(frozen=True, slots=True)
class SavedPass:
    """What one complete pass did, for metrics and the log.

    Attributes:
        entities: Entities saved.
        seconds: Wall time from start to finish.
        bytes: Size of the file.
    """

    entities: int
    seconds: float
    bytes: int


class Snapshotter:
    """Saves the engine a slice at a time, between the scorer's batches.

    A pass is a sequence of steps, each one short: the first notes where the
    pass begins and copies the list of entities; the second writes the
    header; each after that pickles entities for up to `slice_seconds`; and
    the last hands the file to a thread that waits for it to reach the disk
    and renames it into place, since a whole pass is hundreds of megabytes
    and `fsync` on it would hold the scorer for seconds. The waiting is a
    system call, which does not hold the interpreter's lock.
    """

    def __init__(
        self,
        directory: Path,
        scorer: StreamScorer,
        engine: FeatureEngine,
        *,
        topic: str,
        partition: str = "0",
        every_seconds: float = EVERY_SECONDS,
        slice_seconds: float = SLICE_SECONDS,
        gap_seconds: float = GAP_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        on_saved: Callable[[SavedPass], None] | None = None,
        background: bool = True,
    ) -> None:
        """Prepare to save.

        Args:
            directory: Where the snapshot lives: on the data volume, which
                outlives the instance.
            scorer: The scorer whose position the state is tied to.
            engine: Its engine.
            topic: The topic it consumes.
            partition: The partition; the transactions topic has one.
            every_seconds: How often a pass starts.
            slice_seconds: The longest one step pickles for.
            gap_seconds: The least time between the end of one step and the
                start of the next, which caps the share of the thread a pass
                takes.
            clock: A monotonic clock; tests pass a fake.
            on_saved: Called when a pass completes.
            background: Finish a pass on a thread of its own. Tests pass
                False, to see it finish.
        """
        self.directory = directory
        self.scorer = scorer
        self.engine = engine
        self.topic = topic
        self.partition = partition
        self.every_seconds = every_seconds
        self.slice_seconds = slice_seconds
        self.gap_seconds = gap_seconds
        self.clock = clock
        self.on_saved = on_saved
        self.background = background
        self.passes = 0
        self._file: IO[bytes] | None = None
        self._header: Header | None = None
        self._finisher: threading.Thread | None = None
        self._last_start: float | None = None
        self._last_step = float("-inf")
        self._keys: list[tuple[EntityKind, str]] = []
        self._next = 0
        self._saved = 0
        self._base = 0
        self._began = 0.0
        self._fingerprint = fingerprint()

    @property
    def path(self) -> Path:
        """The last complete snapshot.

        Returns:
            Its path.
        """
        return self.directory / SNAPSHOT_NAME

    @property
    def partial(self) -> Path:
        """The pass being written.

        Returns:
            Its path.
        """
        return self.directory / f"{SNAPSHOT_NAME}.partial"

    def step(self) -> None:
        """Do one step's work, if one is due."""
        now = self.clock()
        if now - self._last_step < self.gap_seconds:
            return
        if self._file is None:
            if self._finisher is not None and self._finisher.is_alive():
                return
            if self._last_start is None or now - self._last_start >= self.every_seconds:
                self._begin(now)
        elif self._header is not None:
            self._write_header()
        else:
            self._save_slice()
        self._last_step = self.clock()

    def abandon(self) -> None:
        """Stop a pass in progress, leaving the last complete one in place."""
        if self._finisher is not None:
            self._finisher.join()
        if self._file is not None:
            self._file.close()
            self._file = None
            self._header = None
            self._keys = []
            self.partial.unlink(missing_ok=True)

    def _begin(self, now: float) -> None:
        recent = list(self.scorer.recent)
        pending = self.engine.pending_ids()
        split = len(recent)
        if pending:
            first = next(
                (index for index, (_, event_id) in enumerate(recent) if event_id in pending),
                None,
            )
            if first is None:
                return  # held-back events older than the scorer remembers: try later
            split = first
        after = recent[split - 1][0] if split else self.scorer.before_recent
        since = recent[split:]
        handled_since = {event_id for _, event_id in since if event_id is not None}
        ledger = [
            event_id
            for event_id in self.scorer.decider.ledger_tail(LEDGER_SAVED + len(handled_since))
            if event_id not in handled_since
        ][-LEDGER_SAVED:]
        self._header = Header(
            fingerprint=self._fingerprint,
            topic=self.topic,
            partition=self.partition,
            after=after,
            ledger=ledger,
            started_at=dt.datetime.now(dt.UTC),
        )
        self._base = self.scorer.consumed - len(since)
        self._keys = self.engine.entity_keys()
        self._next = self._saved = 0
        self._last_start = self._began = now
        self.directory.mkdir(parents=True, exist_ok=True)
        self._file = self.partial.open("wb")

    def _write_header(self) -> None:
        assert self._file is not None
        pickle.dump(self._header, self._file, protocol=pickle.HIGHEST_PROTOCOL)
        self._header = None

    def _save_slice(self) -> None:
        assert self._file is not None
        deadline = self.clock() + self.slice_seconds
        entities: list[tuple[EntityKind, str, bytes]] = []
        while self._next < len(self._keys):
            kind, entity_id = self._keys[self._next]
            self._next += 1
            state = self.engine.entity_state(kind, entity_id)
            if state is not None:
                entities.append(
                    (kind, entity_id, pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL))
                )
            if self.clock() >= deadline:
                break
        pickle.dump(
            Slice(
                records=self.scorer.consumed - self._base,
                pending=self.engine.pending_ids(),
                entities=entities,
            ),
            self._file,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        self._saved += len(entities)
        if self._next >= len(self._keys):
            self._finish()

    def _finish(self) -> None:
        file = self._file
        assert file is not None
        pickle.dump(
            Trailer(
                entities=self._saved,
                reached=self.engine.reached,
                finished_at=dt.datetime.now(dt.UTC),
            ),
            file,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        file.flush()
        self._file = None
        self._keys = []
        saved, began = self._saved, self._began

        def settle() -> None:
            os.fsync(file.fileno())
            file.close()
            size = self.partial.stat().st_size
            self.partial.replace(self.path)
            self.passes += 1
            if self.on_saved is not None:
                self.on_saved(SavedPass(entities=saved, seconds=self.clock() - began, bytes=size))

        if self.background:
            self._finisher = threading.Thread(target=settle, name="engine-snapshot", daemon=True)
            self._finisher.start()
        else:
            settle()


@dataclass(slots=True)
class Restored:
    """An engine rebuilt from a snapshot and the records after it.

    Attributes:
        engine: The engine, as the scorer had it at `through`.
        ledger: Event ids to remember as decided, oldest first.
        recent: (position, event id) for the last records replayed.
        before_recent: The record before the first in `recent`.
        through: The last record replayed: where the group had got.
        entities: Entities restored from the snapshot.
        replayed: Records replayed.
        snapshot_started_at: When the pass restored from began.
        seconds: Wall time the restore took.
    """

    engine: FeatureEngine
    ledger: list[str]
    recent: list[tuple[Position, str | None]]
    before_recent: Position | None
    through: Position | None
    entities: int
    replayed: int
    snapshot_started_at: dt.datetime
    seconds: float


@dataclass(frozen=True, slots=True)
class Cold:
    """Why the scorer starts with an empty engine.

    Attributes:
        reason: In a sentence, for the log.
    """

    reason: str


@dataclass(slots=True)
class _Watermarks:
    records: list[int] = field(default_factory=list)
    pending: list[frozenset[str]] = field(default_factory=list)
    slice_of: dict[tuple[EntityKind, str], int] = field(default_factory=dict)


def restore(
    directory: Path,
    stream: Rereadable,
    *,
    group: str,
    topic: str,
    partition: str = "0",
    keep_recent: int = 10_000,
    clock: Callable[[], float] = time.monotonic,
) -> Restored | Cold:
    """Rebuild the engine from the last snapshot and the records after it.

    Args:
        directory: Where the snapshot lives.
        stream: The stream, to read the records after the snapshot again.
        group: The scorer's consumer group, whose checkpoint the replay
            stops at: the scorer carries on from there.
        topic: The topic.
        partition: The partition.
        keep_recent: How many replayed positions to hand back.
        clock: A monotonic clock.

    Returns:
        The rebuilt engine, or why there is none.
    """
    began = clock()
    path = directory / SNAPSHOT_NAME
    if not path.exists():
        return Cold(f"no snapshot at {path}")
    engine = FeatureEngine()
    marks = _Watermarks()
    gc.disable()
    try:
        with path.open("rb") as file:
            # Written only by this scorer, on its own volume.
            header = pickle.load(file)
            if not isinstance(header, Header) or header.fingerprint != fingerprint():
                return Cold("the snapshot was written by other feature code")
            if header.topic != topic or header.partition != partition:
                return Cold(f"the snapshot is of {header.topic} {header.partition}")
            try:
                trailer = _load_slices(file, engine, marks)
            except (pickle.UnpicklingError, AttributeError, ImportError, TypeError) as error:
                return Cold(f"the snapshot could not be read: {error}")
        if trailer is None:
            return Cold("the snapshot is incomplete")
        through = stream.committed(topic, group, partition)
        ledger = list(header.ledger)
        seen = set(ledger)
        recent: list[tuple[Position, str | None]] = []
        before_recent = header.after
        replayed = 0
        if through is not None:
            try:
                for index, record in enumerate(
                    stream.reread(topic, partition, header.after, through)
                ):
                    event_id = _replay_one(record.value, index, engine, marks, seen, ledger)
                    recent.append((record.position, event_id))
                    replayed += 1
                    if len(recent) > 2 * keep_recent:
                        before_recent = recent[-keep_recent - 1][0]
                        del recent[:-keep_recent]
            except PositionGoneError as error:
                return Cold(f"the records after the snapshot have expired: {error}")
        engine.resume_at(trailer.reached)
    finally:
        gc.enable()
        gc.freeze()
    if len(recent) > keep_recent:
        before_recent = recent[-keep_recent - 1][0]
        del recent[:-keep_recent]
    return Restored(
        engine=engine,
        ledger=ledger,
        recent=recent,
        before_recent=before_recent,
        through=through if through is not None else header.after,
        entities=trailer.entities,
        replayed=replayed,
        snapshot_started_at=header.started_at,
        seconds=clock() - began,
    )


def _load_slices(file: IO[bytes], engine: FeatureEngine, marks: _Watermarks) -> Trailer | None:
    while True:
        try:
            item = pickle.load(file)
        except EOFError:
            return None
        if isinstance(item, Trailer):
            return item
        if not isinstance(item, Slice):
            return None
        index = len(marks.records)
        marks.records.append(item.records)
        marks.pending.append(item.pending)
        for kind, entity_id, blob in item.entities:
            engine.restore_entity(kind, entity_id, pickle.loads(blob))
            marks.slice_of[(kind, entity_id)] = index


def _replay_one(
    value: bytes,
    index: int,
    engine: FeatureEngine,
    marks: _Watermarks,
    seen: set[str],
    ledger: list[str],
) -> str | None:
    """Take one record again, as the scorer took it the first time.

    Returns:
        The event id, if the record decoded.
    """
    try:
        event = decode_transaction(value)
    except (UnknownSchemaVersionError, UnicodeDecodeError, ValidationError):
        return None  # set aside the first time, and again now
    if event.event_id in seen:
        return event.event_id  # a duplicate, turned away the first time
    skip: set[tuple[EntityKind, str]] = set()
    for kind in engine.kinds:
        entity_id = entity_id_of(event, kind)
        if entity_id is None:
            continue
        taken = marks.slice_of.get((kind, entity_id))
        if (
            taken is not None
            and index < marks.records[taken]
            and event.event_id not in marks.pending[taken]
        ):
            skip.add((kind, entity_id))
    try:
        engine.replay(event, frozenset(skip))
    except LateEventError:
        return event.event_id  # refused as late the first time too
    seen.add(event.event_id)
    ledger.append(event.event_id)
    return event.event_id
