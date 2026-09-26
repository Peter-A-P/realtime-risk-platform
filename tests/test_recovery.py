"""A scorer restored from saved feature state serves what one never stopped would (ADR 27).

The headline test stops a scorer at points spread through several save
passes, restores a new one from the last complete save and the records after
it, and holds every feature it then serves to what an uninterrupted scorer
served for the same event. The stream carries what makes this hard: events
that share a timestamp (so the engine is holding some back when a pass
begins or a slice is taken), records sent twice, a record that cannot be
decoded, and a stop between deciding a batch and checkpointing it.

The rest hold the ways a save cannot be used, each of which must mean
starting cold rather than serving from wrong state.
"""

from __future__ import annotations

import gc
import pickle
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.schema import TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring import recovery
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.stream.base import Position, StreamError
from verdict.stream.memory import MemoryBroker, MemoryStream

POPULATION = Population(cards=150, devices=100, merchants=20)
BATCH = 7


@pytest.fixture(autouse=True)
def _thaw() -> Iterator[None]:
    """Restoring freezes what it built; the rest of the suite should not inherit it."""
    yield
    gc.unfreeze()


@pytest.fixture(scope="module")
def payloads() -> list[bytes]:
    """The stream: generated traffic, with the awkward cases mixed in."""
    graph = EntityGraph.build(seed=5, population=POPULATION)
    config = GeneratorConfig(
        seed=5, population=POPULATION, events_per_second=40.0, target_fraud_share=0.05
    )
    events = [record.event for record in Generator(config, graph).stream(limit=1_200)]
    out: list[bytes] = []
    for index, event in enumerate(events):
        out.append(event.to_json().encode())
        if index % 11 == 0:
            # A second event at the same instant: the engine holds both back.
            twin = event.model_copy(
                update={
                    "event_id": f"{event.event_id}-twin",
                    "amount_cents": event.amount_cents + 1,
                }
            )
            out.append(twin.to_json().encode())
        if index % 29 == 5 and index >= 20:
            out.append(events[index - 4].to_json().encode())  # sent again, a little later
        if index == 300:
            out.append(b"not a transaction")
    return out


class Recording(EngineFeatures):
    """Serves from the engine, and keeps what it served, by event."""

    def __init__(self, engine: FeatureEngine, served: dict[str, dict[str, float]]) -> None:
        """Wrap an engine, recording into `served`."""
        super().__init__(engine)
        self.served = served

    def serve(self, event: TransactionEvent) -> dict[str, float]:
        """Serve as the engine does, and keep a copy."""
        values = super().serve(event)
        self.served[event.event_id] = values
        return values


def a_broker(payloads: list[bytes]) -> MemoryBroker:
    broker = MemoryBroker()
    for topic, partitions in (("transactions", 1), ("decisions", 2), ("dead-letter", 1)):
        broker.create_topic(topic, partitions)
    stream = broker.open()
    for payload in payloads:
        stream.produce("transactions", "k", payload)
    return broker


def a_scorer(
    stream: MemoryStream, engine: FeatureEngine, served: dict[str, dict[str, float]]
) -> StreamScorer:
    return StreamScorer(
        stream,
        decider=Decider(features=Recording(engine, served), models=FixedModel(StandInModel())),
    )


@dataclass
class Ticker:
    """A clock that moves one unit every time it is read."""

    now: float = 0.0

    def __call__(self) -> float:
        """Move on one unit, and say where."""
        self.now += 1.0
        return self.now


@pytest.fixture(scope="module")
def reference(payloads: list[bytes]) -> dict[str, dict[str, float]]:
    """What a scorer that never stopped served for every event."""
    served: dict[str, dict[str, float]] = {}
    scorer = a_scorer(a_broker(payloads).open(), FeatureEngine(), served)
    while scorer.poll(max_records=BATCH):
        pass
    gc.unfreeze()
    return served


@dataclass
class Run:
    """A scorer saving its state as it goes, stopped part way."""

    broker: MemoryBroker
    directory: Path
    served: dict[str, dict[str, float]] = field(default_factory=dict)
    passes: int = 0


def run_until(payloads: list[bytes], directory: Path, polls: int, *, torn: bool) -> Run:
    """Score with saves every few polls, and stop after `polls`.

    Args:
        payloads: The stream.
        directory: Where saves go.
        polls: How many polls before the stop.
        torn: Stop between deciding the last batch and checkpointing it.
    """
    broker = a_broker(payloads)
    run = Run(broker, directory)
    engine = FeatureEngine()
    stream = broker.open()
    scorer = a_scorer(stream, engine, run.served)
    saver = recovery.Snapshotter(
        directory,
        scorer,
        engine,
        topic="transactions",
        every_seconds=60,
        slice_seconds=12,
        clock=Ticker(),
        background=False,
    )
    for done in range(polls):
        if torn and done == polls - 1:

            def refuse(*args: object) -> None:
                raise StreamError("stopped before the checkpoint")

            setattr(stream, "checkpoint", refuse)  # noqa: B010 - replacing a method
            with pytest.raises(StreamError):
                scorer.poll(max_records=BATCH)
            break
        if not scorer.poll(max_records=BATCH):
            break
        saver.step()
    run.passes = saver.passes
    return run


def resume(run: Run) -> tuple[recovery.Restored, dict[str, dict[str, float]]]:
    """Restore a new scorer from the save and score the rest of the stream."""
    outcome, served, _ = resume_scorer(run)
    return outcome, served


def resume_scorer(
    run: Run,
) -> tuple[recovery.Restored, dict[str, dict[str, float]], StreamScorer]:
    """As `resume`, and hand back the scorer."""
    stream = run.broker.open()
    outcome = recovery.restore(run.directory, stream, group="scorer", topic="transactions")
    assert isinstance(outcome, recovery.Restored), outcome
    served: dict[str, dict[str, float]] = {}
    scorer = a_scorer(stream, outcome.engine, served)
    for event_id in outcome.ledger:
        scorer.decider.remember(event_id)
    while scorer.poll(max_records=BATCH):
        pass
    return outcome, served, scorer


def a_whole_pass(
    directory: Path, scorer: StreamScorer, engine: FeatureEngine
) -> recovery.Snapshotter:
    """Save everything in one slice, between two batches."""
    saver = recovery.Snapshotter(
        directory,
        scorer,
        engine,
        topic="transactions",
        slice_seconds=10**9,
        clock=Ticker(),
        background=False,
    )
    for _ in range(3):  # begin, header, one slice
        saver.step()
    assert saver.passes == 1
    return saver


@pytest.mark.parametrize("torn", [False, True], ids=["after-checkpoint", "before-checkpoint"])
def test_a_restored_scorer_serves_what_one_that_never_stopped_would(
    payloads: list[bytes],
    reference: dict[str, dict[str, float]],
    tmp_path: Path,
    torn: bool,
) -> None:
    stops = range(40, 190, 7)
    restored_from_mid_stream = 0
    for stop in stops:
        directory = tmp_path / f"{stop}"
        run = run_until(payloads, directory, stop, torn=torn)
        if not run.passes:
            continue
        outcome, served = resume(run)
        restored_from_mid_stream += outcome.replayed > 0
        assert served, f"stopped after {stop} polls, the restored scorer decided nothing"
        wrong = [event_id for event_id, values in served.items() if values != reference[event_id]]
        assert not wrong, (
            f"stopped after {stop} polls ({run.passes} saves, {outcome.replayed} replayed): "
            f"{len(wrong)} of {len(served)} served differently, first {wrong[0]}"
        )
        # Nothing decided twice by the two scorers, except what was never checkpointed.
        again = set(served) & set(run.served)
        assert torn or not again
    assert restored_from_mid_stream >= len(stops) // 2


def test_a_save_taken_at_the_checkpoint_restores_exactly(
    payloads: list[bytes], reference: dict[str, dict[str, float]], tmp_path: Path
) -> None:
    """A pass taken whole between two batches, with only held-back events to replay."""
    broker = a_broker(payloads[:200])
    engine = FeatureEngine()
    scorer = a_scorer(broker.open(), engine, {})
    while scorer.poll(max_records=BATCH):
        pass
    a_whole_pass(tmp_path, scorer, engine)
    for payload in payloads[200:]:
        broker.open().produce("transactions", "k", payload)
    outcome, served = resume(Run(broker, tmp_path))
    assert outcome.entities == engine.tracked_entities
    assert all(values == reference[event_id] for event_id, values in served.items())


def test_a_record_sent_again_across_a_save_is_still_turned_away(
    payloads: list[bytes], reference: dict[str, dict[str, float]], tmp_path: Path
) -> None:
    """The original was decided before the save began, the copy arrives after the restore.

    Its features are safe either way, since a copy arriving later is also
    older than the engine's clock and is refused as late. What the saved
    ledger keeps right is the account: a redelivery, not a dead letter.
    """
    broker = a_broker(payloads[:100])
    engine = FeatureEngine()
    scorer = a_scorer(broker.open(), engine, {})
    while scorer.poll(max_records=BATCH):
        pass
    a_whole_pass(tmp_path, scorer, engine)
    sent_again = TransactionEvent.model_validate_json(payloads[80])
    stream = broker.open()
    stream.produce("transactions", "k", payloads[80])
    for payload in payloads[100:]:
        stream.produce("transactions", "k", payload)
    _, served, restored = resume_scorer(Run(broker, tmp_path))
    assert sent_again.event_id not in served
    assert "late" not in restored.dead_letters
    assert all(values == reference[event_id] for event_id, values in served.items())


def test_without_a_save_the_scorer_starts_cold(tmp_path: Path) -> None:
    outcome = recovery.restore(tmp_path, a_broker([]).open(), group="scorer", topic="transactions")
    assert isinstance(outcome, recovery.Cold)
    assert "no snapshot" in outcome.reason


def a_saved_run(payloads: list[bytes], directory: Path) -> Run:
    run = run_until(payloads, directory, 150, torn=False)
    assert run.passes
    return run


def test_state_saved_by_other_feature_code_is_not_read(
    payloads: list[bytes], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = a_saved_run(payloads, tmp_path)
    monkeypatch.setattr(recovery, "fingerprint", lambda: "a different engine")
    outcome = recovery.restore(tmp_path, run.broker.open(), group="scorer", topic="transactions")
    assert isinstance(outcome, recovery.Cold)
    assert "other feature code" in outcome.reason


def test_a_save_cut_short_is_not_read(payloads: list[bytes], tmp_path: Path) -> None:
    run = a_saved_run(payloads, tmp_path)
    path = tmp_path / recovery.SNAPSHOT_NAME
    path.write_bytes(path.read_bytes()[: path.stat().st_size // 2])
    outcome = recovery.restore(tmp_path, run.broker.open(), group="scorer", topic="transactions")
    assert isinstance(outcome, recovery.Cold)


def test_a_save_older_than_the_topic_keeps_is_not_read(
    payloads: list[bytes], tmp_path: Path
) -> None:
    run = a_saved_run(payloads, tmp_path)
    with (tmp_path / recovery.SNAPSHOT_NAME).open("rb") as file:
        header = pickle.load(file)
    assert isinstance(header.after, Position)
    run.broker.expire("transactions", 0, before=int(header.after.token) + 5)
    outcome = recovery.restore(tmp_path, run.broker.open(), group="scorer", topic="transactions")
    assert isinstance(outcome, recovery.Cold)
    assert "expired" in outcome.reason


def test_a_pass_in_progress_leaves_the_last_complete_one_in_place(
    payloads: list[bytes], tmp_path: Path
) -> None:
    run = a_saved_run(payloads, tmp_path)
    complete = (tmp_path / recovery.SNAPSHOT_NAME).read_bytes()
    engine = FeatureEngine()
    scorer = a_scorer(run.broker.open(), engine, {})
    scorer.poll(max_records=BATCH)
    saver = recovery.Snapshotter(
        tmp_path, scorer, engine, topic="transactions", slice_seconds=1, clock=Ticker()
    )
    for _ in range(3):
        saver.step()
    assert (tmp_path / f"{recovery.SNAPSHOT_NAME}.partial").exists()
    saver.abandon()
    assert not (tmp_path / f"{recovery.SNAPSHOT_NAME}.partial").exists()
    assert (tmp_path / recovery.SNAPSHOT_NAME).read_bytes() == complete


def test_the_fingerprint_follows_the_engines_code() -> None:
    """Saved aggregators are only read back into the code that pickled them."""
    first = recovery.fingerprint()
    assert first == recovery.fingerprint()
    assert len(first) == 64
