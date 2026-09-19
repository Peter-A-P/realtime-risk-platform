"""The rollback drill: how long from flipping the flag to the old champion deciding.

`PLAN.md` section 2.5 makes rollback a flag read per event, and the definition
of done asks for the drill timed five times. One run:

1. the pointer names the new champion, with the old one as its rollback
   target (`flags.set_champion`);
2. a stream scorer decides transactions sent at a steady rate, following the
   pointer on every event (`flags.FlaggedModels`);
3. part way through, the drill calls `flags.rollback`, which is exactly what
   an operator's `verdict flag rollback` does, and notes the moment;
4. the run ends when the old champion has made a decision.

It reports the time from the rollback call returning to the first decision by
the old champion, and how many decisions the new one made after the call
returned (the plan's promise is none after the next event). The scorer, the
stream and the flag are the platform's own; only the models are given.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from verdict.events.schema import DecisionEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.flags import FlaggedModels, rollback, set_champion
from verdict.scoring.model import Model
from verdict.scoring.timing import HopSample
from verdict.stream.memory import MemoryBroker


@dataclass(frozen=True, slots=True)
class DrillRun:
    """One rollback, timed.

    Attributes:
        seconds_to_old_champion: From the rollback call returning to the old
            champion's first decision.
        decisions_by_new_after_rollback: Decisions the rolled-back model made
            after the call returned.
        decisions: Decisions in the run.
    """

    seconds_to_old_champion: float
    decisions_by_new_after_rollback: int
    decisions: int


def run_drill(
    flag: Path,
    known: Mapping[str, Model],
    *,
    old: str,
    new: str,
    events: list[TransactionEvent],
    rate: float = 1_000.0,
    rollback_after: int = 2_000,
) -> DrillRun:
    """Run one rollback drill on the in-process stream.

    Args:
        flag: The pointer file for this run.
        known: The models, by version.
        old: The version rolled back to.
        new: The version rolled back from.
        events: Transactions to send, in time order.
        rate: Sends per second.
        rollback_after: Decisions before the rollback.

    Returns:
        The run's timing.

    Raises:
        RuntimeError: If the old champion never decides.
    """
    flag.unlink(missing_ok=True)
    set_champion(flag, old, known)
    set_champion(flag, new, known)

    broker = MemoryBroker()
    for topic, partitions in (("transactions", 1), ("decisions", 4), ("dead-letter", 1)):
        broker.create_topic(topic, partitions)
    decided: list[tuple[int, str]] = []

    def on_decided(event: TransactionEvent, decision: DecisionEvent, sample: HopSample) -> None:
        del event, sample
        decided.append((time.perf_counter_ns(), decision.model_version))

    scorer = StreamScorer(
        broker.open(),
        decider=Decider(
            features=EngineFeatures(FeatureEngine()), models=FlaggedModels(flag, known)
        ),
        on_decided=on_decided,
    )
    done = threading.Event()

    def produce() -> None:
        stream = broker.open()
        gap = 1.0 / rate
        next_at = time.perf_counter()
        for event in events:
            if done.is_set():
                break
            next_at += gap
            while time.perf_counter() < next_at:
                time.sleep(0)
            stream.produce("transactions", event.card_id, event.to_json().encode("utf-8"))

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()
    rolled_at: int | None = None
    try:
        while producer.is_alive() or scorer.poll(max_records=50, timeout_seconds=0.0):
            scorer.poll(max_records=50, timeout_seconds=0.0)
            if rolled_at is None and len(decided) >= rollback_after:
                rollback(flag)
                rolled_at = time.perf_counter_ns()
            if rolled_at is not None and any(
                at > rolled_at and version == old for at, version in decided[-50:]
            ):
                break
    finally:
        done.set()
        producer.join(timeout=5)
    if rolled_at is None:
        msg = f"only {len(decided)} decisions; the rollback never ran"
        raise RuntimeError(msg)
    after = [(at, version) for at, version in decided if at > rolled_at]
    first_old = next((at for at, version in after if version == old), None)
    if first_old is None:
        msg = "the old champion never decided after the rollback"
        raise RuntimeError(msg)
    return DrillRun(
        seconds_to_old_champion=(first_old - rolled_at) / 1e9,
        decisions_by_new_after_rollback=sum(1 for _, version in after if version == new),
        decisions=len(decided),
    )
