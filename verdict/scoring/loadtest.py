"""Driving the scorer at a fixed rate and measuring every decision.

One run: generate events, start the scorer on its own stream client, send the
events at the target rate, and time each decision from the moment its
transaction was handed to the stream to the moment its decision was. Both
timestamps come from `time.perf_counter_ns`. On Windows and Linux that counter
is the same system-wide monotonic source in every process, so a send timed in
the producer's process and a decision timed in the scorer's are on one clock;
`tests/test_scoring.py` checks that before the measurement is trusted.

**What this measures, stated before the numbers.**

- **On a broker, the load producer runs in its own process.** The first
  version of this file ran it in a thread beside the scorer, sharing one
  interpreter lock, and that turned out to be most of what it measured: send
  to receive through Redpanda was 8.5 ms at p50 with the producer in its own
  process and about 70 ms with it in the scorer's, against the same broker,
  the same settings and the same rate, with which of the two a run got
  decided by the scheduler rather than by anything in the platform.
  `docs/latency-budget.md` reports both. The memory backend keeps its
  producer in-process because its broker is a Python object that no other
  process can reach; its figure carries the contention, and is a ceiling.
- The producer paces itself with `time.sleep`. Every run holds a 1 ms Windows
  timer (`timing.fine_grained_timers`), because without one every wait in the
  process, including the Kafka client's, rounds up to about 15.6 ms; the
  report says whether it was granted. Sends still leave in small bursts, and
  the burst shape is the timer's rather than real traffic's. It is reported,
  not corrected.
- **A run reports whether a standing queue formed.** If the consumer cannot
  drain as fast as the producer fills, even briefly, the backlog does not
  clear and every later decision waits behind it. That is a throughput limit
  showing up in a latency statistic, and a percentile does not show it. So
  each run reports the median end to end over its first and last tenth:
  alike means the consumer kept up, and a larger `late_p50` is the depth of
  the queue that stood.
- The model is the stand-in (`model.py`). A real model's time is measured again
  in week 5.
- The first `warmup` decisions of each run are excluded: connections opening
  and the first allocations, not the platform's steady state. How many is a
  parameter, and the report says.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import DEV_SCHEDULE
from verdict.events.schema import DecisionEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, Model, StandInModel
from verdict.scoring.timing import (
    HOPS,
    HopSample,
    fine_grained_timers,
    percentile,
    t_interval,
)
from verdict.stream.base import Stream
from verdict.stream.memory import MemoryBroker

LOAD_POPULATION: Final = Population(cards=20_000, devices=15_000, merchants=500)
"""The reference population from `docs/generator-hashes.json`, small enough to
build in a second and large enough that most cards are not seen twice a
minute."""

LOAD_SEED: Final = 20270201
"""The generator seed every load run uses.

The producer subprocess regenerates the run's events from this rather than
being sent them, so it has to be the seed the parent used. It is a constant
rather than a default written out in two places for that reason.
"""

TIMER_SLACK_NS: Final = 2_000_000
"""Sleep the whole wait when at least this far ahead of schedule.

Below it the wait is still slept, but as a bare yield. The first version of
this file sent immediately when it was less than this far ahead, which at
1000 events per second means always: the interval is 1 ms, so the producer
never reached the threshold, never slept, and spun on the clock holding the
interpreter lock. The scorer thread then ran only when the lock was handed
over, and waits of over 100 ms turned up in the ingest hop of a backend with
no network in it at all. A yield costs the pacing nothing measurable, and the
run reports the rate it actually achieved either way.
"""


@dataclass(frozen=True, slots=True)
class Topics:
    """A run's topics, and how to remove them."""

    transactions: str
    decisions: str
    dead_letter: str
    teardown: Callable[[], None]
    shadow: str = "shadow"


@dataclass(frozen=True, slots=True)
class Sent:
    """When each transaction was handed to the stream, and by whom.

    Attributes:
        at_ns: Send time by event id.
        seconds: Wall time from the first send to the last acknowledgement.
        where: Where the producer ran, for the report.
    """

    at_ns: dict[str, int]
    seconds: float
    where: str


def _send_now(
    stream: Stream, topic: str, events: list[TransactionEvent], rate: float
) -> tuple[dict[str, int], float]:
    """Send events at a rate on this thread, recording when each left.

    Args:
        stream: An open producer.
        topic: Where to send them.
        events: What to send, in time order.
        rate: Target sends per second.

    Returns:
        The send times by event id, and the wall seconds the send took.
    """
    at_ns: dict[str, int] = {}
    interval_ns = int(1e9 / rate)
    started = time.perf_counter_ns()
    for index, event in enumerate(events):
        due = started + index * interval_ns
        ahead = due - time.perf_counter_ns()
        if ahead > TIMER_SLACK_NS:
            time.sleep(ahead / 1e9)
        elif ahead > 0:
            time.sleep(0)
        payload = event.to_json().encode("utf-8")
        at_ns[event.event_id] = time.perf_counter_ns()
        stream.produce(topic, event.card_id, payload)
    stream.flush()
    return at_ns, (time.perf_counter_ns() - started) / 1e9


@dataclass(frozen=True, slots=True)
class Backend:
    """How to open stream clients for one kind of stream, and how to send.

    Attributes:
        name: The stream's name, for the report.
        open: Open a client.
        topics: Make a run's topics and say how to remove them.
        send: Send a run's events, however this backend's producer runs.
    """

    name: str
    open: Callable[[], Stream]
    topics: Callable[[], Topics]
    send: Callable[[Topics, list[TransactionEvent], float], Sent]


def memory_backend() -> Backend:
    """An in-process stream. No broker; measures everything but the network.

    Its producer cannot leave the process, because the broker is an object in
    it. The run therefore has the producer and the scorer contending for one
    interpreter lock, and the result says so.

    Returns:
        The backend.
    """
    broker = MemoryBroker()

    def topics() -> Topics:
        suffix = uuid.uuid4().hex[:8]
        names = Topics(
            f"transactions-{suffix}",
            f"decisions-{suffix}",
            f"dead-letter-{suffix}",
            lambda: None,
            f"shadow-{suffix}",
        )
        broker.create_topic(names.transactions, 1)
        broker.create_topic(names.decisions, 4)
        broker.create_topic(names.dead_letter, 1)
        broker.create_topic(names.shadow, 4)
        return names

    def send(names: Topics, events: list[TransactionEvent], rate: float) -> Sent:
        stream = broker.open()
        try:
            at_ns, seconds = _send_now(stream, names.transactions, events, rate)
        finally:
            stream.close()
        return Sent(at_ns=at_ns, seconds=seconds, where="the scorer's process")

    return Backend("memory", broker.open, topics, send)


def redpanda_backend(bootstrap: str | None = None, *, compression: str = "none") -> Backend:
    """The local Redpanda broker, with fresh topics per run and its own producer.

    Args:
        bootstrap: Broker address. Defaults to the compose stack's.
        compression: The scorer's producer compression (ADR 18), measured
            here before the live stack switches it on.

    Returns:
        The backend.
    """
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    address = bootstrap or DEFAULT_BOOTSTRAP

    def open_stream() -> Stream:
        return RedpandaStream(address, compression=compression)

    def topics() -> Topics:
        admin = RedpandaStream(address)
        suffix = uuid.uuid4().hex[:8]
        transactions, decisions = f"load-transactions-{suffix}", f"load-decisions-{suffix}"
        dead_letter, shadow = f"load-dead-letter-{suffix}", f"load-shadow-{suffix}"
        admin.create_topic(transactions, 1)
        admin.create_topic(decisions, 4)
        admin.create_topic(dead_letter, 1)
        admin.create_topic(shadow, 4)

        def teardown() -> None:
            for topic in (transactions, decisions, dead_letter, shadow):
                admin.delete_topic(topic)
            admin.close()

        return Topics(transactions, decisions, dead_letter, teardown, shadow)

    def send(names: Topics, events: list[TransactionEvent], rate: float) -> Sent:
        return send_from_a_subprocess(address, names.transactions, len(events), rate)

    return Backend("redpanda", open_stream, topics, send)


def send_from_a_subprocess(bootstrap: str, topic: str, count: int, rate: float) -> Sent:
    """Run the producer in its own interpreter and collect its send times.

    The child regenerates the run's events from `LOAD_SEED` rather than being
    sent them, so nothing crosses the process boundary but the times.

    Args:
        bootstrap: Broker address.
        topic: Where to send.
        count: How many events.
        rate: Target sends per second.

    Returns:
        What the child recorded.

    Raises:
        RuntimeError: If the child failed.
    """
    with tempfile.TemporaryDirectory(prefix="verdict-load-") as directory:
        report = Path(directory) / "sent.json"
        finished = subprocess.run(
            [
                sys.executable,
                "-m",
                "verdict.scoring.loadtest",
                bootstrap,
                topic,
                str(count),
                str(rate),
                str(report),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if finished.returncode != 0:
            msg = f"the load producer failed: {finished.stderr.strip()[-500:]}"
            raise RuntimeError(msg)
        written = json.loads(report.read_text(encoding="utf-8"))
    return Sent(
        at_ns={event_id: int(at) for event_id, at in written["at_ns"].items()},
        seconds=float(written["seconds"]),
        where="its own process",
    )


def generate_events(count: int, *, rate: float, seed: int = LOAD_SEED) -> list[TransactionEvent]:
    """Generate the events a run sends, before the clock starts.

    Args:
        count: How many.
        rate: Events per second in event time, matched to the send rate so the
            engine's windows see the density the live stream will have.
        seed: Generator seed.

    Returns:
        The events, in time order.
    """
    graph = EntityGraph.build(seed, LOAD_POPULATION)
    config = GeneratorConfig(
        seed=seed, population=LOAD_POPULATION, events_per_second=rate, schedule=DEV_SCHEDULE
    )
    return [record.event for record in Generator(config, graph=graph).stream(limit=count)]


@dataclass(frozen=True, slots=True)
class RunResult:
    """One run's measurements, in milliseconds unless named otherwise.

    Attributes:
        sent: Events sent.
        measured: Decisions measured, after the warm-up.
        fine_timer: Whether a 1 ms Windows timer was held for the run.
        producer: Where the load producer ran.
        send_rate: Events per second the producer actually achieved.
        end_to_end: p50, p95 and p99 of send to decision handed to the stream.
        backlog: Median end to end over the first and last tenth of the
            measured decisions. Alike means the consumer kept up.
        batches: Batches the scorer consumed, decided and committed.
        records_per_batch: Transactions per batch, on average. The scorer
            pays one flush and one checkpoint per batch whatever its size,
            so this is what that fixed cost was amortised over, and it
            settles where the scorer's throughput meets the offered rate.
        hops: Per hop, p50 and p99.
        commit: p50 and p99 of per-batch flush and checkpoint together.
        flush: p50 and p99 of the flush alone.
        checkpoint: p50 and p99 of the offset commit alone.
        duplicates: Transactions the scorer skipped as already decided.
        set_aside: Transactions sent to the dead-letter topic, by reason. A
            run with any is not comparable to one without: they were never
            decided, so they are not in the latency figures either.
    """

    sent: int
    measured: int
    fine_timer: bool
    producer: str
    send_rate: float
    end_to_end: dict[str, float]
    backlog: dict[str, float]
    batches: int
    records_per_batch: float
    hops: dict[str, dict[str, float]]
    commit: dict[str, float]
    flush: dict[str, float]
    checkpoint: dict[str, float]
    duplicates: int
    set_aside: dict[str, int]


def _ms(values_ns: list[int], q: float) -> float:
    return percentile([value / 1e6 for value in values_ns], q)


def run_once(
    backend: Backend,
    events: list[TransactionEvent],
    *,
    rate: float,
    warmup: int,
    history: Path | None = None,
    model: Model | None = None,
    shadow: Model | None = None,
    shadow_when: Callable[[TransactionEvent, DecisionEvent], bool] | None = None,
) -> RunResult:
    """Send events at a rate through the scorer and time every decision.

    Args:
        backend: The stream to run on.
        events: What to send, in time order.
        rate: Target sends per second.
        warmup: Decisions to leave out of the statistics.
        history: Stage every decision under this directory, as the live
            scorer does (ADR 18), so the run measures that cost too.
        model: The model that decides; the stand-in if None.
        shadow: A model scored in shadow, as the live scorer does (ADR 11).
        shadow_when: Which decisions the shadow scores, if not every one.

    Returns:
        The run's measurements.

    Raises:
        RuntimeError: If the scorer does not decide every event in time, or a
            decision cannot be matched to a send.
    """
    topics = backend.topics()
    samples: list[tuple[str, HopSample]] = []
    done = threading.Event()

    def on_decided(event: TransactionEvent, decision: DecisionEvent, sample: HopSample) -> None:
        del decision
        samples.append((event.event_id, sample))

    scorer_stream = backend.open()
    spool = None
    if history is not None:
        from verdict.history.records import staged_schema
        from verdict.history.spool import SpoolWriter

        spool = SpoolWriter(history / uuid.uuid4().hex[:8], staged_schema())
    scorer = StreamScorer(
        scorer_stream,
        decider=Decider(
            features=EngineFeatures(FeatureEngine()),
            models=FixedModel(model or StandInModel()),
            shadow=None if shadow is None else FixedModel(shadow),
            shadow_when=shadow_when,
        ),
        transactions_topic=topics.transactions,
        decisions_topic=topics.decisions,
        shadow_topic=topics.shadow,
        dead_letter_topic=topics.dead_letter,
        group=f"scorer-{uuid.uuid4().hex[:8]}",
        on_decided=on_decided,
        history=spool,
    )

    def score_until_done() -> None:
        while not (
            done.is_set()
            and scorer.decider.stats.decided
            + scorer.decider.stats.duplicates
            + sum(scorer.dead_letters.values())
            >= len(events)
        ):
            scorer.poll(max_records=500, timeout_seconds=0.01)

    thread = threading.Thread(target=score_until_done, name="scorer", daemon=True)
    timers = fine_grained_timers()
    granted = timers.__enter__()
    try:
        thread.start()
        sent = backend.send(topics, events, rate)
        done.set()
        thread.join(timeout=120)
        if thread.is_alive():
            msg = f"scorer decided {scorer.decider.stats.decided} of {len(events)} in time"
            raise RuntimeError(msg)
    finally:
        done.set()
        if spool is not None:
            spool.close()
        scorer_stream.close()
        topics.teardown()
        timers.__exit__(None, None, None)

    kept = samples[warmup:]
    missing = [event_id for event_id, _ in kept if event_id not in sent.at_ns]
    if missing:
        msg = f"{len(missing)} decisions have no send time, first {missing[0]}"
        raise RuntimeError(msg)
    end_to_end = [sample.finished_ns - sent.at_ns[event_id] for event_id, sample in kept]
    by_hop: dict[str, list[int]] = {
        "ingest": [sample.started_ns - sent.at_ns[event_id] for event_id, sample in kept],
        "features": [sample.features_ns for _, sample in kept],
        "model": [sample.model_ns for _, sample in kept],
        "decision": [sample.decision_ns for _, sample in kept],
        "persist": [sample.persist_ns for _, sample in kept],
    }
    tenth = max(1, len(end_to_end) // 10)
    return RunResult(
        sent=len(events),
        measured=len(kept),
        fine_timer=granted,
        producer=sent.where,
        send_rate=len(events) / sent.seconds,
        end_to_end={f"p{q}": _ms(end_to_end, q) for q in (50, 95, 99)},
        backlog={
            "early_p50": _ms(end_to_end[:tenth], 50),
            "late_p50": _ms(end_to_end[-tenth:], 50),
        },
        batches=scorer.commits.batches,
        records_per_batch=len(events) / max(1, scorer.commits.batches),
        hops={hop: {"p50": _ms(by_hop[hop], 50), "p99": _ms(by_hop[hop], 99)} for hop in HOPS},
        commit={"p50": _ms(scorer.commits.commit_ns, 50), "p99": _ms(scorer.commits.commit_ns, 99)},
        flush={"p50": _ms(scorer.commits.flush_ns, 50), "p99": _ms(scorer.commits.flush_ns, 99)},
        checkpoint={
            "p50": _ms(scorer.commits.checkpoint_ns, 50),
            "p99": _ms(scorer.commits.checkpoint_ns, 99),
        },
        duplicates=scorer.decider.stats.duplicates,
        set_aside=dict(scorer.dead_letters),
    )


def summarise(results: list[RunResult]) -> dict[str, Any]:
    """Across-run means with 95 percent t intervals.

    Args:
        results: At least two runs.

    Returns:
        A report of intervals, in milliseconds.
    """

    def across(values: list[float]) -> dict[str, float | int]:
        return t_interval(values).rounded(3)

    return {
        "end_to_end_ms": {
            q: across([r.end_to_end[q] for r in results]) for q in ("p50", "p95", "p99")
        },
        "backlog_ms": {
            q: across([r.backlog[q] for r in results]) for q in ("early_p50", "late_p50")
        },
        "hops_ms": {
            hop: {q: across([r.hops[hop][q] for r in results]) for q in ("p50", "p99")}
            for hop in HOPS
        },
        "commit_per_batch_ms": {q: across([r.commit[q] for r in results]) for q in ("p50", "p99")},
        "flush_per_batch_ms": {q: across([r.flush[q] for r in results]) for q in ("p50", "p99")},
        "checkpoint_per_batch_ms": {
            q: across([r.checkpoint[q] for r in results]) for q in ("p50", "p99")
        },
        "records_per_batch": across([r.records_per_batch for r in results]),
        "send_rate_per_second": across([r.send_rate for r in results]),
    }


def _produce_for_a_parent() -> None:
    """The producer subprocess: generate, send at the rate, report the times.

    It takes its arguments from the command line, because the only things it
    shares with its parent are the broker and a file to write.
    """
    from verdict.stream.redpanda import RedpandaStream

    bootstrap, topic, count, rate, report = sys.argv[1:6]
    events = generate_events(int(count), rate=float(rate))
    with fine_grained_timers():
        stream = RedpandaStream(bootstrap)
        try:
            at_ns, seconds = _send_now(stream, topic, events, float(rate))
        finally:
            stream.close()
    Path(report).write_text(json.dumps({"at_ns": at_ns, "seconds": seconds}), encoding="utf-8")


if __name__ == "__main__":  # pragma: no cover - the subprocess entry point
    _produce_for_a_parent()
