"""Driving the scorer at a fixed rate and measuring every decision.

One run: generate events, start the scorer on its own stream client, send the
events at the target rate from a second client, and time each decision from
the moment its transaction was handed to the stream to the moment its decision
was. Both timestamps come from `time.perf_counter_ns` in one process, which is
the same host clock `PLAN.md` section 2.4 asks for.

**What this measures, stated before the numbers.**

- The scorer and the load producer share a process, and therefore a Python
  interpreter lock. On the live instance they are separate processes. The
  local figure includes that contention; it is a ceiling, not a floor.
- The producer paces itself with `time.sleep`, and on Windows a sleep resolves
  to about 15 milliseconds, so events leave in small bursts rather than evenly.
  Real card traffic is bursty too, but the burst shape here is the timer's,
  not the traffic's. It is reported, not corrected.
- The model is the stand-in (`model.py`). A real model's time is measured again
  in week 5.
- The first `warmup` decisions of each run are excluded: connections opening
  and the first allocations, not the platform's steady state. How many is a
  parameter, and the report says.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import DEV_SCHEDULE
from verdict.events.schema import DecisionEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.scoring.timing import HOPS, HopSample, percentile, t_interval
from verdict.stream.base import Stream
from verdict.stream.memory import MemoryBroker

LOAD_POPULATION: Final = Population(cards=20_000, devices=15_000, merchants=500)
"""The reference population from `docs/generator-hashes.json`, small enough to
build in a second and large enough that most cards are not seen twice a
minute."""

TIMER_SLACK_NS: Final = 2_000_000
"""Sleep only when at least this far ahead of schedule; otherwise send now."""


@dataclass(frozen=True, slots=True)
class Topics:
    """A pair of topics for one run, and how to remove them."""

    transactions: str
    decisions: str
    teardown: Callable[[], None]


@dataclass(frozen=True, slots=True)
class Backend:
    """How to open stream clients for one kind of stream."""

    name: str
    open: Callable[[], Stream]
    topics: Callable[[], Topics]


def memory_backend() -> Backend:
    """An in-process stream. No broker; measures everything but the network.

    Returns:
        The backend.
    """
    broker = MemoryBroker()

    def topics() -> Topics:
        suffix = uuid.uuid4().hex[:8]
        names = Topics(f"transactions-{suffix}", f"decisions-{suffix}", lambda: None)
        broker.create_topic(names.transactions, 1)
        broker.create_topic(names.decisions, 4)
        return names

    return Backend("memory", broker.open, topics)


def redpanda_backend(bootstrap: str | None = None) -> Backend:
    """The local Redpanda broker, with fresh topics per run.

    Args:
        bootstrap: Broker address. Defaults to the compose stack's.

    Returns:
        The backend.
    """
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

    address = bootstrap or DEFAULT_BOOTSTRAP

    def open_stream() -> Stream:
        return RedpandaStream(address)

    def topics() -> Topics:
        admin = RedpandaStream(address)
        suffix = uuid.uuid4().hex[:8]
        transactions, decisions = f"load-transactions-{suffix}", f"load-decisions-{suffix}"
        admin.create_topic(transactions, 1)
        admin.create_topic(decisions, 4)

        def teardown() -> None:
            admin.delete_topic(transactions)
            admin.delete_topic(decisions)
            admin.close()

        return Topics(transactions, decisions, teardown)

    return Backend("redpanda", open_stream, topics)


def generate_events(count: int, *, rate: float, seed: int = 20270201) -> list[TransactionEvent]:
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
        send_rate: Events per second the producer actually achieved.
        end_to_end: p50, p95 and p99 of send to decision handed to the stream.
        hops: Per hop, p50 and p99.
        commit: p50 and p99 of per-batch flush and checkpoint.
        duplicates: Transactions the scorer skipped as already decided.
    """

    sent: int
    measured: int
    send_rate: float
    end_to_end: dict[str, float]
    hops: dict[str, dict[str, float]]
    commit: dict[str, float]
    duplicates: int


def _ms(values_ns: list[int], q: float) -> float:
    return percentile([value / 1e6 for value in values_ns], q)


def run_once(
    backend: Backend, events: list[TransactionEvent], *, rate: float, warmup: int
) -> RunResult:
    """Send events at a rate through the scorer and time every decision.

    Args:
        backend: The stream to run on.
        events: What to send, in time order.
        rate: Target sends per second.
        warmup: Decisions to leave out of the statistics.

    Returns:
        The run's measurements.

    Raises:
        RuntimeError: If the scorer does not decide every event in time.
    """
    topics = backend.topics()
    sent_at: dict[str, int] = {}
    samples: list[tuple[int, HopSample]] = []
    done = threading.Event()

    def on_decided(event: TransactionEvent, decision: DecisionEvent, sample: HopSample) -> None:
        del decision
        samples.append((sent_at[event.event_id], sample))

    scorer_stream = backend.open()
    producer_stream = backend.open()
    scorer = StreamScorer(
        scorer_stream,
        decider=Decider(
            features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel())
        ),
        transactions_topic=topics.transactions,
        decisions_topic=topics.decisions,
        group=f"scorer-{uuid.uuid4().hex[:8]}",
        on_decided=on_decided,
    )

    def score_until_done() -> None:
        while not (
            done.is_set()
            and scorer.decider.stats.decided + scorer.decider.stats.duplicates >= len(events)
        ):
            scorer.poll(max_records=500, timeout_seconds=0.01)

    thread = threading.Thread(target=score_until_done, name="scorer", daemon=True)
    try:
        thread.start()
        interval_ns = int(1e9 / rate)
        started = time.perf_counter_ns()
        for index, event in enumerate(events):
            due = started + index * interval_ns
            ahead = due - time.perf_counter_ns()
            if ahead > TIMER_SLACK_NS:
                time.sleep(ahead / 1e9)
            payload = event.to_json().encode("utf-8")
            sent_at[event.event_id] = time.perf_counter_ns()
            producer_stream.produce(topics.transactions, event.card_id, payload)
        producer_stream.flush()
        send_seconds = (time.perf_counter_ns() - started) / 1e9
        done.set()
        thread.join(timeout=120)
        if thread.is_alive():
            msg = f"scorer decided {scorer.decider.stats.decided} of {len(events)} in time"
            raise RuntimeError(msg)
    finally:
        done.set()
        scorer_stream.close()
        producer_stream.close()
        topics.teardown()

    kept = samples[warmup:]
    end_to_end = [sample.finished_ns - sent for sent, sample in kept]
    by_hop: dict[str, list[int]] = {
        "ingest": [sample.started_ns - sent for sent, sample in kept],
        "features": [sample.features_ns for _, sample in kept],
        "model": [sample.model_ns for _, sample in kept],
        "decision": [sample.decision_ns for _, sample in kept],
        "persist": [sample.persist_ns for _, sample in kept],
    }
    return RunResult(
        sent=len(events),
        measured=len(kept),
        send_rate=len(events) / send_seconds,
        end_to_end={f"p{q}": _ms(end_to_end, q) for q in (50, 95, 99)},
        hops={hop: {"p50": _ms(by_hop[hop], 50), "p99": _ms(by_hop[hop], 99)} for hop in HOPS},
        commit={"p50": _ms(scorer.commits.commit_ns, 50), "p99": _ms(scorer.commits.commit_ns, 99)},
        duplicates=scorer.decider.stats.duplicates,
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
        "hops_ms": {
            hop: {q: across([r.hops[hop][q] for r in results]) for q in ("p50", "p99")}
            for hop in HOPS
        },
        "commit_per_batch_ms": {q: across([r.commit[q] for r in results]) for q in ("p50", "p99")},
        "send_rate_per_second": across([r.send_rate for r in results]),
    }
