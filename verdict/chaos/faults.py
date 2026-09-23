"""Faults done to the platform while the scorer decides a steady stream.

A measuring instrument, run against the local Redpanda container, not part of
the platform. It sends generated transactions at a steady rate through fresh
topics, runs the real scorer as the service runs it (`scoring/service.py`)
under the live stack's restart policy, does one fault for a set time, and
waits for everything sent to be decided. The faults:

- **`pause`**: the broker's container frozen with `docker pause`, which is
  what a hung or restarting broker looks like to a client: connections
  open, nothing answering.
- **`throttle`**: the broker held to a twentieth of one CPU with `docker
  update --cpus`, which is a broker that answers, slowly: a noisy neighbour,
  a disk stall, a stream being throttled.
- **`scorer-stall`**: the scorer itself stops for the time given, inside a
  batch, while the stream keeps arriving: consumer lag, as a long pause in
  the process or a slow model would make it.

Each can come at a wall-clock moment, or from inside a scorer batch after a
transaction is decided and before its decision is flushed (`mid_batch`),
the moment an outage is worst for the scorer. `scorer-stall` is always the
second.

What it reports, for `docs/failure-modes.md`: whether the scorer survived,
and if not when and why, and that its restart started cold (every card "no
history" until its windows refill, up to a day live, ADR 8); how far behind
the stream decisions fell and how long after the fault they caught up; and
every transaction decided once, more than once, or not at all.

The producer is the instrument's, not a platform feed: it waits as long as
it has to so that it keeps sending the same stream through the fault, and
whatever the scorer did is the scorer's.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import platform
import subprocess
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Final

from verdict.events.schema import DecisionEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring import service
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.loadtest import generate_events
from verdict.scoring.model import FixedModel, StandInModel
from verdict.scoring.timing import HopSample
from verdict.stream.redpanda import DEFAULT_BOOTSTRAP, RedpandaStream

FAULTS: Final = ("pause", "throttle", "scorer-stall")
INSTRUMENT_FLUSH_SECONDS: Final = 900.0
"""How long the instrument's own producer waits for the broker: past any fault tried."""
THROTTLED_CPUS: Final = 0.05
CAUGHT_UP_SECONDS: Final = 1.0
"""A decision this close behind its send counts as caught up."""


@dataclass(slots=True)
class ScorerLife:
    """One scorer process's run.

    Attributes:
        started_s: Seconds from the experiment's start.
        stopped_s: When it stopped, if it did before being asked to.
        error: Why, if it stopped on an error.
        decided: Transactions it decided.
    """

    started_s: float
    stopped_s: float | None = None
    error: str | None = None
    decided: int = 0


@dataclass(slots=True)
class FaultReport:
    """What one fault did.

    Attributes:
        measured_at: When.
        host: The machine, briefly.
        fault: Which fault.
        rate_per_second: Transactions sent a second.
        sent: Transactions sent.
        fault_at_s: When the fault began, from the start.
        fault_seconds: For how long.
        mid_batch: Whether it began inside a scorer's batch.
        scorers: Each scorer process, in order; more than one is a restart.
        restarted_cold: Whether a restart threw away the feature windows.
        max_behind_s: The furthest a decision fell behind its send.
        caught_up_after_s: From the fault's end to the first decision back
            within `CAUGHT_UP_SECONDS` of its send.
        decided_once: Transactions with exactly one decision.
        decided_twice_or_more: Transactions decided more than once.
        undecided: Transactions with no decision and not set aside.
        set_aside: Records sent to the dead-letter topic, by reason.
    """

    measured_at: str
    host: str
    fault: str
    rate_per_second: float
    sent: int
    fault_at_s: float
    fault_seconds: float
    mid_batch: bool
    scorers: list[ScorerLife] = field(default_factory=list)
    restarted_cold: bool = False
    max_behind_s: float = 0.0
    caught_up_after_s: float | None = None
    decided_once: int = 0
    decided_twice_or_more: int = 0
    undecided: int = 0
    set_aside: dict[str, int] = field(default_factory=dict)


def _nothing() -> None:
    """A fault with nothing to do to a container: the scorer stalls itself."""


def _docker(*args: str) -> str:
    done = subprocess.run(["docker", *args], check=True, capture_output=True, text=True)
    return done.stdout.strip()


def container_fault(
    fault: str, container: str
) -> tuple[Callable[[], object], Callable[[], object]]:
    """Begin and end a fault on the broker's container.

    Args:
        fault: `pause` or `throttle`.
        container: The container.

    Returns:
        Begin, and end.

    Raises:
        ValueError: For a fault that is not the container's.
    """
    if fault == "pause":
        return (lambda: _docker("pause", container)), (lambda: _docker("unpause", container))
    if fault == "throttle":
        # `--cpus 0` does not lift a limit once set; the machine's count does.
        everything = _docker("info", "--format", "{{.NCPU}}")
        return (
            (lambda: _docker("update", "--cpus", str(THROTTLED_CPUS), container)),
            (lambda: _docker("update", "--cpus", everything, container)),
        )
    msg = f"{fault} is not a fault on the broker"
    raise ValueError(msg)


def _send(bootstrap: str, topic: str, events: list[TransactionEvent], rate: float) -> None:
    """Send at a steady rate, waiting out the broker rather than failing."""
    stream = RedpandaStream(bootstrap)
    started = time.monotonic()
    try:
        for index, event in enumerate(events):
            ahead = started + index / rate - time.monotonic()
            if ahead > 0:
                time.sleep(ahead)
            stream.produce(topic, event.card_id, event.to_json().encode("utf-8"))
        stream.flush(INSTRUMENT_FLUSH_SECONDS)
    finally:
        stream.close()


def _read_all(bootstrap: str, topic: str, quiet_seconds: float = 5.0) -> list[bytes]:
    """Every record on a topic, read by a group of its own until it goes quiet."""
    stream = RedpandaStream(bootstrap)
    group = f"chaos-reader-{uuid.uuid4().hex[:8]}"
    values: list[bytes] = []
    last = time.monotonic()
    try:
        while time.monotonic() - last < quiet_seconds:
            records = stream.consume(topic, group, max_records=5_000, timeout_seconds=0.5)
            if records:
                values.extend(record.value for record in records)
                last = time.monotonic()
    finally:
        stream.close()
    return values


def run_fault(
    fault: str,
    *,
    seconds: float,
    at_seconds: float = 10.0,
    rate: float = 1_000.0,
    count: int = 90_000,
    bootstrap: str = DEFAULT_BOOTSTRAP,
    container: str = "verdict-redpanda",
    settle_seconds: float = 120.0,
    mid_batch: bool = True,
) -> FaultReport:
    """Do one fault while the scorer decides a steady stream.

    Args:
        fault: One of `FAULTS`.
        seconds: How long it lasts.
        at_seconds: When it begins, from the first send.
        rate: Transactions a second.
        count: How many, in all; at the rate, the run's length.
        bootstrap: The broker.
        container: The broker's container.
        settle_seconds: After the last send, how long the scorer has to
            finish before it is stopped.
        mid_batch: Begin inside a scorer batch rather than at a wall-clock
            moment. Always so for `scorer-stall`.

    Returns:
        The report.

    Raises:
        ValueError: For an unknown fault.
    """
    if fault not in FAULTS:
        msg = f"unknown fault {fault}; one of {', '.join(FAULTS)}"
        raise ValueError(msg)
    stall = fault == "scorer-stall"
    mid_batch = mid_batch or stall
    begin, end = (_nothing, _nothing) if stall else container_fault(fault, container)

    admin = RedpandaStream(bootstrap)
    suffix = uuid.uuid4().hex[:8]
    names = {
        "transactions": (f"chaos-transactions-{suffix}", 1),
        "decisions": (f"chaos-decisions-{suffix}", 4),
        "dead_letter": (f"chaos-dead-letter-{suffix}", 1),
        "shadow": (f"chaos-shadow-{suffix}", 1),
    }
    for topic, partitions in names.values():
        admin.create_topic(topic, partitions)
    events = generate_events(count, rate=rate)
    index = {event.event_id: i for i, event in enumerate(events)}
    report = FaultReport(
        measured_at=dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        host=f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        fault=fault,
        rate_per_second=rate,
        sent=count,
        fault_at_s=at_seconds,
        fault_seconds=seconds,
        mid_batch=mid_batch,
    )
    started = time.monotonic()
    begun = threading.Event()
    ended_at: list[float] = []
    decided: list[tuple[float, float]] = []  # (when, how far behind its send)
    decided_ids: set[str] = set()
    stop = threading.Event()

    def the_fault() -> None:
        begin()
        begun.set()
        time.sleep(seconds)
        end()
        ended_at.append(time.monotonic() - started)

    def on_decided(event: TransactionEvent, decision: DecisionEvent, sample: HopSample) -> None:
        del decision, sample
        now = time.monotonic() - started
        decided.append((now, now - index[event.event_id] / rate))
        decided_ids.add(event.event_id)
        if mid_batch and not begun.is_set() and now >= at_seconds:
            # Inside the batch: this decision is made and not yet flushed.
            if stall:
                begun.set()
                time.sleep(seconds)
                ended_at.append(time.monotonic() - started)
            else:
                threading.Thread(target=the_fault).start()
                begun.wait()

    def one_scorer() -> None:
        life = ScorerLife(started_s=round(time.monotonic() - started, 2))
        report.scorers.append(life)
        stream = RedpandaStream(bootstrap)
        scorer = StreamScorer(
            stream,
            decider=Decider(
                features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel())
            ),
            transactions_topic=names["transactions"][0],
            decisions_topic=names["decisions"][0],
            dead_letter_topic=names["dead_letter"][0],
            shadow_topic=names["shadow"][0],
            group=f"chaos-scorer-{suffix}",
            on_decided=on_decided,
        )
        try:
            service.run(scorer, stop=stop)
        except Exception as error:  # recording why, as a supervisor would
            life.stopped_s = round(time.monotonic() - started, 2)
            life.error = f"{type(error).__name__}: {error}"
        finally:
            life.decided = scorer.decider.stats.decided
            # A client that cannot close is past caring about.
            with contextlib.suppress(Exception):
                stream.close()

    def supervised() -> None:
        # The live stack's restart policy: a scorer that stops on an error is
        # started again, a new process with nothing in memory.
        while not stop.is_set():
            one_scorer()
            if not stop.is_set():
                report.restarted_cold = True
                time.sleep(1.0)

    sending = threading.Thread(
        target=_send, args=(bootstrap, names["transactions"][0], events, rate)
    )
    scoring = threading.Thread(target=supervised)
    scoring.start()
    sending.start()
    if not mid_batch:
        time.sleep(max(0.0, at_seconds - (time.monotonic() - started)))
        the_fault()
    while not ended_at:
        time.sleep(0.1)
    ended = ended_at[0]
    sending.join()
    deadline = time.monotonic() + settle_seconds
    while time.monotonic() < deadline and len(decided_ids) < count:
        time.sleep(0.5)
    stop.set()
    scoring.join()

    report.max_behind_s = round(max((behind for _, behind in decided), default=0.0), 2)
    back = [when for when, behind in decided if when >= ended and behind <= CAUGHT_UP_SECONDS]
    if back:
        report.caught_up_after_s = round(min(back) - ended, 2)
    counts = Counter(
        DecisionEvent.model_validate_json(value).event_id
        for value in _read_all(bootstrap, names["decisions"][0])
    )
    reasons = Counter(
        json.loads(value)["reason"] for value in _read_all(bootstrap, names["dead_letter"][0])
    )
    report.set_aside = dict(reasons)
    report.decided_once = sum(1 for event in events if counts[event.event_id] == 1)
    report.decided_twice_or_more = sum(1 for event in events if counts[event.event_id] > 1)
    report.undecided = sum(1 for event in events if counts[event.event_id] == 0) - sum(
        reasons.values()
    )
    for topic, _ in names.values():
        admin.delete_topic(topic)
    admin.close()
    return report


def as_dict(report: FaultReport) -> dict[str, Any]:
    """The report as JSON-ready data.

    Args:
        report: The report.

    Returns:
        A dict.
    """
    return asdict(report)
