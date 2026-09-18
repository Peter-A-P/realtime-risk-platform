"""The same replay through every stream, and the features that come out must agree.

ADR 3 puts two stream implementations behind one interface and says that
without this check "the interface is a claim, not a fact". It runs a replay
through the scorer on each stream, records every feature vector the scorer
served and every decision that came back off the decisions topic, and compares
them, event by event, against a reference that involves no stream at all: the
same events handed straight to a fresh feature engine in order.

What a stream could get wrong, and so what this can catch:

- **Order.** An event delivered after a later one would be served a
  different window, or set aside as late with no decision at all. One
  partition is what prevents it (ADR 8); this is the check that it does.
- **Duplicates.** Delivery is at least once. A redelivered event that reached
  the engine would be counted twice in every window it falls in. The scorer's
  ledger is what prevents it; a duplicate that got past it shows up here as a
  feature that disagrees with the reference.
- **Bytes.** A payload changed in transit, or a decision that does not
  survive the round trip through the decisions topic. Each transaction is
  compared as the scorer received it, by a digest of its canonical form, and
  not only through its effects: an event's features come from the events
  before it, so a changed amount shows up first in a later transaction on the
  same card or merchant, and a change that never reaches a later window
  would otherwise not show up at all. The first draft of this check compared
  effects only, and its own planted fault found the gap.

Floating-point features are compared exactly, not within a tolerance. The
computation is the same code on the same inputs in the same order; any
difference at all means the inputs or the order differed, and a tolerance
would hide exactly that.

Decisions are compared on action, score, rule and model version.
`decided_at` is the scorer's wall clock and is left out, because two runs
cannot share it.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Final

from verdict.events.schema import DecisionEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.stream.base import Stream

DEFAULT_DEADLINE_SECONDS: Final = 120.0
"""How long a path may take to decide the whole replay before it is a failure."""

DecisionKey = tuple[str, float, str, str]
"""Action, score, rule and model version: what two runs of a decision share."""


def decision_key(decision: DecisionEvent) -> DecisionKey:
    """The parts of a decision that two runs must agree on.

    Args:
        decision: The decision.

    Returns:
        Action, score, rule and model version.
    """
    return (decision.action.value, decision.score, decision.rule, decision.model_version)


@dataclass(frozen=True, slots=True)
class PathResult:
    """What one path served and decided.

    Attributes:
        name: The path, for the report.
        served: Feature vector by event id, as the scorer served it.
        received: Digest of each transaction as the scorer decoded it.
        decided: Decision by event id, as read back off the decisions topic.
        duplicates: Deliveries the scorer's ledger turned away.
    """

    name: str
    served: dict[str, dict[str, float]]
    received: dict[str, str]
    decided: dict[str, DecisionKey]
    duplicates: int


def reference(events: Sequence[TransactionEvent]) -> PathResult:
    """Features and decisions with no stream at all: the events, in order, to one engine.

    Args:
        events: The replay, in time order.

    Returns:
        The reference result.
    """
    served: dict[str, dict[str, float]] = {}
    received: dict[str, str] = {}
    decider = _a_decider(_recorder(served, received))
    decided: dict[str, DecisionKey] = {}
    for event in events:
        outcome = decider.decide(event, time.perf_counter_ns())
        if outcome is None:  # pragma: no cover - a replay with a repeated id
            continue
        decided[event.event_id] = decision_key(outcome.decision)
    return PathResult("reference, no stream", served, received, decided, decider.stats.duplicates)


def through(
    name: str,
    stream: Stream,
    events: Sequence[TransactionEvent],
    *,
    transactions_topic: str,
    decisions_topic: str,
    dead_letter_topic: str = "dead-letter",
    reader: Stream | None = None,
    deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
) -> PathResult:
    """Send the replay through one stream and the scorer, and read the decisions back.

    Args:
        name: The path, for the report.
        stream: The stream to send on and score from.
        events: The replay, in time order.
        transactions_topic: An empty topic for the transactions.
        decisions_topic: An empty topic for the decisions.
        dead_letter_topic: Where the scorer sets aside what it cannot decide.
        reader: A separate client to read the decisions with; the stream
            itself if None.
        deadline_seconds: How long the scorer may take.

    Returns:
        What the path served and decided.

    Raises:
        TimeoutError: If the scorer did not decide the whole replay in time.
    """
    for event in events:
        stream.produce(transactions_topic, event.card_id, event.to_json().encode("utf-8"))
    stream.flush()

    served: dict[str, dict[str, float]] = {}
    received: dict[str, str] = {}
    decider = _a_decider(_recorder(served, received))
    scorer = StreamScorer(
        stream,
        decider=decider,
        transactions_topic=transactions_topic,
        decisions_topic=decisions_topic,
        dead_letter_topic=dead_letter_topic,
        group=f"parity-{uuid.uuid4().hex[:8]}",
    )
    deadline = time.monotonic() + deadline_seconds
    while decider.stats.decided + sum(scorer.dead_letters.values()) < len(events):
        if time.monotonic() > deadline:
            msg = f"{name}: decided {decider.stats.decided} of {len(events)} in {deadline_seconds}s"
            raise TimeoutError(msg)
        scorer.poll(max_records=500, timeout_seconds=0.2)

    decided: dict[str, DecisionKey] = {}
    source = reader or stream
    group = f"parity-reader-{uuid.uuid4().hex[:8]}"
    while len(decided) < decider.stats.decided:
        if time.monotonic() > deadline:
            msg = f"{name}: read back {len(decided)} of {len(events)} decisions"
            raise TimeoutError(msg)
        for record in source.consume(
            decisions_topic, group, max_records=1_000, timeout_seconds=0.2
        ):
            decision = DecisionEvent.model_validate_json(record.value)
            decided.setdefault(decision.event_id, decision_key(decision))
    return PathResult(name, served, received, decided, decider.stats.duplicates)


@dataclass(frozen=True, slots=True)
class Disagreement:
    """One place a path differs from the reference.

    Attributes:
        path: Which path.
        event_id: Which event.
        what: The feature name, or `"transaction"`, `"decision"` or `"missing"`.
        expected: The reference's value, as text.
        found: The path's value, as text.
    """

    path: str
    event_id: str
    what: str
    expected: str
    found: str


@dataclass(frozen=True, slots=True)
class ParityReport:
    """Every path against the reference.

    Attributes:
        events: How many events the replay held.
        paths: The paths compared.
        comparisons: Feature values and decisions compared, all paths together.
        disagreements: Every difference found.
    """

    events: int
    paths: tuple[str, ...]
    comparisons: int
    disagreements: list[Disagreement] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        """Whether every path agreed with the reference everywhere.

        Returns:
            True if nothing disagreed.
        """
        return not self.disagreements

    def summary(self) -> str:
        """One line for a report or an assertion message.

        Returns:
            The line.
        """
        head = f"{self.events} events, {len(self.paths)} paths, {self.comparisons} comparisons"
        if self.clean:
            return f"{head}: identical"
        first = self.disagreements[0]
        return (
            f"{head}: {len(self.disagreements)} disagreements, first on {first.path} "
            f"at {first.event_id} ({first.what}: expected {first.expected}, found {first.found})"
        )


def compare(expected: PathResult, *paths: PathResult) -> ParityReport:
    """Compare each path with the reference, value by value.

    Args:
        expected: The reference.
        paths: The paths to hold to it.

    Returns:
        The report.
    """
    disagreements: list[Disagreement] = []
    comparisons = 0
    for path in paths:
        for event_id, want in expected.served.items():
            got = path.served.get(event_id)
            if got is None:
                disagreements.append(
                    Disagreement(path.name, event_id, "missing", "served", "absent")
                )
                continue
            for feature, value in want.items():
                comparisons += 1
                if got.get(feature) != value:
                    disagreements.append(
                        Disagreement(
                            path.name, event_id, feature, repr(value), repr(got.get(feature))
                        )
                    )
            comparisons += 1
            if path.received.get(event_id) != expected.received[event_id]:
                disagreements.append(
                    Disagreement(
                        path.name,
                        event_id,
                        "transaction",
                        expected.received[event_id][:12],
                        (path.received.get(event_id) or "absent")[:12],
                    )
                )
            comparisons += 1
            if path.decided.get(event_id) != expected.decided[event_id]:
                disagreements.append(
                    Disagreement(
                        path.name,
                        event_id,
                        "decision",
                        repr(expected.decided[event_id]),
                        repr(path.decided.get(event_id)),
                    )
                )
        for extra in sorted(set(path.served) - set(expected.served)):
            disagreements.append(Disagreement(path.name, extra, "missing", "absent", "served"))
    return ParityReport(
        events=len(expected.served),
        paths=tuple(path.name for path in paths),
        comparisons=comparisons,
        disagreements=disagreements,
    )


class _Recording(EngineFeatures):
    """Engine features that also report every vector served."""

    def __init__(
        self,
        engine: FeatureEngine,
        on_served: Callable[[TransactionEvent, dict[str, float]], object],
    ) -> None:
        super().__init__(engine)
        self._on_served = on_served

    def serve(self, event: TransactionEvent) -> dict[str, float]:
        values = super().serve(event)
        self._on_served(event, dict(values))
        return values


def digest(event: TransactionEvent) -> str:
    """A digest of a transaction's canonical JSON.

    Args:
        event: The transaction.

    Returns:
        The SHA-256, hex.
    """
    return hashlib.sha256(event.to_json().encode("utf-8")).hexdigest()


def _recorder(
    served: dict[str, dict[str, float]], received: dict[str, str]
) -> Callable[[TransactionEvent, dict[str, float]], None]:
    def record(event: TransactionEvent, values: dict[str, float]) -> None:
        served.setdefault(event.event_id, values)
        received.setdefault(event.event_id, digest(event))

    return record


def _a_decider(
    on_served: Callable[[TransactionEvent, dict[str, float]], object] | None = None,
) -> Decider:
    engine = FeatureEngine()
    features = EngineFeatures(engine) if on_served is None else _Recording(engine, on_served)
    return Decider(features=features, models=FixedModel(StandInModel()))
