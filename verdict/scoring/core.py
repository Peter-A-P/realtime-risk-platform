"""Deciding one transaction, shared by every way a transaction arrives.

The stream consumer (`consumer.py`) and the HTTP endpoint (`http_api.py`) differ in
how a transaction reaches them and how the decision leaves. They must not
differ in how the decision is made, or the comparison between them in Rule C
candidate 3 would be comparing two scorers rather than two transports. So the
decision is made here, once.

**The ledger records an event the moment the engine has seen it.** Not when
its decision is written. The engine observes an event as part of serving the
events after it, so once `serve` has run, the event is in the windows. If
writing the decision then fails and the event is retried, the engine must not
see it again, or it is counted twice. Recording it earlier means a retried
event is reported as a duplicate even though no decision reached the stream;
that is a lost decision, which is visible and recoverable by replay, instead
of a corrupted window, which is neither.

**A shadow model scores beside the champion and cannot touch its decision.**
It is given the features the champion was served, so the two are comparable
row by row, and its would-be decision is returned separately for the
`shadow` topic. It is timed on its own, after the champion's decision exists,
so the champion's hops do not include it. If the shadow raises, the failure is
counted and the champion's decision stands: a challenger that can break
scoring is not in shadow.
"""

from __future__ import annotations

import datetime as dt
import itertools
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Protocol

import numpy as np

from verdict.events.schema import DecisionEvent, ShadowEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.models.inputs import vector
from verdict.scoring.model import Model, ModelSource
from verdict.scoring.rules import DecisionRules
from verdict.store.features import NO_EVENTS

DEFAULT_LEDGER_SIZE: Final = 1_000_000
"""Decided event ids remembered for duplicate detection.

About twenty minutes at the live rate. A redelivery arrives within a batch or
two of the original, so this is generous; it is bounded because an 87-day
window would otherwise hold seven billion ids.
"""


class FeatureSource(Protocol):
    """Where a decision's features come from."""

    def serve(self, event: TransactionEvent) -> dict[str, float]:
        """Features for an event, as of its own time.

        Args:
            event: The event.

        Returns:
            Feature name to value, with no gaps.
        """
        ...


class EngineFeatures:
    """Serves features from the feature engine, in the scorer's own process.

    The engine is the one computation of every feature (ADR 6), so serving
    from it directly is the same value the stores hold, without a network
    hop. ADR 8 records the choice.
    """

    def __init__(self, engine: FeatureEngine) -> None:
        """Wrap an engine.

        Args:
            engine: The engine, fresh or warmed by a replay.
        """
        self.engine = engine
        self._names = tuple(spec.name for spec in engine.specs)

    def serve(self, event: TransactionEvent) -> dict[str, float]:
        """Features for an event, and hold it back for the ones that follow.

        Args:
            event: The event.

        Returns:
            Every feature in the engine's set. A feature whose entity the
            event does not name carries `NO_EVENTS`, so the model sees a full
            vector with no gaps.
        """
        values = dict.fromkeys(self._names, NO_EVENTS)
        for row in self.engine.process(event):
            values.update(row.values)
        return values


@dataclass(frozen=True, slots=True)
class Outcome:
    """A decision, and how long its parts took.

    Attributes:
        decision: The decision record.
        payload: The record as it goes on the wire.
        features_ns: Time to serve features, including decoding if the
            caller started its clock before decoding.
        model_ns: Time to score, including choosing the model.
        decision_ns: Time to apply the rules and build the record.
        shadow: What the shadow model would have decided, if there is one
            and it did not fail.
        shadow_payload: The shadow record as it goes on the wire.
        shadow_ns: Time the shadow took, reported apart from the champion's
            hops. Zero when there is no shadow.
        features: The features the champion was served, which the shadow
            was given too. The scorer stages them with the decision
            (`verdict/history`), so history holds what the model saw rather
            than a recomputation of it.
    """

    decision: DecisionEvent
    payload: bytes
    features_ns: int
    model_ns: int
    decision_ns: int
    shadow: ShadowEvent | None = None
    shadow_payload: bytes | None = None
    shadow_ns: int = 0
    features: Mapping[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class DeciderStats:
    """Counts a decider keeps.

    Attributes:
        decided: Events decided.
        duplicates: Events refused as already seen.
        shadow_failures: Events on which the shadow model raised.
    """

    decided: int = 0
    duplicates: int = 0
    shadow_failures: int = 0


class Decider:
    """Makes the decision for one event, and remembers that it has."""

    def __init__(
        self,
        *,
        features: FeatureSource,
        models: ModelSource,
        rules: DecisionRules | None = None,
        ledger_size: int = DEFAULT_LEDGER_SIZE,
        shadow: ModelSource | None = None,
        shadow_when: Callable[[TransactionEvent, DecisionEvent], bool] | None = None,
    ) -> None:
        """Assemble the decider.

        Args:
            features: Where features come from.
            models: Which model to use, asked for every event, so a rollback
                takes effect on the next event rather than on a restart.
            rules: The rule set. Defaults to the placeholder thresholds.
            ledger_size: How many event ids to remember.
            shadow: A challenger to score in shadow, if any.
            shadow_when: Which decisions the shadow scores, if not every one.
                The live scorer passes history's `could_be_kept`: the
                promotion gate reads the shadow only on rows history keeps.
        """
        self.features = features
        self.shadow = shadow
        self.shadow_when = shadow_when
        self.models = models
        self.rules = rules or DecisionRules()
        self.ledger_size = ledger_size
        self.stats = DeciderStats()
        self._ledger: OrderedDict[str, None] = OrderedDict()

    def seen(self, event_id: str) -> bool:
        """Whether an event has already reached the engine.

        Args:
            event_id: The event.

        Returns:
            True if it has.
        """
        return event_id in self._ledger

    def decide(self, event: TransactionEvent, started_ns: int) -> Outcome | None:
        """Decide an event, unless it has been seen before.

        Args:
            event: The event.
            started_ns: When the caller started on it, from
                `time.perf_counter_ns`.

        Returns:
            The outcome, or None for a duplicate, which is counted and
            otherwise ignored.
        """
        taken = self.take(event, started_ns)
        return None if taken is None else self.decide_all([taken])[0]

    def take(self, event: TransactionEvent, started_ns: int) -> Served | None:
        """Serve an event its features, unless it has been seen before.

        The first half of a decision. The engine takes events one at a time
        and in order whatever happens after, so serving is never batched;
        only the model is (`decide_all`).

        Args:
            event: The event.
            started_ns: When the caller started on it.

        Returns:
            The served event, or None for a duplicate, which is counted.
        """
        if self.seen(event.event_id):
            self.stats.duplicates += 1
            return None
        served = self.features.serve(event)
        self._remember(event.event_id)
        return Served(event, served, started_ns, time.perf_counter_ns())

    def decide_all(self, taken: Sequence[Served]) -> list[Outcome]:
        """Score served events with one call to each model, and decide each.

        One call to ONNX Runtime costs nearly the same for one row as for
        hundreds (ADR 8's addendum of 2026-09-27: 47 percent of the live
        scorer's time was that overhead, one row at a time). Each event's
        score is the one it would have had alone; the model is asked for once
        per batch, so a rollback takes effect at the next batch.

        Args:
            taken: Served events, in the order they were served.

        Returns:
            One outcome each, in the same order.
        """
        if not taken:
            return []
        model: Model = self.models.current()
        scoring = time.perf_counter_ns()
        scores = _scores(model, taken)
        scored = time.perf_counter_ns()
        decisions: list[tuple[DecisionEvent, bytes, int]] = []
        for item, score in zip(taken, scores, strict=True):
            started = time.perf_counter_ns()
            action, rule = self.rules.decide(score, item.event)
            decision = DecisionEvent(
                event_id=item.event.event_id,
                card_id=item.event.card_id,
                action=action,
                score=score,
                rule=rule,
                model_version=model.version,
                decided_at=dt.datetime.now(dt.UTC),
            )
            payload = decision.to_json().encode("utf-8")
            decisions.append((decision, payload, time.perf_counter_ns() - started))
            self.stats.decided += 1
        shadows = self._shadows(taken, [decision for decision, _, _ in decisions])
        return [
            Outcome(
                decision=decision,
                payload=payload,
                features_ns=item.featured_ns - item.started_ns,
                model_ns=scored - scoring,
                decision_ns=decision_ns,
                shadow=shadow,
                shadow_payload=shadow_payload,
                shadow_ns=shadow_ns,
                features=item.features,
            )
            for item, (decision, payload, decision_ns), (shadow, shadow_payload, shadow_ns) in zip(
                taken, decisions, shadows, strict=True
            )
        ]

    def _shadows(
        self, taken: Sequence[Served], champion: Sequence[DecisionEvent]
    ) -> list[tuple[ShadowEvent | None, bytes | None, int]]:
        none: list[tuple[ShadowEvent | None, bytes | None, int]] = [(None, None, 0)] * len(taken)
        if self.shadow is None:
            return none
        chosen = [
            index
            for index, (item, decision) in enumerate(zip(taken, champion, strict=True))
            if self.shadow_when is None or self.shadow_when(item.event, decision)
        ]
        if not chosen:
            return none
        started = time.perf_counter_ns()
        out = list(none)
        try:
            model = self.shadow.current()
            scores = _scores(model, [taken[index] for index in chosen])
            records = []
            for index, score in zip(chosen, scores, strict=True):
                event, decision = taken[index].event, champion[index]
                action, rule = self.rules.decide(score, event)
                record = ShadowEvent(
                    event_id=event.event_id,
                    card_id=event.card_id,
                    model_version=model.version,
                    score=score,
                    action=action,
                    rule=rule,
                    champion_version=decision.model_version,
                    champion_action=decision.action,
                    decided_at=decision.decided_at,
                )
                records.append((index, record, record.to_json().encode("utf-8")))
        except Exception:  # a shadow must never break the champion
            self.stats.shadow_failures += len(chosen)
            elapsed = time.perf_counter_ns() - started
            for index in chosen:
                out[index] = (None, None, elapsed)
            return out
        elapsed = time.perf_counter_ns() - started
        for index, record, payload in records:
            out[index] = (record, payload, elapsed)
        return out

    @property
    def remembered(self) -> int:
        """How many event ids the ledger holds.

        Returns:
            The count, which never exceeds `ledger_size`.
        """
        return len(self._ledger)

    def ledger_tail(self, count: int) -> list[str]:
        """The most recently remembered event ids, oldest first, to be saved.

        Args:
            count: The most to return.

        Returns:
            The ids.
        """
        tail = list(itertools.islice(reversed(self._ledger), count))
        tail.reverse()
        return tail

    def remember(self, event_id: str) -> None:
        """Remember an event as seen without deciding it: a restore's replay.

        Args:
            event_id: The event.
        """
        self._remember(event_id)

    def _remember(self, event_id: str) -> None:
        self._ledger[event_id] = None
        if len(self._ledger) > self.ledger_size:
            self._ledger.popitem(last=False)


@dataclass(frozen=True, slots=True)
class Served:
    """An event served its features and not yet scored.

    Attributes:
        event: The event.
        features: What it was served.
        started_ns: When the caller started on it.
        featured_ns: When its features were ready.
    """

    event: TransactionEvent
    features: dict[str, float]
    started_ns: int
    featured_ns: int


def _scores(model: Model, taken: Sequence[Served]) -> list[float]:
    """Each served event's score, from one call where the model allows it.

    The batched path gives each row exactly the score the single-row path
    would, clamped the same way; `tests/test_scoring.py` holds the shipped
    champion to that.

    Args:
        model: The model.
        taken: The served events.

    Returns:
        One score each, in order.
    """
    batched = getattr(model, "score_matrix", None)
    if batched is None or len(taken) == 1:
        return [model.score(item.features, item.event) for item in taken]
    rows = np.asarray([vector(item.features, item.event) for item in taken], dtype=np.float64)
    return [float(min(1.0, max(0.0, score))) for score in batched(rows)]
