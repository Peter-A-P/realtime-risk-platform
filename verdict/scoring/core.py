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
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Final, Protocol

from verdict.events.schema import DecisionEvent, ShadowEvent, TransactionEvent
from verdict.features.engine import FeatureEngine
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
        if self.seen(event.event_id):
            self.stats.duplicates += 1
            return None

        served = self.features.serve(event)
        self._remember(event.event_id)
        featured = time.perf_counter_ns()

        model: Model = self.models.current()
        score = model.score(served, event)
        scored = time.perf_counter_ns()

        action, rule = self.rules.decide(score, event)
        decision = DecisionEvent(
            event_id=event.event_id,
            card_id=event.card_id,
            action=action,
            score=score,
            rule=rule,
            model_version=model.version,
            decided_at=dt.datetime.now(dt.UTC),
        )
        payload = decision.to_json().encode("utf-8")
        decided = time.perf_counter_ns()

        self.stats.decided += 1
        shadow, shadow_payload, shadow_ns = self._shadow(served, event, decision)
        return Outcome(
            decision=decision,
            payload=payload,
            features_ns=featured - started_ns,
            model_ns=scored - featured,
            decision_ns=decided - scored,
            shadow=shadow,
            shadow_payload=shadow_payload,
            shadow_ns=shadow_ns,
            features=served,
        )

    def _shadow(
        self, served: dict[str, float], event: TransactionEvent, champion: DecisionEvent
    ) -> tuple[ShadowEvent | None, bytes | None, int]:
        if self.shadow is None or (
            self.shadow_when is not None and not self.shadow_when(event, champion)
        ):
            return None, None, 0
        started = time.perf_counter_ns()
        try:
            model = self.shadow.current()
            score = model.score(served, event)
            action, rule = self.rules.decide(score, event)
            record = ShadowEvent(
                event_id=event.event_id,
                card_id=event.card_id,
                model_version=model.version,
                score=score,
                action=action,
                rule=rule,
                champion_version=champion.model_version,
                champion_action=champion.action,
                decided_at=champion.decided_at,
            )
            payload = record.to_json().encode("utf-8")
        except Exception:  # a shadow must never break the champion
            self.stats.shadow_failures += 1
            return None, None, time.perf_counter_ns() - started
        return record, payload, time.perf_counter_ns() - started

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
