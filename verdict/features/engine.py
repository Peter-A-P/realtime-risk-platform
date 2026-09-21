"""The feature engine: one pass over the stream, one value per feature.

This is the piece ADR 4 originally delegated to Bytewax and, after Bytewax
turned out not to install on this project's Python, the piece this repository
owns. The amendment in ADR 4 sets out the trade.

## The ordering that makes leakage structurally impossible

For every event the engine does two things, in this order and never the other:

1. **Serve.** Compute every feature as of the event's own time, from the
   state built by earlier events.
2. **Observe.** Fold the event into the state, for the events that follow.

The event being scored is therefore not in the state when its features are
computed. Not because a comparison operator says `<` rather than `<=`, but
because it has not been added yet. That is worth more than the comparison: an
operator can be changed by someone who thinks they are fixing an off-by-one,
whereas reordering these two calls is a visible change to the shape of the
loop, and `tests/test_engine.py` fails immediately if anyone makes it.

The same ordering is what the live scorer does: a decision is made from what
was known before the transaction, and the transaction joins the history
afterwards.

**Observing is deferred to the next instant, and that part was a bug first.**
Serving before observing is not sufficient on its own. Two transactions can
carry the same timestamp, which at a thousand events a second is ordinary,
and observing the first immediately places it inside the second's window,
because that window is `[t - w, t)` and the first event sits at exactly `t`.
The leakage test caught this on the day the first features were written: the
engine served a count of 1 where the definition said no history at all. So an
event now waits in a buffer until an event with a strictly later timestamp
arrives, and only then joins the history. `docs/leak-caught.md` records the
episode and what it would have cost offline.

## State

State is per entity, per feature. An entity that goes quiet keeps its
aggregators until they are empty, at which point `prune` drops them; without
that, an 87-day live window would accumulate an aggregator for every card
that ever transacted. Pruning is the engine's only concession to the fact
that it runs for months rather than for a test.
"""

from __future__ import annotations

import datetime as dt
from collections import OrderedDict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from verdict.events.schema import TransactionEvent
from verdict.features.aggregators import Aggregator, build_aggregator
from verdict.store.features import (
    FEATURE_SET,
    EntityKind,
    FeatureSpec,
    entity_id_of,
    validate_feature_set,
)

PRUNE_EVERY: Final = 1_000
"""How many events between sweeps for empty state.

A sweep looks only at the entities least recently seen, and stops at the
first that still holds something, so it costs what it drops and little
more. It used to walk every live entity every fifty thousand events: on the
live stack's first dry run that walk was a quarter of the scorer's CPU, and
each one held every decision behind it for seconds.
"""


class LateEventError(ValueError):
    """Raised when an event arrives after the stream has moved past its time."""

    def __init__(self, event: TransactionEvent, reached: dt.datetime) -> None:
        """Say which event was late, and by how much.

        Args:
            event: The late event.
            reached: The timestamp the engine had already reached.
        """
        super().__init__(
            f"event {event.event_id} at {event.event_time.isoformat()} arrived after the "
            f"engine reached {reached.isoformat()}; folding it in would corrupt every "
            f"window that has already passed it"
        )
        self.event = event
        self.reached = reached


class StaleQueryError(ValueError):
    """Raised when the engine is asked about a moment it has moved past."""

    def __init__(self, asked: dt.datetime, observed_through: dt.datetime) -> None:
        """Say what was asked and how far the engine has got.

        Args:
            asked: The moment the caller asked about.
            observed_through: The last event the engine folded in.
        """
        super().__init__(
            f"the engine has observed events through {observed_through.isoformat()} and "
            f"cannot answer for the earlier moment {asked.isoformat()}; read the offline "
            f"store for what was served then"
        )
        self.asked = asked
        self.observed_through = observed_through


@dataclass(frozen=True, slots=True)
class FeatureRow:
    """The features for one entity, as of one moment.

    Attributes:
        kind: Which entity these are keyed on.
        entity_id: The entity.
        as_of: The moment they describe, which is the scored event's time.
        values: Feature name to value.
    """

    kind: EntityKind
    entity_id: str
    as_of: dt.datetime
    values: dict[str, float]


class FeatureEngine:
    """Computes every feature in one pass over the stream.

    Build it with a feature set, then call `serve` and `observe` in that
    order for each event, or `process` to do both correctly in one call.
    """

    def __init__(self, specs: Sequence[FeatureSpec] = FEATURE_SET) -> None:
        """Prepare the engine.

        Args:
            specs: The features to compute. Defaults to the platform's own
                set.
        """
        validate_feature_set(list(specs))
        self.specs = tuple(specs)
        self._by_entity: dict[EntityKind, tuple[FeatureSpec, ...]] = {}
        for spec in self.specs:
            self._by_entity.setdefault(spec.entity, ())
            self._by_entity[spec.entity] += (spec,)
        # (entity kind, entity id) -> feature name -> aggregator
        self._state: dict[tuple[EntityKind, str], dict[str, Aggregator]] = {}
        # Per kind, the entity ids in the order they were last observed,
        # oldest first. Every observation pushes all of a kind's features at
        # once, so within a kind the entities whose windows have all emptied
        # are the oldest ones, and a sweep can stop at the first that has not.
        self._recency: dict[EntityKind, OrderedDict[str, None]] = {
            kind: OrderedDict() for kind in self._by_entity
        }
        self._since_prune = 0
        self._observed_through: dt.datetime | None = None
        # Events at the newest timestamp, held back until time moves on. See
        # `process` for why this buffer exists.
        self._pending: list[TransactionEvent] = []
        self._pending_time: dt.datetime | None = None

    @property
    def tracked_entities(self) -> int:
        """How many entities the engine is holding state for.

        Returns:
            The count, which is what `prune` keeps bounded.
        """
        return len(self._state)

    def serve(self, event: TransactionEvent) -> list[FeatureRow]:
        """Compute every feature as of this event, without observing it.

        Args:
            event: The event being scored.

        Returns:
            One row per entity the event names that has features defined.
        """
        rows: list[FeatureRow] = []
        for kind, specs in self._by_entity.items():
            entity_id = entity_id_of(event, kind)
            if entity_id is None:
                continue
            state = self._state.get((kind, entity_id))
            values = {
                spec.name: (
                    state[spec.name].value(event.event_time)
                    if state is not None and spec.name in state
                    else _absent(spec)
                )
                for spec in specs
            }
            rows.append(
                FeatureRow(
                    kind=kind,
                    entity_id=entity_id,
                    as_of=event.event_time,
                    values=values,
                )
            )
        return rows

    def observe(self, event: TransactionEvent) -> None:
        """Fold an event into the state, for the events that follow it.

        Args:
            event: The event to record.
        """
        for kind, specs in self._by_entity.items():
            entity_id = entity_id_of(event, kind)
            if entity_id is None:
                continue
            state = self._state.setdefault((kind, entity_id), {})
            recency = self._recency[kind]
            if entity_id in recency:
                recency.move_to_end(entity_id)
            else:
                recency[entity_id] = None
            for spec in specs:
                aggregator = state.get(spec.name)
                if aggregator is None:
                    aggregator = build_aggregator(spec)
                    state[spec.name] = aggregator
                aggregator.push(event.event_time, _field_for(event, spec))
        self._observed_through = event.event_time
        self._since_prune += 1
        if self._since_prune >= PRUNE_EVERY:
            self.prune(event.event_time)

    def process(self, event: TransactionEvent) -> list[FeatureRow]:
        """Serve this event, and hold it back until time actually moves on.

        Serving before observing is not quite enough, and the leakage test
        caught the gap on the day the first features were written. Two
        transactions can share a timestamp, which at a thousand events a
        second is ordinary rather than exotic, and a card-testing burst makes
        it more likely still. Observing the first one immediately put it
        inside the second one's window, because the second one's window is
        `[t - w, t)` and the first is at exactly `t`. The engine served 1
        where the definition said no history at all.

        So an event is not folded into the state when it is served. It waits
        in a buffer until an event with a strictly later timestamp arrives,
        and only then joins the history. The buffer holds one instant's worth
        of events, so it is small, and `flush` empties it when the stream
        ends.

        Args:
            event: The event.

        Returns:
            The features as of the event, computed from everything strictly
            before it.

        Raises:
            LateEventError: If the event precedes one already processed.
                Folding a late event into windows that have moved past it
                would corrupt them silently, so the caller has to decide:
                the chaos tests in week 8 drop late events deliberately, and
                measure what that costs.
        """
        if self._pending_time is not None:
            if event.event_time < self._pending_time:
                raise LateEventError(event, self._pending_time)
            if event.event_time > self._pending_time:
                self.flush()
        rows = self.serve(event)
        self._pending.append(event)
        self._pending_time = event.event_time
        return rows

    def flush(self) -> None:
        """Fold the events waiting at the current instant into the state.

        Called automatically when the stream moves to a later timestamp, and
        by the caller at the end of a replay.
        """
        for event in self._pending:
            self.observe(event)
        self._pending.clear()

    def run(self, events: Iterable[TransactionEvent]) -> list[list[FeatureRow]]:
        """Process a whole replay in order.

        Args:
            events: The events, in event-time order.

        Returns:
            The rows for each event, in the same order.
        """
        rows = [self.process(event) for event in events]
        self.flush()
        return rows

    def prune(self, as_of: dt.datetime) -> int:
        """Drop state for entities whose windows have all emptied.

        Walks each kind's entities from the least recently observed and stops
        at the first that still holds something. What it leaves behind that
        is also empty is dropped by a later sweep, and meanwhile serves the
        same `NO_EVENTS` an unknown entity does, so when state is dropped
        never changes a feature.

        Args:
            as_of: The moment to evaluate emptiness at.

        Returns:
            How many entities were dropped.
        """
        self._since_prune = 0
        dropped = 0
        for kind, recency in self._recency.items():
            while recency:
                entity_id = next(iter(recency))
                state = self._state[(kind, entity_id)]
                for aggregator in state.values():
                    aggregator.value(as_of)
                if not all(aggregator.is_empty() for aggregator in state.values()):
                    break
                del recency[entity_id]
                del self._state[(kind, entity_id)]
                dropped += 1
        return dropped

    def lookup(self, spec: FeatureSpec, entity_id: str, as_of: dt.datetime) -> float:
        """Report one feature for one entity, as of now.

        This answers for the present, which is what the online store is for.
        It cannot answer for the past: the engine keeps one evolving window
        per entity, not a history of them, so asking it about an earlier
        moment would return a window containing events that had not happened
        then. That is a leak, and it is a leak produced by the caller rather
        than by the engine, which is why this refuses instead of answering.

        The record of what was served in the past is the offline store, which
        holds one row per event at the moment it was scored. `verify.py`
        reads it for exactly this reason.

        Args:
            spec: The feature.
            entity_id: The entity.
            as_of: The moment. Must not precede the last observed event.

        Returns:
            The value, or the absent sentinel if the entity is unknown.

        Raises:
            StaleQueryError: If `as_of` precedes the last observed event.
        """
        reached = self._pending_time or self._observed_through
        if reached is not None and as_of < reached:
            raise StaleQueryError(as_of, reached)
        state = self._state.get((spec.entity, entity_id))
        if state is None or spec.name not in state:
            return _absent(spec)
        return state[spec.name].value(as_of)

    @property
    def observed_through(self) -> dt.datetime | None:
        """The time of the last event folded into the state.

        Returns:
            The timestamp, or None if nothing has been observed.
        """
        return self._observed_through


def _field_for(event: TransactionEvent, spec: FeatureSpec) -> float | str | None:
    """Read the field a feature aggregates off an event.

    Args:
        event: The event.
        spec: The feature.

    Returns:
        The field's value, or None where the aggregation needs no field.
    """
    if spec.field is None:
        return None
    value = getattr(event, spec.field)
    if isinstance(value, int | float) and not isinstance(value, bool):
        return float(value)
    return str(value)


def _absent(spec: FeatureSpec) -> float:
    """What to report for an entity with no state yet.

    Args:
        spec: The feature.

    Returns:
        The same sentinel the reference evaluation returns for an empty
        window, so a first-ever transaction and a long-quiet entity are the
        same thing to the model, which is what they are.
    """
    from verdict.store.features import NO_EVENTS

    del spec
    return NO_EVENTS
