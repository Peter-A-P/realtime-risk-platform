"""The point-in-time check, run over a replay too large to check exhaustively.

`leakage.check_point_in_time` recomputes every feature for every row against
the whole log, which is quadratic and exactly right for a test fixture. The
real-data track is 590,540 events. So this samples, and what it samples is
the part worth getting right.

**It samples entities, not rows.** A sampled card has every one of its
events kept and every one of its rows checked, each against a recomputation
from that card's complete history. Sampling rows instead would need the whole
log held for the reference, and would check a dense card at one moment where
a leak is most likely to show at another: in a burst, which is exactly where
same-instant events sit. The sample is a hash of the entity identifier, so it
is deterministic, independent of time order, and not choosable after looking
at the results.

The served values are the rows the engine produced as it ran, the same
objects the dual sink would have written. The reference is
`evaluate_spec`, the definition, unchanged. Nothing here compares within a
tolerance other than the leakage module's own.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from verdict.events.schema import TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.store.features import FEATURE_SET, EntityKind, FeatureSpec, entity_id_of, evaluate_spec
from verdict.store.leakage import TOLERANCE, LeakageReport, LeakageViolation

DEFAULT_SAMPLE_PER_MILLE: Final = 20
"""Entities checked per thousand. Two percent of cards on the real data is
about four thousand cards and a correspondingly larger share of rows, since
busy cards are no more likely to be skipped than quiet ones."""


def is_sampled(entity_id: str, per_mille: int) -> bool:
    """Decide from the identifier alone whether an entity is in the sample.

    Args:
        entity_id: The entity.
        per_mille: How many entities per thousand to keep.

    Returns:
        Whether it is sampled.
    """
    digest = hashlib.sha256(entity_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % 1000 < per_mille


@dataclass(frozen=True, slots=True)
class ReplayCheck:
    """What a sampled replay check found.

    Attributes:
        report: The leakage report over the sampled entities' rows.
        events: Events replayed through the engine.
        entities_sampled: Entities whose every row was checked.
        events_kept: Events belonging to those entities.
    """

    report: LeakageReport
    events: int
    entities_sampled: int
    events_kept: int


def check_replay(
    events: Iterable[TransactionEvent],
    specs: Sequence[FeatureSpec] = FEATURE_SET,
    *,
    per_mille: int = DEFAULT_SAMPLE_PER_MILLE,
    engine: FeatureEngine | None = None,
) -> ReplayCheck:
    """Replay events through the engine and check a sample of entities in full.

    Args:
        events: The replay, in event-time order.
        specs: The features to compute and check.
        per_mille: Entities per thousand to check.
        engine: The engine under test, fresh. Defaults to the platform's own;
            the tests pass the unfixed one kept in `tests/test_engine.py` to
            prove this check still fails when it should.

    Returns:
        The result.

    Raises:
        ValueError: If `per_mille` is outside 1 to 1000.
    """
    if not 1 <= per_mille <= 1000:
        msg = f"per_mille must be between 1 and 1000, got {per_mille}"
        raise ValueError(msg)
    kinds = sorted({spec.entity for spec in specs}, key=str)
    engine = FeatureEngine(specs) if engine is None else engine
    kept: dict[tuple[EntityKind, str], list[TransactionEvent]] = {}
    # Keyed by event, not by moment. Events sharing an instant must each be
    # compared with what was served to them: keyed by moment, the last one's
    # value would overwrite the others', and a leak in a burst would be
    # counted once per event in the burst instead of once per wrong value.
    # The first version of this module did exactly that, and inflated the
    # unfixed engine's count on the real data before it was noticed.
    served: dict[tuple[str, str, str], float] = {}
    total = 0

    for event in events:
        total += 1
        rows = engine.process(event)
        for kind in kinds:
            entity_id = entity_id_of(event, kind)
            if entity_id is not None and is_sampled(entity_id, per_mille):
                kept.setdefault((kind, entity_id), []).append(event)
        for row in rows:
            if not is_sampled(row.entity_id, per_mille):
                continue
            for name, value in row.values.items():
                served[(name, row.entity_id, event.event_id)] = value
    engine.flush()

    violations: list[LeakageViolation] = []
    checked = 0
    for (kind, entity_id), history in kept.items():
        for spec in (spec for spec in specs if spec.entity is kind):
            for event in history:
                reference = evaluate_spec(spec, history, entity_id, event.event_time)
                value = served[(spec.name, entity_id, event.event_id)]
                checked += 1
                if abs(value - reference) > TOLERANCE:
                    violations.append(
                        LeakageViolation(
                            feature=spec.name,
                            entity_id=entity_id,
                            as_of=event.event_time,
                            served=value,
                            reference=reference,
                            check="point-in-time, sampled replay",
                        )
                    )

    return ReplayCheck(
        report=LeakageReport(
            violations=tuple(violations), rows_checked=checked, features_checked=len(specs)
        ),
        events=total,
        entities_sampled=len(kept),
        events_kept=sum(len(history) for history in kept.values()),
    )
