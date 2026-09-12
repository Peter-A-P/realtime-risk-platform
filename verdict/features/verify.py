"""Checking the two things the platform claims about its features.

**Point-in-time correctness.** What the engine served for an event must equal
what the definition says, recomputed from the raw log using only events
strictly before that moment. The record of what was served is the offline
store: one row per entity per event, written at the instant the event was
scored. So the leakage check reads the offline store, not the engine. The
engine holds one evolving window per entity and cannot answer for the past,
which is why it refuses to try.

**Online and offline parity.** The same computed value went to both stores,
so the online store's current value for an entity must equal the last row
written to the offline store for it. Equal exactly, not nearly: they came
from the same object in memory, and anything else means one of the two write
paths lost or reordered something.

The two checks fail differently, which is the point of keeping them apart. A
leakage failure means the feature is wrong. A parity failure means the
feature is right and one of the stores does not have it.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from verdict.features.sinks import OfflineParquetSink, latest_online_values
from verdict.store.features import FEATURE_SET, EntityKind, FeatureSpec
from verdict.store.leakage import ServedLookup
from verdict.store.repo import ENTITY_JOIN_KEYS, TIMESTAMP_FIELD

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from feast import FeatureStore

PARITY_TOLERANCE = 0.0
"""How far apart the two stores may be.

Zero. They are written from the same value in the same call; a difference of
any size is a bug in the write path, not floating-point noise.
"""


def served_lookup_from_offline(
    offline: OfflineParquetSink, specs: Sequence[FeatureSpec] = FEATURE_SET
) -> ServedLookup:
    """Build a lookup over what the engine actually served.

    Reads every offline row once and indexes it by feature, entity and
    moment, so the leakage harness can ask about any moment in the replay
    without touching the engine.

    Args:
        offline: The offline store written during the replay.
        specs: The features in play.

    Returns:
        A callable matching `verdict.store.leakage.ServedLookup`.
    """
    from verdict.store.features import NO_EVENTS

    index: dict[tuple[str, str, dt.datetime], float] = {}
    for kind in {spec.entity for spec in specs}:
        frame = offline.read(kind)
        if frame.empty:
            continue
        join_key = ENTITY_JOIN_KEYS[kind]
        names = [spec.name for spec in specs if spec.entity is kind and spec.name in frame.columns]
        for row in frame.itertuples(index=False):
            entity_id = getattr(row, join_key)
            at = _as_datetime(getattr(row, TIMESTAMP_FIELD))
            for name in names:
                index[(name, entity_id, at)] = float(getattr(row, name))

    def lookup(spec: FeatureSpec, entity_id: str, as_of: dt.datetime) -> float:
        return index.get((spec.name, entity_id, as_of), NO_EVENTS)

    return lookup


def _as_datetime(value: object) -> dt.datetime:
    """Normalise a timestamp read back from Parquet.

    Parquet round-trips timestamps through pandas, which hands them back as
    its own type. Comparing those with the event times the harness holds
    needs one conversion, in one place, or the lookup misses every row and
    the leakage check passes for the wrong reason.

    Args:
        value: The timestamp as read.

    Returns:
        A timezone-aware UTC datetime.
    """
    stamp = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(stamp, dt.datetime):  # pragma: no cover - defensive
        msg = f"expected a timestamp, got {type(stamp).__name__}"
        raise TypeError(msg)
    return stamp if stamp.tzinfo is not None else stamp.replace(tzinfo=dt.UTC)


@dataclass(frozen=True, slots=True)
class ParityViolation:
    """One feature that differs between the online and offline stores.

    Attributes:
        feature: The feature's name.
        entity_id: The entity.
        online: What the online store serves now.
        offline: The last value written to the offline store.
    """

    feature: str
    entity_id: str
    online: float
    offline: float

    def __str__(self) -> str:
        """Render the violation.

        Returns:
            One line naming the feature, the entity and both values.
        """
        return f"{self.feature} for {self.entity_id}: online {self.online}, offline {self.offline}"


@dataclass(frozen=True, slots=True)
class ParityReport:
    """The result of comparing the two stores.

    Attributes:
        violations: Every disagreement.
        entities_checked: How many entities were compared.
        features_checked: How many feature values were compared.
    """

    violations: tuple[ParityViolation, ...]
    entities_checked: int
    features_checked: int

    @property
    def clean(self) -> bool:
        """Whether the stores agreed everywhere.

        Returns:
            True if there were no violations.
        """
        return not self.violations

    @property
    def parity(self) -> float:
        """The share of compared values that agreed.

        Returns:
            A proportion; 1.0 is the only acceptable result, and it is what
            the README reports.
        """
        if self.features_checked == 0:
            return 1.0
        return 1.0 - len(self.violations) / self.features_checked

    def summary(self) -> str:
        """Describe the result.

        Returns:
            A human-readable summary, listing up to five violations.
        """
        if self.clean:
            return (
                f"parity 100%: {self.features_checked} values across "
                f"{self.entities_checked} entities"
            )
        head = "\n".join(f"  {violation}" for violation in self.violations[:5])
        return (
            f"PARITY BROKEN: {len(self.violations)} of {self.features_checked} values "
            f"differ across {self.entities_checked} entities\n{head}"
        )


def check_parity(
    store: FeatureStore,
    offline: OfflineParquetSink,
    specs: Sequence[FeatureSpec] = FEATURE_SET,
    *,
    sample: int | None = None,
) -> ParityReport:
    """Compare the online store with the last row written offline.

    Args:
        store: The Feast store.
        offline: The offline store.
        specs: The features to compare.
        sample: Check at most this many entities per kind. `None` checks all
            of them, which a day's replay can afford and the live window
            cannot.

    Returns:
        The report.
    """
    violations: list[ParityViolation] = []
    entities = 0
    compared = 0

    for kind in sorted({spec.entity for spec in specs}, key=str):
        frame = offline.read(kind)
        if frame.empty:
            continue
        join_key = ENTITY_JOIN_KEYS[kind]
        latest = frame.sort_values(TIMESTAMP_FIELD).groupby(join_key, as_index=False).last()
        if sample is not None:
            latest = latest.head(sample)
        for row in latest.itertuples(index=False):
            entity_id = getattr(row, join_key)
            online = latest_online_values(store, kind, entity_id, specs)
            entities += 1
            for name, online_value in online.items():
                offline_value = float(getattr(row, name))
                compared += 1
                if abs(online_value - offline_value) > PARITY_TOLERANCE:
                    violations.append(
                        ParityViolation(
                            feature=name,
                            entity_id=entity_id,
                            online=online_value,
                            offline=offline_value,
                        )
                    )

    return ParityReport(
        violations=tuple(violations),
        entities_checked=entities,
        features_checked=compared,
    )


def entity_kinds_in(specs: Sequence[FeatureSpec] = FEATURE_SET) -> list[EntityKind]:
    """List the entity kinds a feature set touches.

    Args:
        specs: The features.

    Returns:
        The kinds, in a stable order.
    """
    return sorted({spec.entity for spec in specs}, key=str)
