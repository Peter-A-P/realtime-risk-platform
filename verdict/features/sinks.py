"""One write path: every computed feature reaches both stores, or neither.

This is where "computed once" stops being a claim about the engine and starts
being a property of the data. The engine produces one value; `DualSink.write`
puts that same value into the online store and the offline store in a single
call, from the same object in memory. There is no second job, no scheduled
materialisation, and no window in which the two could hold different numbers
because one of them ran later.

The ordering is offline first, then online, and it matters:

- If the process dies between the two, the offline store holds a value the
  online store does not. Training then knows something serving does not,
  which is a **stale online store**: the next event for that entity
  recomputes and overwrites it, so the damage is bounded to one entity for
  one event.
- The other order loses the offline row instead. Training would then be
  missing a row that serving acted on, permanently, and nothing would ever
  notice, because the absence of a row is not an error anywhere.

Neither is exactly-once. At-least-once with idempotent writes is the choice
here as everywhere else in this platform, and the parity test is what
notices if the two stores ever disagree.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from verdict.features.engine import FeatureRow
from verdict.store.features import FEATURE_SET, EntityKind, FeatureSpec
from verdict.store.repo import ENTITY_JOIN_KEYS, TIMESTAMP_FIELD, push_source_name, view_name

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    import pandas as pd
    from feast import FeatureStore

DEFAULT_BATCH = 500
"""How many rows to accumulate before writing.

One write per event would make the sink the bottleneck long before the engine
was. Batching is bounded by `flush`, which the caller must invoke before
reading anything back, and which the context manager invokes on exit.
"""


def rows_to_frame(kind: EntityKind, rows: Sequence[FeatureRow]) -> pd.DataFrame:
    """Turn feature rows for one entity kind into a frame the store accepts.

    Args:
        kind: The entity kind. Every row must be of this kind.
        rows: The rows.

    Returns:
        A frame with the join key, the event timestamp and one column per
        feature.

    Raises:
        ValueError: If the rows are not all of the given kind, or the rows
            disagree about which features they carry. Both would produce a
            frame with silent nulls in it, and a null feature is a decision
            made by whoever notices it last.
    """
    import pandas as pd

    if not rows:
        return pd.DataFrame()
    if any(row.kind is not kind for row in rows):
        msg = f"rows for {kind} contain another entity kind"
        raise ValueError(msg)
    names = tuple(rows[0].values)
    if any(tuple(row.values) != names for row in rows):
        msg = f"rows for {kind} disagree about which features they carry"
        raise ValueError(msg)

    data: dict[str, list[object]] = {
        ENTITY_JOIN_KEYS[kind]: [row.entity_id for row in rows],
        TIMESTAMP_FIELD: [row.as_of for row in rows],
    }
    for name in names:
        data[name] = [row.values[name] for row in rows]
    return pd.DataFrame(data)


class OfflineParquetSink:
    """Appends feature rows to the offline store as Parquet.

    One file per entity kind, written in the layout the generated Feast file
    sources point at, so the offline store is readable by the point-in-time
    join without a second registration step.
    """

    def __init__(self, directory: Path) -> None:
        """Open the sink.

        Args:
            directory: The Feast repository's `data` directory.
        """
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, kind: EntityKind) -> Path:
        """Where one entity kind's features live.

        Args:
            kind: The entity kind.

        Returns:
            The Parquet path.
        """
        return self.directory / f"{view_name(kind)}.parquet"

    def write(self, kind: EntityKind, frame: pd.DataFrame) -> None:
        """Append a frame, preserving what is already there.

        Args:
            kind: The entity kind.
            frame: The rows to append.
        """
        import pandas as pd

        if frame.empty:
            return
        path = self.path_for(kind)
        if path.exists():
            frame = pd.concat([pd.read_parquet(path), frame], ignore_index=True)
        frame.to_parquet(path, index=False)

    def read(self, kind: EntityKind) -> pd.DataFrame:
        """Read everything written for one entity kind.

        Args:
            kind: The entity kind.

        Returns:
            The frame, empty if nothing has been written.
        """
        import pandas as pd

        path = self.path_for(kind)
        return pd.read_parquet(path) if path.exists() else pd.DataFrame()


class DualSink:
    """Writes each computed value to the offline and online stores.

    Use it as a context manager, or call `flush` before reading anything
    back.
    """

    def __init__(
        self,
        store: FeatureStore,
        offline: OfflineParquetSink,
        specs: Sequence[FeatureSpec] = FEATURE_SET,
        *,
        batch_size: int = DEFAULT_BATCH,
        online: bool = True,
    ) -> None:
        """Prepare the sink.

        Args:
            store: The Feast store, used for the online write.
            offline: Where the Parquet files go.
            specs: The features being written.
            batch_size: How many rows to accumulate per entity kind.
            online: Whether to write the online store. Turning it off writes
                only Parquet, which is what an offline backfill wants; the
                parity test uses it to build an offline store deliberately
                without an online one.
        """
        self.store = store
        self.offline = offline
        self.specs = tuple(specs)
        self.batch_size = batch_size
        self.online = online
        self._pending: dict[EntityKind, list[FeatureRow]] = {}
        self.rows_written = 0

    def write(self, rows: Sequence[FeatureRow]) -> None:
        """Queue rows, flushing whichever entity kinds are full.

        Args:
            rows: The rows produced for one event.
        """
        for row in rows:
            self._pending.setdefault(row.kind, []).append(row)
        for kind, pending in list(self._pending.items()):
            if len(pending) >= self.batch_size:
                self._write_kind(kind, pending)
                self._pending[kind] = []

    def flush(self) -> None:
        """Write everything still queued."""
        for kind, pending in list(self._pending.items()):
            if pending:
                self._write_kind(kind, pending)
                self._pending[kind] = []

    def _write_kind(self, kind: EntityKind, rows: list[FeatureRow]) -> None:
        """Write one entity kind's rows to both stores.

        Offline first: see the module docstring for why the order is not
        arbitrary.

        Args:
            kind: The entity kind.
            rows: The rows.
        """
        from feast.data_source import PushMode

        frame = rows_to_frame(kind, rows)
        if frame.empty:
            return
        self.offline.write(kind, frame)
        if self.online:
            self.store.push(push_source_name(kind), frame, to=PushMode.ONLINE)
        self.rows_written += len(rows)

    def __enter__(self) -> DualSink:
        """Enter the context.

        Returns:
            This sink.
        """
        return self

    def __exit__(self, *exc: object) -> None:
        """Flush on the way out, including when the block raised.

        Args:
            *exc: Exception information, unused.
        """
        del exc
        self.flush()


def latest_online_values(
    store: FeatureStore, kind: EntityKind, entity_id: str, specs: Sequence[FeatureSpec]
) -> dict[str, float]:
    """Read one entity's current features from the online store.

    Args:
        store: The Feast store.
        kind: The entity kind.
        entity_id: The entity.
        specs: The features to read; only those keyed on `kind` are used.

    Returns:
        Feature name to value, with missing values as the absent sentinel.
    """
    from verdict.store.features import NO_EVENTS

    wanted = [spec for spec in specs if spec.entity is kind]
    if not wanted:
        return {}
    refs = [f"{view_name(kind)}:{spec.name}" for spec in wanted]
    served = store.get_online_features(
        features=refs, entity_rows=[{ENTITY_JOIN_KEYS[kind]: entity_id}]
    ).to_dict()
    return {
        spec.name: (NO_EVENTS if served[spec.name][0] is None else float(served[spec.name][0]))
        for spec in wanted
    }
