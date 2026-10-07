"""From staged decisions and spooled labels to a labelled, weighted sample.

Runs off the latency path, once an hour: seal the hours no writer holds, and
finalise every day whose labels have all had time to arrive. ADR 18 records
the design; this is the order of work for one day:

1. **Wait for the labels.** A day is final only once every label for it could
   have arrived: its last instant, plus the label delay (seven days, ADR 10),
   plus a grace period. Finalising earlier would keep a fraud as legitimate
   because its label was late, and weight it a hundredfold.
2. **Draw before joining.** An approved row whose draw is above the highest
   approved rate is dropped whatever its label, so only the acted rows and
   about a tenth of the rest are joined to labels. That is what lets a day of
   86 million rows be finalised an hour at a time on a 4 GB instance.
3. **Only arrived labels.** A label counts if its label time is at or before
   the moment of finalising. A candidate row with none is counted and not
   kept: it is evidence of a lost label, which the manifest reports, not a
   legitimate transaction.
4. **One row per event.** Delivery is at least once and the scorer's ledger is
   in memory, so after a restart a transaction can be staged twice. The first
   is kept.
5. **Write, then delete.** The day's kept rows and its manifest are written
   under temporary names and renamed into place; only then are the day's
   staged hours and its labels deleted. Run again after a crash, a finalised
   day is recognised by its manifest and only the deleting is repeated.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final, cast

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from verdict import durable
from verdict.events.schema import Action
from verdict.history import spool
from verdict.history.records import LABEL_SCHEMA, history_schema, staged_schema
from verdict.history.sampling import SampleRates, Stratum, draw, stratum_of

LABEL_DELAY: Final = dt.timedelta(days=7)
"""How long after a transaction its label arrives, on both tracks (ADR 10)."""

GRACE: Final = dt.timedelta(hours=6)
"""How long past the label delay a day waits before it is final.

Labels travel through the stream like everything else, so they can be a
little behind their label time. Six hours is generous against a consumer that
keeps up in seconds, and costs six hours of extra retention.
"""

SETTLE: Final = dt.timedelta(minutes=10)
"""How long after an hour ends before it is sealed, if no writer holds it."""


class NotYetFinalError(ValueError):
    """Raised on finalising a day whose labels may not all have arrived."""


@dataclass(frozen=True, slots=True)
class HistoryPaths:
    """Where each stage lives under the history root.

    Attributes:
        root: The history directory, on the data volume live.
    """

    root: Path

    @property
    def staged(self) -> Path:
        """Decisions as the scorer wrote them."""
        return self.root / "staged"

    @property
    def labels(self) -> Path:
        """Labels as the label collector wrote them."""
        return self.root / "labels"

    @property
    def kept(self) -> Path:
        """Finalised days: the sample, and a manifest per day."""
        return self.root / "kept"

    def kept_file(self, day: dt.date) -> Path:
        """A finalised day's rows.

        Args:
            day: The day.

        Returns:
            The Parquet path.
        """
        return self.kept / f"{day.isoformat()}.parquet"

    def manifest_file(self, day: dt.date) -> Path:
        """A finalised day's manifest.

        Args:
            day: The day.

        Returns:
            The JSON path.
        """
        return self.kept / f"{day.isoformat()}.json"


@dataclass(slots=True)
class DayManifest:
    """What finalising one day found and kept.

    Attributes:
        day: The day, by event time.
        finalised_at: The moment labels were counted up to.
        rates: The keep probabilities.
        label_delay_hours: The delay assumed.
        staged_rows: Rows the scorer staged, duplicates included.
        duplicates: Staged rows that repeated an event already seen.
        candidates: Rows joined to labels (acted, or drawn under the highest
            approved rate).
        unlabelled: Candidates with no label by `finalised_at`.
        kept: Rows kept, by stratum.
        weights: The weight of each stratum.
        estimated_transactions: The weighted count of kept rows: an estimate
            of the labelled transactions the day held.
        estimated_frauds: The weighted count of kept frauds.
        sha256: Of the kept file, so a published number can name its data.
    """

    day: str
    finalised_at: str
    rates: dict[str, float]
    label_delay_hours: float
    staged_rows: int = 0
    duplicates: int = 0
    candidates: int = 0
    unlabelled: int = 0
    kept: dict[str, int] = field(default_factory=dict)
    weights: dict[str, float] = field(default_factory=dict)
    estimated_transactions: float = 0.0
    estimated_frauds: float = 0.0
    sha256: str = ""


def day_hours(day: dt.date) -> list[str]:
    """The 24 hour keys of a day.

    Args:
        day: The day.

    Returns:
        Its hours, in order.
    """
    start = dt.datetime.combine(day, dt.time(), tzinfo=dt.UTC)
    return [spool.hour_key(start + dt.timedelta(hours=h)) for h in range(24)]


def final_after(
    day: dt.date, *, delay: dt.timedelta = LABEL_DELAY, grace: dt.timedelta = GRACE
) -> dt.datetime:
    """The first moment a day may be finalised.

    Args:
        day: The day.
        delay: The label delay.
        grace: The grace past it.

    Returns:
        The day's end, plus the delay, plus the grace.
    """
    end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), tzinfo=dt.UTC)
    return end + delay + grace


def seal_closed(paths: HistoryPaths, now: dt.datetime, *, limit: int | None = None) -> list[str]:
    """Seal hours, staged or labels, that ended long enough ago.

    Args:
        paths: The history directories.
        now: The current time.
        limit: Seal at most this many hours (staged hours before labels
            hours, oldest first within each) and stop. One
            process reading and re-writing an hour holds it whole in memory
            (`spool.seal`), and a backlog is every hour a crashed run left
            unsealed. Without a limit, one call works the whole backlog: on
            the dry run's first night an instance replaced three times in
            under twelve hours, each crash regrowing the backlog its
            successor then had to clear in one process, and the fourth
            attempt was killed by the kernel's out-of-memory handler at
            9.3 GB, having sealed twelve hours already (`docs/STATE.md`).
            A limit bounds one process to a few hours whatever the backlog,
            and the loop that calls this again shortly clears the rest a
            few hours at a time, each in a process that exits and gives its
            memory back before the next.

    Returns:
        The hours sealed, prefixed with their spool's name.
    """
    sealed: list[str] = []
    for name, directory, schema in (
        ("staged", paths.staged, staged_schema()),
        ("labels", paths.labels, LABEL_SCHEMA),
    ):
        for key in spool.hours(directory):
            if limit is not None and len(sealed) >= limit:
                return sealed
            if spool.hour_start(key) + dt.timedelta(hours=1) + SETTLE > now:
                continue
            if spool.seal(directory, key, schema):
                sealed.append(f"{name}/{key}")
    return sealed


def _strings(column: pa.Array[Any] | pa.ChunkedArray[Any]) -> list[str]:
    """A non-null string column as Python strings."""
    return cast("list[str]", column.to_pylist())


def _labels_for(
    paths: HistoryPaths,
    ids: pa.ChunkedArray[Any],
    since: dt.datetime,
    until: dt.datetime,
    as_of: dt.datetime,
) -> pa.Table:
    """The arrived labels of some events, a batch of labels at a time.

    Only the matching labels are ever held: an hour of labels is about 3.6
    million rows at the live rate, and this reads up to eight such hours for
    each staged hour, so reading each whole (as this did until 2026-09-22)
    held an hour of labels at a time for nothing.

    Args:
        paths: The history directories.
        ids: The events wanted.
        since: No label for these events is filed earlier than this hour.
        until: Nor later than this one.
        as_of: Labels after this had not arrived.

    Returns:
        The matching labels, first per event.
    """
    wanted = pc.unique(ids)
    arrived = pa.scalar(as_of, LABEL_SCHEMA.field("label_time").type)
    first, last = spool.hour_key(since), spool.hour_key(until)
    keys = [k for k in spool.hours(paths.labels) if first <= k <= last]
    found: list[pa.RecordBatch] = []
    for key in keys:
        for batch in spool.iter_hour(paths.labels, key, LABEL_SCHEMA):
            mask = pc.and_(
                pc.is_in(batch["event_id"], value_set=wanted),
                pc.less_equal(batch["label_time"], arrived),
            )
            found.append(batch.filter(mask).cast(LABEL_SCHEMA))
    labels = pa.Table.from_batches(found, schema=LABEL_SCHEMA)
    frame = labels.to_pandas().drop_duplicates("event_id", keep="first")
    return pa.Table.from_pandas(frame, schema=LABEL_SCHEMA, preserve_index=False)


def _hour_draws(paths: HistoryPaths, key: str) -> npt.NDArray[np.float64]:
    """Every staged row's draw in one hour, in `spool.iter_hour`'s order.

    Reads only the event id column of a sealed hour, and holds eight bytes
    a row: about 29 MB for an hour at the live rate.
    """
    parts = [
        np.fromiter(
            (draw(event_id) for event_id in _strings(batch["event_id"])),
            dtype=np.float64,
            count=batch.num_rows,
        )
        for batch in spool.iter_hour(paths.staged, key, staged_schema(), columns=["event_id"])
    ]
    return np.concatenate(parts) if parts else np.empty(0, dtype=np.float64)


def _finalise_hour(
    paths: HistoryPaths,
    key: str,
    *,
    as_of: dt.datetime,
    rates: SampleRates,
    delay: dt.timedelta,
    grace: dt.timedelta,
    manifest: DayManifest,
) -> pa.Table | None:
    """Sample one staged hour and join its candidates to their labels.

    Two passes over the hour, neither holding it whole. An hour is about 3.6
    million rows at the live rate, and until 2026-09-22 this read it into
    one table, the shape of cost that killed the compactor at 9.3 GB when
    sealing did it (`docs/STATE.md`). The first pass computes each row's
    draw; the second streams the rows and keeps only the candidates, which
    are the acted rows and about a tenth of the rest.

    Duplicates are found per hour, not per day: a row is filed by its
    transaction's event time (`StreamScorer`), and a redelivery carries the
    same event, so both copies land in the same hour. Within the hour a
    duplicate shares its draw, so only rows whose draw repeats are checked
    by id; the set of every event id in a day, which this held before,
    would be several gigabytes by the end of a live day.
    """
    draws = _hour_draws(paths, key)
    manifest.staged_rows += len(draws)
    if len(draws) == 0:
        return None
    values, counts = np.unique(draws, return_counts=True)
    repeated = values[counts > 1]

    ceiling = max(rates.fraud, rates.legit)
    seen: set[str] = set()
    pieces: list[pa.RecordBatch] = []
    piece_draws: list[npt.NDArray[np.float64]] = []
    offset = 0
    duplicates = 0
    for batch in spool.iter_hour(paths.staged, key, staged_schema()):
        size = batch.num_rows
        batch_draws = draws[offset : offset + size]
        offset += size
        first = np.ones(size, dtype=np.bool_)
        if len(repeated):
            ids = _strings(batch["event_id"])
            for row in np.flatnonzero(np.isin(batch_draws, repeated)).tolist():
                if ids[row] in seen:
                    first[row] = False
                else:
                    seen.add(ids[row])
        duplicates += int((~first).sum())
        acted = np.array(
            [action != Action.APPROVE.value for action in _strings(batch["action"])],
            dtype=np.bool_,
        )
        candidate = first & (acted | (batch_draws < ceiling))
        if candidate.any():
            pieces.append(batch.filter(pa.array(candidate)).cast(staged_schema()))
            piece_draws.append(batch_draws[candidate])
    if offset != len(draws):
        msg = f"staged hour {key} changed while it was being finalised"
        raise RuntimeError(msg)
    manifest.duplicates += duplicates
    if not pieces:
        return None

    rows = pa.Table.from_batches(pieces, schema=staged_schema())
    row_draws = np.concatenate(piece_draws)
    manifest.candidates += rows.num_rows
    # A label is filed by its own time: for this hour's events, from the hour
    # the delay lands them in, to the hour after, plus the grace.
    start = spool.hour_start(key)
    since = start + delay - dt.timedelta(hours=1)
    until = start + delay + dt.timedelta(hours=1) + grace
    labels = _labels_for(paths, rows["event_id"], since, until, as_of)
    lookup = {event_id: index for index, event_id in enumerate(labels["event_id"].to_pylist())}
    label_is_fraud = labels["is_fraud"].to_pylist()

    keep_rows: list[int] = []
    label_index: list[int] = []
    strata: list[str] = []
    weights: list[float] = []
    for index, (event_id, action) in enumerate(
        zip(_strings(rows["event_id"]), _strings(rows["action"]), strict=True)
    ):
        at = lookup.get(event_id)
        if at is None:
            manifest.unlabelled += 1
            continue
        stratum = stratum_of(Action(action), bool(label_is_fraud[at]))
        rate = rates.rate(stratum)
        if row_draws[index] >= rate:
            continue
        keep_rows.append(index)
        label_index.append(at)
        strata.append(stratum.value)
        weights.append(1.0 / rate)
        manifest.kept[stratum.value] = manifest.kept.get(stratum.value, 0) + 1
        manifest.estimated_transactions += 1.0 / rate
        if label_is_fraud[at]:
            manifest.estimated_frauds += 1.0 / rate

    if not keep_rows:
        return None
    kept = rows.take(pa.array(keep_rows))
    joined = labels.take(pa.array(label_index))
    columns: dict[str, Any] = {name: kept[name] for name in kept.column_names}
    columns["label_time"] = joined["label_time"]
    columns["is_fraud"] = joined["is_fraud"]
    columns["recovered_cents"] = joined["recovered_cents"]
    columns["stratum"] = pa.array(strata, pa.string())
    columns["weight"] = pa.array(weights, pa.float64())
    return pa.table(columns, schema=history_schema())


def finalise_day(
    paths: HistoryPaths,
    day: dt.date,
    *,
    as_of: dt.datetime,
    rates: SampleRates | None = None,
    delay: dt.timedelta = LABEL_DELAY,
    grace: dt.timedelta = GRACE,
) -> DayManifest:
    """Turn one day's staged decisions into its kept, weighted sample.

    Args:
        paths: The history directories.
        day: The day, by event time.
        as_of: The moment of finalising; labels after it had not arrived.
        rates: The keep probabilities.
        delay: The label delay.
        grace: The grace past it.

    Returns:
        The day's manifest, read back if the day was already final.

    Raises:
        NotYetFinalError: If some of the day's labels may not have arrived.
    """
    rates = rates or SampleRates()
    manifest_path = paths.manifest_file(day)
    if manifest_path.exists():
        manifest = DayManifest(**json.loads(manifest_path.read_text(encoding="utf-8")))
        _delete_sources(paths, day, delay)
        return manifest
    ready = final_after(day, delay=delay, grace=grace)
    if as_of < ready:
        msg = (
            f"{day} is final from {ready.isoformat()}; "
            f"labels may still be arriving at {as_of.isoformat()}"
        )
        raise NotYetFinalError(msg)

    manifest = DayManifest(
        day=day.isoformat(),
        finalised_at=as_of.isoformat(),
        rates={s.value: rates.rate(s) for s in Stratum},
        label_delay_hours=delay.total_seconds() / 3600,
        weights={s.value: 1.0 / rates.rate(s) for s in Stratum},
    )
    paths.kept.mkdir(parents=True, exist_ok=True)
    target = paths.kept_file(day)
    temporary = target.with_suffix(".parquet.tmp")
    with pq.ParquetWriter(temporary, history_schema(), compression="zstd") as writer:
        for key in day_hours(day):
            table = _finalise_hour(
                paths,
                key,
                as_of=as_of,
                rates=rates,
                delay=delay,
                grace=grace,
                manifest=manifest,
            )
            if table is not None:
                writer.write_table(table)
    durable.settle(temporary, target)
    with target.open("rb") as kept_file:
        manifest.sha256 = hashlib.file_digest(kept_file, "sha256").hexdigest()
    durable.write_text(manifest_path, json.dumps(asdict(manifest), indent=2, sort_keys=True) + "\n")
    _delete_sources(paths, day, delay)
    return manifest


def _delete_sources(paths: HistoryPaths, day: dt.date, delay: dt.timedelta) -> None:
    """Remove a final day's staged hours, and labels that could only be its own."""
    spool.delete_hours(paths.staged, day_hours(day))
    end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), tzinfo=dt.UTC) + delay
    old = [key for key in spool.hours(paths.labels) if spool.hour_start(key) < end]
    spool.delete_hours(paths.labels, old)


def hours_read(
    day: dt.date, *, delay: dt.timedelta = LABEL_DELAY, grace: dt.timedelta = GRACE
) -> tuple[list[str], list[str]]:
    """The staged hours and the label hours finalising a day reads.

    The label hours are every hour `_finalise_hour` reads for any of the
    day's staged hours: from an hour before the delay lands the day's first
    labels to the grace past the hour after it lands its last.

    Args:
        day: The day.
        delay: The label delay.
        grace: The grace past it.

    Returns:
        The staged hour keys and the label hour keys, each in order.
    """
    start = dt.datetime.combine(day, dt.time(), tzinfo=dt.UTC)
    first = start + delay - dt.timedelta(hours=1)
    last = start + dt.timedelta(hours=23) + delay + dt.timedelta(hours=1) + grace
    labels: list[str] = []
    at = first
    while at <= last:
        labels.append(spool.hour_key(at))
        at += dt.timedelta(hours=1)
    return day_hours(day), labels


def is_sealed_for(paths: HistoryPaths, day: dt.date) -> bool:
    """Whether every hour finalising a day reads is sealed, or absent.

    Sealing runs in a process of its own beside finalising (2026-10-07,
    ADR 18's second addendum). A seal settles the hour's Parquet file and
    then deletes its folder, so a reader that looked for the file before it
    existed and for the folder after it was gone would see neither and miss
    the hour's rows; for labels that would finalise the day with labels it
    never saw, for good. Finalising only once nothing it reads is still a
    folder means the two never touch the same hour.

    Args:
        paths: The history directories.
        day: The day.

    Returns:
        True if no hour the day reads is waiting to be sealed.
    """
    staged, labels = hours_read(day)
    return not any((paths.staged / key).is_dir() for key in staged) and not any(
        (paths.labels / key).is_dir() for key in labels
    )


def finalisable(paths: HistoryPaths, now: dt.datetime) -> Iterator[dt.date]:
    """Days with staged rows that may now be finalised, oldest first.

    A day may be finalised once its labels have all had time to arrive and
    every hour it reads has been sealed (`is_sealed_for`). The last label
    hour it reads is the one that starts at `final_after`, so in a healthy
    run a day becomes finalisable about seventy minutes after that: the hour
    ends, and the seal run after the next takes it.

    Args:
        paths: The history directories.
        now: The current time.

    Yields:
        Each day ready and not yet final.
    """
    days = sorted({spool.hour_start(key).date() for key in spool.hours(paths.staged)})
    for day in days:
        if (
            final_after(day) <= now
            and not paths.manifest_file(day).exists()
            and is_sealed_for(paths, day)
        ):
            yield day


def read_kept(paths: HistoryPaths, days: list[dt.date]) -> pa.Table:
    """The kept rows of finalised days.

    Args:
        paths: The history directories.
        days: The days.

    Returns:
        One table. Every estimate from it must use the `weight` column.
    """
    tables = [pq.read_table(paths.kept_file(day)) for day in days if paths.kept_file(day).exists()]
    return pa.concat_tables(tables) if tables else history_schema().empty_table()
