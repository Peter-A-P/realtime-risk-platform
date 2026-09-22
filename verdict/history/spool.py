"""An append-only spool of rows, one directory per hour, sealed to Parquet.

The scorer writes a row for every decision before it checkpoints the batch,
so the write has to be cheap and it has to survive the process: an Arrow IPC
stream per hour, appended a batch at a time with no compression and no
footer to rewrite. A crash leaves a file whose last batch may be cut short;
reading stops at the last whole batch, and those rows' transactions were
never checkpointed, so the stream delivers them again.

Nothing here fsyncs. A spot interruption is a clean shutdown, which flushes
the page cache; what a kernel crash could lose is the last few batches of
rows, and `docs/failure-modes.md` records that rather than paying a disk
round trip on every batch of the latency path.

Sealing is done later and elsewhere (`compact.py`): an hour that no writer
holds open is read whole, written as one zstd Parquet file, and its IPC
files deleted. Reading an hour reads both forms, so a reader never has to
care which has happened yet.

Layout, under the spool's directory:

    <hour>/<writer>.part     being written by a live writer
    <hour>/<writer>.arrow    closed, waiting to be sealed
    <hour>.parquet           sealed

where `<hour>` is `YYYY-MM-DDTHH` in UTC.
"""

from __future__ import annotations

import datetime as dt
import shutil
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

HOUR_FORMAT = "%Y-%m-%dT%H"


def hour_key(moment: dt.datetime) -> str:
    """The hour a moment falls in, as the spool names it.

    Args:
        moment: A timezone-aware time.

    Returns:
        `YYYY-MM-DDTHH`, in UTC.
    """
    return moment.astimezone(dt.UTC).strftime(HOUR_FORMAT)


def hour_start(key: str) -> dt.datetime:
    """The start of a named hour.

    Args:
        key: `YYYY-MM-DDTHH`.

    Returns:
        The hour's first instant, UTC.
    """
    return dt.datetime.strptime(key, HOUR_FORMAT).replace(tzinfo=dt.UTC)


class SpoolWriter:
    """Appends rows to the spool, an hour to a file."""

    def __init__(self, directory: Path, schema: pa.Schema) -> None:
        """Open a writer. Nothing is created until the first flush.

        Args:
            directory: The spool's directory.
            schema: Every row's columns.
        """
        self.directory = directory
        self.schema = schema
        self._pending: dict[str, list[Mapping[str, Any]]] = {}
        self._open: dict[str, tuple[pa.OSFile, ipc.RecordBatchStreamWriter, Path]] = {}
        self.rows_written = 0

    def append(self, moment: dt.datetime, row: Mapping[str, Any]) -> None:
        """Queue a row for the hour its moment falls in.

        Args:
            moment: The time that files the row.
            row: Column name to value, matching the schema.
        """
        self._pending.setdefault(hour_key(moment), []).append(row)

    def flush(self) -> None:
        """Write everything queued, one record batch per hour touched.

        Raises:
            KeyError: If a row lacks a column of the schema.
        """
        for key, rows in self._pending.items():
            if not rows:
                continue
            batch = pa.RecordBatch.from_pylist(list(rows), schema=self.schema)
            _, writer, _ = self._writer(key)
            writer.write_batch(batch)
            self.rows_written += len(rows)
        for key, (sink, _, _) in self._open.items():
            if key in self._pending:
                sink.flush()
        self._pending = {}

    def close_before(self, moment: dt.datetime) -> list[str]:
        """Close every hour file older than a moment's hour, so it can be sealed.

        A row for a closed hour that arrives later opens a new file for that
        hour; nothing is lost, the hour just has two files.

        Args:
            moment: Hours before this one's are closed.

        Returns:
            The hours closed.
        """
        current = hour_key(moment)
        closed = [key for key in self._open if key < current]
        for key in closed:
            self._close(key)
        return closed

    def close(self) -> None:
        """Flush, then close every hour file."""
        self.flush()
        for key in list(self._open):
            self._close(key)

    def _writer(self, key: str) -> tuple[pa.OSFile, ipc.RecordBatchStreamWriter, Path]:
        if key not in self._open:
            folder = self.directory / key
            folder.mkdir(parents=True, exist_ok=True)
            # A name per file, so an hour reopened for a late row never
            # collides with the file it had before.
            path = folder / f"{uuid.uuid4().hex}.part"
            sink = pa.OSFile(str(path), "wb")
            self._open[key] = (sink, ipc.new_stream(sink, self.schema), path)
        return self._open[key]

    def _close(self, key: str) -> None:
        sink, writer, path = self._open.pop(key)
        writer.close()
        sink.close()
        path.rename(path.with_suffix(".arrow"))


def recover(directory: Path) -> list[Path]:
    """Mark files left by a writer that is gone as closed.

    Call only when no writer is running on this spool: at the start of the
    process that owns it.

    Args:
        directory: The spool's directory.

    Returns:
        The files recovered.
    """
    parts = sorted(directory.glob("*/*.part"))
    for part in parts:
        part.rename(part.with_suffix(".arrow"))
    return parts


def _read_stream(path: Path, schema: pa.Schema) -> list[pa.RecordBatch]:
    """Every whole batch in an IPC stream, stopping at a cut-short tail."""
    return list(_stream_batches(path, schema))


def _stream_batches(path: Path, schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    """Every whole batch in an IPC stream, in order, stopping at a cut-short tail.

    Unlike `_read_stream`, no more than one batch is ever alive at once. What
    `seal` needs: an hour holds about 3.6 million rows at the live rate, and
    holding one whole hour to reseal it, on top of what else the instance
    was carrying, is most of what put the dry run's fourth instance over
    its memory on the first clean hour after the backlog that crashed the
    three before it (`docs/STATE.md`).
    """
    try:
        with pa.OSFile(str(path), "rb") as source:
            reader = ipc.open_stream(source)
            for batch in reader:
                yield batch.cast(schema) if batch.schema != schema else batch
    except (pa.ArrowInvalid, OSError):
        pass  # a truncated last batch, from a writer that died mid-write


def _iter_hour(directory: Path, key: str, schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    """Every row of one hour, sealed and unsealed, a batch at a time.

    In the order `read_hours` would concatenate them, without ever holding
    more of the hour than one batch.

    Args:
        directory: The spool's directory.
        key: The hour.
        schema: The rows' columns.

    Yields:
        Record batches.
    """
    sealed = directory / f"{key}.parquet"
    if sealed.exists():
        yield from pq.ParquetFile(sealed).iter_batches()
    folder = directory / key
    if folder.is_dir():
        for path in sorted([*folder.glob("*.arrow"), *folder.glob("*.part")]):
            yield from _stream_batches(path, schema)


def hours(directory: Path) -> list[str]:
    """Every hour the spool holds, sealed or not, in order.

    Args:
        directory: The spool's directory.

    Returns:
        Hour keys.
    """
    if not directory.exists():
        return []
    found = {p.name for p in directory.iterdir() if p.is_dir()}
    found |= {p.stem for p in directory.glob("*.parquet")}
    return sorted(found)


def read_hours(directory: Path, keys: Iterable[str], schema: pa.Schema) -> pa.Table:
    """Read hours whole, whether sealed or still in IPC files.

    Files a live writer holds (`.part`) are read too, up to their last whole
    batch: a reader wants what has been written, not what has been closed.

    Args:
        directory: The spool's directory.
        keys: The hours.
        schema: The rows' columns.

    Returns:
        One table, in the order the hours were given.
    """
    tables: list[pa.Table] = []
    for key in keys:
        sealed = directory / f"{key}.parquet"
        if sealed.exists():
            tables.append(pq.read_table(sealed, schema=schema))
        folder = directory / key
        if folder.is_dir():
            for path in sorted([*folder.glob("*.arrow"), *folder.glob("*.part")]):
                batches = _read_stream(path, schema)
                if batches:
                    tables.append(pa.Table.from_batches(batches, schema=schema))
    return pa.concat_tables(tables) if tables else schema.empty_table()


SEAL_CHUNK_ROWS: Final = 100_000
"""Rows buffered before one Parquet row group is written while sealing.

An hour is about 3.6 million rows at the live rate; sealing writes it in
chunks this size rather than holding the whole hour, so a chunk's tables
are the only rows alive at once, plus whatever the writer itself buffers.
Large enough that a sealed file's row groups stay a sane size to read back.
"""


def seal(directory: Path, key: str, schema: pa.Schema) -> bool:
    """Turn a closed hour into one zstd Parquet file.

    Refuses an hour a writer still holds open. Safe to repeat: an hour that
    is already sealed and has new closed files is sealed again with them.
    Streams a chunk at a time (`SEAL_CHUNK_ROWS`), so sealing never holds
    the whole hour in memory at once.

    Args:
        directory: The spool's directory.
        key: The hour.
        schema: The rows' columns.

    Returns:
        True if the hour was sealed now, False if there was nothing to seal
        or a writer holds it.
    """
    folder = directory / key
    if not folder.is_dir() or any(folder.glob("*.part")):
        return False
    target = directory / f"{key}.parquet"
    temporary = directory / f"{key}.parquet.tmp"
    wrote_anything = False
    with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
        chunk: list[pa.RecordBatch] = []
        rows = 0
        for batch in _iter_hour(directory, key, schema):
            chunk.append(batch)
            rows += batch.num_rows
            if rows >= SEAL_CHUNK_ROWS:
                writer.write_table(pa.Table.from_batches(chunk, schema=schema).combine_chunks())
                wrote_anything = True
                chunk = []
                rows = 0
        if chunk:
            writer.write_table(pa.Table.from_batches(chunk, schema=schema).combine_chunks())
            wrote_anything = True
        elif not wrote_anything:
            # An empty hour (every row this seal would carry has already been
            # sealed, and nothing new arrived): write the schema and nothing
            # else, so the file still exists and reads back as empty.
            writer.write_table(schema.empty_table())
    temporary.replace(target)
    shutil.rmtree(folder)
    return True


def delete_hours(directory: Path, keys: Sequence[str]) -> None:
    """Remove hours, sealed and unsealed.

    Args:
        directory: The spool's directory.
        keys: The hours.
    """
    for key in keys:
        (directory / f"{key}.parquet").unlink(missing_ok=True)
        folder = directory / key
        if folder.is_dir():
            shutil.rmtree(folder)
