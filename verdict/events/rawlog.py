"""The raw event log: append-only JSON lines, one file per record kind.

This is the bottom of the platform. Every feature the store ever serves has
to be recomputable from these files alone, because that is exactly what the
leakage test does: it replays the raw log up to a point in time, recomputes
the features from scratch, and compares them with what the store served. A
raw log that is lossy, reordered or enriched would make that test meaningless.

So the rules here are narrow:

- **Append only.** Nothing is rewritten, nothing is deduplicated, nothing is
  sorted after the fact.
- **One record per line, as it went on the wire.** The transaction file holds
  exactly the bytes a consumer would have seen.
- **Three files, not one.** Transactions, labels and ground truth are
  separate. Ground truth is generator-side knowledge and a feature that read
  it would be the largest possible leak; keeping it in its own file means
  reading it has to be a deliberate act rather than a careless one.
"""

from __future__ import annotations

import gzip
import io
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Final, Self

from verdict.events.generator.driver import GeneratedRecord
from verdict.events.schema import GroundTruth, LabelEvent, TransactionEvent, decode_transaction

TRANSACTIONS_FILE: Final = "transactions.jsonl"
LABELS_FILE: Final = "labels.jsonl"
GROUND_TRUTH_FILE: Final = "ground_truth.jsonl"

_BUFFER_BYTES: Final = 1 << 20
"""Write buffer. One megabyte keeps the generator off the disk's critical
path without holding enough in memory to lose a meaningful run to a crash."""


@dataclass(frozen=True, slots=True)
class LogCounts:
    """How much a run wrote.

    Attributes:
        transactions: Transactions written.
        labels: Labels written.
        ground_truth: Ground-truth records written.
    """

    transactions: int
    labels: int
    ground_truth: int


class RawEventLog:
    """Writes generated records to the raw log.

    Use it as a context manager; the files are closed and flushed on exit,
    including when the run is interrupted.
    """

    def __init__(self, directory: Path, *, compress: bool = False) -> None:
        """Open the log for writing.

        Args:
            directory: Where the three files live. Created if missing.
            compress: Whether to gzip each file. The live window writes
                compressed; the tests do not, so a failure can be read with
                an ordinary text editor.
        """
        self.directory = directory
        self.compress = compress
        directory.mkdir(parents=True, exist_ok=True)
        suffix = ".gz" if compress else ""
        self._transactions = self._open(directory / f"{TRANSACTIONS_FILE}{suffix}")
        self._labels = self._open(directory / f"{LABELS_FILE}{suffix}")
        self._truth = self._open(directory / f"{GROUND_TRUTH_FILE}{suffix}")
        self._counts = [0, 0, 0]

    def _open(self, path: Path) -> io.TextIOWrapper:
        """Open one file for appending text.

        Args:
            path: The file to open.

        Returns:
            A buffered text handle.
        """
        if self.compress:
            return io.TextIOWrapper(
                gzip.open(path, "ab"),
                encoding="utf-8",
                write_through=False,
            )
        return open(path, "a", encoding="utf-8", buffering=_BUFFER_BYTES)

    def append(self, record: GeneratedRecord) -> None:
        """Write one generated record to all three files.

        Args:
            record: The record to write.
        """
        self._transactions.write(record.event.to_json())
        self._transactions.write("\n")
        self._labels.write(record.label.to_json())
        self._labels.write("\n")
        self._truth.write(record.truth.to_json())
        self._truth.write("\n")
        self._counts[0] += 1
        self._counts[1] += 1
        self._counts[2] += 1

    @property
    def counts(self) -> LogCounts:
        """How much has been written so far.

        Returns:
            The counts.
        """
        return LogCounts(*self._counts)

    def close(self) -> None:
        """Flush and close every file."""
        for handle in (self._transactions, self._labels, self._truth):
            handle.close()

    def __enter__(self) -> Self:
        """Enter the context.

        Returns:
            This log.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the log on the way out.

        Args:
            exc_type: Exception type, if the block raised.
            exc: The exception, if the block raised.
            traceback: The traceback, if the block raised.
        """
        self.close()


def _open_for_read(path: Path) -> io.TextIOBase:
    """Open a log file, compressed or not, for reading.

    Args:
        path: The file, with or without its `.gz` suffix.

    Returns:
        A text handle.

    Raises:
        FileNotFoundError: If neither the plain nor the gzipped file exists.
    """
    if path.exists():
        return open(path, encoding="utf-8")
    gz = path.with_suffix(path.suffix + ".gz")
    if gz.exists():
        return io.TextIOWrapper(gzip.open(gz, "rb"), encoding="utf-8")
    msg = f"no raw log at {path} or {gz}"
    raise FileNotFoundError(msg)


def read_transactions(directory: Path) -> Iterator[TransactionEvent]:
    """Replay the transaction log in the order it was written.

    Args:
        directory: The log directory.

    Yields:
        Each transaction, validated through the same decoder a consumer uses.
    """
    with _open_for_read(directory / TRANSACTIONS_FILE) as handle:
        for line in handle:
            if line.strip():
                yield decode_transaction(line)


def read_labels(directory: Path) -> Iterator[LabelEvent]:
    """Replay the label log.

    Args:
        directory: The log directory.

    Yields:
        Each label.
    """
    with _open_for_read(directory / LABELS_FILE) as handle:
        for line in handle:
            if line.strip():
                yield LabelEvent.model_validate_json(line)


def read_ground_truth(directory: Path) -> Iterator[GroundTruth]:
    """Replay the ground-truth log.

    Reading this outside evaluation is the largest leak available in this
    system. It is a separate function, in a separate file, for that reason.

    Args:
        directory: The log directory.

    Yields:
        Each ground-truth record.
    """
    with _open_for_read(directory / GROUND_TRUTH_FILE) as handle:
        for line in handle:
            if line.strip():
                yield GroundTruth.model_validate_json(line)
