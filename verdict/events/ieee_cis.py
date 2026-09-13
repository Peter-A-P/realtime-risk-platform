"""Taking in the IEEE-CIS competition archive, and finding out what is in it.

Two jobs, deliberately separated from any use of the data.

**Ingest** takes the archive the account holder downloaded, records a hash of
it, extracts it into `data/ieee-cis/` and writes a manifest of what came out
with a hash per file. Nothing here downloads anything: the competition rules
permit use by people who have accepted them, so the download is a deliberate
act by the account holder rather than something a script does on their behalf
(`docs/data.md`). No API token is stored by this project.

**Inspect** reports what the files actually contain: columns, types, row
counts, how much is missing, and the three facts this platform's design turns
on. It exists because the loader that maps this data onto the platform's event
schema should be written against the file, not against a memory of a schema
that was published in 2019.

The three questions inspection answers:

1. **Is there a merchant identifier?** The platform's feature set includes
   merchant-keyed features. If the competition data has no merchant column,
   the real-data track cannot compute them, and the honest response is to say
   so per track rather than to invent a merchant from other columns and then
   measure an entity-graph feature against something fabricated.
2. **How much of the data has device information?** The identity file covers
   only part of the transactions, so device-keyed features exist for a subset.
   The size of that subset decides whether they are usable at all.
3. **What is the time column really?** `TransactionDT` is published as an
   offset rather than a timestamp. Everything in this platform is keyed on
   event time, so the offset's unit, range and resolution have to be
   established before a single row is replayed. Its resolution in particular
   decides how often two transactions share an instant, which is the boundary
   that produced the leak in `docs/leak-caught.md`.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

DEFAULT_DESTINATION: Final = Path("data/ieee-cis")
"""Where the extracted files live. Gitignored, and covered by a test."""

MANIFEST_NAME: Final = "manifest.json"

TRANSACTION_FILE: Final = "train_transaction.csv"
IDENTITY_FILE: Final = "train_identity.csv"

SAMPLE_ROWS: Final = 50_000
"""How many rows to read when reporting types and missing shares.

The transaction file is hundreds of megabytes. Row counts are exact because
they are cheap; the distribution statistics are from a sample, and every
report says so rather than implying a full pass.
"""

EXPECTED_ENTITY_COLUMNS: Final[tuple[str, ...]] = (
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "isFraud",
)
"""Columns the loader cannot work without, whatever else changed."""

MERCHANT_CANDIDATES: Final[tuple[str, ...]] = (
    "merchant",
    "merchant_id",
    "merchantid",
    "merchantname",
    "mcc",
)
"""Column names that would constitute a merchant identifier if present.

Checked case-insensitively. `ProductCD` is deliberately not in this list: it
is a product category with a handful of values, not an identifier of a party,
and treating it as one would give every transaction in the set one of five
"merchants".
"""


class ArchiveError(ValueError):
    """Raised when the archive is not the one this loader expects."""


@dataclass(frozen=True, slots=True)
class ExtractedFile:
    """One file taken out of the archive.

    Attributes:
        name: The file's name inside the archive.
        bytes_written: Its size on disk.
        sha256: Hash of its contents, so a later run can tell it apart from a
            re-download that differs.
    """

    name: str
    bytes_written: int
    sha256: str


@dataclass(frozen=True, slots=True)
class IngestManifest:
    """What an ingest produced.

    Attributes:
        archive_name: The archive's file name.
        archive_sha256: Hash of the archive itself.
        destination: Where the files were written.
        files: One entry per extracted file.
    """

    archive_name: str
    archive_sha256: str
    destination: str
    files: tuple[ExtractedFile, ...] = field(default_factory=tuple)

    def to_json(self) -> str:
        """Render the manifest for `manifest.json`.

        Returns:
            Pretty-printed JSON with a trailing newline.
        """
        payload = {
            "archive_name": self.archive_name,
            "archive_sha256": self.archive_sha256,
            "destination": self.destination,
            "files": [asdict(entry) for entry in self.files],
        }
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def sha256_of(path: Path, chunk_size: int = 1 << 20) -> str:
    """Hash a file without reading it into memory.

    Args:
        path: The file.
        chunk_size: How much to read at a time.

    Returns:
        The hex digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_members(archive: zipfile.ZipFile, destination: Path) -> Iterator[zipfile.ZipInfo]:
    """Yield archive members that are safe to extract.

    A zip entry can name a path outside the directory it is extracted into,
    by using `..` or an absolute path. This is a downloaded file being
    unpacked by a script, so the check is cheap insurance rather than
    paranoia.

    Args:
        archive: The open archive.
        destination: Where files are being written.

    Yields:
        Each safe file member. Directories are skipped.

    Raises:
        ArchiveError: If a member would be written outside `destination`.
    """
    resolved = destination.resolve()
    for member in archive.infolist():
        if member.is_dir():
            continue
        target = (destination / member.filename).resolve()
        if not target.is_relative_to(resolved):
            msg = f"archive member {member.filename!r} would be written outside {destination}"
            raise ArchiveError(msg)
        yield member


def ingest(archive_path: Path, destination: Path = DEFAULT_DESTINATION) -> IngestManifest:
    """Extract the competition archive and record what came out.

    Args:
        archive_path: The downloaded archive.
        destination: Where to extract it. Created if missing.

    Returns:
        The manifest, which is also written to `manifest.json` alongside the
        extracted files.

    Raises:
        FileNotFoundError: If the archive is not there.
        ArchiveError: If it is not a zip, or holds no CSV files.
    """
    if not archive_path.exists():
        msg = (
            f"no archive at {archive_path}. Download it from the competition's Data tab "
            f"with the 'Download as zip' button; nothing here downloads it for you"
        )
        raise FileNotFoundError(msg)
    if not zipfile.is_zipfile(archive_path):
        msg = f"{archive_path} is not a zip archive"
        raise ArchiveError(msg)

    destination.mkdir(parents=True, exist_ok=True)
    extracted: list[ExtractedFile] = []
    with zipfile.ZipFile(archive_path) as archive:
        members = list(_safe_members(archive, destination))
        if not any(member.filename.endswith(".csv") for member in members):
            msg = f"{archive_path} holds no CSV files; is it the competition archive?"
            raise ArchiveError(msg)
        for member in members:
            archive.extract(member, destination)
            written = destination / member.filename
            extracted.append(
                ExtractedFile(
                    name=member.filename,
                    bytes_written=written.stat().st_size,
                    sha256=sha256_of(written),
                )
            )

    manifest = IngestManifest(
        archive_name=archive_path.name,
        archive_sha256=sha256_of(archive_path),
        destination=str(destination),
        files=tuple(sorted(extracted, key=lambda entry: entry.name)),
    )
    (destination / MANIFEST_NAME).write_text(manifest.to_json(), encoding="utf-8")
    return manifest


def manifest_existing(
    destination: Path = DEFAULT_DESTINATION, archive_path: Path | None = None
) -> IngestManifest:
    """Record hashes for files that are already extracted.

    The archive can be unpacked by whoever downloaded it, which is often what
    happens, and re-extracting it here would put a second copy of a gigabyte
    on disk to no purpose. This hashes what is there instead, so the
    provenance record exists either way and a later run can tell whether the
    files changed.

    Args:
        destination: The directory holding the extracted files.
        archive_path: The archive they came from, if it is still around.

    Returns:
        The manifest, also written to `manifest.json`.

    Raises:
        FileNotFoundError: If the directory holds no CSV files.
    """
    files = sorted(path for path in destination.glob("*.csv"))
    if not files:
        msg = f"no CSV files in {destination}"
        raise FileNotFoundError(msg)
    manifest = IngestManifest(
        archive_name=archive_path.name if archive_path else "(extracted elsewhere)",
        archive_sha256=sha256_of(archive_path) if archive_path else "",
        destination=str(destination),
        files=tuple(
            ExtractedFile(name=path.name, bytes_written=path.stat().st_size, sha256=sha256_of(path))
            for path in files
        ),
    )
    (destination / MANIFEST_NAME).write_text(manifest.to_json(), encoding="utf-8")
    return manifest


CHECKSUM_RECORD: Final = Path("docs/ieee-cis-checksums.json")
"""The committed record of what the files should hash to.

Hashes only, never data. It is what makes "the loader verifies a checksum" a
fact rather than an intention: a truncated download, a re-download that
differs, or a file edited in place is an error here rather than a strange
model three weeks later.
"""


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """What verification found.

    Attributes:
        checked: Files compared against the record.
        missing: Files in the record that are not on disk.
        mismatched: Files whose hash differs from the record.
    """

    checked: tuple[str, ...]
    missing: tuple[str, ...]
    mismatched: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """Whether every recorded file was present and unchanged.

        Returns:
            True if nothing is missing or mismatched.
        """
        return not self.missing and not self.mismatched


def verify(
    destination: Path = DEFAULT_DESTINATION, record_path: Path = CHECKSUM_RECORD
) -> VerificationResult:
    """Check local files against the committed checksum record.

    Args:
        destination: Where the files are.
        record_path: The committed record.

    Returns:
        The result.

    Raises:
        FileNotFoundError: If the record itself is missing.
    """
    if not record_path.exists():
        msg = f"no checksum record at {record_path}"
        raise FileNotFoundError(msg)
    record = json.loads(record_path.read_text(encoding="utf-8"))["files"]
    checked: list[str] = []
    missing: list[str] = []
    mismatched: list[str] = []
    for name, expected in sorted(record.items()):
        path = destination / name
        if not path.exists():
            missing.append(name)
            continue
        checked.append(name)
        if sha256_of(path) != expected["sha256"]:
            mismatched.append(name)
    return VerificationResult(
        checked=tuple(checked), missing=tuple(missing), mismatched=tuple(mismatched)
    )


def read_manifest(destination: Path = DEFAULT_DESTINATION) -> IngestManifest:
    """Read back the manifest written by a previous ingest.

    Args:
        destination: Where the files were extracted.

    Returns:
        The manifest.

    Raises:
        FileNotFoundError: If nothing has been ingested there.
    """
    path = destination / MANIFEST_NAME
    if not path.exists():
        msg = f"no manifest at {path}; run `verdict data ingest` first"
        raise FileNotFoundError(msg)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return IngestManifest(
        archive_name=payload["archive_name"],
        archive_sha256=payload["archive_sha256"],
        destination=payload["destination"],
        files=tuple(ExtractedFile(**entry) for entry in payload["files"]),
    )


@dataclass(frozen=True, slots=True)
class ColumnReport:
    """What one column looks like.

    Attributes:
        name: The column.
        dtype: The type pandas inferred from the sample.
        missing_share: Share of sampled rows where it is null.
        distinct_in_sample: How many distinct values the sample held.
        examples: A few values, for reading.
    """

    name: str
    dtype: str
    missing_share: float
    distinct_in_sample: int
    examples: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SchemaReport:
    """What the competition files actually contain.

    Attributes:
        file: Which file this describes.
        rows: Exact row count.
        columns: One report per column, from a sample.
        sampled_rows: How many rows the column statistics came from.
    """

    file: str
    rows: int
    columns: tuple[ColumnReport, ...]
    sampled_rows: int

    def column(self, name: str) -> ColumnReport | None:
        """Find one column's report.

        Args:
            name: The column name.

        Returns:
            The report, or None if there is no such column.
        """
        return next((column for column in self.columns if column.name == name), None)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Every column name, in file order.

        Returns:
            The names.
        """
        return tuple(column.name for column in self.columns)


def count_rows(path: Path, chunk_size: int = 1 << 20) -> int:
    """Count the data rows in a CSV without parsing it.

    Args:
        path: The file.
        chunk_size: How much to read at a time.

    Returns:
        The number of rows, excluding the header.
    """
    newlines = 0
    ends_with_newline = True
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            newlines += chunk.count(b"\n")
            ends_with_newline = chunk.endswith(b"\n")
    if not ends_with_newline:
        newlines += 1
    return max(0, newlines - 1)


def inspect_file(path: Path, sample_rows: int = SAMPLE_ROWS) -> SchemaReport:
    """Describe one competition file.

    Args:
        path: The CSV to inspect.
        sample_rows: How many rows to read for the column statistics.

    Returns:
        The report.

    Raises:
        FileNotFoundError: If the file is not there.
    """
    import pandas as pd

    if not path.exists():
        msg = f"no file at {path}"
        raise FileNotFoundError(msg)

    sample = pd.read_csv(path, nrows=sample_rows, low_memory=False)
    columns = tuple(
        ColumnReport(
            name=str(name),
            dtype=str(sample[name].dtype),
            missing_share=round(float(sample[name].isna().mean()), 4),
            distinct_in_sample=int(sample[name].nunique(dropna=True)),
            examples=tuple(str(value) for value in sample[name].dropna().unique()[:3]),
        )
        for name in sample.columns
    )
    return SchemaReport(
        file=path.name,
        rows=count_rows(path),
        columns=columns,
        sampled_rows=int(len(sample)),
    )


@dataclass(frozen=True, slots=True)
class Findings:
    """The answers the loader's design waits on.

    Attributes:
        has_merchant_identifier: Whether any column identifies a merchant.
        merchant_columns: The columns that would qualify, if any.
        identity_coverage: Share of transactions with a row in the identity
            file, or None if that file is absent.
        time_column: What `TransactionDT` looks like: its minimum, maximum,
            span and the resolution of its values.
        fraud_share: Share of transactions labelled fraud.
        notes: Anything else worth saying in words.
    """

    has_merchant_identifier: bool
    merchant_columns: tuple[str, ...]
    identity_coverage: float | None
    time_column: dict[str, Any]
    fraud_share: float | None
    notes: tuple[str, ...]


def find_answers(destination: Path = DEFAULT_DESTINATION) -> Findings:
    """Answer the three questions the loader's design turns on.

    Args:
        destination: Where the files were extracted.

    Returns:
        The findings.

    Raises:
        FileNotFoundError: If the transaction file is not there.
    """
    import pandas as pd

    transactions = destination / TRANSACTION_FILE
    if not transactions.exists():
        msg = f"no {TRANSACTION_FILE} at {destination}; run `verdict data ingest` first"
        raise FileNotFoundError(msg)

    header = pd.read_csv(transactions, nrows=0)
    names = tuple(str(name) for name in header.columns)
    merchant_columns = tuple(name for name in names if name.lower() in MERCHANT_CANDIDATES)

    notes: list[str] = []
    missing = [name for name in EXPECTED_ENTITY_COLUMNS if name not in names]
    if missing:
        notes.append(
            f"columns this loader needs are absent: {missing}. The published schema has "
            f"changed, and the mapping in this module must not be trusted until it is "
            f"rewritten against the file."
        )

    time_and_label = pd.read_csv(
        transactions,
        usecols=[name for name in ("TransactionDT", "isFraud") if name in names],
    )
    time_column: dict[str, Any] = {}
    if "TransactionDT" in time_and_label:
        values = time_and_label["TransactionDT"]
        deltas = values.sort_values().diff().dropna()
        time_column = {
            "min": int(values.min()),
            "max": int(values.max()),
            "span_days": round(float(values.max() - values.min()) / 86_400, 2),
            "distinct_values": int(values.nunique()),
            "rows": int(len(values)),
            "share_sharing_a_value": round(1 - values.nunique() / len(values), 4),
            "smallest_gap": int(deltas[deltas > 0].min()) if (deltas > 0).any() else 0,
            "looks_like": "whole seconds offset from an unstated reference time",
        }
        notes.append(
            f"TransactionDT resolution matters: {time_column['share_sharing_a_value']:.1%} "
            f"of rows share their value with another row, and same-instant events are the "
            f"boundary that produced the leak in docs/leak-caught.md."
        )

    fraud_share = (
        round(float(time_and_label["isFraud"].mean()), 5) if "isFraud" in time_and_label else None
    )

    identity_coverage: float | None = None
    identity = destination / IDENTITY_FILE
    if identity.exists():
        identity_ids = pd.read_csv(identity, usecols=["TransactionID"])
        identity_coverage = round(len(identity_ids) / max(1, len(time_and_label)), 4)
        notes.append(
            f"device-keyed features exist for {identity_coverage:.1%} of transactions, "
            f"because the identity file covers only part of the set."
        )

    if not merchant_columns:
        notes.append(
            "no merchant identifier: merchant-keyed features cannot be computed on this "
            "track. ProductCD is a product category, not a party, and using it as a "
            "merchant would give the whole set a handful of merchants."
        )

    return Findings(
        has_merchant_identifier=bool(merchant_columns),
        merchant_columns=merchant_columns,
        identity_coverage=identity_coverage,
        time_column=time_column,
        fraud_share=fraud_share,
        notes=tuple(notes),
    )
