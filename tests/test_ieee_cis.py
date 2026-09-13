"""Ingesting the competition archive, and reporting what is in it.

The real archive is not here and is not committed, so these run against a
synthetic one built in the shape the competition published: a transaction
file with the columns the loader needs, and an identity file covering only
part of the rows. When the real archive lands, the same code runs against it
and the report either confirms this shape or says plainly that it has changed.

The test that matters most is the last one: `find_answers` must report the
absence of a merchant identifier rather than inventing one, because a
fabricated entity would make every merchant-keyed feature on the real-data
track a measurement of nothing.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from verdict.events.ieee_cis import (
    IDENTITY_FILE,
    MANIFEST_NAME,
    TRANSACTION_FILE,
    ArchiveError,
    count_rows,
    find_answers,
    ingest,
    inspect_file,
    manifest_existing,
    read_manifest,
    sha256_of,
    verify,
)

TRANSACTION_HEADER = (
    "TransactionID,isFraud,TransactionDT,TransactionAmt,ProductCD,card1,card4,addr1,"
    "P_emaildomain,C1,D1,M1,V1"
)
IDENTITY_HEADER = "TransactionID,id_01,id_31,DeviceType,DeviceInfo"


def a_transaction_row(index: int) -> str:
    """One row shaped like the competition's, with whole-second times."""
    fraud = 1 if index % 30 == 0 else 0
    # Whole seconds, and deliberately repeated, which is how the real file is.
    seconds = 86_400 + (index // 3) * 7
    amount = 25.0 + (index % 17) * 3.5
    return (
        f"{2_987_000 + index},{fraud},{seconds},{amount:.2f},W,{13000 + index % 500},"
        f"visa,{100 + index % 90},gmail.com,{index % 9},{index % 200},T,0.0"
    )


def an_identity_row(index: int) -> str:
    return f"{2_987_000 + index},0.0,chrome 62.0,desktop,Windows"


def build_archive(path: Path, rows: int = 300, identity_rows: int = 90) -> Path:
    """Write a synthetic competition archive."""
    transactions = "\n".join([TRANSACTION_HEADER, *(a_transaction_row(i) for i in range(rows))])
    identity = "\n".join([IDENTITY_HEADER, *(an_identity_row(i) for i in range(identity_rows))])
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(TRANSACTION_FILE, transactions + "\n")
        archive.writestr(IDENTITY_FILE, identity + "\n")
    return path


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    return build_archive(tmp_path / "ieee-fraud-detection.zip")


# --- ingest -----------------------------------------------------------------


def test_ingest_extracts_and_hashes_everything(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    manifest = ingest(archive, destination)

    assert {entry.name for entry in manifest.files} == {TRANSACTION_FILE, IDENTITY_FILE}
    assert manifest.archive_sha256 == sha256_of(archive)
    for entry in manifest.files:
        written = destination / entry.name
        assert written.exists()
        assert entry.sha256 == sha256_of(written)
        assert entry.bytes_written == written.stat().st_size


def test_the_manifest_is_written_and_reads_back(archive: Path, tmp_path: Path) -> None:
    """A hash nobody can check later is a hash nobody took."""
    destination = tmp_path / "ieee-cis"
    written = ingest(archive, destination)
    assert (destination / MANIFEST_NAME).exists()
    assert read_manifest(destination) == written


def test_a_missing_archive_says_where_to_get_it(tmp_path: Path) -> None:
    """The error is the documentation someone reads at the worst moment."""
    with pytest.raises(FileNotFoundError, match="Download as zip"):
        ingest(tmp_path / "nothing.zip", tmp_path / "out")


def test_something_that_is_not_a_zip_is_refused(tmp_path: Path) -> None:
    not_a_zip = tmp_path / "notes.txt"
    not_a_zip.write_text("this is not an archive", encoding="utf-8")
    with pytest.raises(ArchiveError, match="not a zip"):
        ingest(not_a_zip, tmp_path / "out")


def test_an_archive_without_csv_files_is_refused(tmp_path: Path) -> None:
    """A wrong download should fail here, not three steps downstream."""
    wrong = tmp_path / "wrong.zip"
    with zipfile.ZipFile(wrong, "w") as archive:
        archive.writestr("readme.txt", "not the competition data")
    with pytest.raises(ArchiveError, match="no CSV files"):
        ingest(wrong, tmp_path / "out")


def test_an_archive_member_cannot_escape_the_destination(tmp_path: Path) -> None:
    """Zip slip: a member can name a path outside where it is extracted.

    This is a downloaded file being unpacked by a script, so the check is
    cheap insurance rather than paranoia.
    """
    malicious = tmp_path / "evil.zip"
    with zipfile.ZipFile(malicious, "w") as archive:
        archive.writestr("train_transaction.csv", "TransactionID\n1\n")
        archive.writestr("../../escaped.csv", "gotcha")
    with pytest.raises(ArchiveError, match="outside"):
        ingest(malicious, tmp_path / "out" / "nested")
    assert not (tmp_path / "escaped.csv").exists()


def test_reading_a_manifest_that_does_not_exist_says_what_to_run(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="verdict data ingest"):
        read_manifest(tmp_path / "empty")


# --- counting and inspecting ------------------------------------------------


def test_rows_are_counted_exactly(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    assert count_rows(destination / TRANSACTION_FILE) == 300
    assert count_rows(destination / IDENTITY_FILE) == 90


def test_a_file_without_a_trailing_newline_still_counts_correctly(tmp_path: Path) -> None:
    path = tmp_path / "small.csv"
    path.write_text("a,b\n1,2\n3,4", encoding="utf-8")
    assert count_rows(path) == 2


def test_inspection_reports_every_column(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    report = inspect_file(destination / TRANSACTION_FILE)
    assert report.rows == 300
    assert "TransactionDT" in report.column_names
    assert "TransactionAmt" in report.column_names
    amount = report.column("TransactionAmt")
    assert amount is not None
    assert amount.missing_share == 0.0
    assert amount.examples


def test_inspecting_a_file_that_is_not_there_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no file at"):
        inspect_file(tmp_path / "absent.csv")


# --- the three questions the loader's design waits on -----------------------


def test_the_absence_of_a_merchant_identifier_is_reported_not_papered_over(
    archive: Path, tmp_path: Path
) -> None:
    """The finding that decides what the real-data track can prove.

    Reporting a merchant that is not there, by promoting `ProductCD` or some
    other column, would make every merchant-keyed feature on this track a
    measurement of something fabricated here.
    """
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    findings = find_answers(destination)
    assert findings.has_merchant_identifier is False
    assert findings.merchant_columns == ()
    assert any("no merchant identifier" in note for note in findings.notes)
    assert any("ProductCD" in note for note in findings.notes)


def test_a_real_merchant_column_would_be_found(tmp_path: Path) -> None:
    """The check is not hard-coded to fail: give it one and it finds it."""
    destination = tmp_path / "ieee-cis"
    destination.mkdir(parents=True)
    (destination / TRANSACTION_FILE).write_text(
        "TransactionID,isFraud,TransactionDT,TransactionAmt,merchant_id\n"
        "1,0,86400,10.00,mer-1\n2,1,86407,20.00,mer-2\n",
        encoding="utf-8",
    )
    findings = find_answers(destination)
    assert findings.has_merchant_identifier is True
    assert findings.merchant_columns == ("merchant_id",)


def test_device_coverage_is_measured(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    findings = find_answers(destination)
    assert findings.identity_coverage == pytest.approx(0.3, abs=0.01)
    assert any("device-keyed features exist for" in note for note in findings.notes)


def test_the_time_column_is_characterised(archive: Path, tmp_path: Path) -> None:
    """Its resolution decides how often two events share an instant.

    That is the boundary that produced the leak in `docs/leak-caught.md`, so
    it is measured on the real file rather than assumed.
    """
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    findings = find_answers(destination)
    assert findings.time_column["rows"] == 300
    assert findings.time_column["share_sharing_a_value"] > 0.5
    assert findings.time_column["smallest_gap"] >= 1
    assert any("docs/leak-caught.md" in note for note in findings.notes)


def test_the_fraud_share_is_reported(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    findings = find_answers(destination)
    assert findings.fraud_share == pytest.approx(1 / 30, abs=0.01)


def test_a_changed_schema_is_called_out_rather_than_mapped_anyway(tmp_path: Path) -> None:
    """If the published columns have moved, the mapping must not be trusted."""
    destination = tmp_path / "ieee-cis"
    destination.mkdir(parents=True)
    (destination / TRANSACTION_FILE).write_text(
        "TransactionID,something_else\n1,2\n", encoding="utf-8"
    )
    findings = find_answers(destination)
    assert any("must not be trusted" in note for note in findings.notes)


def test_findings_survive_a_missing_identity_file(tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    destination.mkdir(parents=True)
    (destination / TRANSACTION_FILE).write_text(
        "TransactionID,isFraud,TransactionDT,TransactionAmt\n1,0,86400,10.00\n",
        encoding="utf-8",
    )
    findings = find_answers(destination)
    assert findings.identity_coverage is None


def test_find_answers_without_an_ingest_says_what_to_run(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="verdict data ingest"):
        find_answers(tmp_path / "empty")


# --- the command line -------------------------------------------------------


def test_the_cli_ingests_and_inspects(archive: Path, tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from verdict.cli import app

    runner = CliRunner()
    destination = tmp_path / "ieee-cis"

    ingested = runner.invoke(
        app, ["data", "ingest", "--archive", str(archive), "--out", str(destination)]
    )
    assert ingested.exit_code == 0, ingested.output
    assert TRANSACTION_FILE in ingested.stdout

    inspected = runner.invoke(app, ["data", "inspect", "--directory", str(destination)])
    assert inspected.exit_code == 0, inspected.output
    report = json.loads(inspected.stdout)
    assert report["has_merchant_identifier"] is False
    assert report["transaction_time"]["rows"] == 300


# --- provenance -------------------------------------------------------------


def test_a_manifest_can_be_taken_of_files_extracted_elsewhere(
    archive: Path, tmp_path: Path
) -> None:
    """Whoever downloads the archive often unpacks it themselves.

    Re-extracting it here to record hashes would put a second copy of a
    gigabyte on disk to no purpose.
    """
    destination = tmp_path / "already-there"
    destination.mkdir()
    with zipfile.ZipFile(archive) as source:
        source.extractall(destination)

    manifest = manifest_existing(destination, archive)
    assert {entry.name for entry in manifest.files} == {TRANSACTION_FILE, IDENTITY_FILE}
    assert manifest.archive_sha256 == sha256_of(archive)
    assert read_manifest(destination) == manifest


def test_a_manifest_of_an_empty_directory_is_refused(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no CSV files"):
        manifest_existing(tmp_path)


def test_verification_passes_on_unchanged_files(archive: Path, tmp_path: Path) -> None:
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    record = tmp_path / "record.json"
    record.write_text(
        json.dumps(
            {
                "files": {
                    entry.name: {"bytes": entry.bytes_written, "sha256": entry.sha256}
                    for entry in read_manifest(destination).files
                }
            }
        ),
        encoding="utf-8",
    )
    result = verify(destination, record)
    assert result.ok
    assert set(result.checked) == {TRANSACTION_FILE, IDENTITY_FILE}


def test_verification_catches_a_changed_file(archive: Path, tmp_path: Path) -> None:
    """A truncated download should be an error, not a strange model."""
    destination = tmp_path / "ieee-cis"
    ingest(archive, destination)
    record = tmp_path / "record.json"
    record.write_text(
        json.dumps({"files": {TRANSACTION_FILE: {"bytes": 1, "sha256": "0" * 64}}}),
        encoding="utf-8",
    )
    result = verify(destination, record)
    assert not result.ok
    assert result.mismatched == (TRANSACTION_FILE,)


def test_verification_catches_a_missing_file(tmp_path: Path) -> None:
    record = tmp_path / "record.json"
    record.write_text(
        json.dumps({"files": {TRANSACTION_FILE: {"bytes": 1, "sha256": "0" * 64}}}),
        encoding="utf-8",
    )
    result = verify(tmp_path / "nowhere", record)
    assert not result.ok
    assert result.missing == (TRANSACTION_FILE,)


def test_the_committed_record_holds_hashes_and_no_data() -> None:
    """It is checked into a public repository, so this matters.

    Sizes and hex digests are derived metadata. A column name would be a
    judgement call; a row would be a breach of the competition's terms.
    """
    record = json.loads(
        (Path(__file__).resolve().parents[1] / "docs" / "ieee-cis-checksums.json").read_text(
            encoding="utf-8"
        )
    )
    assert set(record["files"]) >= {TRANSACTION_FILE, IDENTITY_FILE}
    for entry in record["files"].values():
        assert set(entry) == {"bytes", "sha256"}
        assert len(entry["sha256"]) == 64
