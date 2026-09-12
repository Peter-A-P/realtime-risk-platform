"""The raw log is complete, replayable, and free of generator knowledge."""

from __future__ import annotations

from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.rawlog import (
    GROUND_TRUTH_FILE,
    LABELS_FILE,
    TRANSACTIONS_FILE,
    RawEventLog,
    read_ground_truth,
    read_labels,
    read_transactions,
)

REFERENCE = Population(cards=5_000, devices=4_000, merchants=200)
SEED = 20270201


@pytest.fixture(scope="module")
def records() -> list[object]:
    graph = EntityGraph.build(seed=SEED, population=REFERENCE)
    config = GeneratorConfig(seed=SEED, population=REFERENCE, events_per_second=500.0)
    return list(Generator(config, graph).stream(limit=500))


def write(directory: Path, records: list[object], *, compress: bool = False) -> None:
    with RawEventLog(directory, compress=compress) as log:
        for record in records:
            log.append(record)  # type: ignore[arg-type]


def test_what_was_written_is_what_is_read_back(tmp_path: Path, records: list[object]) -> None:
    """Every feature has to be recomputable from these files alone."""
    write(tmp_path, records)
    read = list(read_transactions(tmp_path))
    assert [event.to_json() for event in read] == [
        record.event.to_json()  # type: ignore[attr-defined]
        for record in records
    ]


def test_labels_and_ground_truth_round_trip(tmp_path: Path, records: list[object]) -> None:
    write(tmp_path, records)
    assert len(list(read_labels(tmp_path))) == len(records)
    assert len(list(read_ground_truth(tmp_path))) == len(records)


def test_the_transaction_log_contains_no_generator_knowledge(
    tmp_path: Path, records: list[object]
) -> None:
    """The largest leak available in this system is reading the truth file.

    Keeping it out of the transaction log is what makes that a deliberate act
    rather than a careless one, so the absence is asserted on the bytes.
    """
    write(tmp_path, records)
    text = (tmp_path / TRANSACTIONS_FILE).read_text(encoding="utf-8")
    for forbidden in ("is_fraud", "scenario", "regime", "recovered_cents", "label_time"):
        assert forbidden not in text


def test_the_three_files_are_separate(tmp_path: Path, records: list[object]) -> None:
    write(tmp_path, records)
    for name in (TRANSACTIONS_FILE, LABELS_FILE, GROUND_TRUTH_FILE):
        assert (tmp_path / name).exists()


def test_the_log_is_append_only(tmp_path: Path, records: list[object]) -> None:
    """A second run adds to the history; it never rewrites it."""
    write(tmp_path, records)
    write(tmp_path, records)
    assert len(list(read_transactions(tmp_path))) == len(records) * 2


def test_counts_report_what_was_written(tmp_path: Path, records: list[object]) -> None:
    with RawEventLog(tmp_path) as log:
        for record in records:
            log.append(record)  # type: ignore[arg-type]
        counts = log.counts
    assert counts.transactions == counts.labels == counts.ground_truth == len(records)


def test_a_compressed_log_reads_back_the_same(tmp_path: Path, records: list[object]) -> None:
    """The live window writes gzipped; a replay must not notice."""
    plain, packed = tmp_path / "plain", tmp_path / "packed"
    write(plain, records)
    write(packed, records, compress=True)
    assert (packed / f"{TRANSACTIONS_FILE}.gz").exists()
    assert [event.to_json() for event in read_transactions(packed)] == [
        event.to_json() for event in read_transactions(plain)
    ]


def test_a_missing_log_says_so(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no raw log"):
        list(read_transactions(tmp_path / "nowhere"))
