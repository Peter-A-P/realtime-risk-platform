"""The data terms, asserted rather than remembered.

The IEEE-CIS competition rules permit non-commercial use and forbid making
the data available to anyone who has not accepted them (`docs/data.md`, which
quotes sections 7.A and 7.B). Two of those obligations are the kind that get
honoured for three weeks and then broken by a hurried commit, so they are
tests:

- nothing under `data/` can be committed, including a feature store derived
  from the real-data track, which is a transformed copy rather than a
  published result;
- the repository carries an OSI-approved licence that does not limit
  commercial use, which section 8.B requires of publicly shared code.

These are cheap tests for an obligation that is not cheap to breach: a
published row of that data cannot be unpublished.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_the_data_directory_is_ignored_in_its_entirety() -> None:
    """Not just the raw files: anything derived from them, row by row."""
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "\ndata/\n" in ignore


@pytest.mark.parametrize(
    "path",
    [
        "data/ieee-cis/train_transaction.csv",
        "data/raw/live/transactions.jsonl",
        "data/feature_repo/data/card_features.parquet",
        "data/ieee-cis/derived/sample_rows.csv",
    ],
)
def test_git_refuses_to_track_anything_under_data(path: str) -> None:
    """Ask git itself, rather than re-implementing its ignore rules.

    The derived Parquet path is the one worth checking: it is not raw data,
    it is not obviously "the competition data", and it is exactly the file a
    reasonable person might commit as evidence of a working feature store.
    """
    result = subprocess.run(
        ["git", "check-ignore", "-q", path],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, f"{path} is not ignored and could be committed"


def test_no_competition_data_is_tracked_right_now() -> None:
    """The rule holds for the repository as it stands, not just in theory."""
    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    offenders = [
        path
        for path in tracked
        if path.startswith("data/") or (path.endswith((".csv", ".parquet")) and "test" not in path)
    ]
    assert not offenders, f"tracked data files: {offenders}"


def test_the_repository_carries_an_osi_licence_permitting_commercial_use() -> None:
    """Section 8.B deems publicly shared competition code so licensed.

    MIT satisfies it. A licence file that said "all rights reserved", or no
    licence file at all, would not.
    """
    licence = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert "MIT License" in licence
    assert "sublicense, and/or sell" in licence


def test_the_licence_says_it_does_not_cover_the_data() -> None:
    """The licence says what it does not cover.

    An MIT licence sitting beside a non-commercial dataset invites the wrong
    inference, so the file says so itself.
    """
    licence = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert "does not cover the data" in licence
    assert "non-commercial" in licence


def test_the_terms_are_recorded_with_the_clauses_they_come_from() -> None:
    """The terms are recorded with the clauses they come from.

    `docs/data.md` is the record. A summary without the clauses is a memory of
    a reading rather than a record of one.
    """
    terms = (ROOT / "docs" / "data.md").read_text(encoding="utf-8")
    for clause in ("7.A Data Access and Use", "7.B Data Security", "7.C External Data"):
        assert clause in terms
    assert "non-commercial purposes only" in terms
    assert "Rules read and recorded 2026-09-12" in terms
