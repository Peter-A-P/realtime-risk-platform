"""The command line does what the week 1 criteria are stated in terms of."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from verdict.cli import app
from verdict.events.rawlog import TRANSACTIONS_FILE, read_transactions

runner = CliRunner()


def test_generate_writes_a_raw_log_and_reports_a_rate(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "generate",
            "--out",
            str(tmp_path),
            "--events",
            "2000",
            "--cards",
            "5000",
            "--devices",
            "4000",
            "--merchants",
            "200",
        ],
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["events"] == 2000
    assert report["events_per_second"] > 0
    assert len(list(read_transactions(tmp_path))) == 2000
    assert (tmp_path / TRANSACTIONS_FILE).exists()


def test_generate_reports_the_hashes_of_what_it_ran(tmp_path: Path) -> None:
    """A run has to say what it ran.

    A run that cannot name the schedule and the graph it used is not
    reproducible, however deterministic the generator is.
    """
    result = runner.invoke(
        app,
        [
            "generate",
            "--out",
            str(tmp_path),
            "--events",
            "200",
            "--cards",
            "5000",
            "--devices",
            "4000",
            "--merchants",
            "200",
        ],
    )
    report = json.loads(result.stdout)
    for key in ("schedule_sha256", "graph_sha256", "schema_sha256", "seed"):
        assert report[key]


def test_schedule_hash_prints_the_committed_hashes() -> None:
    result = runner.invoke(app, ["schedule", "hash"])
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    committed = json.loads(
        (Path(__file__).resolve().parents[1] / "docs" / "generator-hashes.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["dev_schedule_sha256"] == committed["dev_schedule_sha256"]
    assert report["regimes_source_sha256"] == committed["regimes_source_sha256"]


def test_schedule_show_prints_the_development_schedule() -> None:
    result = runner.invoke(app, ["schedule", "show"])
    assert result.exit_code == 0, result.output
    schedule = json.loads(result.stdout)
    assert schedule["name"] == "dev-2026-09"
    assert schedule["regimes"][0]["starts_after_days"] == 0.0


def test_seal_then_verify(tmp_path: Path) -> None:
    """The whole Jul 1 2027 operation, rehearsed in a test."""
    commitment = tmp_path / "sealed-schedule.json"
    sealed = runner.invoke(
        app,
        ["schedule", "seal", "--window-days", "87", "--out", str(commitment)],
        input="a real secret\n",
    )
    assert sealed.exit_code == 0, sealed.output
    assert "a real secret" not in commitment.read_text(encoding="utf-8")

    good = runner.invoke(
        app,
        ["schedule", "verify", "--commitment", str(commitment), "--reveal"],
        input="a real secret\n",
    )
    assert good.exit_code == 0, good.output
    assert "verified" in good.output

    bad = runner.invoke(
        app,
        ["schedule", "verify", "--commitment", str(commitment)],
        input="a different secret\n",
    )
    assert bad.exit_code == 1
    assert "REFUSED" in bad.output
