"""The retraining job stops where it is supposed to stop.

The job fits a candidate and writes a pull request. The rule that matters is
what it does not do: it never moves the champion pointer, never writes a
flag, and never claims a promotion. `test_promote.py` holds the gate that
judges a challenger; this holds the step before it, which is the one an
automated pipeline would be tempted to let run all the way through.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from verdict.drift.monitors import DailyReport, QuantityResult, Status
from verdict.drift.trigger import RetrainRequest
from verdict.models.dataset import training_schema
from verdict.models.retrain import (
    pull_request_body,
    request_from_report,
    retrain,
    write_pull_request,
)
from verdict.store.features import feature_names

START = dt.datetime(2027, 1, 1, tzinfo=dt.UTC)


def a_request(first_day: dt.date = dt.date(2027, 1, 15)) -> RetrainRequest:
    """A request as the trigger would have made it.

    Args:
        first_day: The first drifted day.

    Returns:
        A request over two days.
    """
    days = tuple(
        DailyReport(
            day=first_day + dt.timedelta(days=offset),
            results=(
                QuantityResult("merchant_txn_count_1h", 52_000, 0.41, 0.18, 1e-9, Status.DRIFTED),
                QuantityResult("amount_cents", 52_000, 0.02, 0.01, 0.4, Status.STABLE),
            ),
        )
        for offset in range(2)
    )
    return RetrainRequest(
        opened_on=first_day + dt.timedelta(days=1),
        quantities=("merchant_txn_count_1h",),
        evidence=days,
    )


def a_table(path: Path, hours: int = 24 * 40) -> Path:
    """A small replayed table, fraud on a signal the fit can find.

    Args:
        path: Where the Parquet file goes.
        hours: How many hourly transactions.

    Returns:
        The path written.
    """
    rng = np.random.default_rng(11)
    rows = []
    for index in range(hours):
        at = START + dt.timedelta(hours=index)
        fraud = index % 7 == 0
        row: dict[str, object] = {
            name: float(rng.normal(8.0 if fraud else 1.0, 0.5)) for name in feature_names()
        }
        row.update(
            event_id=f"evt-{index}",
            event_time=at,
            amount_cents=900 if fraud else 100,
            label_time=at + dt.timedelta(days=7),
            is_fraud=fraud,
            weight=1.0,
        )
        rows.append(row)
    pq.write_table(pa.Table.from_pylist(rows, schema=training_schema()), path)
    return path


def test_a_saved_drift_report_becomes_the_request_the_trigger_made(tmp_path: Path) -> None:
    request = a_request()
    saved = {
        "first_request": {
            "opened_on": request.opened_on.isoformat(),
            "quantities": list(request.quantities),
            "evidence": [
                {
                    "day": report.day.isoformat(),
                    "results": [
                        {
                            "name": result.name,
                            "values": result.values,
                            "psi": result.psi,
                            "ks_statistic": result.ks_statistic,
                            "ks_p_value": result.ks_p_value,
                            "status": str(result.status),
                        }
                        for result in report.results
                    ],
                }
                for report in request.evidence
            ],
        }
    }
    path = tmp_path / "drift.json"
    path.write_text(json.dumps(saved), encoding="utf-8")
    rebuilt = request_from_report(json.loads(path.read_text(encoding="utf-8")))
    assert rebuilt == request


def test_a_run_that_opened_no_request_has_nothing_to_retrain_for() -> None:
    with pytest.raises(ValueError, match="no retraining request"):
        request_from_report({"first_request": None})


def test_the_job_fits_a_candidate_and_never_moves_the_pointer(tmp_path: Path) -> None:
    """The automated path ends at a pull request. This is that line, asserted."""
    from verdict.models.train import export_onnx, fit_champion, split_by_time
    from verdict.scoring.flags import set_champion
    from verdict.scoring.onnx_model import OnnxModel

    table = a_table(tmp_path / "fixed.parquet")
    champion_path = tmp_path / "champion.onnx"
    split = split_by_time(pq.read_table(table))
    export_onnx(fit_champion(split.train, threads=2), champion_path, split.test.inputs[:50])

    pointer = tmp_path / "champion.json"
    champion = OnnxModel(champion_path)
    set_champion(pointer, champion.version, {champion.version: champion})
    before = pointer.read_bytes()

    request = a_request()
    report = retrain(
        request,
        track="real",
        table_path=table,
        champion_path=champion_path,
        candidate_path=tmp_path / "candidate.onnx",
    )

    assert (tmp_path / "candidate.onnx").exists()
    assert report["promoted"] is False
    assert report["opened_by"]["quantities"] == ["merchant_txn_count_1h"]
    assert pointer.read_bytes() == before


def test_the_pull_request_says_what_is_missing_and_does_not_ask_for_promotion(
    tmp_path: Path,
) -> None:
    from verdict.models.train import export_onnx, fit_champion, split_by_time

    table = a_table(tmp_path / "fixed.parquet")
    champion_path = tmp_path / "champion.onnx"
    split = split_by_time(pq.read_table(table))
    export_onnx(fit_champion(split.train, threads=2), champion_path, split.test.inputs[:50])

    request = a_request()
    report = retrain(
        request,
        track="real",
        table_path=table,
        champion_path=champion_path,
        candidate_path=tmp_path / "candidate.onnx",
    )
    body = pull_request_body(report, request)
    assert "does not promote anything" in body
    assert "merchant_txn_count_1h" in body
    assert "A labelled shadow window" in body
    assert "Test PR-AUC" in body
    assert report["request"] in {"answered", "still-open"}
    assert report["beats_incumbent"] is (report["request"] == "answered")
    assert ("stays **open**" in body) is (report["request"] == "still-open")
    written = write_pull_request(body, tmp_path / "pr.md")
    assert written.read_text(encoding="utf-8") == body


def test_a_candidate_that_cannot_have_seen_the_drift_says_so(tmp_path: Path) -> None:
    """Drift is reported in a day and a label takes a week, so the first candidate is blind.

    Neither rule is wrong and the gap cannot be engineered away. A pull
    request that stayed quiet about it would invite a reviewer to read a
    rebuild as an answer to a shift it was never shown.
    """
    from verdict.models.train import export_onnx, fit_champion, split_by_time

    table = a_table(tmp_path / "fixed.parquet")
    champion_path = tmp_path / "champion.onnx"
    split = split_by_time(pq.read_table(table))
    export_onnx(fit_champion(split.train, threads=2), champion_path, split.test.inputs[:50])

    # Drift well after the training cutoff, as a request opened today would be.
    request = a_request(first_day=dt.date(2027, 2, 5))
    report = retrain(
        request,
        track="real",
        table_path=table,
        champion_path=champion_path,
        candidate_path=tmp_path / "candidate.onnx",
    )
    coverage = report["drifted_days_in_training"]
    assert coverage["covers_the_drift"] is False
    assert coverage["days_short"] > 0
    body = pull_request_body(report, request)
    assert "has not seen the drift" in body
    assert "a rebuild, not an answer" in body


def test_coverage_is_judged_by_the_transactions_not_by_the_cutoff(tmp_path: Path) -> None:
    """The cutoff can sit past a drift the model never saw.

    On the synthetic track the model is trained a label delay after its split
    point, so its cutoff is a week later than the last transaction it learned
    from. The first version of this check compared the cutoff to the first
    drifted day, and on the twenty-day replay it would have reported a blind
    candidate as having been trained on the shift.
    """
    from verdict.models.train import export_onnx, fit_champion, split_by_time

    table = a_table(tmp_path / "fixed.parquet")
    champion_path = tmp_path / "champion.onnx"
    split = split_by_time(pq.read_table(table), wait_for_labels=True)
    export_onnx(fit_champion(split.train, threads=2), champion_path, split.test.inputs[:50])

    # Between the last training transaction and the cutoff.
    drifted = split.cutoff.date() - dt.timedelta(days=3)
    report = retrain(
        a_request(first_day=drifted),
        track="synthetic",
        table_path=table,
        champion_path=champion_path,
        candidate_path=tmp_path / "candidate.onnx",
    )
    coverage = report["drifted_days_in_training"]
    assert dt.date.fromisoformat(coverage["training_cutoff"][:10]) > drifted
    assert coverage["covers_the_drift"] is False
    assert dt.date.fromisoformat(coverage["last_day_trained_on"]) < drifted
