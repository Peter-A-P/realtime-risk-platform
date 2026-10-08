"""Decision quality from the kept sample stands for the whole stream (ADR 18).

A day is built whose true counts are known, then kept the way finalising
keeps it: every acted row, and one approved row standing for many by its
weight. The report must give back the true shares, not the sample's.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from verdict.history.quality import decision_quality, report


def a_day() -> pa.Table:
    """1,000 transactions: 900 approved legitimate, 20 approved fraud, 50 declined, 30 reviewed.

    Of the declined, 30 are fraud; of the reviewed, 10. The approved are kept
    as one row each standing for its whole stratum.
    """
    rows: list[dict[str, object]] = []

    def add(action: str, rule: str, score: float, fraud: bool, weight: float, n: int) -> None:
        rows.extend(
            {
                "action": action,
                "rule": rule,
                "champion_score": score,
                "shadow_score": score,
                "is_fraud": fraud,
                "amount_cents": 1000,
                "weight": weight,
            }
            for _ in range(n)
        )

    add("approve", "approve", 0.05, False, 900.0, 1)
    add("approve", "approve", 0.40, True, 20.0, 1)
    add("decline", "decline_score", 0.95, True, 1.0, 30)
    add("decline", "decline_score", 0.92, False, 1.0, 20)
    add("review", "review_score", 0.60, True, 1.0, 10)
    add("review", "review_score", 0.55, False, 1.0, 20)
    return pa.Table.from_pylist(rows)


def test_the_weighted_shares_are_the_streams() -> None:
    quality = decision_quality(a_day())
    assert quality["transactions_weighted"] == 1000
    assert quality["fraud_share"] == pytest.approx(60 / 1000)
    assert quality["acted_share"] == pytest.approx(80 / 1000)
    assert quality["acted_precision"] == pytest.approx(40 / 80)
    assert quality["acted_recall"] == pytest.approx(40 / 60)
    decline = quality["by_action"]["decline"]
    assert decline["share_of_transactions"] == pytest.approx(0.05)
    assert decline["fraud_rate"] == pytest.approx(30 / 50)
    assert decline["share_of_all_fraud"] == pytest.approx(30 / 60)
    approve = quality["by_action"]["approve"]
    assert approve["fraud_rate"] == pytest.approx(20 / 920)


def test_read_without_the_weights_the_sample_would_mislead() -> None:
    """The kept rows alone are 82 of which 41 fraud: half, where the stream is 6%."""
    day = a_day()
    unweighted = day.set_column(
        day.schema.get_field_index("weight"), "weight", pa.array([1.0] * day.num_rows)
    )
    assert decision_quality(unweighted)["fraud_share"] == pytest.approx(41 / 82)
    assert decision_quality(day)["fraud_share"] == pytest.approx(0.06)


def test_calibration_puts_each_row_in_its_score_band() -> None:
    bands = {b["band"]: b for b in decision_quality(a_day())["champion_calibration"]}
    assert bands["0.9 to 1.0"]["fraud_rate"] == pytest.approx(30 / 50)
    assert bands["0.0 to 0.1"]["fraud_rate"] == 0
    assert bands["0.0 to 0.1"]["share_of_transactions"] == pytest.approx(0.9)
    assert sum(b["share_of_transactions"] for b in bands.values()) == pytest.approx(1.0)


def test_a_report_covers_each_day_and_all_together() -> None:
    out = report([("2026-09-30", a_day()), ("2026-10-01", a_day())])
    assert set(out["days"]) == {"2026-09-30", "2026-10-01"}
    assert out["all"]["transactions_weighted"] == 2000
    assert out["all"]["fraud_share"] == pytest.approx(0.06)
