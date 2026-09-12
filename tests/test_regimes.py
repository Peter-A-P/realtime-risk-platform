"""The regime schedule: well formed, deterministic, and genuinely sealed."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from verdict.events.generator.regimes import (
    AMOUNT_LOG_SHIFT_RANGE,
    ATTACK_INTENSITY_RANGE,
    DEV_SCHEDULE,
    FRAUD_RATE_MULTIPLIER_RANGE,
    MIN_REGIME_DAYS,
    ONLINE_SHARE_SHIFT_RANGE,
    REGIME_COUNT_RANGE,
    Regime,
    RegimeSchedule,
    SealedCommitment,
    derive_schedule,
    source_fingerprint,
)

HASHES = json.loads(
    (Path(__file__).resolve().parents[1] / "docs" / "generator-hashes.json").read_text(
        encoding="utf-8"
    )
)

WINDOW_DAYS = 87.0
"""The planned live window, Apr 5 to Jun 30 2027."""


def a_regime(name: str = "r", starts: float = 0.0) -> Regime:
    return Regime(
        name=name,
        starts_after_days=starts,
        fraud_rate_multiplier=1.0,
        scenario_weights=(1.0, 1.0, 1.0),
        amount_log_shift=0.0,
        online_share_shift=0.0,
        attack_intensity=1.0,
    )


def test_the_dev_schedule_matches_its_committed_hash() -> None:
    """The development schedule is public, but it still cannot drift."""
    assert DEV_SCHEDULE.fingerprint() == HASHES["dev_schedule_sha256"]


def test_the_regimes_source_matches_its_committed_hash() -> None:
    """The file is frozen from sealing until Jul 1 2027.

    This is the test that makes that a fact rather than an intention. If it
    fails, `regimes.py` was edited: either revert the edit or, before the
    seal, update `docs/generator-hashes.json` deliberately and say why in the
    commit message.
    """
    assert source_fingerprint() == HASHES["regimes_source_sha256"]


def test_the_dev_schedule_holds_the_case_that_catches_a_naive_monitor() -> None:
    """Feature drift with no change in fraud: the honest false positive."""
    regime = next(r for r in DEV_SCHEDULE.regimes if r.name == "amount-drift-no-fraud-change")
    assert regime.amount_log_shift != 0.0
    assert regime.fraud_rate_multiplier == 1.0


@pytest.mark.parametrize(
    ("elapsed_days", "expected"),
    [
        (-5.0, "baseline"),
        (0.0, "baseline"),
        (13.99, "baseline"),
        (14.0, "card-testing-wave"),
        (29.0, "card-testing-wave"),
        (30.0, "amount-drift-no-fraud-change"),
        (46.0, "takeover-season"),
        (900.0, "takeover-season"),
    ],
)
def test_the_regime_in_force_is_the_one_that_started_last(
    elapsed_days: float, expected: str
) -> None:
    assert DEV_SCHEDULE.at(elapsed_days).name == expected


@pytest.mark.parametrize(
    "regimes",
    [
        (),
        (a_regime(starts=3.0),),
        (a_regime("a"), a_regime("b", 5.0), a_regime("c", 2.0)),
        (a_regime("a"), a_regime("a", 5.0)),
    ],
)
def test_a_malformed_schedule_is_refused(regimes: tuple[Regime, ...]) -> None:
    with pytest.raises(ValueError, match=".+"):
        RegimeSchedule(name="bad", regimes=regimes)


def test_derivation_is_a_pure_function_of_the_secret() -> None:
    """Jul 1 2027 depends on this: the secret alone rebuilds the schedule."""
    first = derive_schedule("correct horse", window_days=WINDOW_DAYS, name="live")
    second = derive_schedule("correct horse", window_days=WINDOW_DAYS, name="live")
    assert first.fingerprint() == second.fingerprint()


def test_a_different_secret_gives_a_different_schedule() -> None:
    first = derive_schedule("correct horse", window_days=WINDOW_DAYS, name="live")
    other = derive_schedule("correct horsf", window_days=WINDOW_DAYS, name="live")
    assert first.fingerprint() != other.fingerprint()


@pytest.mark.parametrize("secret", ["one", "two", "three", "four", "five"])
def test_a_derived_schedule_stays_inside_the_published_ranges(secret: str) -> None:
    """The design is public even while the realisation is not.

    Anyone reading `regimes.py` before Jul 1 knows exactly what the sealed
    schedule can and cannot do. This test is what makes that claim true.
    """
    schedule = derive_schedule(secret, window_days=WINDOW_DAYS, name="live")
    assert REGIME_COUNT_RANGE[0] <= len(schedule.regimes) <= REGIME_COUNT_RANGE[1]
    assert schedule.regimes[-1].starts_after_days < WINDOW_DAYS
    for earlier, later in zip(schedule.regimes, schedule.regimes[1:], strict=False):
        assert later.starts_after_days - earlier.starts_after_days >= MIN_REGIME_DAYS - 1e-9
    for regime in schedule.regimes[1:]:
        assert FRAUD_RATE_MULTIPLIER_RANGE[0] <= regime.fraud_rate_multiplier
        assert regime.fraud_rate_multiplier <= FRAUD_RATE_MULTIPLIER_RANGE[1]
        assert AMOUNT_LOG_SHIFT_RANGE[0] <= regime.amount_log_shift <= AMOUNT_LOG_SHIFT_RANGE[1]
        assert ONLINE_SHARE_SHIFT_RANGE[0] <= regime.online_share_shift
        assert regime.online_share_shift <= ONLINE_SHARE_SHIFT_RANGE[1]
        assert ATTACK_INTENSITY_RANGE[0] <= regime.attack_intensity <= ATTACK_INTENSITY_RANGE[1]


def test_the_opening_regime_is_the_baseline() -> None:
    """Nothing has drifted yet on day one, whatever the secret says."""
    schedule = derive_schedule("anything", window_days=WINDOW_DAYS, name="live")
    opening = schedule.regimes[0]
    assert opening.starts_after_days == 0.0
    assert opening.fraud_rate_multiplier == 1.0
    assert opening.amount_log_shift == 0.0
    assert opening.attack_intensity == 1.0


def test_a_window_too_short_for_its_regimes_is_refused() -> None:
    with pytest.raises(ValueError, match="cannot hold"):
        derive_schedule("x", window_days=10.0, name="live")


def test_a_commitment_verifies_against_its_own_secret() -> None:
    now = dt.datetime(2027, 4, 1, tzinfo=dt.UTC)
    commitment = SealedCommitment.seal(
        "the sealed secret", window_days=WINDOW_DAYS, name="live", now=now
    )
    assert commitment.verify("the sealed secret")


def test_a_commitment_refuses_the_wrong_secret() -> None:
    """The point of the seal: a secret invented later will not verify."""
    now = dt.datetime(2027, 4, 1, tzinfo=dt.UTC)
    commitment = SealedCommitment.seal(
        "the sealed secret", window_days=WINDOW_DAYS, name="live", now=now
    )
    assert not commitment.verify("a more convenient secret")


def test_a_commitment_does_not_contain_the_secret() -> None:
    """`schedule seal` writes this document into a public repository."""
    secret = "extremely-guessable-but-still-secret"
    commitment = SealedCommitment.seal(
        secret, window_days=WINDOW_DAYS, name="live", now=dt.datetime(2027, 4, 1, tzinfo=dt.UTC)
    )
    document = commitment.to_document()
    assert secret not in document
    schedule = derive_schedule(secret, window_days=WINDOW_DAYS, name="live")
    for regime in schedule.regimes:
        assert f"{regime.fraud_rate_multiplier:.6f}" not in document


def test_a_commitment_notices_an_edited_source(tmp_path: Path) -> None:
    """An edit to `regimes.py` after sealing breaks verification."""
    original = Path(source_fingerprint.__globals__["__file__"])
    edited = tmp_path / "regimes.py"
    edited.write_text(
        original.read_text(encoding="utf-8") + "\n# a quiet change\n", encoding="utf-8"
    )
    assert source_fingerprint(edited) != source_fingerprint()


def test_line_endings_do_not_change_the_source_hash(tmp_path: Path) -> None:
    """Built on Windows, run on Linux. A checkout is not a tamper."""
    original = Path(source_fingerprint.__globals__["__file__"]).read_bytes()
    crlf = tmp_path / "regimes.py"
    crlf.write_bytes(original.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    assert source_fingerprint(crlf) == source_fingerprint()
