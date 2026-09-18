"""The rollback flag: honoured on the next event, and never a way to stop scoring.

`PLAN.md` section 4 lists "the rollback flag is honoured within one event" as
one of the tests that matter. It is here, against the real scorer: a pointer
flipped between two events changes the model on the second.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from pathlib import Path

import pytest

from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.flags import FlagError, FlaggedModels, read_pointer, rollback, set_champion
from verdict.scoring.model import Model

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)


class Constant:
    """A model that always says the same thing, so which one scored is visible."""

    def __init__(self, version: str, score: float) -> None:
        """Fix the version and the score."""
        self.version = version
        self._score = score

    def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
        """Return the fixed score, whatever the event."""
        del features, event
        return self._score


KNOWN: dict[str, Model] = {
    "champion-a": Constant("champion-a", 0.1),
    "champion-b": Constant("champion-b", 0.2),
}


def an_event(index: int) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{index}",
        event_time=START + dt.timedelta(seconds=index),
        card_id="card-1",
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=1_000,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


@pytest.fixture
def flag(tmp_path: Path) -> Path:
    path = tmp_path / "flags" / "champion.json"
    set_champion(path, "champion-a", KNOWN)
    return path


def test_the_flag_is_honoured_on_the_very_next_event(flag: Path) -> None:
    models = FlaggedModels(flag, KNOWN)
    decider = Decider(features=EngineFeatures(FeatureEngine()), models=models)
    first = decider.decide(an_event(1), 0)
    set_champion(flag, "champion-b", KNOWN)
    second = decider.decide(an_event(2), 0)
    rollback(flag)
    third = decider.decide(an_event(3), 0)
    outcomes = [outcome for outcome in (first, second, third) if outcome is not None]
    assert len(outcomes) == 3
    assert [o.decision.model_version for o in outcomes] == [
        "champion-a",
        "champion-b",
        "champion-a",
    ]


def test_rollback_swaps_champion_and_previous(flag: Path) -> None:
    set_champion(flag, "champion-b", KNOWN)
    assert read_pointer(flag).previous == "champion-a"
    pointer = rollback(flag)
    assert (pointer.champion, pointer.previous) == ("champion-a", "champion-b")


def test_rolling_back_with_nowhere_to_go_is_refused(flag: Path) -> None:
    with pytest.raises(FlagError, match="no previous"):
        rollback(flag)


def test_pointing_at_a_model_the_scorer_lacks_is_refused_before_it_is_written(
    flag: Path,
) -> None:
    before = flag.read_text(encoding="utf-8")
    with pytest.raises(FlagError, match="no model"):
        set_champion(flag, "champion-z", KNOWN)
    assert flag.read_text(encoding="utf-8") == before


def test_a_bad_pointer_is_refused_and_scoring_continues(flag: Path) -> None:
    """A rollback mechanism that can stop the scorer is worse than none."""
    models = FlaggedModels(flag, KNOWN)
    assert models.current().version == "champion-a"

    flag.write_text("{ not json", encoding="utf-8")
    assert models.current().version == "champion-a"

    flag.write_text('{"champion": "champion-z"}', encoding="utf-8")
    assert models.current().version == "champion-a"
    assert models.refused == 2

    flag.write_text('{"champion": "champion-b", "previous": "champion-a"}', encoding="utf-8")
    assert models.current().version == "champion-b"


def test_a_missing_file_mid_run_keeps_the_last_champion(flag: Path) -> None:
    models = FlaggedModels(flag, KNOWN)
    flag.unlink()
    assert models.current().version == "champion-a"


def test_a_scorer_will_not_start_without_a_known_champion(tmp_path: Path) -> None:
    path = tmp_path / "champion.json"
    path.write_text('{"champion": "champion-z"}', encoding="utf-8")
    with pytest.raises(FlagError, match="does not have"):
        FlaggedModels(path, KNOWN)
    with pytest.raises(FlagError, match="cannot read"):
        FlaggedModels(tmp_path / "absent.json", KNOWN)


def test_writing_leaves_no_temporary_files(flag: Path) -> None:
    for version in ("champion-b", "champion-a", "champion-b"):
        set_champion(flag, version, KNOWN)
    assert [path.name for path in flag.parent.iterdir()] == ["champion.json"]


def test_the_pointer_is_not_reread_when_nothing_changed(
    flag: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One stat per event, and a parse only on change."""
    import verdict.scoring.flags as flags

    models = FlaggedModels(flag, KNOWN)
    reads = 0
    real = flags.read_pointer

    def counting(path: Path) -> flags.Pointer:
        nonlocal reads
        reads += 1
        return real(path)

    monkeypatch.setattr(flags, "read_pointer", counting)
    for _ in range(100):
        models.current()
    assert reads == 0
    set_champion(flag, "champion-b", KNOWN)
    models.current()
    models.current()
    assert reads == 2  # one inside set_champion, one on the change


def test_the_command_line_refuses_in_one_line_rather_than_a_traceback(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from verdict.cli import app

    path = tmp_path / "champion.json"
    runner = CliRunner()
    assert runner.invoke(app, ["flag", "set", "stand-in-0", "--path", str(path)]).exit_code == 0
    refused = runner.invoke(app, ["flag", "rollback", "--path", str(path)])
    assert refused.exit_code == 1
    assert "REFUSED: there is no previous champion" in refused.output
    assert "Traceback" not in refused.output
    unknown = runner.invoke(app, ["flag", "set", "no-such-model", "--path", str(path)])
    assert unknown.exit_code == 1
    assert "REFUSED: no model" in unknown.output
