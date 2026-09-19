"""The champion, the challenger and the drill: small, fast, and able to fail.

Each test fits on a few thousand synthetic rows with a known signal, so it
runs in seconds; the measured numbers come from `verdict train` on the real
tracks, not from here. What these hold in place:

- the exported ONNX file scores what the fitted model scores, on the scorer's
  own single-row path as well as in batches;
- the reported interval contains its point and is reproducible from its seed;
- weights change the fit's view of the classes, as a sample's must;
- a time split tests only after its cutoff, and trains only on labels that
  had arrived by it;
- a rollback reaches the next decision, with none by the rolled-back model.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent
from verdict.models.dataset import TrainingSet, training_schema
from verdict.models.evaluate import paired_difference, pr_auc
from verdict.models.inputs import MODEL_INPUTS
from verdict.store.features import feature_names

pytest.importorskip("xgboost")
pytest.importorskip("onnxruntime")

START = dt.datetime(2027, 4, 5, tzinfo=dt.UTC)


def a_set(n: int = 6_000, *, seed: int = 3, fraud_share: float = 0.08) -> TrainingSet:
    """Rows where fraud raises the first two inputs: learnable, not trivial."""
    rng = np.random.default_rng(seed)
    labels = rng.random(n) < fraud_share
    inputs = rng.exponential(1.0, (n, len(MODEL_INPUTS)))
    inputs[:, 0] += labels * rng.exponential(1.5, n)
    inputs[:, 1] += labels * rng.exponential(1.0, n)
    inputs[rng.random((n, len(MODEL_INPUTS))) < 0.2] = -1.0  # some "no history"
    return TrainingSet(
        cutoff=START,
        inputs=inputs,
        labels=labels,
        weights=np.ones(n),
        event_ids=tuple(f"evt-{i}" for i in range(n)),
        excluded_unarrived=0,
    )


# --- the interval -----------------------------------------------------------


def test_the_interval_contains_its_point_and_repeats_from_its_seed() -> None:
    data = a_set()
    scores = data.inputs[:, 0] + data.inputs[:, 1]
    first = pr_auc(data.labels, scores, resamples=200)
    again = pr_auc(data.labels, scores, resamples=200)
    assert first.low <= first.value <= first.high
    assert first == again


def test_identical_scores_differ_by_exactly_nothing() -> None:
    data = a_set()
    scores = data.inputs[:, 0]
    diff = paired_difference(data.labels, scores, scores.copy(), resamples=100)
    assert diff.value == diff.low == diff.high == 0.0


# --- the champion -------------------------------------------------------------


def test_the_exported_champion_scores_what_the_booster_scores(tmp_path: Path) -> None:
    from verdict.models.train import booster_scores, export_onnx, fit_champion
    from verdict.scoring.onnx_model import OnnxModel

    data = a_set()
    fitted = fit_champion(data, threads=2)
    path = export_onnx(fitted, tmp_path / "champion.onnx", data.inputs[:1_000])
    model = OnnxModel(path)
    expected = booster_scores(fitted, data.inputs[:1_000])
    assert np.allclose(model.score_matrix(data.inputs[:1_000]), expected, atol=1e-4)
    assert model.version.startswith("champion-")

    # The scorer's own path: one event, its served features, one call.
    event = TransactionEvent(
        event_id="evt-0",
        event_time=START,
        card_id="card-1",
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=int(data.inputs[0, -1]) if data.inputs[0, -1] > 0 else 1,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )
    row = data.inputs[0].copy()
    row[-1] = float(event.amount_cents)
    served = dict(zip(feature_names(), row[:-1].tolist(), strict=True))
    assert model.score(served, event) == pytest.approx(
        float(booster_scores(fitted, row[None, :])[0]), abs=1e-4
    )


def test_the_champion_learns_the_signal() -> None:
    from verdict.models.train import booster_scores, fit_champion

    train, test = a_set(seed=3), a_set(seed=4)
    scores = booster_scores(fit_champion(train, threads=2), test.inputs)
    assert pr_auc(test.labels, scores, resamples=50).value > 2 * test.labels.mean()


def test_weights_are_what_the_fit_sees() -> None:
    """Weighting the frauds up moves every score up: the fit reads the weights."""
    from verdict.models.train import booster_scores, fit_champion

    data = a_set()
    heavier = TrainingSet(
        cutoff=data.cutoff,
        inputs=data.inputs,
        labels=data.labels,
        weights=np.where(data.labels, 10.0, 1.0),
        event_ids=data.event_ids,
        excluded_unarrived=0,
    )
    plain = booster_scores(fit_champion(data, threads=2), data.inputs).mean()
    weighted = booster_scores(fit_champion(heavier, threads=2), data.inputs).mean()
    assert weighted > plain * 2


def test_a_time_split_trains_on_arrived_labels_and_tests_after_the_cutoff() -> None:
    from verdict.models.train import split_by_time

    n = 24 * 40
    rows = []
    for i in range(n):
        at = START + dt.timedelta(hours=i)
        row: dict[str, object] = {name: 1.0 for name in feature_names()}
        row.update(
            event_id=f"evt-{i}",
            event_time=at,
            amount_cents=100,
            label_time=at + dt.timedelta(days=7),
            is_fraud=i % 9 == 0,
            weight=1.0,
        )
        rows.append(row)
    split = split_by_time(pa.Table.from_pylist(rows, schema=training_schema()))
    assert split.train.excluded_unarrived == 24 * 7
    assert all(int(e[4:]) < 24 * 21 for e in split.train.event_ids)
    assert min(int(e[4:]) for e in split.test.event_ids) >= int((n - 1) * 0.7)
    assert split.test.excluded_unarrived == 0


# --- the challenger ---------------------------------------------------------------


def test_the_exported_challenger_scores_what_torch_scores(tmp_path: Path) -> None:
    pytest.importorskip("torch")
    from verdict.models import challenger

    data = a_set(n=3_000)
    fitted = challenger.fit_challenger(data, threads=2)
    path = challenger.export_challenger(fitted, tmp_path / "challenger.onnx", data.inputs[:500])
    from verdict.scoring.onnx_model import OnnxModel

    exported = OnnxModel(path, prefix="challenger").score_matrix(data.inputs[:500])
    assert np.allclose(exported, challenger.challenger_scores(fitted, data.inputs[:500]), atol=1e-4)
    assert fitted.best_epoch >= 1


# --- the drill ------------------------------------------------------------------------


def test_a_rollback_reaches_the_next_decision(tmp_path: Path) -> None:
    from verdict.scoring.drill import run_drill
    from verdict.scoring.loadtest import generate_events
    from verdict.scoring.model import Model, StandInModel

    class Renamed(StandInModel):
        version = "stand-in-new"

    old, new = StandInModel(), Renamed()
    known: dict[str, Model] = {old.version: old, new.version: new}
    run = run_drill(
        tmp_path / "champion.json",
        known,
        old=old.version,
        new=new.version,
        events=generate_events(1_500, rate=1_000.0),
        rollback_after=500,
    )
    assert run.decisions_by_new_after_rollback == 0
    assert run.seconds_to_old_champion < 1.0


def test_the_synthetic_training_stream_keeps_the_live_per_entity_rate() -> None:
    """Scaled down for memory, not for dynamics: rate over population is unchanged."""
    from verdict.models.champion import LIVE, SYNTHETIC

    for entity in ("cards", "devices", "merchants"):
        live = LIVE.events_per_second / getattr(LIVE.population, entity)
        scaled = SYNTHETIC.events_per_second / getattr(SYNTHETIC.population, entity)
        assert scaled == pytest.approx(live)
    assert SYNTHETIC.schedule == LIVE.schedule
