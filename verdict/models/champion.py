"""Train the champion on a track, and measure what the leak would have claimed.

Two tracks (ADR 2), each reported with its own interval and never mixed:

- **real, offline**: the competition data mapped to events (ADR 17) and
  replayed through the engine. Six of the sixteen features exist on it.
- **synthetic**: the generator, at a population scaled down fifty times and a
  rate scaled down with it, so that every card, device and merchant sees
  transactions at the live stack's per-entity rate while the engine's memory
  stays inside this machine's (ADR 15's finding). Ten days, on the public
  development schedule's opening regime.

`leak_inflation` trains twice on the real track, once on features from the
fixed engine and once from the unfixed one (`features/unfixed.py`), and
reports the difference in test PR-AUC on the same rows, paired: what the
same-instant leak would have added to an offline number that production
could never have matched (`docs/leak-caught.md`).
"""

from __future__ import annotations

import datetime as dt
import itertools
import time
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from typing import Any, Final

import numpy as np
import pyarrow.parquet as pq

from verdict.events.generator.driver import GeneratedRecord, Generator, GeneratorConfig
from verdict.events.generator.entities import Population
from verdict.features.engine import FeatureEngine
from verdict.models.dataset import ReplayReport, replay_to_parquet
from verdict.models.evaluate import paired_difference, pr_auc
from verdict.models.inputs import MODEL_INPUTS
from verdict.models.train import (
    PARITY_TOLERANCE,
    booster_scores,
    export_onnx,
    fit_champion,
    split_by_time,
)

SCALE: Final = 50
LIVE = GeneratorConfig()
SYNTHETIC: Final = GeneratorConfig(
    population=Population(
        cards=LIVE.population.cards // SCALE,
        devices=LIVE.population.devices // SCALE,
        merchants=LIVE.population.merchants // SCALE,
    ),
    events_per_second=LIVE.events_per_second / SCALE,
)
"""The live configuration with population and rate both divided by fifty."""

SYNTHETIC_LEGIT_RATE: Final = 0.05
"""Legitimate rows kept from the synthetic replay, each weighted by twenty."""


def synthetic_records(
    days: float, config: GeneratorConfig = SYNTHETIC
) -> Iterator[GeneratedRecord]:
    """The generator's first `days` of stream.

    Args:
        days: How much stream time.
        config: The configuration.

    Returns:
        The records, in event-time order.
    """
    end = config.start_time + dt.timedelta(days=days)
    return itertools.takewhile(
        lambda record: record.event.event_time < end, Generator(config).stream()
    )


def real_records(directory: Path) -> Iterator[Any]:
    """The competition data as mapped records (ADR 17).

    Args:
        directory: Where the competition files are.

    Returns:
        The records, in event-time order.
    """
    from verdict.events.ieee_cis_events import iter_records

    return iter_records(directory)


def replay(
    track: str,
    path: Path,
    *,
    source: Path | None = None,
    days: float = 10.0,
    engine: FeatureEngine | None = None,
) -> ReplayReport:
    """Replay a track through an engine into a training table.

    Args:
        track: `real` or `synthetic`.
        path: Where the table goes.
        source: The competition files, for `real`.
        days: Stream time, for `synthetic`.
        engine: The engine; the platform's own by default.

    Returns:
        What the replay served and kept.

    Raises:
        ValueError: On an unknown track, or `real` without a source.
    """
    if track == "real":
        if source is None:
            msg = "the real track needs the competition files' directory"
            raise ValueError(msg)
        return replay_to_parquet(real_records(source), path, engine=engine, legit_rate=1.0)
    if track == "synthetic":
        return replay_to_parquet(
            synthetic_records(days), path, engine=engine, legit_rate=SYNTHETIC_LEGIT_RATE
        )
    msg = f"track must be real or synthetic, got {track!r}"
    raise ValueError(msg)


def _model_hop(model_path: Path, rows: np.ndarray, count: int = 5_000) -> dict[str, float]:
    """Single-row scoring time, as the scorer calls it, in milliseconds."""
    from verdict.scoring.onnx_model import OnnxModel

    model = OnnxModel(model_path)
    timings = np.empty(min(count, len(rows)))
    for index in range(len(timings)):
        row = rows[index : index + 1]
        started = time.perf_counter_ns()
        model.score_matrix(row)
        timings[index] = time.perf_counter_ns() - started
    return {
        "p50": float(np.percentile(timings, 50) / 1e6),
        "p99": float(np.percentile(timings, 99) / 1e6),
        "calls": float(len(timings)),
    }


def train_track(track: str, table_path: Path, model_path: Path) -> dict[str, Any]:
    """Fit, test, export and time the champion on one track's table.

    Args:
        track: For the report.
        table_path: The replayed table.
        model_path: Where the ONNX model goes.

    Returns:
        The report: data, split, fit, test PR-AUC with its interval, export
        parity, and the model hop's time.
    """
    from verdict.scoring.onnx_model import model_version

    table = pq.read_table(table_path)
    split = split_by_time(table, wait_for_labels=track == "synthetic")
    fitted = fit_champion(split.train)
    scores = booster_scores(fitted, split.test.inputs)
    tested = pr_auc(split.test.labels, scores, split.test.weights)
    check = split.test.inputs[:5_000]
    export_onnx(fitted, model_path, check)
    return {
        "track": "real data, offline" if track == "real" else "synthetic, offline replay",
        "model": model_version(model_path),
        "inputs": list(MODEL_INPUTS),
        "table_rows": table.num_rows,
        "cutoff": split.cutoff.isoformat(),
        "train": {
            "rows": len(split.train.labels),
            "frauds": split.train.frauds,
            "excluded_unarrived_labels": split.train.excluded_unarrived,
        },
        "test": {"rows": len(split.test.labels), "frauds": split.test.frauds},
        "fit": {
            "params": fitted.params,
            "trees": fitted.best_iteration,
            "fit_rows": fitted.train_rows,
            "early_stopping_rows": fitted.validation_rows,
            "seconds": round(fitted.seconds, 1),
        },
        "test_pr_auc": asdict(tested),
        "onnx_parity": {"rows_checked": len(check), "tolerance": PARITY_TOLERANCE},
        "model_hop_ms": _model_hop(model_path, split.test.inputs),
    }


def compare_models(
    track: str,
    table_path: Path,
    champion_path: Path,
    challenger_path: Path,
    *,
    train_share: float = 0.7,
) -> dict[str, Any]:
    """Score two exported models on the same test rows and compare them, paired.

    Used when one of them has been rebuilt: the comparison is recomputed from
    the files, without refitting the other.

    Args:
        track: For the report.
        table_path: The replayed table both were trained on.
        champion_path: The champion's ONNX file.
        challenger_path: The challenger's ONNX file.
        train_share: Where the split falls. It must be the share the newer of
            the two was fitted with, or the test rows would include rows it
            was trained on.

    Returns:
        Both PR-AUCs with intervals, and the paired difference.
    """
    from verdict.scoring.onnx_model import OnnxModel, model_version

    split = split_by_time(
        pq.read_table(table_path),
        train_share=train_share,
        wait_for_labels=track == "synthetic",
    )
    labels, weights = split.test.labels, split.test.weights
    champion = OnnxModel(champion_path).score_matrix(split.test.inputs)
    challenger = OnnxModel(challenger_path, prefix="challenger").score_matrix(split.test.inputs)
    return {
        "track": "real data, offline" if track == "real" else "synthetic, offline replay",
        "champion": model_version(champion_path),
        "challenger": model_version(challenger_path, "challenger"),
        "cutoff": split.cutoff.isoformat(),
        "test": {"rows": len(labels), "frauds": int(labels.sum())},
        "champion_pr_auc": asdict(pr_auc(labels, champion, weights)),
        "challenger_pr_auc": asdict(pr_auc(labels, challenger, weights)),
        "challenger_minus_champion": asdict(
            paired_difference(labels, champion, challenger, weights)
        ),
        "model_hop_ms": _model_hop(challenger_path, split.test.inputs),
    }


def leak_inflation(fixed_path: Path, leaky_path: Path) -> dict[str, Any]:
    """What the same-instant leak would have added to the offline PR-AUC.

    Both tables come from the same events; only the engine differs. Each is
    split at the same cutoff, a model is fitted on each, and each is tested
    on its own features for the same test rows. The difference is paired.

    Args:
        fixed_path: The table from the platform's engine.
        leaky_path: The table from the unfixed engine.

    Returns:
        Both PR-AUCs, their paired difference with its interval, and how many
        test rows differ at all between the two tables.

    Raises:
        ValueError: If the two tables do not hold the same events in order.
    """
    fixed = pq.read_table(fixed_path)
    leaky = pq.read_table(leaky_path)
    if not fixed["event_id"].equals(leaky["event_id"]):
        msg = "the two replays do not hold the same events in the same order"
        raise ValueError(msg)
    fixed_split, leaky_split = split_by_time(fixed), split_by_time(leaky)
    fixed_scores = booster_scores(fit_champion(fixed_split.train), fixed_split.test.inputs)
    leaky_scores = booster_scores(fit_champion(leaky_split.train), leaky_split.test.inputs)
    labels, weights = fixed_split.test.labels, fixed_split.test.weights
    differing = int(np.any(fixed_split.test.inputs != leaky_split.test.inputs, axis=1).sum())
    return {
        "track": "real data, offline",
        "test_rows": len(labels),
        "test_frauds": int(labels.sum()),
        "test_rows_whose_features_differ": differing,
        "fixed_pr_auc": asdict(pr_auc(labels, fixed_scores, weights)),
        "leaky_pr_auc": asdict(pr_auc(labels, leaky_scores, weights)),
        "inflation": asdict(paired_difference(labels, fixed_scores, leaky_scores, weights)),
    }


def challenge_track(
    track: str, table_path: Path, champion_path: Path, challenger_path: Path
) -> dict[str, Any]:
    """Fit the FT-Transformer on the champion's split and compare them, paired.

    Args:
        track: For the report.
        table_path: The replayed table the champion was trained on.
        champion_path: The champion's ONNX file.
        challenger_path: Where the challenger's ONNX file goes.

    Returns:
        Both models' test PR-AUC with intervals, the paired difference, the
        challenger's fit, and its model hop time.
    """
    from verdict.models.challenger import challenger_scores, export_challenger, fit_challenger
    from verdict.scoring.onnx_model import OnnxModel, model_version

    table = pq.read_table(table_path)
    split = split_by_time(table, wait_for_labels=track == "synthetic")
    labels, weights = split.test.labels, split.test.weights
    champion = OnnxModel(champion_path).score_matrix(split.test.inputs)
    fitted = fit_challenger(split.train)
    challenger = challenger_scores(fitted, split.test.inputs)
    export_challenger(fitted, challenger_path, split.test.inputs[:5_000])
    return {
        "track": "real data, offline" if track == "real" else "synthetic, offline replay",
        "champion": model_version(champion_path),
        "challenger": model_version(challenger_path, "challenger"),
        "cutoff": split.cutoff.isoformat(),
        "test": {"rows": len(labels), "frauds": int(labels.sum())},
        "champion_pr_auc": asdict(pr_auc(labels, champion, weights)),
        "challenger_pr_auc": asdict(pr_auc(labels, challenger, weights)),
        "challenger_minus_champion": asdict(
            paired_difference(labels, champion, challenger, weights)
        ),
        "fit": {
            "settings": fitted.settings,
            "epochs": fitted.epochs,
            "best_epoch": fitted.best_epoch,
            "validation_pr_auc_by_epoch": [round(v, 5) for v in fitted.validation_pr_auc],
            "seconds": round(fitted.seconds, 1),
        },
        "model_hop_ms": _model_hop(challenger_path, split.test.inputs),
    }
