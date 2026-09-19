"""Training sets: the labels that had arrived, and the inputs the scorer served.

Two properties, each a way a model goes wrong without any metric noticing:

- **ADR 10, rule 4.** A training set at a cutoff holds transactions from
  before it whose labels had arrived by it. A transaction whose label came
  later is left out and counted, never read as legitimate.
- **Training sees what serving saw.** The matrix built from recorded rows is
  the vector the scorer builds at decision time, column for column; and an
  offline replay serves the same feature values the scorer served, because
  it runs the same engine path.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pyarrow as pa
import pytest

from verdict.events.schema import EntryMode, LabelEvent, MerchantCategory, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.history.records import staged_row, staged_schema
from verdict.models.dataset import at_cutoff, replay_table, training_schema
from verdict.models.inputs import MODEL_INPUTS, MissingInputError, matrix, vector
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.store.features import NO_EVENTS, feature_names

START = dt.datetime(2027, 4, 5, 12, tzinfo=dt.UTC)
DELAY = dt.timedelta(days=7)


def events(n: int) -> list[TransactionEvent]:
    return [
        TransactionEvent(
            event_id=f"evt-{i}",
            event_time=START + dt.timedelta(hours=i),
            card_id=f"card-{i % 3}",
            device_id=f"dev-{i % 2}",
            merchant_id=f"mer-{i % 4}",
            amount_cents=1_000 + 37 * i,
            merchant_category=MerchantCategory.GROCERY_POS,
            entry_mode=EntryMode.CHIP,
        )
        for i in range(n)
    ]


def labels_for(evts: list[TransactionEvent]) -> dict[str, LabelEvent]:
    return {
        e.event_id: LabelEvent(
            event_id=e.event_id,
            label_time=e.event_time + DELAY,
            is_fraud=int(e.event_id[4:]) % 5 == 0,
        )
        for e in evts
    }


# --- rule 4 -----------------------------------------------------------------


def test_a_label_not_yet_arrived_is_left_out_and_counted_not_read_as_legitimate() -> None:
    evts = events(24 * 20)  # twenty days, one an hour
    table = replay_table(evts, labels_for(evts)).table
    cutoff = START + dt.timedelta(days=10)
    training = at_cutoff(table, cutoff)
    # 240 transactions before the cutoff. Those of the first three days, and
    # the one exactly three days in, have labels by it; the other 167 do not.
    assert len(training.event_ids) == 24 * 3 + 1
    assert training.excluded_unarrived == 24 * 7 - 1
    arrived = {e.event_id for e in evts if e.event_time + DELAY <= cutoff}
    assert set(training.event_ids) == arrived


def test_a_label_arriving_exactly_at_the_cutoff_counts() -> None:
    evts = events(3)
    table = replay_table(evts, labels_for(evts)).table
    training = at_cutoff(table, evts[1].event_time + DELAY)
    assert training.event_ids == ("evt-0", "evt-1")
    assert training.excluded_unarrived == 1  # evt-2, an hour short


def test_a_transaction_at_or_after_the_cutoff_is_not_in_the_set_at_all() -> None:
    evts = events(5)
    table = replay_table(evts, labels_for(evts)).table
    training = at_cutoff(table, evts[2].event_time + DELAY * 2)
    assert "evt-4" in training.event_ids
    training = at_cutoff(table, evts[2].event_time)
    assert training.event_ids == ()
    assert training.excluded_unarrived == 2


def test_a_table_without_label_time_is_refused() -> None:
    evts = events(3)
    table = replay_table(evts, labels_for(evts)).table.drop_columns(["label_time"])
    with pytest.raises(ValueError, match="label_time"):
        at_cutoff(table, START + DELAY * 3)


def test_a_naive_cutoff_is_refused() -> None:
    evts = events(3)
    table = replay_table(evts, labels_for(evts)).table
    with pytest.raises(ValueError, match="timezone"):
        at_cutoff(table, dt.datetime(2027, 5, 1))


def test_weights_travel_with_their_rows() -> None:
    evts = events(10)
    table = replay_table(evts, labels_for(evts)).table
    weights = pa.array([float(i + 1) for i in range(10)], pa.float64())
    table = table.set_column(table.schema.get_field_index("weight"), "weight", weights)
    training = at_cutoff(table, START + DELAY * 3)
    assert training.weights.tolist() == [float(i + 1) for i in range(10)]


def test_an_event_without_a_label_is_counted_out_of_a_replay() -> None:
    evts = events(6)
    labels = labels_for(evts)
    del labels["evt-3"]
    replayed = replay_table(evts, labels)
    assert replayed.unlabelled == 1
    assert "evt-3" not in replayed.table["event_id"].to_pylist()


# --- training sees what serving saw -------------------------------------------


def _decide(evts: list[TransactionEvent]) -> list[tuple[TransactionEvent, dict[str, float]]]:
    decider = Decider(features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel()))
    served: list[tuple[TransactionEvent, dict[str, float]]] = []
    for event in evts:
        outcome = decider.decide(event, 0)
        assert outcome is not None
        served.append((event, dict(outcome.features)))
    return served


def test_the_training_matrix_is_the_scorers_vector_row_for_row() -> None:
    evts = events(30)
    served = _decide(evts)
    decider = Decider(features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel()))
    rows = []
    for event in evts:
        outcome = decider.decide(event, 0)
        assert outcome is not None
        rows.append(staged_row(event, outcome.features, outcome.decision, None))
    table = pa.Table.from_pylist(rows, schema=staged_schema())
    expected = np.array([vector(features, event) for event, features in served])
    assert np.array_equal(matrix(table), expected)
    assert matrix(table).shape == (30, len(MODEL_INPUTS))


def test_an_offline_replay_serves_what_the_scorer_served() -> None:
    """Same engine path, same order, same values: no second computation."""
    evts = events(48)
    served = _decide(evts)
    table = replay_table(evts, labels_for(evts)).table
    replayed = table.to_pylist()
    assert len(replayed) == len(served)
    for row, (event, features) in zip(replayed, served, strict=True):
        assert row["event_id"] == event.event_id
        assert {name: row[name] for name in feature_names()} == features
    assert any(row["card_txn_count_24h"] not in (NO_EVENTS, 0.0) for row in replayed), (
        "the fixture should exercise windows that hold history"
    )


def test_the_inputs_are_the_features_then_the_amount() -> None:
    assert (*feature_names(), "amount_cents") == MODEL_INPUTS
    assert set(MODEL_INPUTS) <= set(training_schema().names)


def test_a_feature_the_scorer_did_not_serve_is_an_error_not_a_zero() -> None:
    event = events(1)[0]
    features = dict.fromkeys(feature_names(), NO_EVENTS)
    del features["card_txn_count_1h"]
    with pytest.raises(MissingInputError):
        vector(features, event)
