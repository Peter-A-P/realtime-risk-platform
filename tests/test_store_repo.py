"""The Feast repository is generated from the specifications, and works.

The end-to-end test is the one that matters: apply a generated repository,
push a feature the way the dataflow will, and read it back both online and as
of a past moment. It is what ADR 5 rests on, so it runs against real Feast
rather than a mock.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from verdict.store.features import Aggregation, EntityKind, FeatureSpec
from verdict.store.leakage import TrainingRow
from verdict.store.repo import (
    ENTITY_JOIN_KEYS,
    PROJECT,
    UNBOUNDED_TTL,
    apply_repo,
    build_definitions,
    feature_refs,
    specs_by_entity,
    store_config,
    view_ttl,
    write_repo,
)
from verdict.store.retrieval import (
    MissingEntityError,
    build_entity_frame,
    build_training_features,
)

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)

CARD_COUNT_1H = FeatureSpec(
    name="card_txn_count_1h",
    entity=EntityKind.CARD,
    aggregation=Aggregation.COUNT,
    window=dt.timedelta(hours=1),
)
CARD_SUM_24H = FeatureSpec(
    name="card_amount_sum_24h",
    entity=EntityKind.CARD,
    aggregation=Aggregation.SUM,
    field="amount_cents",
    window=dt.timedelta(hours=24),
)
DEVICE_CARDS = FeatureSpec(
    name="device_distinct_cards_24h",
    entity=EntityKind.DEVICE,
    aggregation=Aggregation.DISTINCT_COUNT,
    field="card_id",
    window=dt.timedelta(hours=24),
)
CARD_LIFETIME = FeatureSpec(
    name="card_txn_count_lifetime",
    entity=EntityKind.CARD,
    aggregation=Aggregation.COUNT,
)

SPECS = [CARD_COUNT_1H, CARD_SUM_24H, DEVICE_CARDS]

FIXTURE_SPECS_MODULE = '''"""One feature, for the end-to-end store test."""

import datetime as dt

from verdict.store.features import Aggregation, EntityKind, FeatureSpec

SPECS = [
    FeatureSpec(
        name="card_txn_count_1h",
        entity=EntityKind.CARD,
        aggregation=Aggregation.COUNT,
        window=dt.timedelta(hours=1),
    )
]
'''


def test_features_are_grouped_into_one_view_per_entity() -> None:
    grouped = specs_by_entity(SPECS)
    assert set(grouped) == {EntityKind.CARD, EntityKind.DEVICE}
    assert len(grouped[EntityKind.CARD]) == 2


def test_a_view_ttl_is_its_longest_window() -> None:
    """Serving an hour-old feature a day later is serving a made-up number."""
    assert view_ttl([CARD_COUNT_1H, CARD_SUM_24H]) == dt.timedelta(hours=24)


def test_an_unbounded_feature_pins_the_view_ttl() -> None:
    assert view_ttl([CARD_COUNT_1H, CARD_LIFETIME]) == UNBOUNDED_TTL


def test_definitions_come_from_the_specifications() -> None:
    entities, views = build_definitions(SPECS)
    assert {entity.name for entity in entities} == {"card", "device"}
    by_name = {view.name: view for view in views}
    assert set(by_name) == {"card_features", "device_features"}
    assert {field.name for field in by_name["card_features"].schema} == {
        CARD_COUNT_1H.name,
        CARD_SUM_24H.name,
    }
    assert by_name["card_features"].ttl == dt.timedelta(hours=24)


def test_no_features_means_no_repository_objects() -> None:
    """Week 2's state, and it has to be a legal one rather than a crash."""
    assert build_definitions([]) == ([], [])


def test_feature_references_name_their_view() -> None:
    assert feature_refs([CARD_COUNT_1H]) == ["card_features:card_txn_count_1h"]


def test_the_entity_join_key_is_the_event_s_own_field_name() -> None:
    """A rename here would silently break the join, not fail it."""
    assert ENTITY_JOIN_KEYS[EntityKind.CARD] == "card_id"
    assert ENTITY_JOIN_KEYS[EntityKind.SESSION] == "session_id"


def test_the_online_store_can_be_redis_for_the_live_stack() -> None:
    config = store_config(Path("."), online_store="redis", redis_connection="redis:6379")
    assert config["online_store"]["type"] == "redis"
    assert config["project"] == PROJECT


def test_redis_without_a_connection_string_is_refused() -> None:
    with pytest.raises(ValueError, match="connection string"):
        store_config(Path("."), online_store="redis")


def test_an_unknown_online_store_is_refused() -> None:
    with pytest.raises(ValueError, match="unsupported online store"):
        store_config(Path("."), online_store="postgres")


def test_the_key_serialization_version_is_pinned() -> None:
    """An upgrade that changed the key layout would strand the live store."""
    assert store_config(Path("."))["entity_key_serialization_version"] == 3


def test_a_written_repository_holds_no_feature_knowledge(tmp_path: Path) -> None:
    """The generated module must not become a second place to edit."""
    write_repo(tmp_path / "fr", SPECS)
    definitions = (tmp_path / "fr" / "definitions.py").read_text(encoding="utf-8")
    assert "build_definitions()" in definitions
    assert "Aggregation" not in definitions
    assert "timedelta" not in definitions


# --- the entity frame refuses to carry the label time -----------------------


def rows() -> list[TrainingRow]:
    return [
        TrainingRow(
            entity_ids={"card": "card-1", "device": "dev-1", "merchant": "mer-1"},
            event_time=START + dt.timedelta(minutes=minutes),
            label_time=START + dt.timedelta(days=7, minutes=minutes),
        )
        for minutes in (0, 30, 90)
    ]


def test_the_entity_frame_carries_the_event_time_and_not_the_label_time() -> None:
    """The one-word mistake this whole module exists to prevent."""
    frame = build_entity_frame(rows(), SPECS)
    assert set(frame.columns) == {"card_id", "device_id", "event_timestamp"}
    assert "label_time" not in frame.columns
    assert list(frame["event_timestamp"]) == [row.event_time for row in rows()]


def test_a_row_missing_an_entity_is_refused_rather_than_dropped() -> None:
    """Dropping it shrinks the training set where nobody is looking."""
    session_feature = FeatureSpec(
        name="session_txn_count",
        entity=EntityKind.SESSION,
        aggregation=Aggregation.COUNT,
        window=dt.timedelta(minutes=30),
    )
    with pytest.raises(MissingEntityError, match="no session identifier"):
        build_entity_frame(rows(), [session_feature])


def test_with_no_features_the_frame_is_the_training_set(tmp_path: Path) -> None:
    """Week 2 again: the path has to work before any feature exists."""
    frame = build_training_features(store=None, rows=rows(), specs=[])  # type: ignore[arg-type]
    assert len(frame) == 3
    del tmp_path


# --- end to end, against real Feast -----------------------------------------


@pytest.mark.slow
def test_push_then_read_online_and_point_in_time(tmp_path: Path) -> None:
    """The evidence behind ADR 5.

    The dataflow computes once and pushes to both stores; the online read
    serves the newest value, and the historical read serves what was true at
    a past moment. If Feast could not do both from one push, the feature
    store would have to be built by hand.
    """
    import pandas as pd
    from feast import FeatureStore
    from feast.data_source import PushMode

    repo = tmp_path / "fr"
    # The generated repository imports its features rather than restating
    # them; week 2 has none of its own, so it is pointed at a fixture module
    # written beside it.
    (repo).mkdir(parents=True, exist_ok=True)
    (repo / "fixture_specs.py").write_text(
        FIXTURE_SPECS_MODULE,
        encoding="utf-8",
    )
    write_repo(repo, [CARD_COUNT_1H], specs_ref="fixture_specs:SPECS")
    (repo / "data").mkdir(exist_ok=True)
    seed = pd.DataFrame(
        {
            "card_id": ["card-1"],
            "event_timestamp": [START],
            CARD_COUNT_1H.name: [1.0],
        }
    )
    seed.to_parquet(repo / "data" / "card_features.parquet")

    apply_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    later = START + dt.timedelta(hours=2)
    store.push(
        "card_features_push",
        pd.DataFrame(
            {"card_id": ["card-1"], "event_timestamp": [later], CARD_COUNT_1H.name: [5.0]}
        ),
        to=PushMode.ONLINE_AND_OFFLINE,
    )

    online = store.get_online_features(
        features=feature_refs([CARD_COUNT_1H]), entity_rows=[{"card_id": "card-1"}]
    ).to_dict()
    assert online[CARD_COUNT_1H.name] == [5.0]

    history = store.get_historical_features(
        entity_df=pd.DataFrame(
            {
                "card_id": ["card-1", "card-1"],
                "event_timestamp": [START + dt.timedelta(hours=1), later + dt.timedelta(hours=1)],
            }
        ),
        features=feature_refs([CARD_COUNT_1H]),
    ).to_df()
    values = history.sort_values("event_timestamp")[CARD_COUNT_1H.name].tolist()
    assert values == [1.0, 5.0], "the point-in-time join served the wrong vintage"
