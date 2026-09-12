"""Online and offline hold the same numbers, because one write put them there.

Training-serving skew is the failure this project is built around, and this
is the test that would see it. The engine computes each feature once; the
sink writes that value to the offline store and pushes it to the online
store. If the two ever disagree, the model is trained on one thing and serves
another, and no metric anywhere would show it.

The week 3 criterion is parity of 100 percent on a replay, so the assertion
is exact equality rather than a tolerance.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.schema import TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.features.sinks import DualSink, OfflineParquetSink, rows_to_frame
from verdict.features.verify import check_parity
from verdict.store.features import FEATURE_SET, EntityKind
from verdict.store.repo import apply_repo, write_repo

REFERENCE = Population(cards=200, devices=150, merchants=30)

FIXTURE_SPECS_MODULE = '''"""The platform's own feature set, for the generated repository."""

from verdict.store.features import FEATURE_SET

SPECS = FEATURE_SET
'''


def a_replay(limit: int = 600) -> list[TransactionEvent]:
    graph = EntityGraph.build(seed=5, population=REFERENCE)
    config = GeneratorConfig(
        seed=5, population=REFERENCE, events_per_second=40.0, target_fraud_share=0.05
    )
    return [record.event for record in Generator(config, graph).stream(limit=limit)]


@pytest.mark.slow
def test_online_and_offline_agree_after_a_replay(tmp_path: Path) -> None:
    """The headline parity number, end to end through real Feast."""
    from feast import FeatureStore

    repo = tmp_path / "fr"
    repo.mkdir(parents=True, exist_ok=True)
    (repo / "fixture_specs.py").write_text(FIXTURE_SPECS_MODULE, encoding="utf-8")
    write_repo(repo, FEATURE_SET, specs_ref="fixture_specs:SPECS")
    apply_repo(repo)
    store = FeatureStore(repo_path=str(repo))

    offline = OfflineParquetSink(repo / "data")
    engine = FeatureEngine(FEATURE_SET)
    with DualSink(store, offline, FEATURE_SET, batch_size=100) as sink:
        for event in a_replay():
            sink.write(engine.process(event))
        engine.flush()

    report = check_parity(store, offline, FEATURE_SET, sample=25)
    assert report.clean, report.summary()
    assert report.parity == 1.0
    assert report.features_checked > 0


def test_a_row_written_to_both_stores_carries_the_same_values() -> None:
    """The narrow version of the same claim, without a store.

    One `FeatureRow` becomes one frame; the frame is what both stores are
    given. Anything that disagreed would have to be introduced after this
    point, which is what the end-to-end test above covers.
    """
    engine = FeatureEngine(FEATURE_SET)
    events = a_replay(limit=50)
    for event in events:
        rows = engine.process(event)
        for row in rows:
            frame = rows_to_frame(row.kind, [row])
            for name, value in row.values.items():
                assert frame[name].iloc[0] == value


def test_rows_of_mixed_entity_kinds_are_refused() -> None:
    """Rows of mixed entity kinds are refused.

    A frame with the wrong join key would be written under the wrong entity
    and silently serve nonsense.
    """
    engine = FeatureEngine(FEATURE_SET)
    rows = engine.process(a_replay(limit=1)[0])
    with pytest.raises(ValueError, match="another entity kind"):
        rows_to_frame(EntityKind.CARD, rows)


def test_rows_that_disagree_about_their_features_are_refused() -> None:
    """Nulls in a feature column are a decision nobody made on purpose."""
    engine = FeatureEngine(FEATURE_SET)
    rows = engine.process(a_replay(limit=1)[0])
    card_row = next(row for row in rows if row.kind is EntityKind.CARD)
    trimmed = type(card_row)(
        kind=card_row.kind,
        entity_id=card_row.entity_id,
        as_of=card_row.as_of,
        values={next(iter(card_row.values)): 1.0},
    )
    with pytest.raises(ValueError, match="disagree about which features"):
        rows_to_frame(EntityKind.CARD, [card_row, trimmed])


def test_the_offline_store_appends_rather_than_replacing(tmp_path: Path) -> None:
    """Training needs the history, not the latest state."""
    offline = OfflineParquetSink(tmp_path / "data")
    engine = FeatureEngine(FEATURE_SET)
    events = a_replay(limit=40)
    with DualSink(store=None, offline=offline, specs=FEATURE_SET, online=False) as sink:  # type: ignore[arg-type]
        for event in events[:20]:
            sink.write(engine.process(event))
    first = len(offline.read(EntityKind.CARD))
    with DualSink(store=None, offline=offline, specs=FEATURE_SET, online=False) as sink:  # type: ignore[arg-type]
        for event in events[20:]:
            sink.write(engine.process(event))
        engine.flush()
    assert len(offline.read(EntityKind.CARD)) > first


def test_the_offline_store_records_the_moment_each_value_described(
    tmp_path: Path,
) -> None:
    """Without the timestamp, the point-in-time join has nothing to join on."""
    offline = OfflineParquetSink(tmp_path / "data")
    engine = FeatureEngine(FEATURE_SET)
    events = a_replay(limit=30)
    with DualSink(store=None, offline=offline, specs=FEATURE_SET, online=False) as sink:  # type: ignore[arg-type]
        for event in events:
            sink.write(engine.process(event))
        engine.flush()
    frame = offline.read(EntityKind.CARD)
    stamps = {stamp.to_pydatetime().replace(tzinfo=dt.UTC) for stamp in frame["event_timestamp"]}
    assert stamps <= {event.event_time for event in events}
