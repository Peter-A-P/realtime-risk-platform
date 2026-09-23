"""What the platform keeps: a labelled, weighted sample that tells the truth.

The tests that matter most:

- **the sample's estimates match the full data** on a replay: weighted counts,
  PR-AUC and decision cost from the kept rows against the same quantities on
  every row. If this fails, every number published from live history is
  wrong by the sampling rate;
- **a day is not finalised before its labels could have arrived**, and a
  label that arrived after the moment of finalising does not count;
- **the scorer stages a decision before it checkpoints it**, so a
  checkpointed transaction always has its row;
- **a crash mid-write loses only the batch being written**, and a duplicate
  delivery is kept once.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow as pa
import pytest

from verdict.events.schema import (
    Action,
    DecisionEvent,
    EntryMode,
    LabelEvent,
    MerchantCategory,
    TransactionEvent,
)
from verdict.features.engine import FeatureEngine
from verdict.history import spool
from verdict.history.compact import (
    HistoryPaths,
    NotYetFinalError,
    final_after,
    finalisable,
    finalise_day,
    read_kept,
    seal_closed,
)
from verdict.history.labels import LabelCollector
from verdict.history.records import LABEL_SCHEMA, label_row, staged_row, staged_schema
from verdict.history.sampling import SampleRates, Stratum, draw, keep, stratum_of
from verdict.models.promote import average_precision, decision_cost
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.model import FixedModel, StandInModel
from verdict.scoring.rules import DecisionRules
from verdict.store.features import NO_EVENTS, feature_names
from verdict.stream.base import StreamError
from verdict.stream.memory import MemoryBroker, MemoryStream

DAY = dt.date(2027, 4, 5)
DAY_START = dt.datetime(2027, 4, 5, tzinfo=dt.UTC)
DELAY = dt.timedelta(days=7)
FEATURES = dict.fromkeys(feature_names(), NO_EVENTS)
TEST_RATES = SampleRates(acted=1.0, fraud=0.5, legit=0.1)


def an_event(index: int, *, card: str = "card-1", amount: int = 2_500) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{index}",
        event_time=DAY_START + dt.timedelta(hours=12, seconds=index),
        card_id=card,
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=amount,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


def a_decider() -> Decider:
    return Decider(features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel()))


def send(stream: MemoryStream, events: Iterable[TransactionEvent]) -> None:
    for event in events:
        stream.produce("transactions", event.card_id, event.to_json().encode("utf-8"))


# --- the draw and the strata ----------------------------------------------


def test_the_draw_is_fixed_by_the_event_id() -> None:
    assert draw("evt-1") == draw("evt-1")
    assert draw("evt-1") != draw("evt-2")
    assert all(0.0 <= draw(f"evt-{i}") < 1.0 for i in range(1_000))


def test_the_draw_is_uniform_enough_to_sample_at_a_stated_rate() -> None:
    """Within four binomial standard errors of the rate, over 100,000 ids."""
    n, rate = 100_000, 0.01
    kept = sum(draw(f"evt-{i}") < rate for i in range(n))
    assert abs(kept - n * rate) < 4 * (n * rate * (1 - rate)) ** 0.5


def test_a_kept_row_carries_the_inverse_of_its_rate() -> None:
    rates = SampleRates(acted=1.0, fraud=0.5, legit=0.25)
    for i in range(200):
        weight = keep(f"evt-{i}", Stratum.LEGIT, rates)
        assert weight is None or weight == 4.0
        assert keep(f"evt-{i}", Stratum.ACTED, rates) == 1.0


@pytest.mark.parametrize("bad", [0.0, -0.1, 1.5])
def test_a_rate_must_keep_something_and_be_a_probability(bad: float) -> None:
    with pytest.raises(ValueError, match="rate"):
        SampleRates(legit=bad)


def test_reviewed_and_declined_rows_are_one_stratum_whatever_the_label() -> None:
    assert stratum_of(Action.REVIEW, is_fraud=False) is Stratum.ACTED
    assert stratum_of(Action.DECLINE, is_fraud=True) is Stratum.ACTED
    assert stratum_of(Action.APPROVE, is_fraud=True) is Stratum.FRAUD
    assert stratum_of(Action.APPROVE, is_fraud=False) is Stratum.LEGIT


# --- the spool ------------------------------------------------------------


def _label(i: int, at: dt.datetime, *, fraud: bool = False) -> dict[str, object]:
    return label_row(LabelEvent(event_id=f"evt-{i}", label_time=at, is_fraud=fraud))


def test_rows_are_filed_by_hour_and_read_back_whole(tmp_path: Path) -> None:
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    for i in range(10):
        writer.append(DAY_START + dt.timedelta(minutes=15 * i), _label(i, DAY_START))
    writer.close()
    assert spool.hours(tmp_path) == [f"2027-04-05T0{h}" for h in range(3)]
    table = spool.read_hours(tmp_path, spool.hours(tmp_path), LABEL_SCHEMA)
    assert table["event_id"].to_pylist() == [f"evt-{i}" for i in range(10)]


def test_a_late_row_for_a_closed_hour_gets_its_own_file(tmp_path: Path) -> None:
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    writer.append(DAY_START, _label(1, DAY_START))
    writer.flush()
    assert writer.close_before(DAY_START + dt.timedelta(hours=1)) == ["2027-04-05T00"]
    writer.append(DAY_START, _label(2, DAY_START))
    writer.close()
    files = list((tmp_path / "2027-04-05T00").glob("*.arrow"))
    assert len(files) == 2
    table = spool.read_hours(tmp_path, ["2027-04-05T00"], LABEL_SCHEMA)
    assert sorted(str(e) for e in table["event_id"].to_pylist()) == ["evt-1", "evt-2"]


def test_a_cut_short_file_yields_every_whole_batch_before_the_cut(tmp_path: Path) -> None:
    """A writer that died mid-batch loses that batch and nothing before it."""
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    writer.append(DAY_START, _label(1, DAY_START))
    writer.flush()
    writer.append(DAY_START, _label(2, DAY_START))
    writer.flush()
    writer.close()
    (path,) = (tmp_path / "2027-04-05T00").glob("*.arrow")
    data = path.read_bytes()
    path.write_bytes(data[: len(data) - 40])
    table = spool.read_hours(tmp_path, ["2027-04-05T00"], LABEL_SCHEMA)
    assert table["event_id"].to_pylist() == ["evt-1"]


def test_an_hour_a_writer_holds_is_not_sealed(tmp_path: Path) -> None:
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    writer.append(DAY_START, _label(1, DAY_START))
    writer.flush()
    assert not spool.seal(tmp_path, "2027-04-05T00", LABEL_SCHEMA)
    writer.close()
    assert spool.seal(tmp_path, "2027-04-05T00", LABEL_SCHEMA)
    assert (tmp_path / "2027-04-05T00.parquet").exists()
    assert not (tmp_path / "2027-04-05T00").exists()
    table = spool.read_hours(tmp_path, ["2027-04-05T00"], LABEL_SCHEMA)
    assert table["event_id"].to_pylist() == ["evt-1"]


def test_files_left_by_a_dead_writer_are_recovered_for_sealing(tmp_path: Path) -> None:
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    writer.append(DAY_START, _label(1, DAY_START))
    writer.flush()
    del writer  # the process dies holding the file
    assert len(spool.recover(tmp_path)) == 1
    assert spool.seal(tmp_path, "2027-04-05T00", LABEL_SCHEMA)


# --- finalising a day -----------------------------------------------------


def _decision(event: TransactionEvent, action: Action, score: float) -> DecisionEvent:
    return DecisionEvent(
        event_id=event.event_id,
        card_id=event.card_id,
        action=action,
        score=score,
        rule="test",
        model_version="m-1",
        decided_at=event.event_time,
    )


def _a_day(
    paths: HistoryPaths, n: int, *, seed: int = 7, duplicate_every: int = 0
) -> dict[str, tuple[bool, float, int, Action]]:
    """Stage a day of decisions and spool their labels; return the truth.

    Scores are informative but imperfect, so PR-AUC is neither 0 nor 1.
    """
    rng = np.random.default_rng(seed)
    rules = DecisionRules()
    staged = spool.SpoolWriter(paths.staged, staged_schema())
    labels = spool.SpoolWriter(paths.labels, LABEL_SCHEMA)
    truth: dict[str, tuple[bool, float, int, Action]] = {}
    for i in range(n):
        at = DAY_START + dt.timedelta(seconds=86_399 * i / n)
        fraud = bool(rng.random() < 0.05)
        score = float(np.clip(rng.normal(0.6 if fraud else 0.2, 0.2), 0.0, 1.0))
        amount = int(rng.integers(500, 600_000))
        event = an_event(i, card=f"card-{i % 97}", amount=amount).model_copy(
            update={"event_time": at}
        )
        action, _ = rules.decide(score, event)
        row = staged_row(event, FEATURES, _decision(event, action, score), None)
        staged.append(at, row)
        if duplicate_every and i % duplicate_every == 0:
            staged.append(at, row)
        labels.append(at + DELAY, _label(i, at + DELAY, fraud=fraud))
        truth[event.event_id] = (fraud, score, amount, action)
    staged.close()
    labels.close()
    return truth


def test_a_day_is_not_final_until_its_labels_could_all_have_arrived(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 200)
    early = final_after(DAY) - dt.timedelta(seconds=1)
    with pytest.raises(NotYetFinalError):
        finalise_day(paths, DAY, as_of=early, rates=TEST_RATES)
    assert list(finalisable(paths, early)) == []
    assert list(finalisable(paths, final_after(DAY))) == [DAY]


def test_every_reviewed_or_declined_row_is_kept_at_weight_one(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    truth = _a_day(paths, 3_000)
    finalise_day(paths, DAY, as_of=final_after(DAY), rates=TEST_RATES)
    kept = read_kept(paths, [DAY]).to_pandas()
    acted = {e for e, (_, _, _, a) in truth.items() if a is not Action.APPROVE}
    assert acted, "the fixture should produce some reviews and declines"
    acted_kept = kept[kept["stratum"] == Stratum.ACTED.value]
    assert set(acted_kept["event_id"]) == acted
    assert (acted_kept["weight"] == 1.0).all()


def test_the_sample_estimates_what_the_full_day_holds(tmp_path: Path) -> None:
    """The one that matters: the kept rows' weighted estimates against every row.

    Counts, PR-AUC and decision cost, each computed on the full day and on the
    sample with its weights. The draw is a fixed hash, so this is
    deterministic; the tolerances are what a sample of this size should meet,
    and a sample read without its weights misses them by the sampling rate.
    """
    paths = HistoryPaths(tmp_path)
    truth = _a_day(paths, 20_000)
    manifest = finalise_day(paths, DAY, as_of=final_after(DAY), rates=TEST_RATES)
    kept = read_kept(paths, [DAY]).to_pandas()

    labels = np.array([t[0] for t in truth.values()])
    scores = np.array([t[1] for t in truth.values()])
    amounts = np.array([t[2] for t in truth.values()], dtype=np.float64)
    rules = DecisionRules()

    k_labels = kept["is_fraud"].to_numpy(dtype=np.bool_)
    k_scores = kept["champion_score"].to_numpy(dtype=np.float64)
    k_amounts = kept["amount_cents"].to_numpy(dtype=np.float64)
    k_weights = kept["weight"].to_numpy(dtype=np.float64)

    assert manifest.estimated_transactions == pytest.approx(len(truth), rel=0.05)
    assert manifest.estimated_frauds == pytest.approx(labels.sum(), rel=0.10)

    full_ap = average_precision(labels, scores)
    sample_ap = average_precision(k_labels, k_scores, k_weights)
    assert sample_ap == pytest.approx(full_ap, abs=0.03)
    unweighted_ap = average_precision(k_labels, k_scores)
    assert abs(unweighted_ap - full_ap) > 0.03, "the weights should be doing the work"

    full_cost = decision_cost(labels, amounts, scores, rules, 500.0, 0.1)
    sample_cost = decision_cost(k_labels, k_amounts, k_scores, rules, 500.0, 0.1, k_weights)
    assert sample_cost == pytest.approx(full_cost, rel=0.10)


def test_a_duplicate_delivery_is_kept_once(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 1_000, duplicate_every=10)
    manifest = finalise_day(paths, DAY, as_of=final_after(DAY), rates=TEST_RATES)
    assert manifest.duplicates == 100
    assert manifest.staged_rows == 1_100
    kept = read_kept(paths, [DAY]).to_pandas()
    assert kept["event_id"].is_unique


def _a_redelivered_day(paths: HistoryPaths, n: int) -> None:
    """Stage a day in many small batches, some rows delivered again later in the hour.

    The way a restarted scorer stages: every few rows a flush, and a
    redelivery lands a few batches after the first copy, never beside it.
    """
    rng = np.random.default_rng(11)
    rules = DecisionRules()
    staged = spool.SpoolWriter(paths.staged, staged_schema())
    labels = spool.SpoolWriter(paths.labels, LABEL_SCHEMA)
    again: list[tuple[dt.datetime, dict[str, object]]] = []
    for i in range(n):
        at = DAY_START + dt.timedelta(seconds=86_399 * i / n)
        score = float(rng.random())
        event = an_event(i, card=f"card-{i % 97}").model_copy(update={"event_time": at})
        action, _ = rules.decide(score, event)
        row = staged_row(event, FEATURES, _decision(event, action, score), None)
        staged.append(at, row)
        if i % 13 == 0:
            again.append((at, row))
        if i % 7 == 6:
            staged.flush()
            if len(again) > 3:
                staged.append(*again.pop(0))
        labels.append(at + DELAY, _label(i, at + DELAY, fraud=bool(rng.random() < 0.05)))
    staged.close()
    labels.close()


def test_finalising_does_not_depend_on_how_an_hour_is_stored(tmp_path: Path) -> None:
    """Sealed or not, in one batch or many, a day finalises to the same rows.

    Finalising streams each hour twice and finds duplicates by their draw
    before their id, so a duplicate split across batches, or an hour whose
    batches differ between its two forms, is where a row could be lost or
    kept twice.
    """
    unsealed, sealed = HistoryPaths(tmp_path / "unsealed"), HistoryPaths(tmp_path / "sealed")
    _a_redelivered_day(unsealed, 2_000)
    _a_redelivered_day(sealed, 2_000)
    seal_closed(sealed, DAY_START + dt.timedelta(days=9))
    assert all(not (sealed.staged / key).is_dir() for key in spool.hours(sealed.staged))

    as_of = final_after(DAY)
    a = finalise_day(unsealed, DAY, as_of=as_of, rates=TEST_RATES)
    b = finalise_day(sealed, DAY, as_of=as_of, rates=TEST_RATES)
    assert a.duplicates > 0
    assert a.staged_rows == 2_000 + a.duplicates
    assert asdict(a) | {"sha256": ""} == asdict(b) | {"sha256": ""}
    kept_a, kept_b = read_kept(unsealed, [DAY]), read_kept(sealed, [DAY])
    assert kept_a.equals(kept_b)
    assert kept_a["event_id"].to_pandas().is_unique


def test_finalising_never_reads_an_hour_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An hour is about 3.6 million rows live; finalising streams it.

    Reading an hour whole is what put the compactor over the instance's
    memory when sealing did it (docs/STATE.md), and finalising read both
    staged and label hours whole until 2026-09-22.
    """
    paths = HistoryPaths(tmp_path)
    _a_redelivered_day(paths, 500)

    def refuse(*_: object, **__: object) -> pa.Table:
        raise AssertionError("finalising read an hour whole")

    monkeypatch.setattr(spool, "read_hours", refuse)
    manifest = finalise_day(paths, DAY, as_of=final_after(DAY), rates=TEST_RATES)
    assert manifest.candidates > 0


def test_a_label_that_arrived_after_finalising_does_not_count(tmp_path: Path) -> None:
    """A fraud whose label came late is not kept as legitimate.

    It is not kept at all, and the manifest says a label was missing.
    """
    paths = HistoryPaths(tmp_path)
    staged = spool.SpoolWriter(paths.staged, staged_schema())
    event = an_event(1).model_copy(update={"event_time": DAY_START})
    staged.append(
        DAY_START, staged_row(event, FEATURES, _decision(event, Action.REVIEW, 0.7), None)
    )
    staged.close()
    as_of = final_after(DAY)
    labels = spool.SpoolWriter(paths.labels, LABEL_SCHEMA)
    labels.append(as_of, _label(1, as_of + dt.timedelta(seconds=1), fraud=True))
    labels.close()
    manifest = finalise_day(paths, DAY, as_of=as_of, rates=TEST_RATES)
    assert manifest.unlabelled == 1
    assert read_kept(paths, [DAY]).num_rows == 0


def test_finalising_writes_then_deletes_and_can_be_repeated(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 500)
    first = finalise_day(paths, DAY, as_of=final_after(DAY), rates=TEST_RATES)
    assert spool.hours(paths.staged) == []
    assert spool.hours(paths.labels) == []
    again = finalise_day(paths, DAY, as_of=final_after(DAY) + dt.timedelta(days=1))
    assert again == first
    stored = json.loads(paths.manifest_file(DAY).read_text(encoding="utf-8"))
    assert stored["sha256"] == first.sha256
    assert stored["rates"] == {"acted": 1.0, "fraud": 0.5, "legit": 0.1}


def test_sealing_leaves_the_last_hour_alone_until_it_has_settled(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 100)
    sealed = seal_closed(paths, DAY_START + dt.timedelta(hours=3, minutes=5))
    assert "staged/2027-04-05T01" in sealed
    assert "staged/2027-04-05T02" not in sealed


def test_a_limit_works_a_backlog_off_a_few_hours_at_a_time(tmp_path: Path) -> None:
    """A crashed compactor leaves a backlog; the next run must not hold it all at once.

    On the dry run's first night a run with no limit read and rewrote every
    unsealed hour in one process, and a run twelve hours into a backlog was
    killed by the kernel at 9.3 GB. A limit bounds one run to a few hours
    whatever the backlog, worked off over several calls instead.
    """
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 240)
    well_settled = DAY_START + dt.timedelta(days=9)
    everything = seal_closed(paths, well_settled)
    assert len(everything) > 10  # both staged (day 0) and labels (day 7) hours
    _a_day(paths, 240, seed=11)  # a fresh, unsealed backlog to work off with a limit

    first = seal_closed(paths, well_settled, limit=5)
    assert len(first) == 5

    rest: list[str] = []
    while batch := seal_closed(paths, well_settled, limit=5):
        assert len(batch) <= 5
        rest.extend(batch)
    assert len(first) + len(rest) == len(everything)
    assert seal_closed(paths, well_settled, limit=5) == []  # nothing left to work off


def test_a_limit_of_zero_seals_nothing(tmp_path: Path) -> None:
    paths = HistoryPaths(tmp_path)
    _a_day(paths, 100)
    assert seal_closed(paths, DAY_START + dt.timedelta(days=1), limit=0) == []


def test_sealing_streams_a_chunk_at_a_time_and_still_carries_every_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`seal` used to read a whole hour into one table before writing it back out.

    On the dry run's first clean hour after the backlog that crashed three
    instances, that alone came within about 260 MB of the instance's memory
    (docs/STATE.md). Sealing now streams a chunk at a time
    (`spool.SEAL_CHUNK_ROWS`); shrunk here so a small hour still crosses
    several chunk boundaries, which is where a row could be lost or
    doubled.
    """
    monkeypatch.setattr(spool, "SEAL_CHUNK_ROWS", 37)
    writer = spool.SpoolWriter(tmp_path, staged_schema())
    events = [an_event(i, card=f"card-{i % 11}") for i in range(953)]  # not a multiple of 37
    for event in events:
        row = staged_row(event, FEATURES, _decision(event, Action.APPROVE, 0.1), None)
        writer.append(event.event_time, row)
        if event.event_id.endswith(("3", "7")):  # many small, uneven batches, like the scorer's
            writer.flush()
    writer.close()

    assert spool.seal(tmp_path, spool.hour_key(events[0].event_time), staged_schema())

    read_back = spool.read_hours(tmp_path, [spool.hour_key(events[0].event_time)], staged_schema())
    ids = cast("list[str]", read_back["event_id"].to_pylist())
    assert sorted(ids) == sorted(e.event_id for e in events)


# --- the scorer and the collector -----------------------------------------


def _setup(tmp_path: Path) -> tuple[MemoryBroker, MemoryStream, spool.SpoolWriter]:
    broker = MemoryBroker()
    for topic, partitions in (("transactions", 1), ("decisions", 2), ("dead-letter", 1)):
        broker.create_topic(topic, partitions)
    return broker, broker.open(), spool.SpoolWriter(tmp_path, staged_schema())


def _staged(tmp_path: Path) -> pa.Table:
    return spool.read_hours(tmp_path, spool.hours(tmp_path), staged_schema())


def test_the_scorer_stages_every_decision_with_the_features_it_served(tmp_path: Path) -> None:
    _, stream, history = _setup(tmp_path)
    send(stream, [an_event(i, card=f"card-{i % 3}") for i in range(40)])
    scorer = StreamScorer(stream, decider=a_decider(), history=history)
    while scorer.poll(max_records=7):
        pass
    table = _staged(tmp_path)
    assert table.num_rows == 40
    assert sorted(str(e) for e in table["event_id"].to_pylist()) == sorted(
        f"evt-{i}" for i in range(40)
    )
    # The third event of card-0 has seen two before it within the hour.
    (row,) = [r for r in table.to_pylist() if r["event_id"] == "evt-6"]
    assert row["card_txn_count_1h"] == 2.0


def test_a_duplicate_is_not_staged_twice_by_one_scorer(tmp_path: Path) -> None:
    _, stream, history = _setup(tmp_path)
    events = [an_event(i) for i in range(5)]
    send(stream, [*events, events[2]])
    scorer = StreamScorer(stream, decider=a_decider(), history=history)
    while scorer.poll():
        pass
    assert _staged(tmp_path).num_rows == 5


class _FailingFlush(MemoryStream):
    def flush(self, timeout_seconds: float = 10.0) -> None:
        raise StreamError("decisions not acknowledged")


def test_a_decision_is_staged_before_its_transaction_is_checkpointed(tmp_path: Path) -> None:
    """If the batch fails after staging, the rows exist and nothing is checkpointed.

    The transactions come again and are staged again, and finalising keeps
    one of each.
    """
    broker, _, history = _setup(tmp_path)
    stream = _FailingFlush(broker)
    send(stream, [an_event(i) for i in range(3)])
    scorer = StreamScorer(stream, decider=a_decider(), history=history)
    with pytest.raises(StreamError):
        scorer.poll()
    assert _staged(tmp_path).num_rows == 3
    again = broker.open().consume("transactions", "scorer", max_records=10)
    assert len(again) == 3


def _labels_on(broker: MemoryBroker, labels: Iterable[bytes]) -> None:
    stream = broker.open()
    for i, value in enumerate(labels):
        stream.produce("labels", f"k-{i}", value)


def test_the_collector_spools_labels_by_their_own_time_and_skips_garbage(
    tmp_path: Path,
) -> None:
    broker = MemoryBroker()
    broker.create_topic("labels", 1)
    at = DAY_START + DELAY
    good = [
        LabelEvent(
            event_id=f"evt-{i}", label_time=at + dt.timedelta(minutes=40 * i), is_fraud=i == 1
        )
        for i in range(4)
    ]
    _labels_on(broker, [*(label.to_json().encode("utf-8") for label in good), b"not a label"])
    writer = spool.SpoolWriter(tmp_path, LABEL_SCHEMA)
    collector = LabelCollector(broker.open(), writer)
    while collector.poll():
        pass
    writer.close()
    assert collector.stats.written == 4
    assert collector.stats.unreadable == 1
    # What the collector-behind alert reads (ADR 26).
    assert collector.metrics.written._value.get() == 4
    assert collector.metrics.unreadable._value.get() == 1
    assert collector.metrics.latest._value.get() == good[-1].label_time.timestamp()
    table = spool.read_hours(tmp_path, spool.hours(tmp_path), LABEL_SCHEMA)
    assert table["event_id"].to_pylist() == [f"evt-{i}" for i in range(4)]
    assert spool.hours(tmp_path) == ["2027-04-12T00", "2027-04-12T01", "2027-04-12T02"]
