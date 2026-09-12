"""A replay is in time order, or it is not a replay."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.rawlog import RawEventLog
from verdict.events.replay import (
    OutOfOrderError,
    before,
    in_time_order,
    replay,
    replay_until,
    span,
)
from verdict.events.schema import EntryMode, MerchantCategory, TransactionEvent

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)
REFERENCE = Population(cards=2_000, devices=1_500, merchants=100)


def an_event(minutes: int) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{minutes}",
        event_time=START + dt.timedelta(minutes=minutes),
        card_id="card-1",
        device_id="dev-1",
        merchant_id="mer-1",
        amount_cents=1_000,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.CHIP,
    )


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    graph = EntityGraph.build(seed=7, population=REFERENCE)
    config = GeneratorConfig(seed=7, population=REFERENCE, events_per_second=200.0)
    with RawEventLog(tmp_path) as log:
        for record in Generator(config, graph).stream(limit=2_000):
            log.append(record)
    return tmp_path


def test_a_generated_log_replays_in_order(log_dir: Path) -> None:
    events = list(replay(log_dir))
    assert len(events) == 2_000
    assert [event.event_time for event in events] == sorted(event.event_time for event in events)


def test_an_out_of_order_log_is_refused_not_quietly_reordered() -> None:
    """A reordered replay corrupts every window after the offending row.

    Sorting it silently would hide a real transport fault; the chaos tests in
    week 8 inject out-of-order delivery deliberately and assert what happens.
    """
    events = [an_event(0), an_event(10), an_event(5)]
    with pytest.raises(OutOfOrderError) as caught:
        list(in_time_order(events))
    assert caught.value.offending.event_id == "evt-5"
    assert caught.value.previous.event_id == "evt-10"
    assert "out of order" in str(caught.value)


def test_out_of_order_events_can_be_dropped_instead(log_dir: Path) -> None:
    events = [an_event(0), an_event(10), an_event(5), an_event(20)]
    kept = list(in_time_order(events, strict=False))
    assert [event.event_id for event in kept] == ["evt-0", "evt-10", "evt-20"]
    del log_dir


def test_events_at_the_same_instant_are_allowed(log_dir: Path) -> None:
    """Non-decreasing, not strictly increasing.

    A thousand events a second on a coarse clock will collide, and two events
    sharing an instant is not a fault.
    """
    events = [an_event(0), an_event(0), an_event(1)]
    assert len(list(in_time_order(events))) == 3
    del log_dir


def test_a_replay_can_be_cut_at_a_moment(log_dir: Path) -> None:
    """The question the leakage test asks: what had happened by then?"""
    everything = list(replay(log_dir))
    cut = everything[len(everything) // 2].event_time
    early = list(replay_until(log_dir, cut))
    assert all(event.event_time < cut for event in early)
    assert len(early) < len(everything)


def test_the_cut_is_exclusive(log_dir: Path) -> None:
    """Strictly before. An event at the instant is not yet knowable."""
    everything = list(replay(log_dir))
    cut = everything[10].event_time
    ids = {event.event_id for event in replay_until(log_dir, cut)}
    assert everything[10].event_id not in ids


def test_before_selects_and_sorts(log_dir: Path) -> None:
    events = list(replay(log_dir))
    cut = events[100].event_time
    selected = before(list(reversed(events)), cut)
    assert all(event.event_time < cut for event in selected)
    assert selected == sorted(selected, key=lambda event: event.event_time)


def test_span_reports_the_window_of_a_replay(log_dir: Path) -> None:
    events = list(replay(log_dir))
    first, last = span(events)  # type: ignore[misc]
    assert first == events[0].event_time
    assert last == events[-1].event_time


def test_an_empty_replay_has_no_span() -> None:
    assert span([]) is None


def test_two_runs_appended_to_one_log_are_caught(tmp_path: Path) -> None:
    """The realistic way a log goes out of order: two runs, same directory.

    Both runs start at the same event time, so concatenating them puts the
    clock back. The replay refuses rather than producing windows that are
    quietly wrong.
    """
    graph = EntityGraph.build(seed=7, population=REFERENCE)
    config = GeneratorConfig(seed=7, population=REFERENCE, events_per_second=200.0)
    for _ in range(2):
        with RawEventLog(tmp_path) as log:
            for record in Generator(config, graph).stream(limit=200):
                log.append(record)
    with pytest.raises(OutOfOrderError):
        list(replay(tmp_path))
