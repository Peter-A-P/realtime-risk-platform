"""The generator: deterministic, time-ordered, and honest about labels."""

from __future__ import annotations

import datetime as dt
from collections import Counter
from dataclasses import replace

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import DEV_SCHEDULE, Regime, RegimeSchedule
from verdict.events.schema import FraudScenario

REFERENCE = Population(cards=20_000, devices=15_000, merchants=500)
SEED = 20270201


@pytest.fixture(scope="module")
def graph() -> EntityGraph:
    return EntityGraph.build(seed=SEED, population=REFERENCE)


def config(**overrides: object) -> GeneratorConfig:
    base = GeneratorConfig(seed=SEED, population=REFERENCE, events_per_second=1000.0)
    return replace(base, **overrides)  # type: ignore[arg-type]


def test_the_same_seed_produces_the_same_stream(graph: EntityGraph) -> None:
    """A replay is only a replay if it is the same stream."""
    first = [record.event.to_json() for record in Generator(config(), graph).stream(limit=2_000)]
    second = [record.event.to_json() for record in Generator(config(), graph).stream(limit=2_000)]
    assert first == second


def test_a_different_seed_produces_a_different_stream(graph: EntityGraph) -> None:
    other_graph = EntityGraph.build(seed=SEED + 1, population=REFERENCE)
    first = [record.event.to_json() for record in Generator(config(), graph).stream(limit=500)]
    second = [
        record.event.to_json()
        for record in Generator(config(seed=SEED + 1), other_graph).stream(limit=500)
    ]
    assert first != second


def test_events_are_emitted_in_time_order(graph: EntityGraph) -> None:
    """The generator never reorders.

    Out-of-order delivery is a transport property, injected deliberately in
    the chaos tests. It is never an accident of the generator.
    """
    times = [record.event.event_time for record in Generator(config(), graph).stream(limit=20_000)]
    assert times == sorted(times)


def test_event_ids_are_unique(graph: EntityGraph) -> None:
    """Decisions are keyed on the event id.

    A collision is therefore a wrong decision, not a duplicate row.
    """
    ids = [record.event.event_id for record in Generator(config(), graph).stream(limit=20_000)]
    assert len(set(ids)) == len(ids)


def test_a_label_arrives_exactly_the_delay_after_its_event(graph: EntityGraph) -> None:
    for record in Generator(config(), graph).stream(limit=1_000):
        assert record.label.event_id == record.event.event_id
        assert record.label.label_time - record.event.event_time == dt.timedelta(days=7)


def test_ground_truth_agrees_with_the_label(graph: EntityGraph) -> None:
    for record in Generator(config(), graph).stream(limit=1_000):
        assert record.truth.is_fraud == record.label.is_fraud
        assert (record.truth.scenario is FraudScenario.NONE) != record.label.is_fraud


def test_only_fraud_has_anything_to_recover(graph: EntityGraph) -> None:
    """Expected loss in week 6 is built on this being true."""
    for record in Generator(config(), graph).stream(limit=2_000):
        if not record.truth.is_fraud:
            assert record.label.recovered_cents == 0
        assert record.label.recovered_cents <= record.event.amount_cents


def test_the_fraud_share_lands_near_the_target(graph: EntityGraph) -> None:
    """The generator is asked for a share, not for an attack rate.

    It has to hold that share as the scenario mix and the attack intensity
    move, which is why the driver derives the rate instead of taking it.
    """
    records = list(Generator(config(target_fraud_share=0.03), graph).stream(limit=100_000))
    share = sum(record.truth.is_fraud for record in records) / len(records)
    assert share == pytest.approx(0.03, abs=0.008)


def test_asking_for_no_fraud_gives_no_fraud(graph: EntityGraph) -> None:
    records = list(Generator(config(target_fraud_share=0.0), graph).stream(limit=5_000))
    assert not any(record.truth.is_fraud for record in records)


def test_every_scenario_appears(graph: EntityGraph) -> None:
    counts = Counter(
        record.truth.scenario for record in Generator(config(), graph).stream(limit=100_000)
    )
    for scenario in FraudScenario:
        assert counts[scenario] > 0


def test_a_regime_shift_moves_what_it_says_it_moves(graph: EntityGraph) -> None:
    """The amount-drift regime must move amounts and nothing else.

    This is the case the drift monitors are graded on in week 6: a feature
    distribution moves while the fraud rate does not.
    """
    baseline = DEV_SCHEDULE.regimes[0]
    shifted = Regime(
        name="shifted",
        starts_after_days=0.0,
        fraud_rate_multiplier=1.0,
        scenario_weights=baseline.scenario_weights,
        amount_log_shift=0.6,
        online_share_shift=0.0,
        attack_intensity=1.0,
    )
    flat = RegimeSchedule(name="flat", regimes=(baseline,))
    drifted = RegimeSchedule(name="drifted", regimes=(shifted,))

    def legitimate_amounts(schedule: RegimeSchedule) -> list[int]:
        return [
            record.event.amount_cents
            for record in Generator(config(schedule=schedule), graph).stream(limit=20_000)
            if not record.truth.is_fraud
        ]

    before = legitimate_amounts(flat)
    after = legitimate_amounts(drifted)
    median_before = sorted(before)[len(before) // 2]
    median_after = sorted(after)[len(after) // 2]
    assert median_after > median_before * 1.5


def test_the_regime_in_force_is_recorded_on_every_record(graph: EntityGraph) -> None:
    names = {record.truth.regime for record in Generator(config(), graph).stream(limit=5_000)}
    assert names <= {regime.name for regime in DEV_SCHEDULE.regimes}
    assert names == {"baseline"}


def test_turning_the_warm_up_off_still_generates(graph: EntityGraph) -> None:
    records = list(Generator(config(warm_up=False), graph).stream(limit=1_000))
    assert len(records) == 1_000


def test_the_warm_up_is_what_fixes_the_opening_minutes(graph: EntityGraph) -> None:
    """Without it, the stream opens with no attack already in flight."""
    cold = list(Generator(config(warm_up=False), graph).stream(limit=20_000))
    warm = list(Generator(config(warm_up=True), graph).stream(limit=20_000))
    cold_share = sum(record.truth.is_fraud for record in cold) / len(cold)
    warm_share = sum(record.truth.is_fraud for record in warm) / len(warm)
    assert warm_share > cold_share * 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"events_per_second": 0.0},
        {"target_fraud_share": 1.0},
        {"target_fraud_share": -0.1},
        {"label_delay_days": -1.0},
        {"start_time": dt.datetime(2027, 1, 1)},
    ],
)
def test_an_impossible_configuration_is_refused(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=".+"):
        config(**kwargs)
