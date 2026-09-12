"""Each fraud pattern has the signature the features will have to find."""

from __future__ import annotations

import numpy as np
import pytest

from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.generator.regimes import DEV_SCHEDULE, Regime
from verdict.events.generator.scenarios import (
    CARD_TESTING_AMOUNT_CENTS,
    TAKEOVER_AMOUNT_MULTIPLIER,
    mean_attack_duration,
    mean_attack_size,
    plan_attack,
)
from verdict.events.schema import EntryMode, FraudScenario

REFERENCE = Population(cards=20_000, devices=15_000, merchants=500)


@pytest.fixture(scope="module")
def graph() -> EntityGraph:
    return EntityGraph.build(seed=20270201, population=REFERENCE)


@pytest.fixture
def rng() -> np.random.Generator:
    return np.random.default_rng(99)


@pytest.fixture
def baseline() -> Regime:
    return DEV_SCHEDULE.regimes[0]


def test_card_testing_is_one_device_against_many_cards(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    """The signature is in the graph, not in any single transaction."""
    events = plan_attack(FraudScenario.CARD_TESTING, rng, graph, baseline, 100.0, 1)
    assert len({event.device_index for event in events}) == 1
    assert len({event.merchant_index for event in events}) == 1
    assert len({event.card_index for event in events}) > len(events) * 0.8
    assert all(event.entry_mode is EntryMode.ECOMMERCE for event in events)
    assert all(event.amount_cents <= CARD_TESTING_AMOUNT_CENTS[1] for event in events)


def test_card_testing_is_a_burst(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    """Minutes, not days: a velocity window has to be able to see it."""
    events = plan_attack(FraudScenario.CARD_TESTING, rng, graph, baseline, 100.0, 1)
    span = events[-1].at_seconds - events[0].at_seconds
    assert span < 3600.0


def test_takeover_uses_a_device_the_card_has_never_used(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    events = plan_attack(FraudScenario.ACCOUNT_TAKEOVER, rng, graph, baseline, 100.0, 1)
    card = events[0].card_index
    known = {int(device) for device in graph.devices_of(card)}
    assert len({event.card_index for event in events}) == 1
    assert events[0].device_index not in known


def test_takeover_spends_above_the_card_s_own_usual(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    """Unusual for this card, not unusual in general: that is the point."""
    events = plan_attack(FraudScenario.ACCOUNT_TAKEOVER, rng, graph, baseline, 100.0, 1)
    typical = float(np.exp(graph.card_amount_mu[events[0].card_index])) * 100
    assert min(event.amount_cents for event in events) >= typical * (
        TAKEOVER_AMOUNT_MULTIPLIER[0] * 0.99
    )


def test_collusion_concentrates_on_one_colluding_merchant(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    events = plan_attack(FraudScenario.MERCHANT_COLLUSION, rng, graph, baseline, 100.0, 1)
    merchants = {event.merchant_index for event in events}
    assert len(merchants) == 1
    assert bool(graph.merchant_is_colluding[merchants.pop()])
    assert len({event.card_index for event in events}) > 5


def test_every_plan_is_in_time_order_and_starts_after_the_attack(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    for scenario in (
        FraudScenario.CARD_TESTING,
        FraudScenario.ACCOUNT_TAKEOVER,
        FraudScenario.MERCHANT_COLLUSION,
    ):
        events = plan_attack(scenario, rng, graph, baseline, 100.0, 1)
        times = [event.at_seconds for event in events]
        assert times == sorted(times)
        assert times[0] > 100.0
        assert all(event.scenario is scenario for event in events)
        assert [event.sequence for event in events] == list(range(len(events)))


def test_planning_nothing_is_an_error(
    graph: EntityGraph, rng: np.random.Generator, baseline: Regime
) -> None:
    with pytest.raises(ValueError, match="not an attack"):
        plan_attack(FraudScenario.NONE, rng, graph, baseline, 0.0, 1)


def test_intensity_scales_an_attack(graph: EntityGraph, baseline: Regime) -> None:
    """A regime that says twice the intensity gets about twice the attack."""
    from dataclasses import replace

    fierce = replace(baseline, attack_intensity=2.5)
    small = plan_attack(
        FraudScenario.CARD_TESTING, np.random.default_rng(3), graph, baseline, 0.0, 1
    )
    large = plan_attack(FraudScenario.CARD_TESTING, np.random.default_rng(3), graph, fierce, 0.0, 1)
    assert len(large) == pytest.approx(len(small) * 2.5, rel=0.05)


def test_the_published_means_describe_what_is_generated(
    graph: EntityGraph, baseline: Regime
) -> None:
    """The published means describe what is actually generated.

    The driver sizes the attack rate from them; if they lie, so does the
    fraud share.
    """
    rng = np.random.default_rng(11)
    sizes: list[int] = []
    spans: list[float] = []
    for number in range(400):
        scenario = (
            FraudScenario.CARD_TESTING,
            FraudScenario.ACCOUNT_TAKEOVER,
            FraudScenario.MERCHANT_COLLUSION,
        )[number % 3]
        events = plan_attack(scenario, rng, graph, baseline, 0.0, number)
        sizes.append(len(events))
        spans.append(events[-1].at_seconds)
    even = Regime(
        name="even",
        starts_after_days=0.0,
        fraud_rate_multiplier=1.0,
        scenario_weights=(1.0, 1.0, 1.0),
        amount_log_shift=0.0,
        online_share_shift=0.0,
        attack_intensity=1.0,
    )
    assert float(np.mean(sizes)) == pytest.approx(mean_attack_size(even), rel=0.15)
    assert float(np.mean(spans)) == pytest.approx(mean_attack_duration(even), rel=0.25)
