"""The entity graph is deterministic, and has the structure fraud needs."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from verdict.events.generator.entities import (
    CATEGORIES,
    SPEND_PROFILES,
    EntityGraph,
    Population,
)

HASHES = json.loads(
    (Path(__file__).resolve().parents[1] / "docs" / "generator-hashes.json").read_text(
        encoding="utf-8"
    )
)

REFERENCE = Population(cards=20_000, devices=15_000, merchants=500)
"""The published reference population, small enough to build in a test."""


@pytest.fixture(scope="module")
def graph() -> EntityGraph:
    return EntityGraph.build(seed=20270201, population=REFERENCE)


def test_the_reference_graph_matches_its_committed_hash(graph: EntityGraph) -> None:
    """The replay of a run depends on the graph being the same graph."""
    assert graph.fingerprint() == HASHES["reference_graph_sha256"]


def test_the_same_seed_gives_the_same_graph(graph: EntityGraph) -> None:
    assert EntityGraph.build(seed=20270201, population=REFERENCE).fingerprint() == (
        graph.fingerprint()
    )


def test_a_different_seed_gives_a_different_graph(graph: EntityGraph) -> None:
    assert EntityGraph.build(seed=20270202, population=REFERENCE).fingerprint() != (
        graph.fingerprint()
    )


def test_every_card_has_at_least_one_device(graph: EntityGraph) -> None:
    for index in range(0, REFERENCE.cards, 137):
        devices = graph.devices_of(index)
        assert 1 <= devices.size <= REFERENCE.max_devices_per_card


def test_some_devices_are_shared(graph: EntityGraph) -> None:
    """Shared-device count is a feature in week 3. It needs variance now."""
    shared = int((graph.device_card_count > 1).sum())
    assert shared > REFERENCE.devices * 0.01
    assert int(graph.device_card_count.max()) > 2


def test_every_category_has_a_merchant(graph: EntityGraph) -> None:
    """A card's profile can choose any category, so every one must exist."""
    for index in range(len(CATEGORIES)):
        assert graph.category_merchants[index].size > 0
        assert graph.category_merchant_cum[index][-1] == pytest.approx(1.0)


def test_merchant_popularity_is_long_tailed(graph: EntityGraph) -> None:
    """A flat merchant distribution would make velocity features useless.

    Real card volume concentrates on a few merchants. If every merchant were
    equally likely, a merchant-level velocity window would see the same tiny
    count everywhere and separate nothing.
    """
    import numpy as np

    for cum in graph.category_merchant_cum:
        weights = np.diff(np.concatenate([[0.0], cum]))
        decile = max(1, int(len(weights) * 0.1))
        share = float(np.sort(weights)[-decile:].sum())
        flat_share = decile / len(weights)
        assert share > flat_share * 2.0


def test_the_population_always_has_a_colluding_merchant(graph: EntityGraph) -> None:
    """A scenario that silently cannot fire is worse than one that is off."""
    assert graph.colluding_merchants.size >= 1


def test_every_profile_is_used(graph: EntityGraph) -> None:
    used = {graph.card(index).profile for index in range(0, REFERENCE.cards, 7)}
    assert used == set(SPEND_PROFILES)


def test_identifiers_are_stable_and_distinct(graph: EntityGraph) -> None:
    assert graph.card(0).card_id == "card-00000000"
    assert graph.device(12).device_id == "dev-00000012"
    assert graph.merchant(3).merchant_id == "mer-000003"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cards": 0},
        {"merchants": 3},
        {"shared_device_rate": 1.5},
        {"max_devices_per_card": 0},
        {"colluding_merchant_rate": -0.1},
    ],
)
def test_an_impossible_population_is_refused(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match=".+"):
        Population(**kwargs)  # type: ignore[arg-type]
