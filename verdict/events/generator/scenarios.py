"""The fraud patterns the generator can produce.

Three patterns, all publicly documented, all acting on the entity graph rather
than on a single row. Sources are cited in `docs/adr/0002-two-tracks.md`.

- **Card testing.** One device runs a long list of stolen card numbers
  through a card-not-present merchant in small amounts, to find out which
  numbers still work. The signature is in the graph, not the amount: one
  device, many cards, minutes apart.
- **Account takeover.** A card starts transacting from a device it has never
  used, in categories that do not match its history, for amounts well above
  its usual. The signature is the mismatch with that card's own past, which
  is why cards have published spend profiles at all.
- **Merchant collusion.** One merchant posts inflated amounts across many
  cards over hours. No single transaction looks wrong; the merchant's share
  of later chargebacks does.

Each scenario plans a whole attack up front as a list of `PlannedEvent`, with
absolute times. The driver merges those plans into the legitimate stream in
time order, so an attack overlaps normal traffic the way it would in life.

## Made harder on 2026-09-19 (ADR 21)

The first version was far too easy to catch: a champion scored 0.9996 PR-AUC
on it, and the number of transactions in a session alone scored 0.64, because
every attack ran in one long session no legitimate card ever had. Attackers
who are caught that easily are not the ones a fraud platform exists for. So:

- **Sessions look like everyone's.** An attack's online transactions take the
  same per-card, per-half-hour session a legitimate one would, rather than one
  session across the whole attack.
- **Card testing is slow and spread out**: a handful to a few dozen cards,
  minutes apart rather than seconds, from a small pool of devices, at a few
  merchants, for amounts that overlap ordinary small purchases.
- **Takeovers spend like the owner, only more so**: amounts from just under
  to four times the card's usual, sometimes from a device the card already
  uses (malware, a stolen phone), and half the time in the card's own
  categories.
- **Collusion inflates gently**: 5 to 80 percent over the merchant's ticket.

The legitimate side gets its own look-alikes in `driver.py`: big-ticket
purchases and first-time devices happen to honest cardholders too.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np

from verdict.events.generator.entities import CATEGORIES, EntityGraph
from verdict.events.generator.regimes import ATTACK_SCENARIOS, Regime
from verdict.events.schema import EntryMode, FraudScenario, MerchantCategory

CARD_TESTING_CATEGORIES: Final[tuple[MerchantCategory, ...]] = (
    MerchantCategory.MISC_NET,
    MerchantCategory.GROCERY_NET,
    MerchantCategory.SHOPPING_NET,
)
"""Card testing needs a card-not-present merchant that takes small amounts."""

TAKEOVER_CATEGORIES: Final[tuple[MerchantCategory, ...]] = (
    MerchantCategory.SHOPPING_NET,
    MerchantCategory.TRAVEL,
    MerchantCategory.MISC_NET,
    MerchantCategory.ENTERTAINMENT,
)
"""Where a taken-over account gets spent: resellable and remote."""

CARD_TESTING_SIZE_RANGE: Final[tuple[int, int]] = (4, 30)
"""Cards touched in one testing run, before regime intensity."""

CARD_TESTING_GAP_SECONDS: Final[tuple[float, float]] = (20.0, 900.0)
"""Seconds between attempts. Slow enough to stay under a velocity rule: the
careful tester trades speed for not being noticed."""

CARD_TESTING_AMOUNT_CENTS: Final[tuple[int, int]] = (100, 4_000)
"""Small, but inside the range of ordinary small purchases."""

CARD_TESTING_DEVICES: Final[tuple[int, int]] = (1, 3)
"""Devices the tester rotates through."""

CARD_TESTING_MERCHANTS: Final[tuple[int, int]] = (1, 3)
"""Merchants the tester spreads the attempts over."""

TAKEOVER_SIZE_RANGE: Final[tuple[int, int]] = (3, 14)
"""Transactions in one takeover, before regime intensity."""

TAKEOVER_GAP_SECONDS: Final[tuple[float, float]] = (300.0, 5_400.0)
"""Takeovers run over hours, paced like the owner's own spending."""

TAKEOVER_AMOUNT_MULTIPLIER: Final[tuple[float, float]] = (0.9, 4.0)
"""How the takeover's amounts compare with the card's own usual."""

TAKEOVER_KNOWN_DEVICE_SHARE: Final = 0.35
"""Takeovers run from a device the card already uses: malware, a stolen phone."""

TAKEOVER_OWN_CATEGORY_SHARE: Final = 0.5
"""Takeover purchases made in the card's own spending categories."""

COLLUSION_SIZE_RANGE: Final[tuple[int, int]] = (15, 90)
"""Cards run through a colluding merchant in one episode."""

COLLUSION_GAP_SECONDS: Final[tuple[float, float]] = (60.0, 900.0)
"""Collusion is paced to look like ordinary trading."""

COLLUSION_AMOUNT_MULTIPLIER: Final[tuple[float, float]] = (1.0, 1.5)
"""Inflation over the merchant's usual ticket: enough to pay, not to show."""

COLLUSION_FRONT_SHARE: Final = 0.5
"""Share of a collusion ring's charges put through front merchants in the same
category rather than the colluding merchant itself, so no one merchant's
figures carry the whole episode."""

SESSION_SECONDS: Final = 1800.0
"""Online activity is sessioned per card per half hour, fraud or not."""


def card_session(card: int, at_seconds: float) -> str:
    """The session a card's online transaction belongs to, as for everyone.

    Args:
        card: The card index.
        at_seconds: Seconds since the start of the window.

    Returns:
        The session identifier, the same one a legitimate transaction on this
        card in this half hour would carry.
    """
    return f"ses-{card:08d}-{int(at_seconds // SESSION_SECONDS)}"


_MEAN_GAP_SECONDS: Final[dict[FraudScenario, float]] = {
    FraudScenario.CARD_TESTING: sum(CARD_TESTING_GAP_SECONDS) / 2,
    FraudScenario.ACCOUNT_TAKEOVER: sum(TAKEOVER_GAP_SECONDS) / 2,
    FraudScenario.MERCHANT_COLLUSION: sum(COLLUSION_GAP_SECONDS) / 2,
}

_MEAN_ATTACK_SIZE: Final[dict[FraudScenario, float]] = {
    FraudScenario.CARD_TESTING: sum(CARD_TESTING_SIZE_RANGE) / 2,
    FraudScenario.ACCOUNT_TAKEOVER: sum(TAKEOVER_SIZE_RANGE) / 2,
    FraudScenario.MERCHANT_COLLUSION: sum(COLLUSION_SIZE_RANGE) / 2,
}


@dataclass(frozen=True, slots=True)
class PlannedEvent:
    """One transaction an attack will produce, at an absolute stream time.

    Attributes:
        at_seconds: Seconds since the start of the window.
        card_index: The card charged.
        device_index: The device used.
        merchant_index: The merchant charged at.
        amount_cents: The amount.
        entry_mode: How the card was presented.
        scenario: Which pattern produced it.
        session_token: Session identifier, or None when there is no session.
        attack_number: Which attack this belongs to, monotonic within a run.
        sequence: Position within that attack, from zero. With
            `attack_number` it gives the event a stable identifier that does
            not depend on how the attack interleaved with legitimate traffic.
    """

    at_seconds: float
    card_index: int
    device_index: int
    merchant_index: int
    amount_cents: int
    entry_mode: EntryMode
    scenario: FraudScenario
    session_token: str | None
    attack_number: int
    sequence: int


def mean_attack_size(regime: Regime) -> float:
    """Expected number of events per attack under a regime.

    The driver needs this to turn a target fraud share into an attack arrival
    rate, so the share stays where it was asked for as the scenario mix moves.

    Args:
        regime: The regime in force.

    Returns:
        The expected number of events one attack produces.
    """
    weights = regime.normalised_weights()
    expected = sum(
        weight * _MEAN_ATTACK_SIZE[scenario]
        for weight, scenario in zip(weights, ATTACK_SCENARIOS, strict=True)
    )
    return expected * regime.attack_intensity


def mean_attack_duration(regime: Regime) -> float:
    """Expected wall of stream time one attack spans, in seconds.

    The driver needs this to warm the stream up. At any instant a real stream
    carries attacks that started hours ago and are still running; a run that
    began with an empty heap would under-report fraud until the longest attack
    had had time to finish.

    Args:
        regime: The regime in force.

    Returns:
        The expected duration of one attack, in seconds.
    """
    weights = regime.normalised_weights()
    return sum(
        weight * _MEAN_ATTACK_SIZE[scenario] * regime.attack_intensity * _MEAN_GAP_SECONDS[scenario]
        for weight, scenario in zip(weights, ATTACK_SCENARIOS, strict=True)
    )


def plan_attack(
    scenario: FraudScenario,
    rng: np.random.Generator,
    graph: EntityGraph,
    regime: Regime,
    start_seconds: float,
    attack_number: int,
) -> list[PlannedEvent]:
    """Plan one attack of the given kind.

    Args:
        scenario: Which pattern to produce.
        rng: The generator's random source.
        graph: The entity graph.
        regime: The regime in force at the start of the attack.
        start_seconds: When the attack begins, seconds into the window.
        attack_number: Monotonic counter, used to build session tokens.

    Returns:
        The attack's events, in time order.

    Raises:
        ValueError: If asked to plan `FraudScenario.NONE`.
    """
    match scenario:
        case FraudScenario.CARD_TESTING:
            return _plan_card_testing(rng, graph, regime, start_seconds, attack_number)
        case FraudScenario.ACCOUNT_TAKEOVER:
            return _plan_takeover(rng, graph, regime, start_seconds, attack_number)
        case FraudScenario.MERCHANT_COLLUSION:
            return _plan_collusion(rng, graph, regime, start_seconds, attack_number)
        case FraudScenario.NONE:
            msg = "NONE is not an attack"
            raise ValueError(msg)


def _scaled_size(rng: np.random.Generator, bounds: tuple[int, int], intensity: float) -> int:
    """Draw an attack size and scale it by the regime's intensity.

    Args:
        rng: The generator's random source.
        bounds: Inclusive low and high bounds before scaling.
        intensity: The regime's attack intensity multiplier.

    Returns:
        At least two events, so every attack is a pattern rather than a point.
    """
    base = int(rng.integers(bounds[0], bounds[1] + 1))
    return max(2, int(round(base * intensity)))


def _pick_merchant_in(
    rng: np.random.Generator, graph: EntityGraph, categories: tuple[MerchantCategory, ...]
) -> int:
    """Pick a merchant from one of the given categories, by popularity.

    Args:
        rng: The generator's random source.
        graph: The entity graph.
        categories: The categories to choose among.

    Returns:
        A merchant index.
    """
    category = CATEGORIES.index(categories[int(rng.integers(0, len(categories)))])
    members = graph.category_merchants[category]
    cum = graph.category_merchant_cum[category]
    position = int(np.searchsorted(cum, rng.random(), side="right"))
    return int(members[min(position, len(members) - 1)])


def _plan_card_testing(
    rng: np.random.Generator,
    graph: EntityGraph,
    regime: Regime,
    start_seconds: float,
    attack_number: int,
) -> list[PlannedEvent]:
    """Plan a card-testing burst: one device, many cards, small amounts.

    Args:
        rng: The generator's random source.
        graph: The entity graph.
        regime: The regime in force.
        start_seconds: When the burst begins.
        attack_number: Monotonic attack counter.

    Returns:
        The burst's events, in time order.
    """
    size = _scaled_size(rng, CARD_TESTING_SIZE_RANGE, regime.attack_intensity)
    devices = rng.integers(
        0,
        graph.population.devices,
        size=int(rng.integers(CARD_TESTING_DEVICES[0], CARD_TESTING_DEVICES[1] + 1)),
    )
    merchants = [
        _pick_merchant_in(rng, graph, CARD_TESTING_CATEGORIES)
        for _ in range(int(rng.integers(CARD_TESTING_MERCHANTS[0], CARD_TESTING_MERCHANTS[1] + 1)))
    ]
    cards = rng.integers(0, graph.population.cards, size=size)
    gaps = rng.uniform(*CARD_TESTING_GAP_SECONDS, size=size)
    amounts = rng.integers(CARD_TESTING_AMOUNT_CENTS[0], CARD_TESTING_AMOUNT_CENTS[1], size=size)
    times = start_seconds + np.cumsum(gaps)
    return [
        PlannedEvent(
            at_seconds=float(times[i]),
            card_index=int(cards[i]),
            device_index=int(devices[int(rng.integers(0, devices.size))]),
            merchant_index=merchants[int(rng.integers(0, len(merchants)))],
            amount_cents=int(amounts[i]),
            entry_mode=EntryMode.ECOMMERCE,
            scenario=FraudScenario.CARD_TESTING,
            session_token=card_session(int(cards[i]), float(times[i])),
            attack_number=attack_number,
            sequence=i,
        )
        for i in range(size)
    ]


def _plan_takeover(
    rng: np.random.Generator,
    graph: EntityGraph,
    regime: Regime,
    start_seconds: float,
    attack_number: int,
) -> list[PlannedEvent]:
    """Plan an account takeover: one card, a device it has never used.

    Args:
        rng: The generator's random source.
        graph: The entity graph.
        regime: The regime in force.
        start_seconds: When the takeover begins.
        attack_number: Monotonic attack counter.

    Returns:
        The takeover's events, in time order.
    """
    size = _scaled_size(rng, TAKEOVER_SIZE_RANGE, regime.attack_intensity)
    card = int(rng.integers(0, graph.population.cards))
    owned = graph.devices_of(card)
    known = {int(d) for d in owned}
    if owned.size and rng.random() < TAKEOVER_KNOWN_DEVICE_SHARE:
        # Malware or a stolen phone: the takeover runs from the card's own device.
        device = int(owned[int(rng.integers(0, owned.size))])
    else:
        device = int(rng.integers(0, graph.population.devices))
        for _ in range(8):
            if device not in known:
                break
            device = int(rng.integers(0, graph.population.devices))
    profile = int(graph.card_profile[card])

    card_mu = float(graph.card_amount_mu[card])
    multipliers = rng.uniform(*TAKEOVER_AMOUNT_MULTIPLIER, size=size)
    gaps = rng.uniform(*TAKEOVER_GAP_SECONDS, size=size)
    times = start_seconds + np.cumsum(gaps)
    events: list[PlannedEvent] = []
    for i in range(size):
        amount = float(np.exp(card_mu)) * float(multipliers[i])
        if rng.random() < TAKEOVER_OWN_CATEGORY_SHARE:
            category = int(
                np.searchsorted(graph.profile_category_cum[profile], rng.random(), side="right")
            )
            merchant = _pick_merchant_in(
                rng, graph, (CATEGORIES[min(category, len(CATEGORIES) - 1)],)
            )
        else:
            merchant = _pick_merchant_in(rng, graph, TAKEOVER_CATEGORIES)
        events.append(
            PlannedEvent(
                at_seconds=float(times[i]),
                card_index=card,
                device_index=device,
                merchant_index=merchant,
                amount_cents=max(1, int(round(amount * 100))),
                entry_mode=EntryMode.ECOMMERCE,
                scenario=FraudScenario.ACCOUNT_TAKEOVER,
                session_token=card_session(card, float(times[i])),
                attack_number=attack_number,
                sequence=i,
            )
        )
    return events


def _plan_collusion(
    rng: np.random.Generator,
    graph: EntityGraph,
    regime: Regime,
    start_seconds: float,
    attack_number: int,
) -> list[PlannedEvent]:
    """Plan a collusion episode: one merchant, many cards, inflated amounts.

    Args:
        rng: The generator's random source.
        graph: The entity graph.
        regime: The regime in force.
        start_seconds: When the episode begins.
        attack_number: Monotonic attack counter.

    Returns:
        The episode's events, in time order. Empty if the graph has no
        colluding merchants, which is a valid population choice.
    """
    eligible = graph.colluding_merchants
    if eligible.size == 0:
        return []
    size = _scaled_size(rng, COLLUSION_SIZE_RANGE, regime.attack_intensity)
    merchant = int(eligible[int(rng.integers(0, eligible.size))])
    merchant_mu = float(graph.merchant_amount_mu[merchant])
    category = int(graph.merchant_category[merchant])
    fronts = graph.category_merchants[category]
    cards = rng.integers(0, graph.population.cards, size=size)
    multipliers = rng.uniform(*COLLUSION_AMOUNT_MULTIPLIER, size=size)
    gaps = rng.uniform(*COLLUSION_GAP_SECONDS, size=size)
    times = start_seconds + np.cumsum(gaps)
    events: list[PlannedEvent] = []
    for i in range(size):
        card = int(cards[i])
        devices = graph.devices_of(card)
        device = int(devices[int(rng.integers(0, devices.size))]) if devices.size else 0
        charged = (
            int(fronts[int(rng.integers(0, len(fronts)))])
            if rng.random() < COLLUSION_FRONT_SHARE
            else merchant
        )
        amount = float(np.exp(merchant_mu)) * float(multipliers[i])
        entry_mode = graph.entry_mode_for(
            category, is_online_device=bool(graph.device_is_mobile[device])
        )
        events.append(
            PlannedEvent(
                at_seconds=float(times[i]),
                card_index=card,
                device_index=device,
                merchant_index=charged,
                amount_cents=max(1, int(round(amount * 100))),
                entry_mode=entry_mode,
                scenario=FraudScenario.MERCHANT_COLLUSION,
                session_token=(
                    card_session(card, float(times[i]))
                    if entry_mode is EntryMode.ECOMMERCE
                    else None
                ),
                attack_number=attack_number,
                sequence=i,
            )
        )
    return events
