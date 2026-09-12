"""The generator driver: legitimate traffic and attacks, merged in time order.

The driver holds three things together:

- a Poisson stream of legitimate transactions whose shape comes from the
  entity graph and the regime in force;
- a Poisson stream of attacks, each planned whole by `scenarios` and merged
  into the legitimate stream by a heap, so an attack overlaps normal traffic;
- a label for every event, timestamped seven days later, because a platform
  that trains on labels it would not yet have had is the thing this project
  exists to prevent.

Two properties matter more than realism here.

**Determinism.** The same seed produces the same events, byte for byte. A
replay is only a replay if it is the same stream, and the parity test between
the Redpanda and Kinesis paths compares two runs of this generator.

**Time order.** Events are emitted in non-decreasing event time. Out-of-order
delivery is a property of the transport, injected deliberately in the chaos
tests, not an accident of the generator.

Randomness is drawn in blocks. Drawing one number at a time from numpy costs
more than everything else in the loop put together; blocks are what take this
from roughly two thousand events per second to tens of thousands.
"""

from __future__ import annotations

import datetime as dt
import heapq
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Final

import numpy as np

from verdict.events.generator.entities import (
    CATEGORIES,
    EntityGraph,
    Population,
    card_id,
    device_id,
    merchant_id,
)
from verdict.events.generator.regimes import (
    ATTACK_SCENARIOS,
    DEV_SCHEDULE,
    Regime,
    RegimeSchedule,
)
from verdict.events.generator.scenarios import (
    PlannedEvent,
    mean_attack_duration,
    mean_attack_size,
    plan_attack,
)
from verdict.events.schema import (
    EntryMode,
    FraudScenario,
    GroundTruth,
    LabelEvent,
    MerchantCategory,
    TransactionEvent,
    require_utc,
)

_BLOCK: Final = 8192
"""How many random draws to take at a time on the legitimate path."""

_ONLINE_CATEGORY_INDICES: Final[tuple[int, ...]] = tuple(
    CATEGORIES.index(category)
    for category in (
        MerchantCategory.GROCERY_NET,
        MerchantCategory.MISC_NET,
        MerchantCategory.SHOPPING_NET,
        MerchantCategory.ENTERTAINMENT,
    )
)

_OFFLINE_CATEGORY_INDICES: Final[tuple[int, ...]] = tuple(
    index for index in range(len(CATEGORIES)) if index not in _ONLINE_CATEGORY_INDICES
)

_SESSION_SECONDS: Final = 1800.0
"""A card's online activity is bucketed into half-hour sessions."""

_RECURRING_SHARE: Final = 0.035
"""Share of legitimate transactions that are recurring charges."""

DEFAULT_START: Final = dt.datetime(2027, 1, 1, tzinfo=dt.UTC)
"""Neutral default start time. The live window sets its own."""

WARM_UP_DURATIONS: Final = 3.0
"""How many mean attack durations to warm the stream up over.

A stream that starts with an empty heap has no attack already in flight, so
its first hours carry less fraud than its steady state. The driver plans
attacks that began before the window opened and keeps only the events that
land inside it. Three mean durations covers the overwhelming majority of
attacks; the few longer ones are the reason a run's first minutes are still
slightly light, which `docs/generator.md` records rather than hides.
"""


@dataclass(frozen=True, slots=True)
class GeneratorConfig:
    """Everything the generator needs, and nothing it does not.

    Attributes:
        seed: The one seed. The graph and the event stream take separate
            substreams from it, so changing the rate does not change the graph.
        population: Size of the entity graph.
        events_per_second: Nominal rate of legitimate transactions. Attacks
            add to this.
        start_time: Event time of the start of the window, timezone-aware UTC.
        schedule: The regime schedule in force.
        target_fraud_share: Share of events the attack stream aims to produce
            under the opening regime. Later regimes multiply it.
        label_delay_days: How long a label takes to arrive.
        amount_sigma: Spread of log-amount around a card and merchant's means.
        warm_up: Whether to plan the attacks already in flight when the window
            opens. On by default, because a run without it under-reports
            fraud at the start. Turn it off only to measure the emit loop
            itself.
    """

    seed: int = 20270201
    population: Population = field(default_factory=Population)
    events_per_second: float = 1000.0
    start_time: dt.datetime = DEFAULT_START
    schedule: RegimeSchedule = DEV_SCHEDULE
    target_fraud_share: float = 0.03
    label_delay_days: float = 7.0
    amount_sigma: float = 0.75
    warm_up: bool = True

    def __post_init__(self) -> None:
        """Check the configuration is usable.

        Raises:
            ValueError: If the rate is non-positive, the fraud share is not a
                proportion below one, or the start time is not UTC.
        """
        if self.events_per_second <= 0:
            msg = "events_per_second must be positive"
            raise ValueError(msg)
        if not 0.0 <= self.target_fraud_share < 1.0:
            msg = f"target_fraud_share must be in [0, 1), got {self.target_fraud_share}"
            raise ValueError(msg)
        if self.label_delay_days < 0:
            msg = "label_delay_days cannot be negative"
            raise ValueError(msg)
        require_utc(self.start_time)


@dataclass(frozen=True, slots=True)
class GeneratedRecord:
    """One generated event with everything the generator knows about it.

    The three parts go to three different sinks. Only `event` reaches the
    transaction topic; `truth` never leaves the generator's own log, and
    `label` is held back until its own timestamp.

    Attributes:
        event: The transaction, as a consumer sees it.
        truth: What the generator knows. Never a feature, never an input.
        label: The outcome, timestamped in the future.
    """

    event: TransactionEvent
    truth: GroundTruth
    label: LabelEvent


class Generator:
    """The synthetic transaction generator.

    Build it once and call `stream`. Two generators with the same config
    produce the same records; `verdict generate` writes them to the raw log
    and the throughput test measures how fast this loop runs.
    """

    def __init__(
        self, config: GeneratorConfig | None = None, graph: EntityGraph | None = None
    ) -> None:
        """Build the generator.

        Args:
            config: The configuration. Defaults to `GeneratorConfig()`.
            graph: A pre-built entity graph, to avoid rebuilding it between
                runs. It must have been built with the config's seed and
                population; if it was not, the run is still deterministic but
                is no longer reproducible from the config alone.
        """
        self.config = config or GeneratorConfig()
        self.graph = graph or EntityGraph.build(self.config.seed, self.config.population)
        self._label_delay = dt.timedelta(days=self.config.label_delay_days)

    def stream(self, limit: int | None = None) -> Iterator[GeneratedRecord]:
        """Yield records in non-decreasing event time.

        Args:
            limit: Stop after this many records. `None` runs forever, which is
                what the live producer wants.

        Yields:
            One record per transaction, legitimate or fraudulent.
        """
        rng = np.random.default_rng([self.config.seed, 0xE7E4])
        draws = _Draws(rng, self.config)

        elapsed = 0.0
        emitted = 0
        counter = 0
        attack_number = 0
        pending: list[tuple[float, int, PlannedEvent]] = []
        regime = self.config.schedule.at(0.0)
        if self.config.warm_up:
            counter, attack_number = self._warm_up(rng, draws, pending, regime)
        next_attack_at = elapsed + draws.attack_gap(self._attack_rate(regime))

        while limit is None or emitted < limit:
            next_legit_at = elapsed + draws.inter_arrival()

            while next_attack_at <= next_legit_at:
                attack_regime = self.config.schedule.at(next_attack_at / 86_400.0)
                attack_number += 1
                for planned in plan_attack(
                    _choose_scenario(attack_regime, draws.unit()),
                    rng,
                    self.graph,
                    attack_regime,
                    next_attack_at,
                    attack_number,
                ):
                    counter += 1
                    heapq.heappush(pending, (planned.at_seconds, counter, planned))
                next_attack_at += draws.attack_gap(self._attack_rate(attack_regime))

            while pending and pending[0][0] <= next_legit_at:
                at_seconds, _, planned = heapq.heappop(pending)
                emitted += 1
                yield self._record_from_planned(planned, at_seconds, draws)
                if limit is not None and emitted >= limit:
                    return

            elapsed = next_legit_at
            regime = self.config.schedule.at(elapsed / 86_400.0)
            emitted += 1
            counter += 1
            yield self._legitimate_record(elapsed, regime, draws, counter)

    def _warm_up(
        self,
        rng: np.random.Generator,
        draws: _Draws,
        pending: list[tuple[float, int, PlannedEvent]],
        regime: Regime,
    ) -> tuple[int, int]:
        """Plan the attacks that were already running when the window opened.

        Attacks are planned back to `WARM_UP_DURATIONS` mean durations before
        time zero; only the events that fall inside the window are kept. The
        cost is paid once, at start-up, and is a fraction of a second even at
        the live rate.

        Args:
            rng: The event stream's random generator.
            draws: The block random source.
            pending: The heap to push surviving events onto.
            regime: The opening regime.

        Returns:
            The counter and attack number to continue from.
        """
        rate = self._attack_rate(regime)
        if rate <= 0.0:
            return 0, 0
        horizon = WARM_UP_DURATIONS * mean_attack_duration(regime)
        at = -horizon
        counter = 0
        attack_number = 0
        while at < 0.0:
            attack_number += 1
            for planned in plan_attack(
                _choose_scenario(regime, draws.unit()), rng, self.graph, regime, at, attack_number
            ):
                if planned.at_seconds >= 0.0:
                    counter += 1
                    heapq.heappush(pending, (planned.at_seconds, counter, planned))
            at += draws.attack_gap(rate)
        return counter, attack_number

    def _attack_rate(self, regime: Regime) -> float:
        """Attacks per second under a regime.

        Derived from the target fraud share rather than set directly, so that
        changing the scenario mix or the attack intensity does not silently
        change how much fraud there is.

        Args:
            regime: The regime in force.

        Returns:
            The Poisson rate of attack arrivals, per second.
        """
        share = self.config.target_fraud_share * regime.fraud_rate_multiplier
        if share <= 0.0:
            return 0.0
        fraud_events_per_second = self.config.events_per_second * share / max(1e-9, 1.0 - share)
        return fraud_events_per_second / mean_attack_size(regime)

    def _legitimate_record(
        self, elapsed: float, regime: Regime, draws: _Draws, counter: int
    ) -> GeneratedRecord:
        """Build one legitimate transaction.

        Args:
            elapsed: Seconds since the start of the window.
            regime: The regime in force.
            draws: The block random source.
            counter: Monotonic event counter, used for the event id.

        Returns:
            The record.
        """
        graph = self.graph
        card = int(np.searchsorted(graph.card_activity_cum, draws.unit(), side="right"))
        card = min(card, graph.population.cards - 1)

        profile = int(graph.card_profile[card])
        category = int(
            np.searchsorted(graph.profile_category_cum[profile], draws.unit(), side="right")
        )
        category = min(category, len(CATEGORIES) - 1)
        category = _apply_online_shift(category, regime.online_share_shift, draws.unit())

        members = graph.category_merchants[category]
        position = int(
            np.searchsorted(graph.category_merchant_cum[category], draws.unit(), side="right")
        )
        merchant = int(members[min(position, len(members) - 1)])

        devices = graph.devices_of(card)
        device = int(devices[int(draws.unit() * devices.size)]) if devices.size else 0

        log_amount = (
            0.5 * float(graph.card_amount_mu[card])
            + 0.5 * float(graph.merchant_amount_mu[merchant])
            + regime.amount_log_shift
            + self.config.amount_sigma * draws.normal()
        )
        amount_cents = max(1, int(round(float(np.exp(log_amount)) * 100)))

        entry_mode = graph.entry_mode_for(
            category, is_online_device=bool(graph.device_is_mobile[device])
        )
        session = (
            f"ses-{card:08d}-{int(elapsed // _SESSION_SECONDS)}"
            if entry_mode is EntryMode.ECOMMERCE
            else None
        )

        event = TransactionEvent(
            event_id=self._event_id(counter),
            event_time=self._event_time(elapsed),
            card_id=card_id(card),
            device_id=device_id(device),
            merchant_id=merchant_id(merchant),
            session_id=session,
            amount_cents=amount_cents,
            merchant_category=CATEGORIES[category],
            entry_mode=entry_mode,
            is_recurring=draws.unit() < _RECURRING_SHARE,
        )
        return self._assemble(event, FraudScenario.NONE, regime, draws)

    def _record_from_planned(
        self, planned: PlannedEvent, at_seconds: float, draws: _Draws
    ) -> GeneratedRecord:
        """Turn one planned attack event into a record.

        Args:
            planned: The planned event.
            at_seconds: Its time, seconds since the start of the window.
            draws: The block random source.

        Returns:
            The record.
        """
        regime = self.config.schedule.at(at_seconds / 86_400.0)
        event = TransactionEvent(
            event_id=self._attack_event_id(planned),
            event_time=self._event_time(at_seconds),
            card_id=card_id(planned.card_index),
            device_id=device_id(planned.device_index),
            merchant_id=merchant_id(planned.merchant_index),
            session_id=planned.session_token,
            amount_cents=planned.amount_cents,
            merchant_category=CATEGORIES[int(self.graph.merchant_category[planned.merchant_index])],
            entry_mode=planned.entry_mode,
            is_recurring=False,
        )
        return self._assemble(event, planned.scenario, regime, draws)

    def _assemble(
        self,
        event: TransactionEvent,
        scenario: FraudScenario,
        regime: Regime,
        draws: _Draws,
    ) -> GeneratedRecord:
        """Attach ground truth and a delayed label to an event.

        Args:
            event: The transaction.
            scenario: Which pattern produced it, or `NONE`.
            regime: The regime in force.
            draws: The block random source.

        Returns:
            The assembled record.
        """
        is_fraud = scenario is not FraudScenario.NONE
        recovered = int(event.amount_cents * draws.unit() * 0.4) if is_fraud else 0
        return GeneratedRecord(
            event=event,
            truth=GroundTruth(
                event_id=event.event_id,
                is_fraud=is_fraud,
                scenario=scenario,
                regime=regime.name,
            ),
            label=LabelEvent(
                event_id=event.event_id,
                label_time=event.event_time + self._label_delay,
                is_fraud=is_fraud,
                recovered_cents=recovered,
            ),
        )

    def _event_id(self, counter: int) -> str:
        """Build a deterministic event id for a legitimate transaction.

        Args:
            counter: Monotonic counter within this run.

        Returns:
            An identifier unique within the run and stable across runs.
        """
        return f"evt-{self.config.seed:08x}-L{counter:012d}"

    def _attack_event_id(self, planned: PlannedEvent) -> str:
        """Build a deterministic event id for an attack transaction.

        Keyed on the attack and the position within it, so the identifier
        does not depend on how the attack interleaved with legitimate
        traffic. Two runs of the same seed therefore agree event by event,
        which is what the replay and parity tests compare.

        Args:
            planned: The planned event.

        Returns:
            An identifier unique within the run and stable across runs.
        """
        return f"evt-{self.config.seed:08x}-A{planned.attack_number:09d}-{planned.sequence:05d}"

    def _event_time(self, elapsed: float) -> dt.datetime:
        """Convert seconds since the start of the window to an event time.

        Args:
            elapsed: Seconds since the start of the window.

        Returns:
            A timezone-aware UTC timestamp.
        """
        return self.config.start_time + dt.timedelta(seconds=elapsed)


def _choose_scenario(regime: Regime, draw: float) -> FraudScenario:
    """Pick a scenario according to the regime's weights.

    Args:
        regime: The regime in force.
        draw: A uniform draw on [0, 1).

    Returns:
        The chosen scenario.
    """
    cumulative = 0.0
    weights = regime.normalised_weights()
    for weight, scenario in zip(weights, ATTACK_SCENARIOS, strict=True):
        cumulative += weight
        if draw < cumulative:
            return scenario
    return ATTACK_SCENARIOS[-1]


def _apply_online_shift(category: int, shift: float, draw: float) -> int:
    """Move a category across the card-present boundary, as the regime asks.

    A positive shift moves some in-person transactions online; a negative one
    does the reverse. This is the categorical drift a regime can impose
    without touching fraud at all.

    Args:
        category: The category index the card's profile chose.
        shift: The regime's `online_share_shift`.
        draw: A uniform draw on [0, 1).

    Returns:
        The category index to use.
    """
    if shift == 0.0:
        return category
    is_online = category in _ONLINE_CATEGORY_INDICES
    if shift > 0.0 and not is_online and draw < shift:
        return _ONLINE_CATEGORY_INDICES[int(draw / shift * len(_ONLINE_CATEGORY_INDICES)) % 4]
    if shift < 0.0 and is_online and draw < -shift:
        index = int(draw / -shift * len(_OFFLINE_CATEGORY_INDICES))
        return _OFFLINE_CATEGORY_INDICES[index % len(_OFFLINE_CATEGORY_INDICES)]
    return category


class _Draws:
    """Block random draws.

    Calling `rng.random()` once per value dominates the generator's cost. This
    draws `_BLOCK` values at a time and hands them out, which is the single
    largest throughput decision in the generator.
    """

    def __init__(self, rng: np.random.Generator, config: GeneratorConfig) -> None:
        """Prepare the blocks.

        Args:
            rng: The event stream's random generator.
            config: The generator configuration.
        """
        self._rng = rng
        self._mean_gap = 1.0 / config.events_per_second
        self._units = rng.random(_BLOCK)
        self._unit_at = 0
        self._normals = rng.standard_normal(_BLOCK)
        self._normal_at = 0
        self._gaps = rng.exponential(self._mean_gap, _BLOCK)
        self._gap_at = 0
        self._exponentials = rng.exponential(1.0, _BLOCK)
        self._exponential_at = 0

    def unit(self) -> float:
        """Return the next uniform draw on [0, 1).

        Returns:
            The draw.
        """
        if self._unit_at >= _BLOCK:
            self._units = self._rng.random(_BLOCK)
            self._unit_at = 0
        value = float(self._units[self._unit_at])
        self._unit_at += 1
        return value

    def normal(self) -> float:
        """Return the next standard normal draw.

        Returns:
            The draw.
        """
        if self._normal_at >= _BLOCK:
            self._normals = self._rng.standard_normal(_BLOCK)
            self._normal_at = 0
        value = float(self._normals[self._normal_at])
        self._normal_at += 1
        return value

    def inter_arrival(self) -> float:
        """Return the next gap between legitimate transactions, in seconds.

        Returns:
            The gap.
        """
        if self._gap_at >= _BLOCK:
            self._gaps = self._rng.exponential(self._mean_gap, _BLOCK)
            self._gap_at = 0
        value = float(self._gaps[self._gap_at])
        self._gap_at += 1
        return value

    def attack_gap(self, rate_per_second: float) -> float:
        """Return the next gap between attacks, in seconds.

        Args:
            rate_per_second: The Poisson rate of attack arrivals.

        Returns:
            The gap, or a very large number if the rate is zero, which parks
            the next attack beyond any run.
        """
        if rate_per_second <= 0.0:
            return 1e18
        if self._exponential_at >= _BLOCK:
            self._exponentials = self._rng.exponential(1.0, _BLOCK)
            self._exponential_at = 0
        value = float(self._exponentials[self._exponential_at])
        self._exponential_at += 1
        return value / rate_per_second
