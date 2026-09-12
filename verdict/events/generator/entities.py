"""The entity graph: cards, devices and merchants, and the links between them.

Fraud is a property of a graph, not of a row. A card-testing burst is one
device touching hundreds of cards; an account takeover is a card appearing on
a device it has never used; collusion is one merchant sitting under an
implausible share of the chargebacks. None of that is visible in a single
transaction, which is why the entity graph is built first and the features are
built on top of it.

The graph is a pure function of its seed. The hot path holds integer indices
in numpy arrays and formats identifiers only when an event is constructed:
formatting a string per candidate entity, rather than per emitted event, was
the difference between hundreds and tens of thousands of events per second.

Population parameters are published, not tuned in secret. They are chosen to
put the synthetic stream in the same broad shape as the public references
recorded in `docs/data.md`: a few percent fraud, a long-tailed amount
distribution, and a small number of merchants taking most of the volume.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Final, Self

import numpy as np

from verdict.events.schema import EntryMode, MerchantCategory

CATEGORIES: Final[tuple[MerchantCategory, ...]] = tuple(MerchantCategory)
"""The category vocabulary, in a fixed order. Index into it is stable."""

N_CATEGORIES: Final = len(CATEGORIES)

SPEND_PROFILES: Final[tuple[str, ...]] = (
    "everyday",
    "commuter",
    "family",
    "online_heavy",
    "traveller",
    "high_spender",
)
"""Published cardholder spend profiles.

Each profile is a different weighting over merchant categories and a different
amount scale. They exist so that "unusual for this card" means something, which
is what an account-takeover scenario has to violate to be detectable at all.
"""

_PROFILE_CATEGORY_WEIGHTS: Final[dict[str, dict[MerchantCategory, float]]] = {
    "everyday": {
        MerchantCategory.GROCERY_POS: 6.0,
        MerchantCategory.FOOD_DINING: 3.0,
        MerchantCategory.PERSONAL_CARE: 2.0,
        MerchantCategory.MISC_POS: 2.0,
        MerchantCategory.HEALTH_FITNESS: 1.0,
        MerchantCategory.SHOPPING_POS: 2.0,
    },
    "commuter": {
        MerchantCategory.GAS_TRANSPORT: 7.0,
        MerchantCategory.FOOD_DINING: 3.0,
        MerchantCategory.GROCERY_POS: 2.0,
        MerchantCategory.MISC_POS: 1.0,
    },
    "family": {
        MerchantCategory.GROCERY_POS: 5.0,
        MerchantCategory.KIDS_PETS: 4.0,
        MerchantCategory.HOME: 2.0,
        MerchantCategory.HEALTH_FITNESS: 1.5,
        MerchantCategory.SHOPPING_POS: 2.0,
        MerchantCategory.ENTERTAINMENT: 1.5,
    },
    "online_heavy": {
        MerchantCategory.SHOPPING_NET: 6.0,
        MerchantCategory.GROCERY_NET: 3.0,
        MerchantCategory.MISC_NET: 3.0,
        MerchantCategory.ENTERTAINMENT: 2.0,
    },
    "traveller": {
        MerchantCategory.TRAVEL: 5.0,
        MerchantCategory.FOOD_DINING: 3.0,
        MerchantCategory.GAS_TRANSPORT: 2.0,
        MerchantCategory.SHOPPING_NET: 1.5,
        MerchantCategory.ENTERTAINMENT: 1.5,
    },
    "high_spender": {
        MerchantCategory.SHOPPING_POS: 4.0,
        MerchantCategory.TRAVEL: 3.0,
        MerchantCategory.ENTERTAINMENT: 2.0,
        MerchantCategory.FOOD_DINING: 2.0,
        MerchantCategory.HOME: 2.0,
    },
}

_PROFILE_AMOUNT_MU: Final[dict[str, float]] = {
    "everyday": 3.3,
    "commuter": 3.4,
    "family": 3.6,
    "online_heavy": 3.7,
    "traveller": 4.1,
    "high_spender": 4.5,
}
"""Mean of log-amount in cents-free units; amounts are lognormal per card."""

_PROFILE_WEIGHT: Final[tuple[float, ...]] = (0.30, 0.15, 0.20, 0.18, 0.10, 0.07)
"""How common each profile is. Sums to one."""

MOBILE_DEVICE_SHARE: Final = 0.72
"""Share of devices that are phones rather than point-of-sale terminals.

It decides whether an in-person transaction is a tap or a dip, which is the
only thing the entry mode needs from a device.
"""

_CATEGORY_IS_ONLINE: Final[frozenset[MerchantCategory]] = frozenset(
    {
        MerchantCategory.GROCERY_NET,
        MerchantCategory.MISC_NET,
        MerchantCategory.SHOPPING_NET,
        MerchantCategory.ENTERTAINMENT,
    }
)
"""Categories that are card-not-present. Drives the entry mode."""


@dataclass(frozen=True, slots=True)
class Population:
    """How many of each entity the graph holds.

    The defaults are sized so the graph fits comfortably in memory on the
    build laptop while still giving every card a history worth aggregating at
    the live rate: at 1,000 events per second a 200,000-card population sees
    each card about twice a week.

    Attributes:
        cards: Number of cards.
        devices: Number of devices. Fewer than cards, so sharing exists.
        merchants: Number of merchants.
        shared_device_rate: Share of devices that serve more than one card.
        max_devices_per_card: Upper bound on a card's device list.
        colluding_merchant_rate: Share of merchants eligible for the
            merchant-collusion scenario.
    """

    cards: int = 200_000
    devices: int = 150_000
    merchants: int = 4_000
    shared_device_rate: float = 0.12
    max_devices_per_card: int = 3
    colluding_merchant_rate: float = 0.004

    def __post_init__(self) -> None:
        """Reject a population that cannot produce a usable graph.

        Raises:
            ValueError: If any count is non-positive or a rate is not a
                proportion.
        """
        if min(self.cards, self.devices, self.merchants) < 1:
            msg = "population counts must be positive"
            raise ValueError(msg)
        if self.merchants < N_CATEGORIES:
            msg = f"need at least {N_CATEGORIES} merchants, one per category"
            raise ValueError(msg)
        for name, rate in (
            ("shared_device_rate", self.shared_device_rate),
            ("colluding_merchant_rate", self.colluding_merchant_rate),
        ):
            if not 0.0 <= rate <= 1.0:
                msg = f"{name} must be a proportion, got {rate}"
                raise ValueError(msg)
        if self.max_devices_per_card < 1:
            msg = "max_devices_per_card must be at least 1"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class Card:
    """A readable view of one card. Not used on the hot path."""

    index: int
    card_id: str
    profile: str
    amount_mu: float
    country: str
    device_indices: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Device:
    """A readable view of one device. Not used on the hot path."""

    index: int
    device_id: str
    card_count: int


@dataclass(frozen=True, slots=True)
class Merchant:
    """A readable view of one merchant. Not used on the hot path."""

    index: int
    merchant_id: str
    category: MerchantCategory
    country: str
    amount_mu: float
    is_colluding: bool


def card_id(index: int) -> str:
    """Format a card identifier.

    Args:
        index: The card's index in the graph.

    Returns:
        The wire identifier.
    """
    return f"card-{index:08d}"


def device_id(index: int) -> str:
    """Format a device identifier.

    Args:
        index: The device's index in the graph.

    Returns:
        The wire identifier.
    """
    return f"dev-{index:08d}"


def merchant_id(index: int) -> str:
    """Format a merchant identifier.

    Args:
        index: The merchant's index in the graph.

    Returns:
        The wire identifier.
    """
    return f"mer-{index:06d}"


@dataclass(frozen=True, slots=True)
class EntityGraph:
    """Cards, devices and merchants, with the links between them.

    The arrays are the hot path; the `card`, `device` and `merchant` methods
    are the readable view used by tests and reports. Everything here is
    derived from `seed` and `population`, so two builds with the same seed are
    byte-identical, which `fingerprint` asserts cheaply.
    """

    seed: int
    population: Population
    card_profile: np.ndarray
    """int8 index into `SPEND_PROFILES`, per card."""
    card_amount_mu: np.ndarray
    """float64 mean of log-amount, per card."""
    card_activity_cum: np.ndarray
    """float64 cumulative activity weight, per card. Normalised to 1."""
    card_device_offsets: np.ndarray
    """int64 CSR offsets into `card_device_indices`, length cards + 1."""
    card_device_indices: np.ndarray
    """int32 device indices, grouped by card."""
    device_card_count: np.ndarray
    """int32 number of cards that legitimately use each device."""
    device_is_mobile: np.ndarray
    """bool, per device. A phone taps; a point-of-sale terminal is dipped."""
    merchant_category: np.ndarray
    """int8 index into `CATEGORIES`, per merchant."""
    merchant_amount_mu: np.ndarray
    """float64 mean of log-amount, per merchant."""
    merchant_is_colluding: np.ndarray
    """bool, per merchant. Eligible for the collusion scenario."""
    category_merchants: tuple[np.ndarray, ...]
    """Per category, the int32 merchant indices in that category."""
    category_merchant_cum: tuple[np.ndarray, ...]
    """Per category, the float64 cumulative popularity of those merchants."""
    profile_category_cum: np.ndarray
    """float64 (profiles, categories) cumulative category weights per profile."""

    @classmethod
    def build(cls, seed: int, population: Population | None = None) -> Self:
        """Build the graph.

        Args:
            seed: The graph seed. The same seed gives the same graph.
            population: How many of each entity. Defaults to `Population()`.

        Returns:
            The built graph.
        """
        pop = population or Population()
        rng = np.random.default_rng(seed)

        profile_cum = _profile_category_cum()
        card_profile = _sample_profiles(rng, pop.cards)
        card_amount_mu = np.array([_PROFILE_AMOUNT_MU[p] for p in SPEND_PROFILES])[card_profile]
        card_amount_mu = card_amount_mu + rng.normal(0.0, 0.25, size=pop.cards)

        # Activity is long-tailed: a minority of cards make most of the
        # transactions, as in every published transaction dataset.
        activity = rng.lognormal(mean=0.0, sigma=0.8, size=pop.cards)
        card_activity_cum = np.cumsum(activity)
        card_activity_cum /= card_activity_cum[-1]

        offsets, device_indices, device_card_count = _link_devices(rng, pop)
        device_is_mobile = rng.random(pop.devices) < MOBILE_DEVICE_SHARE

        merchant_category = _sample_merchant_categories(rng, pop.merchants)
        merchant_amount_mu = rng.normal(3.6, 0.45, size=pop.merchants)
        colluding = _choose_colluding(rng, pop)
        category_merchants, category_cum = _index_merchants_by_category(
            rng, merchant_category, pop.merchants
        )

        return cls(
            seed=seed,
            population=pop,
            card_profile=card_profile,
            card_amount_mu=card_amount_mu,
            card_activity_cum=card_activity_cum,
            card_device_offsets=offsets,
            card_device_indices=device_indices,
            device_card_count=device_card_count,
            device_is_mobile=device_is_mobile,
            merchant_category=merchant_category,
            merchant_amount_mu=merchant_amount_mu,
            merchant_is_colluding=colluding,
            category_merchants=category_merchants,
            category_merchant_cum=category_cum,
            profile_category_cum=profile_cum,
        )

    def card(self, index: int) -> Card:
        """Return the readable view of one card.

        Args:
            index: The card's index.

        Returns:
            The card.
        """
        start = int(self.card_device_offsets[index])
        stop = int(self.card_device_offsets[index + 1])
        return Card(
            index=index,
            card_id=card_id(index),
            profile=SPEND_PROFILES[int(self.card_profile[index])],
            amount_mu=float(self.card_amount_mu[index]),
            country="US",
            device_indices=tuple(int(d) for d in self.card_device_indices[start:stop]),
        )

    def device(self, index: int) -> Device:
        """Return the readable view of one device.

        Args:
            index: The device's index.

        Returns:
            The device.
        """
        return Device(
            index=index,
            device_id=device_id(index),
            card_count=int(self.device_card_count[index]),
        )

    def merchant(self, index: int) -> Merchant:
        """Return the readable view of one merchant.

        Args:
            index: The merchant's index.

        Returns:
            The merchant.
        """
        return Merchant(
            index=index,
            merchant_id=merchant_id(index),
            category=CATEGORIES[int(self.merchant_category[index])],
            country="US",
            amount_mu=float(self.merchant_amount_mu[index]),
            is_colluding=bool(self.merchant_is_colluding[index]),
        )

    @property
    def colluding_merchants(self) -> np.ndarray:
        """Indices of merchants eligible for the collusion scenario.

        Returns:
            An int64 array of merchant indices.
        """
        return np.flatnonzero(self.merchant_is_colluding)

    def devices_of(self, card_index: int) -> np.ndarray:
        """Return the devices a card legitimately uses.

        Args:
            card_index: The card's index.

        Returns:
            An int32 array of device indices.
        """
        start = int(self.card_device_offsets[card_index])
        stop = int(self.card_device_offsets[card_index + 1])
        return self.card_device_indices[start:stop]

    def fingerprint(self) -> str:
        """Hash the graph, so a determinism test is one comparison.

        Returns:
            A hex sha256 over every array that defines the graph.
        """
        digest = hashlib.sha256()
        digest.update(str(self.seed).encode("utf-8"))
        digest.update(repr(self.population).encode("utf-8"))
        for array in (
            self.card_profile,
            np.round(self.card_amount_mu, 9),
            np.round(self.card_activity_cum, 9),
            self.card_device_offsets,
            self.card_device_indices,
            self.device_card_count,
            self.device_is_mobile,
            self.merchant_category,
            np.round(self.merchant_amount_mu, 9),
            self.merchant_is_colluding,
        ):
            digest.update(np.ascontiguousarray(array).tobytes())
        return digest.hexdigest()

    def entry_mode_for(self, category_index: int, *, is_online_device: bool) -> EntryMode:
        """Choose the entry mode implied by a category.

        Args:
            category_index: Index into `CATEGORIES`.
            is_online_device: Whether the device is a phone or computer rather
                than a point-of-sale terminal.

        Returns:
            The entry mode.
        """
        if CATEGORIES[category_index] in _CATEGORY_IS_ONLINE:
            return EntryMode.ECOMMERCE
        return EntryMode.CONTACTLESS if is_online_device else EntryMode.CHIP


def _profile_category_cum() -> np.ndarray:
    """Build cumulative category weights per spend profile.

    Returns:
        A float64 (profiles, categories) array, each row ending at 1.
    """
    matrix = np.zeros((len(SPEND_PROFILES), N_CATEGORIES), dtype=np.float64)
    for row, profile in enumerate(SPEND_PROFILES):
        weights = _PROFILE_CATEGORY_WEIGHTS[profile]
        for category, weight in weights.items():
            matrix[row, CATEGORIES.index(category)] = weight
        # Every category keeps a small floor so no purchase is impossible,
        # only unlikely. A zero would make "unusual for this card" infinite.
        matrix[row] += 0.05
        matrix[row] /= matrix[row].sum()
    return np.cumsum(matrix, axis=1)


def _sample_profiles(rng: np.random.Generator, n: int) -> np.ndarray:
    """Assign a spend profile to every card.

    Args:
        rng: The graph's random generator.
        n: Number of cards.

    Returns:
        An int8 array of profile indices.
    """
    cum = np.cumsum(np.array(_PROFILE_WEIGHT, dtype=np.float64))
    cum /= cum[-1]
    draws = rng.random(n)
    return np.searchsorted(cum, draws, side="right").astype(np.int8)


def _link_devices(
    rng: np.random.Generator, pop: Population
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Link cards to devices, leaving a measurable share of devices shared.

    Args:
        rng: The graph's random generator.
        pop: The population spec.

    Returns:
        The CSR offsets, the device indices grouped by card, and the count of
        cards per device.
    """
    counts = rng.integers(1, pop.max_devices_per_card + 1, size=pop.cards, dtype=np.int64)
    offsets = np.zeros(pop.cards + 1, dtype=np.int64)
    np.cumsum(counts, out=offsets[1:])
    total = int(offsets[-1])

    shared_pool_size = max(1, int(pop.devices * pop.shared_device_rate))
    shared_pool = rng.choice(pop.devices, size=shared_pool_size, replace=False)
    # A link lands in the shared pool with probability `shared_device_rate`,
    # so shared devices pick up several cards each while the rest stay
    # single-card. Shared-device count is then a real signal, not noise.
    use_shared = rng.random(total) < pop.shared_device_rate
    device_indices = np.where(
        use_shared,
        shared_pool[rng.integers(0, shared_pool_size, size=total)],
        rng.integers(0, pop.devices, size=total),
    ).astype(np.int32)

    device_card_count = np.bincount(device_indices, minlength=pop.devices).astype(np.int32)
    return offsets, device_indices, device_card_count


def _choose_colluding(rng: np.random.Generator, pop: Population) -> np.ndarray:
    """Choose the merchants eligible for the collusion scenario.

    The count is fixed rather than drawn per merchant. A Bernoulli draw at
    this rate can return none at all on a small population, which would
    silently switch off a whole fraud pattern and leave the scenario mix
    quietly wrong: the generator would still report the mix it was asked for
    while producing something else.

    Args:
        rng: The graph's random generator.
        pop: The population spec.

    Returns:
        A bool array, one entry per merchant.
    """
    colluding = np.zeros(pop.merchants, dtype=bool)
    if pop.colluding_merchant_rate <= 0.0:
        return colluding
    count = max(1, int(round(pop.merchants * pop.colluding_merchant_rate)))
    colluding[rng.choice(pop.merchants, size=count, replace=False)] = True
    return colluding


def _sample_merchant_categories(rng: np.random.Generator, n: int) -> np.ndarray:
    """Assign a category to every merchant, with at least one per category.

    Args:
        rng: The graph's random generator.
        n: Number of merchants.

    Returns:
        An int8 array of category indices.
    """
    categories = rng.integers(0, N_CATEGORIES, size=n).astype(np.int8)
    categories[:N_CATEGORIES] = np.arange(N_CATEGORIES, dtype=np.int8)
    return categories


def _index_merchants_by_category(
    rng: np.random.Generator, merchant_category: np.ndarray, n_merchants: int
) -> tuple[tuple[np.ndarray, ...], tuple[np.ndarray, ...]]:
    """Group merchants by category and give each group a popularity curve.

    Args:
        rng: The graph's random generator.
        merchant_category: Category index per merchant.
        n_merchants: Number of merchants.

    Returns:
        Per category, the merchant indices and their cumulative popularity.
    """
    popularity = rng.lognormal(mean=0.0, sigma=1.1, size=n_merchants)
    indices: list[np.ndarray] = []
    cumulative: list[np.ndarray] = []
    for category in range(N_CATEGORIES):
        members = np.flatnonzero(merchant_category == category).astype(np.int32)
        weights = popularity[members]
        cum = np.cumsum(weights)
        cum /= cum[-1]
        indices.append(members)
        cumulative.append(cum)
    return tuple(indices), tuple(cumulative)
