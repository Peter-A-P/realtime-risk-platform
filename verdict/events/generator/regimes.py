"""The regime schedule: how the world changes under the model's feet.

A drift monitor that was tuned against the shifts it is later graded on
proves nothing. So this module separates three things that are usually
confused:

- **The design is public.** The kinds of shift, and the range each parameter
  may take, are the constants below. Anyone can read them, and the drift
  monitors are built against them.
- **The development realisation is public.** `DEV_SCHEDULE` is a fixed,
  readable schedule. Every test and every week of the build uses it.
- **The live realisation is sealed.** The schedule that runs during the live
  window is derived from a secret the repository does not contain. Before
  go-live the repository commits three hashes: of the secret, of the derived
  schedule, and of this file. On Jul 1 2027 the secret is published, anyone
  re-derives the schedule, and the committed hashes prove it was fixed in
  advance and never edited.

That is what "sealed" has to mean for the drift numbers to be worth reading.
Committing the schedule itself in the clear would have sealed nothing, because
the monitors would have been written by someone who had read it.

This file is frozen from the moment it is sealed until Jul 1 2027. Its own
hash is one of the three commitments, so an edit is detectable rather than
merely discouraged.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final, Self

import numpy as np

from verdict.events.schema import FraudScenario, Record, require_utc

ATTACK_SCENARIOS: Final[tuple[FraudScenario, ...]] = (
    FraudScenario.CARD_TESTING,
    FraudScenario.ACCOUNT_TAKEOVER,
    FraudScenario.MERCHANT_COLLUSION,
)
"""The scenarios a regime can weight. `FraudScenario.NONE` is not one."""


# --- The published design ---------------------------------------------------
#
# A regime draw is uniform within each range below. Widening a range changes
# the hash of this file, so it cannot be done quietly after sealing.

FRAUD_RATE_MULTIPLIER_RANGE: Final[tuple[float, float]] = (0.4, 3.0)
"""How much more or less fraud than the baseline rate this regime carries."""

AMOUNT_LOG_SHIFT_RANGE: Final[tuple[float, float]] = (-0.35, 0.35)
"""Additive shift to the mean of log-amount for legitimate transactions.

This is feature drift with no label change: the amount distribution moves and
nothing about fraud does. A monitor that fires on it and a retraining that
follows are both wrong, and the sealed schedule is what makes that case
appear without anyone arranging it.
"""

ONLINE_SHARE_SHIFT_RANGE: Final[tuple[float, float]] = (-0.10, 0.25)
"""Additive shift to the share of card-not-present transactions."""

ATTACK_INTENSITY_RANGE: Final[tuple[float, float]] = (0.6, 2.5)
"""Multiplier on the number of events an individual attack produces."""

SCENARIO_WEIGHT_RANGE: Final[tuple[float, float]] = (0.05, 1.0)
"""Range for each scenario's weight before normalisation.

A regime that pushes most of its weight onto one scenario is the concept
drift case: the fraud rate can hold steady while what fraud looks like
changes completely.
"""

REGIME_COUNT_RANGE: Final[tuple[int, int]] = (4, 7)
"""How many regimes a derived schedule holds, including the opening one."""

MIN_REGIME_DAYS: Final = 7.0
"""No regime is shorter than a week, so a monitor has time to see it."""


@dataclass(frozen=True, slots=True)
class Regime:
    """One stretch of time with its own generator parameters.

    Attributes:
        name: Readable name, unique within a schedule.
        starts_after_days: Offset from the start of the window. The first
            regime starts at zero.
        fraud_rate_multiplier: Multiplier on the baseline fraud rate.
        scenario_weights: Relative weight of each entry in
            `ATTACK_SCENARIOS`, normalised on use.
        amount_log_shift: Additive shift to the mean of log-amount.
        online_share_shift: Additive shift to the card-not-present share.
        attack_intensity: Multiplier on the size of each attack.
    """

    name: str
    starts_after_days: float
    fraud_rate_multiplier: float
    scenario_weights: tuple[float, float, float]
    amount_log_shift: float
    online_share_shift: float
    attack_intensity: float

    def normalised_weights(self) -> tuple[float, ...]:
        """Return the scenario weights as a probability vector.

        Returns:
            One probability per entry in `ATTACK_SCENARIOS`.
        """
        total = sum(self.scenario_weights)
        return tuple(weight / total for weight in self.scenario_weights)


@dataclass(frozen=True, slots=True)
class RegimeSchedule:
    """An ordered set of regimes covering a window.

    Attributes:
        name: Readable name of the schedule.
        regimes: The regimes, ordered by start, the first starting at zero.
    """

    name: str
    regimes: tuple[Regime, ...]

    def __post_init__(self) -> None:
        """Check the schedule is well formed.

        Raises:
            ValueError: If it is empty, does not start at zero, is not
                strictly ordered, or repeats a regime name.
        """
        if not self.regimes:
            msg = "a schedule needs at least one regime"
            raise ValueError(msg)
        if self.regimes[0].starts_after_days != 0.0:
            msg = "the first regime must start at day zero"
            raise ValueError(msg)
        starts = [regime.starts_after_days for regime in self.regimes]
        if starts != sorted(starts) or len(set(starts)) != len(starts):
            msg = "regimes must be strictly ordered by start day"
            raise ValueError(msg)
        names = [regime.name for regime in self.regimes]
        if len(set(names)) != len(names):
            msg = "regime names must be unique within a schedule"
            raise ValueError(msg)

    def at(self, elapsed_days: float) -> Regime:
        """Return the regime in force at an offset into the window.

        Args:
            elapsed_days: Days since the start of the window. Negative values
                are treated as zero.

        Returns:
            The regime in force.
        """
        current = self.regimes[0]
        for regime in self.regimes:
            if regime.starts_after_days <= elapsed_days:
                current = regime
            else:
                break
        return current

    def to_json(self) -> str:
        """Serialise the schedule canonically, for hashing and publication.

        Returns:
            A canonical JSON document: sorted keys, no insignificant space.
        """
        payload = {
            "name": self.name,
            "regimes": [asdict(regime) for regime in self.regimes],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def fingerprint(self) -> str:
        """Hash the schedule.

        Returns:
            The hex sha256 of `to_json`.
        """
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()


DEV_SCHEDULE: Final = RegimeSchedule(
    name="dev-2026-09",
    regimes=(
        Regime(
            name="baseline",
            starts_after_days=0.0,
            fraud_rate_multiplier=1.0,
            scenario_weights=(0.5, 0.3, 0.2),
            amount_log_shift=0.0,
            online_share_shift=0.0,
            attack_intensity=1.0,
        ),
        Regime(
            name="card-testing-wave",
            starts_after_days=14.0,
            fraud_rate_multiplier=2.1,
            scenario_weights=(0.85, 0.1, 0.05),
            amount_log_shift=0.0,
            online_share_shift=0.05,
            attack_intensity=1.8,
        ),
        Regime(
            name="amount-drift-no-fraud-change",
            starts_after_days=30.0,
            fraud_rate_multiplier=1.0,
            scenario_weights=(0.5, 0.3, 0.2),
            amount_log_shift=0.3,
            online_share_shift=0.0,
            attack_intensity=1.0,
        ),
        Regime(
            name="takeover-season",
            starts_after_days=45.0,
            fraud_rate_multiplier=1.4,
            scenario_weights=(0.15, 0.7, 0.15),
            amount_log_shift=0.1,
            online_share_shift=0.15,
            attack_intensity=1.2,
        ),
    ),
)
"""The published development schedule.

Used by every test and by the whole build. It deliberately contains the case
that catches a naive monitor: `amount-drift-no-fraud-change` moves a feature
distribution while the fraud rate and the scenario mix hold still.
"""


def derive_schedule(secret: str, *, window_days: float, name: str) -> RegimeSchedule:
    """Derive a schedule from a secret.

    The derivation is a pure function: publishing the secret on Jul 1 2027
    lets anyone reproduce the schedule exactly and check it against the hash
    committed before go-live.

    Args:
        secret: The sealed secret. Never committed, never logged.
        window_days: Length of the window the schedule must cover.
        name: Readable name for the derived schedule.

    Returns:
        The derived schedule.

    Raises:
        ValueError: If the window is too short to hold the minimum regimes.
    """
    if window_days < REGIME_COUNT_RANGE[0] * MIN_REGIME_DAYS:
        msg = (
            f"window of {window_days} days cannot hold {REGIME_COUNT_RANGE[0]} regimes "
            f"of at least {MIN_REGIME_DAYS} days"
        )
        raise ValueError(msg)

    digest = hmac.new(secret.encode("utf-8"), b"verdict/regime-schedule/v1", hashlib.sha256)
    rng = np.random.default_rng(int.from_bytes(digest.digest()[:8], "big"))

    count = int(rng.integers(REGIME_COUNT_RANGE[0], REGIME_COUNT_RANGE[1] + 1))
    starts = _draw_starts(rng, count=count, window_days=window_days)

    regimes: list[Regime] = []
    for position, start in enumerate(starts):
        weights = rng.uniform(*SCENARIO_WEIGHT_RANGE, size=len(ATTACK_SCENARIOS))
        regimes.append(
            Regime(
                name=f"regime-{position + 1}",
                starts_after_days=float(start),
                fraud_rate_multiplier=(
                    1.0 if position == 0 else float(rng.uniform(*FRAUD_RATE_MULTIPLIER_RANGE))
                ),
                scenario_weights=(float(weights[0]), float(weights[1]), float(weights[2])),
                amount_log_shift=(
                    0.0 if position == 0 else float(rng.uniform(*AMOUNT_LOG_SHIFT_RANGE))
                ),
                online_share_shift=(
                    0.0 if position == 0 else float(rng.uniform(*ONLINE_SHARE_SHIFT_RANGE))
                ),
                attack_intensity=(
                    1.0 if position == 0 else float(rng.uniform(*ATTACK_INTENSITY_RANGE))
                ),
            )
        )
    return RegimeSchedule(name=name, regimes=tuple(regimes))


def _draw_starts(rng: np.random.Generator, *, count: int, window_days: float) -> list[float]:
    """Draw regime start days, spaced by at least `MIN_REGIME_DAYS`.

    Args:
        rng: The derivation's random generator.
        count: How many regimes.
        window_days: Length of the window.

    Returns:
        Start offsets in days, beginning at zero and strictly increasing.
    """
    # Split the window into `count` gaps that each clear the minimum, by
    # drawing the slack and distributing it. Rounding to whole hours keeps
    # the published schedule readable without making starts collide.
    slack = window_days - count * MIN_REGIME_DAYS
    shares = rng.dirichlet(np.ones(count))
    gaps = MIN_REGIME_DAYS + shares * slack
    starts = np.concatenate([[0.0], np.cumsum(gaps)[:-1]])
    return [float(np.round(start * 24.0) / 24.0) for start in starts]


def source_fingerprint(path: Path | None = None) -> str:
    """Hash this module's source, so an edit after sealing is detectable.

    Line endings are normalised before hashing: the repository is developed on
    Windows and runs on Linux, and a checkout that rewrites CRLF must not look
    like a tampered schedule.

    Args:
        path: The file to hash. Defaults to this module.

    Returns:
        The hex sha256 of the normalised source bytes.
    """
    target = path or Path(__file__)
    raw = target.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(raw).hexdigest()


class SealedCommitment(Record):
    """The three hashes published before go-live.

    Holding all three is what makes the live drift numbers checkable: the
    secret hash proves the secret published on Jul 1 is the one used, the
    schedule hash proves the derivation was not swapped, and the source hash
    proves this module was not edited in between.
    """

    secret_sha256: str
    """Hash of the sealed secret. The secret itself is published on Jul 1."""
    schedule_sha256: str
    """Hash of the derived schedule's canonical JSON."""
    source_sha256: str
    """Hash of `regimes.py` at sealing time."""
    schedule_name: str
    window_days: float
    sealed_at: dt.datetime
    """When the seal was taken, timezone-aware UTC."""

    def to_document(self) -> str:
        """Render the commitment for `docs/sealed-schedule.json`.

        Returns:
            A pretty-printed JSON document with a trailing newline.
        """
        return json.dumps(json.loads(self.model_dump_json()), indent=2, sort_keys=True) + "\n"

    @classmethod
    def seal(cls, secret: str, *, window_days: float, name: str, now: dt.datetime) -> Self:
        """Derive the live schedule and commit to it without revealing it.

        Args:
            secret: The sealed secret.
            window_days: Length of the live window in days.
            name: Readable name for the derived schedule.
            now: The sealing time, timezone-aware UTC.

        Returns:
            The commitment to publish.
        """
        schedule = derive_schedule(secret, window_days=window_days, name=name)
        return cls(
            secret_sha256=hashlib.sha256(secret.encode("utf-8")).hexdigest(),
            schedule_sha256=schedule.fingerprint(),
            source_sha256=source_fingerprint(),
            schedule_name=name,
            window_days=window_days,
            sealed_at=require_utc(now),
        )

    def verify(self, secret: str) -> bool:
        """Check a revealed secret against this commitment.

        This is the Jul 1 2027 operation, and the one a stranger runs to check
        the live drift numbers were not arranged after the fact.

        Args:
            secret: The revealed secret.

        Returns:
            True if the secret, the schedule it derives and the current source
            all match what was committed.
        """
        if hashlib.sha256(secret.encode("utf-8")).hexdigest() != self.secret_sha256:
            return False
        schedule = derive_schedule(secret, window_days=self.window_days, name=self.schedule_name)
        return (
            schedule.fingerprint() == self.schedule_sha256
            and source_fingerprint() == self.source_sha256
        )
