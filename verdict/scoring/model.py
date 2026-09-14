"""The model interface, and the stand-in that holds its place until week 5.

The scorer calls a model through one method, `score`, which takes the served
features and the event and returns a number in [0, 1]. Week 5 puts the
XGBoost champion behind that method, exported to ONNX, and the challenger
beside it in shadow. Nothing in the scorer changes when it does.

**The stand-in is not a model of fraud and must not be read as one.** It is a
fixed logistic function of four features with weights written down here, not
fitted to anything, so that the path from an event to a decision is complete
and can be timed in week 4, which is when the plan measures latency. Its
scores are never evaluated and no result is published from them. The latency
of a real model is measured again when one exists, and the budget says so.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Final, Protocol, runtime_checkable

from verdict.events.schema import TransactionEvent
from verdict.store.features import NO_EVENTS


@runtime_checkable
class Model(Protocol):
    """What the scorer needs from a model."""

    @property
    def version(self) -> str:
        """An identifier written into every decision the model makes."""
        ...

    def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
        """Score one event.

        Args:
            features: Feature name to served value. A feature with no history
                carries the `NO_EVENTS` sentinel, never a gap.
            event: The event itself, for fields such as the amount.

        Returns:
            A score in [0, 1].
        """
        ...


STAND_IN_VERSION: Final = "stand-in-0"


def _history(features: Mapping[str, float], name: str) -> float:
    """A feature's value, with no history read as zero."""
    value = features.get(name, NO_EVENTS)
    return 0.0 if value == NO_EVENTS else value


class StandInModel:
    """A fixed logistic function, for timing the path. Not fitted, not evaluated."""

    version: str = STAND_IN_VERSION

    def score(self, features: Mapping[str, float], event: TransactionEvent) -> float:
        """Score an event from four features and the amount.

        Args:
            features: The served features.
            event: The event.

        Returns:
            A number in [0, 1].
        """
        burst = math.log1p(_history(features, "card_txn_count_1h"))
        shared = math.log1p(_history(features, "device_distinct_cards_1h"))
        usual = _history(features, "card_amount_mean_24h")
        unusual = 1.0 if usual > 0 and event.amount_cents > 3 * usual else 0.0
        z = -4.0 + 0.6 * burst + 0.8 * shared + 1.5 * unusual
        return 1.0 / (1.0 + math.exp(-z))
