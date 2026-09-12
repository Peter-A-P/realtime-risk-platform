"""The `verdict` synthetic transaction generator.

The synthetic track exists because real card-fraud data has no streaming
timestamps and no volume (see `docs/adr/0002-two-tracks.md`). This package
produces an unbounded, deterministic, replayable stream with:

- an entity graph of cards, devices and merchants, so entity-graph features
  such as shared-device counts have something real to measure (`entities`);
- three publicly documented fraud patterns that act on that graph
  (`scenarios`);
- a schedule of regime shifts whose realisation is sealed before go-live, so
  the drift monitors are graded against shifts they could not have been tuned
  to (`regimes`);
- a driver that merges the two into one time-ordered stream (`driver`).

Everything is a pure function of the seed. The same seed produces the same
bytes, which is what makes a replay a replay.
"""

from verdict.events.generator.driver import (
    GeneratedRecord,
    Generator,
    GeneratorConfig,
)
from verdict.events.generator.entities import Card, Device, EntityGraph, Merchant, Population
from verdict.events.generator.regimes import DEV_SCHEDULE, Regime, RegimeSchedule

__all__ = [
    "DEV_SCHEDULE",
    "Card",
    "Device",
    "EntityGraph",
    "GeneratedRecord",
    "Generator",
    "GeneratorConfig",
    "Merchant",
    "Population",
    "Regime",
    "RegimeSchedule",
]
