"""The week 1 criterion: 1,000 events per second, generated and logged.

This is a test rather than a note in a report because throughput is a
property the rest of the build depends on. If the generator cannot outrun the
live rate on the build laptop, the live rate is not 1,000 events per second,
whatever the dashboard says.

The assertion is deliberately loose. It exists to catch a regression that
makes the generator an order of magnitude slower, not to police the variance
of a shared machine. The measured number goes in the README, with its
interval, from `loadtest`, not from here.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.events.generator.entities import EntityGraph, Population
from verdict.events.rawlog import RawEventLog

TARGET_EVENTS_PER_SECOND = 1_000.0
"""The live rate. The generator has to clear it with room to spare."""

EVENTS = 50_000
REFERENCE = Population(cards=20_000, devices=15_000, merchants=500)


@pytest.mark.slow
def test_the_generator_outruns_the_live_rate(tmp_path: Path) -> None:
    graph = EntityGraph.build(seed=20270201, population=REFERENCE)
    config = GeneratorConfig(
        seed=20270201, population=REFERENCE, events_per_second=TARGET_EVENTS_PER_SECOND
    )
    generator = Generator(config, graph)

    started = time.perf_counter()
    with RawEventLog(tmp_path) as log:
        for record in generator.stream(limit=EVENTS):
            log.append(record)
    elapsed = time.perf_counter() - started

    achieved = EVENTS / elapsed
    assert achieved > TARGET_EVENTS_PER_SECOND, (
        f"generated and logged {achieved:.0f} events per second, "
        f"below the live rate of {TARGET_EVENTS_PER_SECOND:.0f}"
    )
