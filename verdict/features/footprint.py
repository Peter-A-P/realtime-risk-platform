"""How much memory the feature engine holds as the stream goes by.

A measuring instrument, not the platform. It runs the generator at the live
population through the engine, in the scorer's own serving path, and samples
the process's resident memory and the engine's tracked entities as it goes.
Fitting memory against entities and events separates what each entity costs
once from what each event costs while it is inside a window, which is what
decides whether 24 hours of windows at the live rate fit an instance (ADR 15).

Every event here lands inside every window: the run covers minutes of stream
time, and nothing has expired yet. So the per-event figure is what an event
costs while held, and the whole-day figure is that times the events a day
holds per window, which the report computes for each window length.
"""

from __future__ import annotations

import datetime as dt
import gc
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np

from verdict.events.generator.driver import Generator, GeneratorConfig
from verdict.features.engine import FeatureEngine
from verdict.scoring.core import EngineFeatures
from verdict.store.features import FEATURE_SET


@dataclass(slots=True)
class Sample:
    """One reading.

    Attributes:
        events: Events served so far.
        entities: Entities the engine tracks.
        rss_bytes: The process's resident memory above the baseline.
        seconds: Wall time since the start.
    """

    events: int
    entities: int
    rss_bytes: int
    seconds: float


@dataclass(slots=True)
class Footprint:
    """The measurement and what it implies.

    Attributes:
        measured_at: When.
        host: The machine, briefly.
        rate_per_second: The generator's configured rate.
        population: The entity graph's size.
        samples: The readings.
        bytes_per_entity: Fitted cost of an entity, once.
        bytes_per_event: Fitted cost of an event while inside the windows.
        events_per_second_served: How fast generator and engine went together.
        day_estimate_gb: Entities, plus a day of events at the live rate held
            for each feature's window, from the fitted costs.
        notes: What the figures do and do not say.
    """

    measured_at: str
    host: str
    rate_per_second: float
    population: dict[str, Any]
    samples: list[Sample] = field(default_factory=list)
    bytes_per_entity: float = 0.0
    bytes_per_event: float = 0.0
    events_per_second_served: float = 0.0
    day_estimate_gb: float = 0.0
    notes: list[str] = field(default_factory=list)


def measure(events: int, *, every: int = 50_000, ceiling_bytes: int = 5_000_000_000) -> Footprint:
    """Serve events through the engine and sample memory as it grows.

    Args:
        events: The most events to serve.
        every: How often to sample.
        ceiling_bytes: Stop early if resident memory grows by this much, so
            the measurement cannot take the machine down.

    Returns:
        The footprint, with the fit.
    """
    import platform

    import psutil

    config = GeneratorConfig()
    process = psutil.Process(os.getpid())
    source = EngineFeatures(FeatureEngine())
    gc.collect()
    baseline = process.memory_info().rss
    report = Footprint(
        measured_at=dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        host=f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        rate_per_second=config.events_per_second,
        population=asdict(config.population),
    )
    started = time.perf_counter()
    served = 0
    for record in Generator(config).stream(limit=events):
        source.serve(record.event)
        served += 1
        if served % every == 0:
            gc.collect()
            grown = process.memory_info().rss - baseline
            report.samples.append(
                Sample(
                    events=served,
                    entities=source.engine.tracked_entities,
                    rss_bytes=grown,
                    seconds=time.perf_counter() - started,
                )
            )
            if grown > ceiling_bytes:
                report.notes.append(f"stopped at the memory ceiling after {served:,} events")
                break
    elapsed = time.perf_counter() - started
    report.events_per_second_served = served / elapsed if elapsed else 0.0

    rows = np.array([[s.entities, s.events, 1.0] for s in report.samples], dtype=np.float64)
    rss = np.array([s.rss_bytes for s in report.samples], dtype=np.float64)
    (per_entity, per_event, _), *_ = np.linalg.lstsq(rows, rss, rcond=None)
    report.bytes_per_entity = float(per_entity)
    report.bytes_per_event = float(per_event)

    # A day at the live rate: each feature holds the events of its own window.
    # The fitted per-event cost covers all sixteen features at once, so it is
    # shared out by the fraction of features on each window length.
    day = 86_400.0
    held = 0.0
    for spec in FEATURE_SET:
        window = spec.window.total_seconds() if spec.window is not None else day
        held += min(window, day) * config.events_per_second / len(FEATURE_SET)
    entities = report.samples[-1].entities if report.samples else 0
    report.day_estimate_gb = (per_entity * entities + per_event * held) / 1e9
    report.notes.append(
        "per-event cost is shared evenly across features; a feature on a longer "
        "window may hold more per event than one on a shorter window, so the day "
        "estimate is an order of magnitude, not a sizing"
    )
    report.notes.append(
        f"entities at the end: {entities:,}; the population may not have been seen in full"
    )
    return report
