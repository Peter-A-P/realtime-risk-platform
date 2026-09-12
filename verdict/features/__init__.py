"""Computing the features, once, from the stream.

`aggregators` holds the sliding-window aggregations, each maintaining bounded
state and answering in amortised constant time. `engine` keys them by entity
and enforces the ordering that makes point-in-time correctness structural:
every event is **served before it is observed**, so the event being scored
cannot be inside its own features. `sinks` writes each computed value to the
online and offline stores in one call, so training and serving cannot
disagree about what a feature was.

This package is the one place features are computed. The brute-force
evaluation in `verdict.store.features` is the definition it is checked
against, not a second pipeline; ADR 6 sets out the distinction and ADR 4
records why the engine is written here rather than taken off the shelf.
"""

from verdict.features.engine import FeatureEngine, FeatureRow, StaleQueryError
from verdict.features.sinks import DualSink, OfflineParquetSink
from verdict.features.verify import ParityReport, check_parity, served_lookup_from_offline

__all__ = [
    "DualSink",
    "FeatureEngine",
    "FeatureRow",
    "OfflineParquetSink",
    "ParityReport",
    "StaleQueryError",
    "check_parity",
    "served_lookup_from_offline",
]
