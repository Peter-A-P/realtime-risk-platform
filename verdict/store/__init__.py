"""The feature store: definitions, point-in-time retrieval, and the test.

The order of the files in this package is the order they were written in, and
that order is the argument:

1. `features.py` defines what a feature *is*, as a specification and a
   reference evaluation over raw events.
2. `leakage.py` is the test that a served feature equals that reference,
   computed using only events strictly before the moment it describes.
3. Only then, in week 3, does `features/dataflow.py` compute them for real.

Writing the test first is not a process preference. A point-in-time bug is
invisible in every offline metric, because the leaked information makes the
model look better, and it is invisible in production too, because the model
merely underperforms rather than failing. The only thing that catches it is a
test that existed before the feature did.
"""

from verdict.store.features import (
    FEATURE_SET,
    Aggregation,
    EntityKind,
    FeatureSpec,
    evaluate_spec,
)
from verdict.store.leakage import (
    LeakageError,
    LeakageReport,
    LeakageViolation,
    TrainingRow,
    check_label_shift_invariance,
    check_point_in_time,
    training_rows_from,
)

__all__ = [
    "FEATURE_SET",
    "Aggregation",
    "EntityKind",
    "FeatureSpec",
    "LeakageError",
    "LeakageReport",
    "LeakageViolation",
    "TrainingRow",
    "check_label_shift_invariance",
    "check_point_in_time",
    "evaluate_spec",
    "training_rows_from",
]
