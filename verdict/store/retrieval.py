"""Point-in-time retrieval: building a training set that could have existed.

One rule, and everything here exists to enforce it:

> **A training row's features are computed as of its event time, never its
> label time.**

The label arrives seven days after the transaction. Joining features as of the
label time would hand the model a week of hindsight on every row. It is a
one-word mistake, it is invisible in every offline metric, and it produces a
model that looks superb in a notebook and is worthless in the stream.

So `build_entity_frame` takes rows that carry both times and deliberately
drops the label time on the floor before Feast ever sees it. Feast's
`get_historical_features` keys its join on the column named
`event_timestamp`, so what goes in that column is the only thing that matters,
and it is the one thing this module refuses to make configurable.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from verdict.store.features import FEATURE_SET, FeatureSpec, validate_feature_set
from verdict.store.leakage import RetrievalLookup, TrainingRow
from verdict.store.repo import ENTITY_JOIN_KEYS, TIMESTAMP_FIELD, feature_refs

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    import pandas as pd
    from feast import FeatureStore


class MissingEntityError(KeyError):
    """Raised when a training row lacks an entity a feature is keyed on."""


def build_entity_frame(
    rows: Sequence[TrainingRow], specs: Sequence[FeatureSpec] = FEATURE_SET
) -> pd.DataFrame:
    """Build the entity dataframe Feast joins against.

    The frame holds one column per entity the features need, plus
    `event_timestamp`, which is the row's **event** time. The label time is
    not carried into the frame at all: it cannot be leaked through a column
    that does not exist.

    Args:
        rows: The training rows, each carrying both of its times.
        specs: The features that will be retrieved. Defaults to the
            platform's own set.

    Returns:
        The entity dataframe.

    Raises:
        MissingEntityError: If a row has no identifier for an entity that one
            of the requested features is keyed on. Silently dropping the row
            would quietly shrink the training set; silently filling it would
            be worse.
    """
    import pandas as pd

    validate_feature_set(list(specs))
    kinds = sorted({spec.entity for spec in specs}, key=str)
    columns: dict[str, list[Any]] = {ENTITY_JOIN_KEYS[kind]: [] for kind in kinds}
    timestamps: list[dt.datetime] = []

    for index, row in enumerate(rows):
        for kind in kinds:
            entity_id = row.entity_ids.get(str(kind))
            if entity_id is None:
                msg = (
                    f"row {index} at {row.event_time.isoformat()} has no {kind} identifier, "
                    f"but a requested feature is keyed on {kind}"
                )
                raise MissingEntityError(msg)
            columns[ENTITY_JOIN_KEYS[kind]].append(entity_id)
        timestamps.append(row.event_time)

    columns[TIMESTAMP_FIELD] = timestamps
    return pd.DataFrame(columns)


def build_training_features(
    store: FeatureStore,
    rows: Sequence[TrainingRow],
    specs: Sequence[FeatureSpec] = FEATURE_SET,
) -> pd.DataFrame:
    """Retrieve features for training rows, as of each row's event time.

    Args:
        store: The Feast store.
        rows: The training rows.
        specs: The features to retrieve. Defaults to the platform's own set.

    Returns:
        A frame of the entity columns, the timestamp and one column per
        feature. With no features requested, the entity frame is returned
        unchanged, which is week 2's state.
    """
    frame = build_entity_frame(rows, specs)
    if not specs:
        return frame
    return store.get_historical_features(entity_df=frame, features=feature_refs(specs)).to_df()


def historical_lookup(
    store: FeatureStore, specs: Sequence[FeatureSpec] = FEATURE_SET
) -> RetrievalLookup:
    """Adapt the store to the leakage harness's retrieval signature.

    The harness asks for one feature, one entity and two times, and this
    answers from Feast's historical retrieval. It is one query per call and
    therefore slow, which is why the leakage check runs on a sample rather
    than on a day of traffic.

    The `label_time` argument is accepted and discarded, which is exactly what
    the label-shift check is there to verify: if a future version of this
    function ever starts using it, that check goes red.

    Args:
        store: The Feast store.
        specs: The features in play. Defaults to the platform's own set.

    Returns:
        A callable matching `verdict.store.leakage.RetrievalLookup`.
    """
    import pandas as pd

    by_name = {spec.name: spec for spec in specs}

    def retrieve(
        spec: FeatureSpec, entity_id: str, event_time: dt.datetime, label_time: dt.datetime
    ) -> float:
        del label_time
        known = by_name.get(spec.name, spec)
        frame = pd.DataFrame(
            {
                ENTITY_JOIN_KEYS[known.entity]: [entity_id],
                TIMESTAMP_FIELD: [event_time],
            }
        )
        result = store.get_historical_features(
            entity_df=frame, features=feature_refs([known])
        ).to_df()
        value = result[known.name].iloc[0]
        # A miss comes back as NaN. It becomes the same "no history" sentinel
        # the reference evaluation returns, so the two are comparable rather
        # than incomparable-but-both-arguably-right.
        return -1.0 if pd.isna(value) else float(value)

    return retrieve
