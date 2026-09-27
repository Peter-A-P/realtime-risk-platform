"""Retraining and the promotion gate on the live window, from what history kept (ADR 28).

Offline, a candidate was fitted on a replayed table and judged against the
champion on its later rows (ADR 24), and the gate was judged on shadow rows
(ADR 11). Live, both read the same thing: the finalised history, a day of
kept rows per file, each labelled and weighted (ADR 18). This module turns
those days into the tables the existing code already takes, and decides when
each job has something new to say.

**Tables are bounded, and the bound is another weighted sample.** A live day
keeps millions of rows (every reviewed or declined one, a tenth of approved
frauds, a hundredth of the rest), and the instance fits beside a scorer
holding 7.5 GB. So a table keeps every row up to a target per label and,
past it, a hash-drawn share of each label with the weight scaled to match:
the same case-control sampling history already uses, unbiased for anything
that reads the weight, which the fit and every estimate here do.

**When a candidate is fitted.** Only while a drift request is open, only
once the window has `MIN_DAYS` finalised days to fit on, and again only when
`NEW_DAYS` more have been finalised since the last candidate: ADR 24 found the
first candidate a request can build is blind to the drift, and the one that
helps is the one fitted after the drifted days' labels arrive, which is a
week and a few days later. One candidate a day would be a pull request a day
saying the same thing.

**When the gate is run.** Once the shadow model has scored rows on at least
`SHADOW_DAYS` finalised days, and then only if its verdict would change: a
pull request is opened for the first verdict on each shadow model, and again
if it later turns eligible. Eligible is not promoted; merging the pull
request is the approval, and the pointer moves by `verdict flag set` after it
(ADR 11).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from verdict.history.compact import HistoryPaths
from verdict.models.dataset import training_schema
from verdict.models.promote import ShadowRow

MIN_DAYS: Final = 3
"""Finalised days a candidate needs: a fit needs frauds on both sides of its
validation split and a test period after it."""

NEW_DAYS: Final = 3
"""Newly finalised days before the next candidate for the same request: ADR
24's recovery came from three labelled drifted days."""

SHADOW_DAYS: Final = 7
"""Finalised days of shadow scores before the gate is asked: ADR 11's week."""

FIT_DAYS: Final = 14
"""The most recent finalised days a candidate is fitted on."""

TARGET_ROWS: Final = {True: 400_000, False: 2_000_000}
"""The most rows of each label a table keeps before drawing a share of them."""

_SALT: Final = b"verdict/models/live/table/v1"


def finalised_days(paths: HistoryPaths) -> list[dt.date]:
    """The days whose kept rows are final, in order.

    Args:
        paths: The history root.

    Returns:
        Their dates.
    """
    if not paths.kept.is_dir():
        return []
    return sorted(dt.date.fromisoformat(path.stem) for path in paths.kept.glob("*.parquet"))


def _draw(event_id: str) -> float:
    digest = hashlib.sha256(_SALT + event_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def bounded_table(
    paths: HistoryPaths,
    days: list[dt.date],
    *,
    columns: list[str],
    targets: dict[bool, int] = TARGET_ROWS,
    keep: pc.Expression | None = None,
) -> tuple[pa.Table, dict[str, float]]:
    """Kept rows from some days, thinned by label to fit, with weights scaled to match.

    Args:
        paths: The history root.
        days: Which finalised days.
        columns: Which columns; `event_id`, `is_fraud` and `weight` are
            always read.
        targets: The most rows of each label kept whole.
        keep: A filter applied first, such as "the shadow model scored it".

    Returns:
        The rows, in event-time order, and the share of each label kept.
    """
    wanted = list(dict.fromkeys(["event_id", "event_time", "is_fraud", "weight", *columns]))
    counts = {True: 0, False: 0}
    for day in days:
        labels = pq.read_table(paths.kept_file(day), columns=["is_fraud", *_filter_columns(keep)])
        if keep is not None:
            labels = labels.filter(keep)
        frauds = int(pc.sum(labels["is_fraud"]).as_py() or 0)
        counts[True] += frauds
        counts[False] += labels.num_rows - frauds
    share = {
        label: min(1.0, targets[label] / counts[label]) if counts[label] else 1.0
        for label in (True, False)
    }
    parts = []
    for day in days:
        read = list(dict.fromkeys([*wanted, *_filter_columns(keep)]))
        table = pq.read_table(paths.kept_file(day), columns=read)
        if keep is not None:
            table = table.filter(keep).select(wanted)
        if share[True] < 1.0 or share[False] < 1.0:
            ids = cast("list[str]", table["event_id"].to_pylist())
            is_fraud = cast("list[bool]", table["is_fraud"].to_pylist())
            mask = np.array(
                [_draw(str(i)) < share[bool(f)] for i, f in zip(ids, is_fraud, strict=True)],
                dtype=np.bool_,
            )
            table = table.filter(pa.array(mask))
            scale = np.where(
                table["is_fraud"].to_numpy(zero_copy_only=False),
                1.0 / share[True],
                1.0 / share[False],
            )
            weights = table["weight"].to_numpy(zero_copy_only=False) * scale
            table = table.set_column(
                table.schema.get_field_index("weight"), "weight", pa.array(weights)
            )
        parts.append(table)
    if not parts:
        return pa.table({name: [] for name in wanted}), {"fraud": 1.0, "legit": 1.0}
    joined = pa.concat_tables(parts).sort_by("event_time")
    return joined, {"fraud": share[True], "legit": share[False]}


def _filter_columns(keep: pc.Expression | None) -> list[str]:
    return [] if keep is None else ["shadow_version", "shadow_score"]


def training_table(paths: HistoryPaths, days: list[dt.date]) -> tuple[pa.Table, dict[str, float]]:
    """The table a live candidate is fitted and tested on.

    Args:
        paths: The history root.
        days: The finalised days to use.

    Returns:
        Rows in the training schema, and the share of each label kept.
    """
    names = training_schema().names
    table, share = bounded_table(paths, days, columns=names)
    return table.select(names).cast(training_schema()), share


def shadow_rows(
    paths: HistoryPaths, days: list[dt.date], *, shadow_version: str
) -> tuple[list[ShadowRow], dict[str, float]]:
    """The labelled rows a shadow model scored, for the gate.

    Args:
        paths: The history root.
        days: The finalised days to use.
        shadow_version: The shadow model's version.

    Returns:
        The rows, and the share of each label kept.
    """
    keep = (pc.field("shadow_version") == shadow_version) & pc.field("shadow_score").is_valid()
    table, share = bounded_table(
        paths,
        days,
        columns=["label_time", "amount_cents", "champion_score", "shadow_score"],
        keep=keep,
        targets={True: 200_000, False: 400_000},
    )
    rows = [
        ShadowRow(
            event_id=row["event_id"],
            is_fraud=row["is_fraud"],
            label_time=row["label_time"],
            amount_cents=row["amount_cents"],
            champion_score=row["champion_score"],
            challenger_score=row["shadow_score"],
            weight=row["weight"],
        )
        for row in table.to_pylist()
    ]
    return rows, share


def current_shadow(paths: HistoryPaths, finalised: list[dt.date]) -> str | None:
    """The shadow model the scorer was running on the latest finalised day.

    Read from history rather than configured, so the job cannot disagree with
    the scorer about which model is in shadow.

    Args:
        paths: The history root.
        finalised: The finalised days.

    Returns:
        The version seen most often that day, or None if nothing was scored.
    """
    if not finalised:
        return None
    column = pq.read_table(paths.kept_file(finalised[-1]), columns=["shadow_version"])
    seen = Counter(str(v) for v in column["shadow_version"].to_pylist() if v is not None)
    return seen.most_common(1)[0][0] if seen else None


def shadow_days(paths: HistoryPaths, days: list[dt.date], *, shadow_version: str) -> list[dt.date]:
    """The finalised days on which a shadow model scored anything.

    Args:
        paths: The history root.
        days: The finalised days.
        shadow_version: The shadow model's version.

    Returns:
        Those days.
    """
    found = []
    for day in days:
        versions = pq.read_table(paths.kept_file(day), columns=["shadow_version"])
        if pc.any(pc.equal(versions["shadow_version"], pa.scalar(shadow_version))).as_py():
            found.append(day)
    return found


# --- what has been done, on the data volume -----------------------------------------


@dataclass(slots=True)
class ModelsState:
    """The candidates fitted and the gate's verdicts, as the live job recorded them.

    Attributes:
        path: The record, a JSON file.
    """

    path: Path

    def read(self) -> dict[str, Any]:
        """The record.

        Returns:
            `candidates` and `verdicts`, each a list, oldest first.
        """
        if not self.path.exists():
            return {"candidates": [], "verdicts": []}
        loaded: dict[str, Any] = json.loads(self.path.read_text(encoding="utf-8"))
        return loaded

    def add(self, kind: str, item: dict[str, Any]) -> None:
        """Append to the record.

        Args:
            kind: `candidates` or `verdicts`.
            item: What happened.
        """
        record = self.read()
        record[kind].append(item)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        partial = self.path.with_suffix(".partial")
        partial.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        partial.replace(self.path)

    def candidates_for(self, opened_on: dt.date) -> list[dict[str, Any]]:
        """Candidates already fitted for a request.

        Args:
            opened_on: The request's day.

        Returns:
            Them, oldest first.
        """
        return [c for c in self.read()["candidates"] if c["opened_on"] == opened_on.isoformat()]

    def verdicts_for(self, shadow_version: str) -> list[dict[str, Any]]:
        """The gate's verdicts on a shadow model.

        Args:
            shadow_version: The model.

        Returns:
            Them, oldest first.
        """
        return [v for v in self.read()["verdicts"] if v["shadow_version"] == shadow_version]


def candidate_due(
    state: ModelsState, *, opened_on: dt.date, finalised: list[dt.date]
) -> list[dt.date] | None:
    """Whether a candidate should be fitted now for an open request, and on which days.

    Args:
        state: What has been fitted.
        opened_on: The open request's day.
        finalised: The finalised days of the window.

    Returns:
        The days to fit on, or None if nothing is due.
    """
    if len(finalised) < MIN_DAYS:
        return None
    earlier = state.candidates_for(opened_on)
    if earlier:
        last = dt.date.fromisoformat(earlier[-1]["last_day"])
        if sum(1 for day in finalised if day > last) < NEW_DAYS:
            return None
    return finalised[-FIT_DAYS:]


def gate_due(
    state: ModelsState, *, shadow_version: str, days_scored: list[dt.date], eligible_now: bool
) -> bool:
    """Whether a verdict is worth a pull request.

    Args:
        state: The verdicts already told.
        shadow_version: The shadow model.
        days_scored: Finalised days it has scored on.
        eligible_now: What the gate says today.

    Returns:
        True for the first verdict on a model, and for its first eligible one.
    """
    if len(days_scored) < SHADOW_DAYS:
        return False
    told = state.verdicts_for(shadow_version)
    if not told:
        return True
    return eligible_now and not any(v["eligible"] for v in told)
