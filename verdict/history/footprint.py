"""How much memory finalising a day holds, per staged hour.

A measuring instrument, not the platform. It writes one staged hour of
synthetic decisions the way the scorer does (small IPC batches, then
sealed) and its labels a week later, then finalises the day in a fresh
process and samples that process's resident memory as it goes. Every size
runs in its own processes, one to write and one to finalise, so no reading
carries another's allocations.

An hour at the live rate is about 3.6 million rows. The instrument is run at
fractions of that and fitted, so it does not need a machine with an hour's
worth of memory to spare, which is the point: the old finaliser held an hour
whole (`docs/adr/0018-history-is-a-weighted-sample.md`, the addendum).

The rows are not generated events: ids, scores and features are random, and
the champion reviews or declines about four percent of them. The candidates,
and so most of what finalising holds, scale with that share, which the live
rules set.
"""

from __future__ import annotations

import datetime as dt
import multiprocessing
import platform
import shutil
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.ipc as ipc

from verdict.history import spool
from verdict.history.compact import HistoryPaths, final_after, finalise_day
from verdict.history.records import LABEL_SCHEMA, staged_schema
from verdict.history.sampling import SampleRates

LIVE_HOUR_ROWS = 3_600_000
"""An hour at the live rate of 1,000 a second."""

_DAY = dt.date(2027, 4, 5)
_START = np.datetime64("2027-04-05T12:00:00", "us")
_DELAY = np.timedelta64(7, "D")
_BATCH = 500


@dataclass(slots=True)
class Reading:
    """One finalised hour.

    Attributes:
        rows: Staged rows in the hour.
        candidates: Rows joined to labels.
        kept_rows: Rows kept.
        peak_rss_mb: Resident memory at its highest, above the process's
            own before finalising began.
        arrow_pool_peak_mb: Arrow's allocator at its highest.
        seconds: Wall time to finalise.
        sha256: Of the kept file, so two versions of the code can be shown
            to keep the same rows.
    """

    rows: int
    candidates: int
    kept_rows: int
    peak_rss_mb: float
    arrow_pool_peak_mb: float
    seconds: float
    sha256: str


def write_hour(root: Path, rows: int, seed: int = 1) -> None:
    """Stage one hour of decisions and spool their labels, sealed.

    Args:
        root: The history root; must not exist.
        rows: Staged rows.
        seed: For the random columns.
    """
    schema = staged_schema()
    rng = np.random.default_rng(seed)
    offsets = (np.arange(rows, dtype=np.int64) * 3_600_000_000) // rows
    times = pa.array(_START + offsets.astype("timedelta64[us]"), pa.timestamp("us", tz="UTC"))
    ids = pa.array([f"evt-{seed:08x}-L{i:012d}" for i in range(rows)], pa.string())
    scores = rng.random(rows)
    actions = pa.array(
        np.where(scores > 0.985, "decline", np.where(scores > 0.96, "review", "approve")),
        pa.string(),
    )
    columns: dict[str, pa.Array[Any]] = {
        "event_id": ids,
        "card_id": pa.array([f"card-{i % 50_000}" for i in range(rows)], pa.string()),
        "event_time": times,
        "amount_cents": pa.array(rng.integers(100, 500_000, rows), pa.int64()),
        "decided_at": times,
        "champion_version": pa.array(["champion-8d960d985749"] * rows, pa.string()),
        "champion_score": pa.array(scores, pa.float64()),
        "action": actions,
        "rule": pa.array(["model"] * rows, pa.string()),
        "shadow_version": pa.array(["challenger-136b21035bfe"] * rows, pa.string()),
        "shadow_score": pa.array(rng.random(rows), pa.float64()),
        "shadow_action": actions,
    }
    for name in schema.names:
        if name not in columns:
            columns[name] = pa.array(rng.random(rows), pa.float64())
    _write_sealed(root / "staged", pa.table(columns, schema=schema))

    label_offsets = offsets.astype("timedelta64[us]") + _DELAY
    labels = pa.table(
        {
            "event_id": ids,
            "label_time": pa.array(_START + label_offsets, pa.timestamp("us", tz="UTC")),
            "is_fraud": pa.array(rng.random(rows) < 0.03),
            "recovered_cents": pa.array(np.zeros(rows, dtype=np.int64)),
        },
        schema=LABEL_SCHEMA,
    )
    _write_sealed(root / "labels", labels)


def _write_sealed(directory: Path, table: pa.Table) -> None:
    """Write a table as one hour in small IPC batches, then seal it."""
    first = table["event_time" if "event_time" in table.column_names else "label_time"][0]
    key = spool.hour_key(first.as_py())
    folder = directory / key
    folder.mkdir(parents=True)
    with (
        pa.OSFile(str(folder / "writer.arrow"), "wb") as sink,
        ipc.new_stream(sink, table.schema) as writer,
    ):
        for batch in table.to_batches(max_chunksize=_BATCH):
            writer.write_batch(batch)
    if not spool.seal(directory, key, table.schema):
        msg = f"could not seal {folder}"
        raise RuntimeError(msg)


def finalise_measured(root: Path) -> Reading:
    """Finalise the written day and watch this process's memory while it does.

    Args:
        root: The history root `write_hour` wrote.

    Returns:
        The reading.
    """
    process = psutil.Process()
    base = process.memory_info().rss
    peak = [base]
    done = threading.Event()

    def watch() -> None:
        while not done.is_set():
            peak[0] = max(peak[0], process.memory_info().rss)
            time.sleep(0.005)

    watcher = threading.Thread(target=watch)
    watcher.start()
    started = time.perf_counter()
    try:
        manifest = finalise_day(
            HistoryPaths(root), _DAY, as_of=final_after(_DAY), rates=SampleRates()
        )
    finally:
        seconds = time.perf_counter() - started
        done.set()
        watcher.join()
    return Reading(
        rows=manifest.staged_rows,
        candidates=manifest.candidates,
        kept_rows=sum(manifest.kept.values()),
        peak_rss_mb=round((peak[0] - base) / 2**20, 1),
        arrow_pool_peak_mb=round((pa.default_memory_pool().max_memory() or 0) / 2**20, 1),
        seconds=round(seconds, 2),
        sha256=manifest.sha256,
    )


def _in_a_child[R](function: Callable[..., R], *args: object) -> R:
    """Run one call in a fresh process that exits after it."""
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
        return pool.submit(function, *args).result()


def measure(sizes: Sequence[int], work: Path) -> dict[str, Any]:
    """Finalise an hour at each size and fit memory against rows.

    Args:
        sizes: Staged rows per hour, at least two distinct.
        work: A scratch directory; emptied per size and removed after.

    Returns:
        The report: every reading, the fit, and the projection to a live hour.
    """
    readings: list[Reading] = []
    for rows in sizes:
        root = work / f"hour-{rows}"
        shutil.rmtree(root, ignore_errors=True)
        _in_a_child(write_hour, root, rows)
        readings.append(_in_a_child(finalise_measured, root))
        shutil.rmtree(root, ignore_errors=True)
    x = np.array([r.rows for r in readings], dtype=np.float64)
    y = np.array([r.peak_rss_mb for r in readings], dtype=np.float64)
    slope, intercept = np.polyfit(x, y, 1)
    return {
        "measured_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "host": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        "what": "peak resident memory above baseline while finalising a day of one staged hour",
        "acted_share": 0.04,
        "readings": [asdict(r) for r in readings],
        "mb_per_million_rows": round(float(slope) * 1e6, 1),
        "fixed_mb": round(float(intercept), 1),
        "live_hour_estimate_mb": round(float(slope * LIVE_HOUR_ROWS + intercept)),
    }
