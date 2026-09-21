"""The scorer as a long-running service: poll until told to stop, then stop cleanly.

Everything a decision needs is in `consumer.py` and `core.py`; this is the loop
around them, and the three things a loop adds.

- **Stopping never splits a batch.** The stop request is checked between
  polls, never inside one, so the batch in hand is decided, flushed and
  checkpointed before the loop returns. A stop is therefore at most one batch
  late, and never leaves a decision written and its transaction unacknowledged
  in a way a restart would have to sort out beyond what at least once already
  covers.
- **Metrics are drained as it goes.** See `observe/metrics.py`: the scorer's
  per-batch timing lists are for the load test, and would grow without bound
  in a service.
- **The collector never walks the feature state.** The engine holds tens of
  millions of small objects with no reference cycles among them, and
  Python's full collection walks them all: measured on the engine alone, a
  2 s pause at half a million entities and 8.7 s at two million, which on
  the live stack is every decision behind it. So what has survived a batch
  is frozen (`gc.freeze`): reference counting still frees it, and the
  collector looks only at what is newer. Cyclic garbage that is frozen
  before it is collected is kept, which is why the dry run watches memory.
- **What it does not do yet, stated rather than discovered.** A scorer that
  starts cold serves every card "no history" until its windows refill, which
  for the longest window is a day. ADR 8 records that; rebuilding the windows
  by replaying the stream before scoring is recovery work for the live stack.
"""

from __future__ import annotations

import gc
import threading
from dataclasses import dataclass

from verdict.observe.metrics import ScorerMetrics
from verdict.scoring.consumer import StreamScorer


@dataclass(frozen=True, slots=True)
class RunSummary:
    """What a service run did, for the log line it ends with.

    Attributes:
        polls: Polls made.
        records: Records consumed, duplicates and dead letters included.
        decided: Transactions decided.
        set_aside: Records sent to the dead-letter topic, by reason.
    """

    polls: int
    records: int
    decided: int
    set_aside: dict[str, int]


def run(
    scorer: StreamScorer,
    *,
    stop: threading.Event,
    metrics: ScorerMetrics | None = None,
    max_records: int = 500,
    timeout_seconds: float = 0.1,
) -> RunSummary:
    """Poll until `stop` is set, finishing the batch in hand.

    Args:
        scorer: The scorer, already built on its stream.
        stop: Set it to ask the loop to stop after the current batch.
        metrics: Where to report, if anywhere. When given, the scorer's
            per-batch timing lists are drained into it after every poll.
        max_records: The most records per batch.
        timeout_seconds: How long one poll waits when the stream is quiet,
            which is also the longest a stop request waits to be seen.

    Returns:
        What the run did.
    """
    polls = records = 0
    while not stop.is_set():
        got = scorer.poll(max_records=max_records, timeout_seconds=timeout_seconds)
        polls += 1
        records += got
        if got:
            gc.freeze()
        if metrics is not None:
            metrics.after_poll(scorer, got)
    return RunSummary(
        polls=polls,
        records=records,
        decided=scorer.decider.stats.decided,
        set_aside=dict(scorer.dead_letters),
    )
