"""The scorer's metrics, in Prometheus form.

What a running scorer reports, and why each one is there:

- `verdict_decisions_total{action,model_version}`, counter: the decision
  mix, and which model made each; a rollback shows here within one event.
- `verdict_hop_seconds{hop}`, histogram: the hops the latency budget divides
  into, the same ones the load test reports.
- `verdict_commit_seconds{part}`, histogram: flush and checkpoint per batch,
  which ADR 9 found is the platform's own ceiling.
- `verdict_batch_records`, histogram: what that per-batch cost was spread
  over; batches that keep growing mean a queue is standing.
- `verdict_duplicates_total`, counter: redeliveries the ledger turned away.
- `verdict_dead_letters_total{reason}`, counter: records set aside undecided
  (ADR 8 addendum), which nothing else counts.
- `verdict_last_decision_timestamp_seconds`, gauge: liveness; a scorer that
  has stopped deciding stops moving it.
- `verdict_engine_snapshot_timestamp_seconds`, gauge, with
  `verdict_engine_snapshot_seconds` and `_bytes`: when the feature state was
  last saved whole, how long the pass took and how big it was (ADR 27). A
  save that stops moving means a replacement will replay further, or start
  cold.
- `verdict_engine_restored`, gauge, with `verdict_engine_restore_seconds`:
  whether this scorer started from saved state (1) or empty (0), and how long
  the restore took; `verdict_engine_replayed_records` is how far it replayed.

What it does not report, and why: end-to-end latency from the moment of ingest,
because the scorer does not know when a transaction entered the stream. That
figure is the load test's (`scoring/loadtest.py`), which times the send
itself. A metric that approximated it from event time would be measuring the
generator's clock, not the platform.

Histogram buckets are fixed here and not tuned: 50 microseconds to 1 second,
dense below 5 ms where every scorer hop lives, so a hop that moves from 0.3 ms
to 3 ms is visible, and wide above, where the commit and a standing queue
live.

The registry is this object's own, never the process-global one, so two
scorers in one test do not share counts and nothing imported by accident
appears on the scrape.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Final

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from verdict.scoring.timing import HopSample

if TYPE_CHECKING:  # pragma: no cover - types only
    from verdict.events.schema import DecisionEvent, TransactionEvent
    from verdict.scoring.consumer import StreamScorer
    from verdict.scoring.recovery import SavedPass

HOP_BUCKETS: Final[tuple[float, ...]] = (
    0.00005,
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
)
"""Seconds. Dense below 5 ms, where the scorer's hops are; wide above."""

AGE_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    1.0,
    5.0,
    30.0,
    60.0,
    120.0,
    300.0,
    3600.0,
)
"""Seconds from event time to decision: the budget's 50 ms in the middle, and
room above for a feed catching up after an interruption, which is reported,
not dropped. 60 and 120 s since 2026-09-22: without them a catch-up of a
few minutes read as five on the dashboard."""

BATCH_BUCKETS: Final[tuple[float, ...]] = (1, 2, 5, 10, 20, 50, 100, 200, 500)
"""Records per batch, up to the scorer's own maximum of 500."""

SCORER_HOPS: Final[tuple[str, ...]] = ("features", "model", "decision", "persist")
"""The hops the scorer can time itself. `ingest` needs the send time, which it does not have."""


class ScorerMetrics:
    """Metrics for one scorer, on a registry of their own."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Create the metrics.

        Args:
            registry: Where to register them. A new one if None.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self.decisions = Counter(
            "verdict_decisions",
            "Decisions made, by action and by the model that made them.",
            ("action", "model_version"),
            registry=self.registry,
        )
        self.hops = Histogram(
            "verdict_hop_seconds",
            "Time per hop of a decision, as the scorer measures it.",
            ("hop",),
            buckets=HOP_BUCKETS,
            registry=self.registry,
        )
        self.commits = Histogram(
            "verdict_commit_seconds",
            "Time per batch spent waiting for decisions to be durable, and checkpointing.",
            ("part",),
            buckets=HOP_BUCKETS,
            registry=self.registry,
        )
        self.batch_records = Histogram(
            "verdict_batch_records",
            "Records per batch the scorer consumed.",
            buckets=BATCH_BUCKETS,
            registry=self.registry,
        )
        self.duplicates = Counter(
            "verdict_duplicates",
            "Redelivered transactions the ledger turned away.",
            registry=self.registry,
        )
        self.dead_letters = Counter(
            "verdict_dead_letters",
            "Records set aside undecided, by reason.",
            ("reason",),
            registry=self.registry,
        )
        self.event_age = Histogram(
            "verdict_event_to_decision_seconds",
            "Event time to decision time. On the live stack the feed sends each "
            "transaction when its event time comes, so this is ingest to decision; "
            "in a load test, whose event times are synthetic, it means nothing.",
            buckets=AGE_BUCKETS,
            registry=self.registry,
        )
        self.last_decision = Gauge(
            "verdict_last_decision_timestamp_seconds",
            "Wall-clock time of the most recent decision.",
            registry=self.registry,
        )
        self.snapshot_at = Gauge(
            "verdict_engine_snapshot_timestamp_seconds",
            "Wall-clock time the feature state was last saved whole.",
            registry=self.registry,
        )
        self.snapshot_seconds = Gauge(
            "verdict_engine_snapshot_seconds",
            "How long the last complete save of the feature state took.",
            registry=self.registry,
        )
        self.snapshot_bytes = Gauge(
            "verdict_engine_snapshot_bytes",
            "Size of the last complete save of the feature state.",
            registry=self.registry,
        )
        self.restored = Gauge(
            "verdict_engine_restored",
            "1 if this scorer started from saved feature state, 0 if from empty windows.",
            registry=self.registry,
        )
        self.restore_seconds = Gauge(
            "verdict_engine_restore_seconds",
            "How long restoring the feature state took at start.",
            registry=self.registry,
        )
        self.replayed = Gauge(
            "verdict_engine_replayed_records",
            "Records replayed onto the saved feature state at start.",
            registry=self.registry,
        )
        self._seen_duplicates = 0
        self._seen_dead: dict[str, int] = {}

    def on_saved(self, saved: SavedPass) -> None:
        """Record a complete save of the feature state. Shaped to be `on_saved`.

        Args:
            saved: What the pass did.
        """
        self.snapshot_at.set(time.time())
        self.snapshot_seconds.set(saved.seconds)
        self.snapshot_bytes.set(saved.bytes)

    def on_decided(
        self, event: TransactionEvent, decision: DecisionEvent, sample: HopSample
    ) -> None:
        """Record one decision. Shaped to be the scorer's `on_decided` hook.

        Args:
            event: The transaction.
            decision: Its decision.
            sample: How long each hop took.
        """
        self.event_age.observe(max(0.0, (decision.decided_at - event.event_time).total_seconds()))
        self.decisions.labels(decision.action.value, decision.model_version).inc()
        self.hops.labels("features").observe(sample.features_ns / 1e9)
        self.hops.labels("model").observe(sample.model_ns / 1e9)
        self.hops.labels("decision").observe(sample.decision_ns / 1e9)
        self.hops.labels("persist").observe(sample.persist_ns / 1e9)
        self.last_decision.set(time.time())

    def after_poll(self, scorer: StreamScorer, records: int) -> None:
        """Record what one poll did, and take the scorer's per-batch timings off it.

        The scorer keeps its per-batch timings in lists so the load test can
        compute percentiles from them. A service that ran for months with
        those lists growing would hold roughly a gigabyte a day at the live
        rate, so this drains them into the histograms as it goes.

        Args:
            scorer: The scorer that was polled.
            records: How many records the poll returned.
        """
        if records:
            self.batch_records.observe(records)
        commit_ns, flush_ns, checkpoint_ns = scorer.commits.drain()
        del commit_ns
        for value in flush_ns:
            self.commits.labels("flush").observe(value / 1e9)
        for value in checkpoint_ns:
            self.commits.labels("checkpoint").observe(value / 1e9)
        duplicates = scorer.decider.stats.duplicates
        if duplicates > self._seen_duplicates:
            self.duplicates.inc(duplicates - self._seen_duplicates)
            self._seen_duplicates = duplicates
        for reason, count in scorer.dead_letters.items():
            seen = self._seen_dead.get(reason, 0)
            if count > seen:
                self.dead_letters.labels(reason).inc(count - seen)
                self._seen_dead[reason] = count

    def exposition(self) -> bytes:
        """The metrics in Prometheus's text format, as a scrape would see them.

        Returns:
            The text, UTF-8.
        """
        return generate_latest(self.registry)
