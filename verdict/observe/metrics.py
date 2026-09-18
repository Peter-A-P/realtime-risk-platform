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
        self.last_decision = Gauge(
            "verdict_last_decision_timestamp_seconds",
            "Wall-clock time of the most recent decision.",
            registry=self.registry,
        )
        self._seen_duplicates = 0
        self._seen_dead: dict[str, int] = {}

    def on_decided(
        self, event: TransactionEvent, decision: DecisionEvent, sample: HopSample
    ) -> None:
        """Record one decision. Shaped to be the scorer's `on_decided` hook.

        Args:
            event: The transaction.
            decision: Its decision.
            sample: How long each hop took.
        """
        del event
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
