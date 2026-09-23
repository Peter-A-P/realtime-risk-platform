"""The label collector: labels off the stream, into the label spool.

Labels arrive seven days after their transactions (ADR 10), on their own
topic, and the topic keeps a day (ADR 18). So they are written down as they
come, filed by their own label time, and checkpointed only after they are
written: the same order the scorer keeps with decisions, for the same reason.

A label this build cannot read is counted and skipped rather than stopping
the collector. A lost label is visible later, in the day's manifest, as a
candidate with no label; a stopped collector loses every label after it once
the topic's day has passed.

So the collector reports the label time it has reached, and the labels feed
reports the label time it has sent up to: the difference is how far behind
the collector is, and an alert fires long before it nears the topic's day
(`deploy/live/compose.yml`, the alert rules; ADR 26).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Final

from prometheus_client import CollectorRegistry, Counter, Gauge
from pydantic import ValidationError

from verdict.events.schema import LabelEvent
from verdict.history.records import label_row
from verdict.history.spool import SpoolWriter
from verdict.stream.base import Stream

LABELS_TOPIC: Final = "labels"
COLLECTOR_GROUP: Final = "label-collector"


@dataclass(slots=True)
class CollectorStats:
    """Counts a collector keeps.

    Attributes:
        written: Labels written to the spool.
        unreadable: Records that were not a label this build can read.
        latest: The latest label time seen.
    """

    written: int = 0
    unreadable: int = 0
    latest: dt.datetime | None = None


class CollectorMetrics:
    """What the collector reports, on a registry of its own."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        """Create the metrics.

        Args:
            registry: Where to register them. A new one if None.
        """
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self.written = Counter(
            "verdict_labels_written",
            "Labels the collector has written to the label spool.",
            registry=self.registry,
        )
        self.unreadable = Counter(
            "verdict_labels_unreadable",
            "Records on the label topic this build could not read, skipped.",
            registry=self.registry,
        )
        self.latest = Gauge(
            "verdict_labels_latest_label_timestamp_seconds",
            "The latest label time the collector has written; 0 until its first label.",
            registry=self.registry,
        )


class LabelCollector:
    """Consumes labels and spools them."""

    def __init__(
        self,
        stream: Stream,
        spool: SpoolWriter,
        *,
        topic: str = LABELS_TOPIC,
        group: str = COLLECTOR_GROUP,
        metrics: CollectorMetrics | None = None,
    ) -> None:
        """Assemble the collector.

        Args:
            stream: Where labels come from.
            spool: Where they are written.
            topic: The label topic.
            group: The consumer group.
            metrics: Where counts go.
        """
        self.stream = stream
        self.spool = spool
        self.topic = topic
        self.group = group
        self.stats = CollectorStats()
        self.metrics = metrics or CollectorMetrics()

    def poll(self, max_records: int = 5_000, timeout_seconds: float = 0.5) -> int:
        """Consume one batch, spool it, checkpoint it.

        Args:
            max_records: The most labels to take.
            timeout_seconds: How long to wait for any.

        Returns:
            How many records the batch held.
        """
        records = self.stream.consume(
            self.topic, self.group, max_records=max_records, timeout_seconds=timeout_seconds
        )
        if not records:
            return 0
        written = unreadable = 0
        for record in records:
            try:
                label = LabelEvent.model_validate_json(record.value)
            except ValidationError:
                unreadable += 1
                continue
            self.spool.append(label.label_time, label_row(label))
            written += 1
            if self.stats.latest is None or label.label_time > self.stats.latest:
                self.stats.latest = label.label_time
        self.spool.flush()
        self.stats.written += written
        self.stats.unreadable += unreadable
        if self.stats.latest is not None:
            # An hour older than the one before the latest is done: labels come
            # nearly in order, and one that does not opens a new file.
            self.spool.close_before(self.stats.latest - dt.timedelta(hours=1))
        self.stream.checkpoint(self.topic, self.group, [record.position for record in records])
        # Counted once the batch is durable and checkpointed, so the metric
        # never claims a label a crash could still lose.
        self.metrics.written.inc(written)
        self.metrics.unreadable.inc(unreadable)
        if self.stats.latest is not None:
            self.metrics.latest.set(self.stats.latest.timestamp())
        return len(records)
