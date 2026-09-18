"""What the path to the broker costs, apart from the broker.

The scorer's latency is dominated by one call: `flush`, which waits for the
decisions of a batch to be acknowledged before their transactions are
checkpointed. On the development host that call was measured at either about
6 ms or about 47 ms, with no setting of the client changing which, and with
the figure fixed for the life of a producer and redrawn when a new one opened.
This module is what established that: it opens a series of producers to one
topic, flushes small batches through each, and reports the median per
producer, so the spread between connections is visible rather than averaged
away.

Run it twice, from the host and from inside the broker's own network, and the
difference between them is the host's path to the broker and nothing else:

    verdict flush-probe --out docs/latency-week4-flush-host.json

    docker run --rm --network verdict_default -v "$PWD:/app" -w /app \
        python:3.13-slim sh -c \
        "pip install -q confluent-kafka && \
         python -m verdict.stream.probe redpanda:9092 /app/in-network.json"

The module runs from the command line as well as importing, and needs only
`confluent-kafka` to do it, so the second command carries no more of this
repository into the container than the measurement itself.

`docs/latency-budget.md` reports what that came to and what follows from it.
Nothing here is part of the platform: it is a measuring instrument, kept in
the repository because a number nobody else can reproduce is not a result.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Final

from verdict.scoring.timing import percentile
from verdict.stream.redpanda import DEFAULT_BOOTSTRAP

SLOW_FLUSH_MS: Final = 25.0
"""Above this, a connection is counted as having drawn the slow path.

The two modes measured on the development host are about 6 ms and about
47 ms, so any threshold between them divides the same way; this one is
halfway in the log sense and far from both. It is a label for reporting, not
a threshold anything depends on.
"""

PAYLOAD_BYTES: Final = 400
"""About the size of a decision record, so the flush moves a realistic batch."""


@dataclass(frozen=True, slots=True)
class ConnectionFlushes:
    """How long flushing cost on one producer connection, in milliseconds.

    Attributes:
        connection: Which producer, in the order they were opened.
        p50: Median flush.
        p95: 95th percentile flush.
        fastest: The quickest flush.
        slowest: The slowest.
        path: `"fast"` or `"slow"`, by `SLOW_FLUSH_MS`.
    """

    connection: int
    p50: float
    p95: float
    fastest: float
    slowest: float
    path: str


def flush_by_connection(
    bootstrap: str = DEFAULT_BOOTSTRAP,
    *,
    connections: int = 10,
    batches: int = 60,
    batch_size: int = 20,
    linger_ms: int = 2,
) -> list[ConnectionFlushes]:
    """Open producers in turn and time the flushes on each.

    Every producer gets the same configuration and the same topic, and the
    first record through it is sent and flushed before the clock starts, so
    that connecting and settling the partition leaders is not being measured.

    Args:
        bootstrap: Broker address.
        connections: How many producers to open, one after another.
        batches: Flushes per producer.
        batch_size: Records per flush.
        linger_ms: The producer's batching wait, matched to the platform's.

    Returns:
        One result per connection, in the order opened.
    """
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.cimpl import NewTopic

    base: dict[str, Any] = {"bootstrap.servers": bootstrap, "client.id": "verdict-probe"}
    admin = AdminClient(base)
    topic = f"probe-{uuid.uuid4().hex[:8]}"
    for future in admin.create_topics([NewTopic(topic, 4, 1)]).values():
        future.result(timeout=15)
    settings = {**base, "enable.idempotence": True, "acks": "all", "linger.ms": linger_ms}

    results: list[ConnectionFlushes] = []
    try:
        for connection in range(1, connections + 1):
            producer = Producer(settings)
            producer.produce(topic, key="warm", value=b"warm")
            producer.flush(10)
            time.sleep(0.3)
            flushes: list[float] = []
            for batch in range(batches):
                for index in range(batch_size):
                    producer.produce(topic, key=f"{batch}-{index}", value=b"x" * PAYLOAD_BYTES)
                started = time.perf_counter_ns()
                producer.flush(10)
                flushes.append((time.perf_counter_ns() - started) / 1e6)
                time.sleep(0.02)
            middle = percentile(flushes, 50)
            results.append(
                ConnectionFlushes(
                    connection=connection,
                    p50=middle,
                    p95=percentile(flushes, 95),
                    fastest=min(flushes),
                    slowest=max(flushes),
                    path="slow" if middle > SLOW_FLUSH_MS else "fast",
                )
            )
            del producer
    finally:
        for future in admin.delete_topics([topic]).values():
            future.result(timeout=15)
    return results


def summarise(results: list[ConnectionFlushes]) -> dict[str, Any]:
    """Count the connections on each path and report each path's median.

    A mean across connections would land between the two modes, where no
    connection ever is, so the two are reported apart.

    Args:
        results: One per connection.

    Returns:
        The counts and the medians, in milliseconds.

    Raises:
        ValueError: If there are no results.
    """
    if not results:
        msg = "no connections were measured"
        raise ValueError(msg)
    by_path: dict[str, list[float]] = {"fast": [], "slow": []}
    for result in results:
        by_path[result.path].append(result.p50)
    return {
        "connections": len(results),
        "slow_flush_threshold_ms": SLOW_FLUSH_MS,
        "by_path": {
            path: {
                "connections": len(values),
                "median_flush_p50_ms": round(percentile(values, 50), 3) if values else None,
            }
            for path, values in by_path.items()
        },
    }


def _run_from_the_command_line() -> None:  # pragma: no cover - the container entry point
    """Measure and write a report, for the run inside the broker's network.

    Takes the broker address and where to write, because the container this
    runs in has none of the project's configuration.
    """
    import dataclasses
    import json
    import sys
    from pathlib import Path

    bootstrap = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BOOTSTRAP
    results = flush_by_connection(bootstrap)
    report = {
        "bootstrap": bootstrap,
        "summary": summarise(results),
        "per_connection": [dataclasses.asdict(result) for result in results],
    }
    text = json.dumps(report, indent=2)
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":  # pragma: no cover - the container entry point
    _run_from_the_command_line()
