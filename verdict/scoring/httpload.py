"""Driving the HTTP endpoint at a fixed rate, for the comparison it loses.

`PLAN.md` section 2.4 keeps a synchronous HTTP endpoint beside the stream
consumer and compares them at the live rate on the same host (Rule C candidate
3). Both make the same decision through the same `core.Decider`, so the
comparison is between transports. This is the client for the HTTP half;
`loadtest.py` is the stream half, and the two report the same statistics from
the same generated events so that their numbers can be set beside each other.

**What this measures, stated before the numbers.**

- **The server runs in its own process**, started here under uvicorn and shut
  down at the end, for the same reason the stream test's producer does: a load
  client behind the same interpreter lock as the thing it is loading measures
  itself. The client is this process; the server is another.
- **Three timestamps per transaction, not two.** `offered` is when the
  transaction was due to be sent, `sent` is when a connection was free to send
  it, and `received` is when the answer came back. `end_to_end` runs from
  `offered`, which is the same thing the stream test measures from, and it
  includes `wait`, the time a transaction spent with no free connection.
  Reporting only the round trip would hide the queue that forms when the
  offered rate exceeds what the connections can carry, and that queue is the
  point of the comparison.
- **Connections are a parameter and the result says how many.** One connection
  cannot exceed one transaction per round trip, so reaching the live rate
  needs several; but several deliver out of event-time order, and the engine
  refuses a late transaction rather than corrupt its windows (409). Both the
  achieved rate and the refusals are reported, because that trade is the
  finding, not a defect to tune away.
- **The server's own `Server-Timing` header is read back**, so the round trip
  splits into the hops the endpoint reports and a remainder, `transport`,
  which is the network, the web server and the serialisation.
- The model is the stand-in (`model.py`), as in the stream test.
"""

from __future__ import annotations

import contextlib
import http.client
import queue
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Final

from verdict.events.schema import TransactionEvent
from verdict.scoring.http_api import SCORE_PATH
from verdict.scoring.timing import fine_grained_timers, percentile, t_interval

SERVER_HOPS: Final[tuple[str, ...]] = ("queue", "features", "model", "decision", "persist")
"""The hops `http_api.py` reports in `Server-Timing`, in the order it makes them."""

READY_TIMEOUT_SECONDS: Final = 30.0
"""How long to wait for the server subprocess to answer `/healthz`."""

TIMER_SLACK_NS: Final = 2_000_000
"""Sleep the whole wait when this far ahead of schedule, otherwise yield.

The same rule as `loadtest.TIMER_SLACK_NS`, and for the same reason: a pacing
loop that spins rather than sleeps starves every other thread in its process.
"""


@dataclass(frozen=True, slots=True)
class Exchange:
    """One request, in nanoseconds on this process's clock.

    Attributes:
        offered_ns: When the transaction was due to be sent.
        sent_ns: When a connection was free and the request began.
        received_ns: When the response was complete.
        status: The HTTP status.
        server_ns: The hops the server reported, empty if it reported none.
    """

    offered_ns: int
    sent_ns: int
    received_ns: int
    status: int
    server_ns: dict[str, float]


def parse_server_timing(header: str | None) -> dict[str, float]:
    """Read a `Server-Timing` header into nanoseconds per hop.

    Args:
        header: The header value, or None.

    Returns:
        Hop to nanoseconds. Entries without a duration are skipped, and so is
        anything the endpoint does not name a hop.
    """
    if not header:
        return {}
    out: dict[str, float] = {}
    for part in header.split(","):
        name, _, rest = part.strip().partition(";")
        if name not in SERVER_HOPS:
            continue
        for field in rest.split(";"):
            key, _, value = field.partition("=")
            if key.strip() == "dur":
                with contextlib.suppress(ValueError):
                    out[name] = float(value) * 1e6
    return out


@contextlib.contextmanager
def a_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8099,
    stream: str = "none",
    bootstrap: str | None = None,
    durable: bool = True,
) -> Iterator[str]:
    """Start the endpoint in its own process and wait for it to answer.

    Args:
        host: Where it listens.
        port: The port.
        stream: `"none"` to write decisions nowhere, or `"redpanda"`.
        bootstrap: Broker address when the stream is a broker.
        durable: Whether the endpoint flushes each decision before responding.

    Yields:
        The base URL.

    Raises:
        RuntimeError: If the server does not answer in time.
    """
    from verdict.stream.redpanda import DEFAULT_BOOTSTRAP

    child = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "verdict.scoring.httpload",
            host,
            str(port),
            stream,
            bootstrap or DEFAULT_BOOTSTRAP,
            "durable" if durable else "fast",
        ]
    )
    base = f"{host}:{port}"
    try:
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while True:
            if child.poll() is not None:
                msg = f"the scorer's server exited with {child.returncode} before answering"
                raise RuntimeError(msg)
            try:
                connection = http.client.HTTPConnection(base, timeout=1.0)
                connection.request("GET", "/healthz")
                if connection.getresponse().status == 200:
                    connection.close()
                    break
                connection.close()
            except OSError:
                pass
            if time.monotonic() > deadline:
                msg = f"the scorer's server did not answer at {base} in {READY_TIMEOUT_SECONDS}s"
                raise RuntimeError(msg)
            time.sleep(0.1)
        yield base
    finally:
        child.terminate()
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - a wedged server
            child.kill()


@dataclass(frozen=True, slots=True)
class HttpRunResult:
    """One run's measurements, in milliseconds unless named otherwise.

    Attributes:
        sent: Transactions offered.
        measured: Exchanges kept, after the warm-up.
        fine_timer: Whether a 1 ms Windows timer was held.
        connections: How many connections carried the load.
        offered_rate: Transactions per second the client offered.
        achieved_rate: Responses per second it actually completed.
        end_to_end: p50, p95, p99 from due to answered.
        wait: Of that, time with no free connection.
        round_trip: Of that, time from the request beginning to the answer.
        transport: Round trip less everything the server reported, which is
            the network, the web server and the serialisation.
        server: Per hop, p50 and p99, as the endpoint reported them.
        backlog: Median end to end over the first and last tenth.
        answered: Responses by status code.
    """

    sent: int
    measured: int
    fine_timer: bool
    connections: int
    offered_rate: float
    achieved_rate: float
    end_to_end: dict[str, float]
    wait: dict[str, float]
    round_trip: dict[str, float]
    transport: dict[str, float]
    server: dict[str, dict[str, float]]
    backlog: dict[str, float]
    answered: dict[str, int]


def _ms(values: list[float], q: float) -> float:
    return percentile([value / 1e6 for value in values], q)


def _send_one(connection: http.client.HTTPConnection, payload: bytes, offered_ns: int) -> Exchange:
    """Send one transaction and read its answer.

    Args:
        connection: A connection, kept open between requests.
        payload: The transaction, as JSON.
        offered_ns: When it was due.

    Returns:
        The exchange.
    """
    sent_ns = time.perf_counter_ns()
    connection.request(
        "POST", SCORE_PATH, body=payload, headers={"Content-Type": "application/json"}
    )
    response = connection.getresponse()
    header = response.getheader("Server-Timing")
    response.read()
    return Exchange(
        offered_ns=offered_ns,
        sent_ns=sent_ns,
        received_ns=time.perf_counter_ns(),
        status=response.status,
        server_ns=parse_server_timing(header),
    )


def run_once(
    base: str,
    events: list[TransactionEvent],
    *,
    rate: float,
    warmup: int,
    connections: int = 1,
) -> HttpRunResult:
    """Offer transactions at a rate over a fixed number of connections.

    Args:
        base: Host and port of the running endpoint.
        events: What to send, in time order.
        rate: Target transactions per second offered.
        warmup: Exchanges to leave out of the statistics.
        connections: How many connections carry the load.

    Returns:
        The run's measurements.

    Raises:
        RuntimeError: If a worker could not complete its requests.
    """
    work: queue.Queue[tuple[bytes, int] | None] = queue.Queue()
    done: list[Exchange] = []
    failures: list[str] = []
    guard = threading.Lock()

    def worker() -> None:
        connection = http.client.HTTPConnection(base, timeout=30.0)
        try:
            while (item := work.get()) is not None:
                payload, offered_ns = item
                try:
                    exchange = _send_one(connection, payload, offered_ns)
                except OSError as error:  # pragma: no cover - a dropped connection
                    with guard:
                        failures.append(str(error))
                    return
                with guard:
                    done.append(exchange)
        finally:
            connection.close()

    timers = fine_grained_timers()
    granted = timers.__enter__()
    threads = [
        threading.Thread(target=worker, name=f"client-{index}", daemon=True)
        for index in range(connections)
    ]
    try:
        for thread in threads:
            thread.start()
        payloads = [event.to_json().encode("utf-8") for event in events]
        interval_ns = int(1e9 / rate)
        started = time.perf_counter_ns()
        for index, payload in enumerate(payloads):
            due = started + index * interval_ns
            ahead = due - time.perf_counter_ns()
            if ahead > TIMER_SLACK_NS:
                time.sleep(ahead / 1e9)
            elif ahead > 0:
                time.sleep(0)
            work.put((payload, max(due, time.perf_counter_ns())))
        offered_seconds = (time.perf_counter_ns() - started) / 1e9
        for _ in threads:
            work.put(None)
        for thread in threads:
            thread.join(timeout=120)
        finished = time.perf_counter_ns()
    finally:
        timers.__exit__(None, None, None)
    if failures:
        msg = f"{len(failures)} connections failed, first: {failures[0]}"
        raise RuntimeError(msg)

    done.sort(key=lambda exchange: exchange.offered_ns)
    kept = done[warmup:]
    if not kept:
        msg = f"the warm-up of {warmup} left nothing of {len(done)} exchanges"
        raise RuntimeError(msg)
    end_to_end = [float(e.received_ns - e.offered_ns) for e in kept]
    waits = [float(e.sent_ns - e.offered_ns) for e in kept]
    round_trips = [float(e.received_ns - e.sent_ns) for e in kept]
    transport = [
        float(e.received_ns - e.sent_ns) - sum(e.server_ns.values()) for e in kept if e.server_ns
    ]
    answered: dict[str, int] = {}
    for exchange in kept:
        key = str(exchange.status)
        answered[key] = answered.get(key, 0) + 1
    tenth = max(1, len(end_to_end) // 10)
    return HttpRunResult(
        sent=len(events),
        measured=len(kept),
        fine_timer=granted,
        connections=connections,
        offered_rate=len(events) / offered_seconds,
        achieved_rate=len(done) / ((finished - started) / 1e9),
        end_to_end={f"p{q}": _ms(end_to_end, q) for q in (50, 95, 99)},
        wait={f"p{q}": _ms(waits, q) for q in (50, 95, 99)},
        round_trip={f"p{q}": _ms(round_trips, q) for q in (50, 95, 99)},
        transport={f"p{q}": _ms(transport, q) if transport else 0.0 for q in (50, 99)},
        server={
            hop: {
                f"p{q}": _ms([e.server_ns[hop] for e in kept if hop in e.server_ns], q)
                if any(hop in e.server_ns for e in kept)
                else 0.0
                for q in (50, 99)
            }
            for hop in SERVER_HOPS
        },
        backlog={
            "early_p50": _ms(end_to_end[:tenth], 50),
            "late_p50": _ms(end_to_end[-tenth:], 50),
        },
        answered=answered,
    )


def summarise(results: list[HttpRunResult]) -> dict[str, Any]:
    """Across-run means with 95 percent t intervals.

    Args:
        results: At least two runs.

    Returns:
        A report of intervals, in milliseconds.
    """

    def across(values: list[float]) -> dict[str, float | int]:
        return t_interval(values).rounded(3)

    return {
        "end_to_end_ms": {
            q: across([r.end_to_end[q] for r in results]) for q in ("p50", "p95", "p99")
        },
        "wait_for_a_connection_ms": {
            q: across([r.wait[q] for r in results]) for q in ("p50", "p95", "p99")
        },
        "round_trip_ms": {
            q: across([r.round_trip[q] for r in results]) for q in ("p50", "p95", "p99")
        },
        "transport_ms": {q: across([r.transport[q] for r in results]) for q in ("p50", "p99")},
        "server_hops_ms": {
            hop: {q: across([r.server[hop][q] for r in results]) for q in ("p50", "p99")}
            for hop in SERVER_HOPS
        },
        "backlog_ms": {
            q: across([r.backlog[q] for r in results]) for q in ("early_p50", "late_p50")
        },
        "offered_rate_per_second": across([r.offered_rate for r in results]),
        "achieved_rate_per_second": across([r.achieved_rate for r in results]),
        "answered": {
            status: sum(r.answered.get(status, 0) for r in results)
            for status in sorted({status for r in results for status in r.answered})
        },
    }


def _serve_for_a_parent() -> None:  # pragma: no cover - the server subprocess
    """Run the endpoint, for a parent that is about to load it.

    Takes its arguments from the command line, because the only thing it
    shares with its parent is a port.
    """
    import uvicorn

    from verdict.features.engine import FeatureEngine
    from verdict.scoring.core import Decider, EngineFeatures
    from verdict.scoring.http_api import create_app
    from verdict.scoring.model import FixedModel, StandInModel

    host, port, which, bootstrap, durability = sys.argv[1:6]
    stream = None
    if which == "redpanda":
        from verdict.stream.redpanda import RedpandaStream

        stream = RedpandaStream(bootstrap)
        suffix = uuid.uuid4().hex[:8]
        stream.create_topic(f"http-decisions-{suffix}", 4)
        decisions = f"http-decisions-{suffix}"
    else:
        decisions = "decisions"
    app = create_app(
        Decider(features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel())),
        stream=stream,
        decisions_topic=decisions,
        durable=durability == "durable",
    )
    uvicorn.run(app, host=host, port=int(port), log_level="warning", access_log=False)


if __name__ == "__main__":  # pragma: no cover - the server subprocess
    _serve_for_a_parent()


__all__ = [
    "Exchange",
    "HttpRunResult",
    "a_server",
    "parse_server_timing",
    "run_once",
    "summarise",
]
