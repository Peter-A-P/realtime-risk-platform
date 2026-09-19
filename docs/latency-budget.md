# The latency budget, and what the development host costs

`PLAN.md` section 2.4 budgets a decision in under 50 ms at the 99th percentile
at a thousand transactions a second, end to end from the moment a transaction
is handed to the stream to the moment its decision is handed back. ADR 9 is
the decision this document reports under; it says why the host's own costs are
measured apart from the platform's, and why the 50 ms figure is claimed from
the live stack rather than from here.

**Track: synthetic live, local.** Every number below was measured on the
development host, a Windows 11 laptop with the broker in Docker Desktop, with
the week 4 stand-in model (`verdict/scoring/model.py`), not a trained one.
Nothing here is the live-window result; that is week 7's, on Linux, and fills
the README's table.

## How to reproduce any of it

    docker compose -f deploy/compose/docker-compose.yml up -d

    verdict loadtest --stream memory   --out docs/latency-week4-memory.json
    verdict loadtest --stream redpanda --out docs/latency-week4-redpanda.json
    verdict flush-probe --connections 12 --out docs/latency-week4-flush-host.json

    docker run --rm --network verdict_default -v "$PWD:/app" -w /app \
        python:3.13-slim sh -c \
        "pip install -q confluent-kafka && python -m verdict.stream.probe \
         redpanda:9092 /app/docs/latency-week4-flush-in-network.json"

    # the whole load test inside the broker's network, no port forwarder
    docker run --rm --network verdict_default -v "$PWD:/app" -w /app \
        python:3.13-slim sh -c \
        "pip install -q -e . && verdict loadtest --stream redpanda \
         --bootstrap redpanda:9092 --out /app/docs/latency-week4-redpanda-in-network.json"

**Gate every run on the machine's load, not on a named process.** Other
projects train and backtest on the same machine. The second attempt below was
gated on one named job finishing, and another started two minutes before the
first run.

Each writes the JSON the tables here are read from. A run reports the rate it
achieved, whether it held a fine-grained timer, where its load producer ran,
how large its batches were, and whether a queue stood; two reports are
comparable only if those agree.

## What the scorer costs

This part is settled, and it is the smallest part of the total. Per hop, in
milliseconds, from `docs/latency-week4-redpanda-untuned.json` (five runs, 20 000
events each at 1 000/s, first 1 000 decisions of each run excluded):

| Hop | p50 | p99 |
|---|---|---|
| `features` | 0.081 | 0.340 |
| `model` | 0.003 | 0.009 |
| `decision` | 0.018 | 0.065 |
| `persist` | 0.010 | 0.048 |

Everything the scorer does to turn a transaction into a decision comes to
under 0.5 ms at the 99th percentile, with a stand-in model. The remaining hop,
`ingest`, is the wait from the send to the scorer starting work, and it is
where the rest of this document lives: 59.1 ms at p50 in the same runs.

## The three costs that belong to the host

Each was mistaken for a platform property first. Each is measured on its own.

### 1. The process's timer resolution: about 44 ms

Windows gives each process a timer of about 15.6 ms unless it asks for better,
and every wait in the process rounds up to the next tick, including the ones
inside the Kafka client.

| Produce to acknowledgement | p50 |
|---|---|
| Default process timer | 48.44 ms |
| `timeBeginPeriod(1)` held | 3.74 ms |

Same broker, same run, nothing else changed. The load test now holds a 1 ms
timer for every run and reports whether it was granted; the reports made
without one are kept as `docs/latency-week4-memory-untuned.json` and
`docs/latency-week4-redpanda-untuned.json`. On Linux there is nothing to ask
for.

### 2. The load producer sharing the scorer's interpreter: about 60 ms

Send to receive through Redpanda with no scorer in the loop, twelve runs in
shuffled order, six with the producer in its own process and six with it in a
thread beside the consumer:

| Producer | median of run p50 | range | runs with a standing queue |
|---|---|---|---|
| Its own process | 8.64 ms | 8.49 to 10.59 | 1 of 6 |
| A thread beside the consumer | 70.57 ms | 8.56 to 71.96 | 5 of 6 |

The same lock, held by a pacing loop that never slept at 1 000/s, also put
waits of over 100 ms into the `ingest` hop of the in-process backend, which
has no network in it at all. Both are fixed: on a broker the producer runs in
its own process, and the pacing loop yields.

### 3. Docker Desktop's port forwarder on Windows: about 41 ms, on some connections

The scorer's flush, timed on a series of fresh producer connections to one
topic, with identical settings (`verdict/stream/probe.py`):

| Where the client ran | Connections | Median flush p50 |
|---|---|---|
| Windows host, slow path | 9 of 12 | 47.72 ms |
| Windows host, fast path | 3 of 12 | 7.10 ms |
| Inside the broker's Docker network | 10 of 10 | 3.99 ms |

From `docs/latency-week4-flush-host.json` and
`docs/latency-week4-flush-in-network.json`, against the same broker minutes
apart. Which path a host connection draws is fixed for that connection's life
and redrawn when a producer opens; no client setting changed it (ADR 9 lists
what was tried). The container's bridge address is not routable from the host,
so this is not avoidable from Windows, only measurable.

**This is the reason the local stack cannot support the 50 ms claim.** A
scorer whose flush costs 47 ms per batch cannot hold a 50 ms end-to-end budget
at any rate, and that 47 ms is a Windows port forwarder rather than anything
the platform does.

## What is left: the per-batch durability cycle

After the three above, the platform's own cost is the order ADR 8 requires:
decide the batch, flush the decisions, then checkpoint the transactions. The
deciding is under 0.4 ms at p99. The flush and the synchronous offset commit
cost tens of milliseconds together, per batch, whatever the batch holds, so
they set a throughput ceiling; batches then grow until the scorer's throughput
meets the offered rate, and the queue that produces is the latency.

That is why every run now reports `backlog` (the median end to end over its
first and last tenth) and `records_per_batch`. A latency percentile on its own
cannot tell a fast pipeline from a queue that has stopped growing.

ADR 9 lists the two remedies, why neither is adopted in week 4, and what
evidence should decide them.

## End-to-end figures, on an idle machine

Measured 2026-09-18 from 23:20 to 23:57 UTC, after the machine had spent five
minutes under 15 percent CPU, with no other job's Python running at any point
and the CPU logged around every run (the history below explains why that
gate exists). 1,000 transactions per second offered, 20,000
per run, the first 1,000 of each run excluded, the week 4 stand-in model.
Milliseconds; mean of the per-run figure across runs, with a 95 percent t
interval.

| Path | Runs | p50 | p95 | p99 | Decided |
|---|---|---|---|---|---|
| In-process stream, Windows host | 5 | 1.16 [1.14, 1.17] | 1.98 [1.95, 2.01] | 5.50 [3.48, 7.52] | all |
| In-process stream, Linux container | 5 | 0.50 [0.49, 0.51] | 0.92 [0.89, 0.96] | 5.93 [2.97, 8.88] | all |
| Redpanda, from inside its network | 20 | 11.94 [11.36, 12.53] | 24.21 [16.33, 32.09] | 52.81 [36.27, 69.34] | all |
| Redpanda, from the Windows host | 5 | two modes, see below | | | all |

From `latency-week4-memory.json`, `latency-week4-memory-in-network.json`, the
four `latency-week4-redpanda-in-network-healthcheck-*.json` blocks, and
`latency-week4-redpanda.json`. The in-process rows have the load producer in
the scorer's own process, so they are a ceiling on what the scorer adds, not a
floor.

**Inside its network, Redpanda holds a p50 near 12 ms and a p95 near 17 ms in
most runs, and the 99th percentile is where the budget is lost.** 13 of the 20
runs had a p99 under 50 ms; the other 7 ranged from 55 to 132 ms. The slow runs
are episodes, not a standing queue (in all but one, the median over the last
tenth of the run stays near 11 ms), and in four of the seven the flush's own
p99 rises from about 1.6 ms to between 10 and 37 ms, so at least some of it
is on the broker's side of the flush. What, is not known. It is not the broker's health check (below). One earlier set of
five runs in the same place, `latency-week4-redpanda-in-network.json`, had a
run whose queue grew to several seconds (p99 16.7 s) and four clean runs; that
size of stall did not recur in the twenty after it, and it is reported rather
than dropped.

**From the Windows host the runs split by connection, not by setting.** Two of
five had a p50 near 13 ms and a p99 near 32 ms, two sat at a p50 near 76 ms
with a fast flush, and one was slow throughout (flush p50 79 ms). The port
forwarder (host cost 3, above) assigns a connection its path when it opens, and
the scorer opens two, a consumer and a producer; either drawing the slow path
puts about 60 ms under every decision. The t interval across those five runs
(p50 61.67, from -0.32 to 123.67) is arithmetic on two populations and is not
reported as a figure.

**What this does and does not say about the 50 ms budget.** On this laptop, with
the forwarder out of the path, the median and the 95th percentile are well
inside it and the 99th percentile is not reliably: 52.81 ms on average, with
runs on both sides. It is a local, Docker-on-Windows, single-core-broker
measurement of a stand-in model, and ADR 9 keeps the published claim for the
live stack. It is the best local evidence of where the live stack's tail will
be decided: at the broker, not in the scorer, whose own hops stay under 0.5 ms
at p99 in every run above.

### Rejected: the broker's health check as the cause of the stalls

The health check runs `rpk` inside the broker's container every five seconds,
on a broker started with `--smp=1 --overprovisioned`, which made it the obvious
suspect. Tested in four blocks of five runs, alternating on, off, off, on, so
drift over the half hour could not pass for an effect:

| Block | Health check | p50 | p99 | Runs with p99 over 50 ms |
|---|---|---|---|---|
| 1 | on | 13.13 [10.63, 15.63] | 73.84 [10.96, 136.72] | 3 of 5 |
| 2 | off | 11.33 [11.11, 11.54] | 37.94 [22.08, 53.80] | 1 of 5 |
| 3 | off | 11.85 [10.83, 12.87] | 65.62 [15.23, 116.02] | 3 of 5 |
| 4 | on | 11.47 [11.23, 11.71] | 33.82 [23.62, 44.02] | 0 of 5 |

Stalling runs appear with it on and with it off, in about equal number. The
broker is back on the compose file's own health check.

## The transport comparison (Rule C candidate 3)

The same events at the same rate into the HTTP endpoint (`verdict
http-loadtest`), the endpoint in its own process, writing its decisions
nowhere so that neither side has a broker in it; set beside the in-process
stream above, which also has none.

| Connections | Decided | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | all | 4 runs 2.1 to 2.4; 1 run fell behind | | 4 runs 96 to 132; 1 run 1,774 |
| 2 | 83.5 percent | 1.44 [1.31, 1.56] | 5.26 [-0.88, 11.40] | 49.24 [39.66, 58.81] |
| 4 | 61.4 percent | 1.70 [1.67, 1.73] | 2.22 [2.08, 2.35] | 7.75 [1.02, 14.48] |
| 8 | 60.9 percent | 1.69 [1.67, 1.70] | 2.14 [2.06, 2.21] | 6.03 [1.95, 10.12] |

From `latency-week4-http-c{1,2,4,8}.json`, taken in the same idle window.
"Decided" is the share the endpoint answered 200; the rest it refused with
409 as arriving after a later transaction, which the engine will not fold into
windows that have moved past it.

- **One connection keeps order and runs at its limit.** A request takes about
  0.8 ms there and back, so a single connection carries 1,000 a second with
  little to spare: four of five runs kept up with a p99 near 100 ms from
  requests queueing behind each other, and one fell behind by a second and did
  not recover (963 a second achieved).
- **More connections buy headroom by giving up order.** Over two or more, the
  tail drops to single-digit milliseconds at four and eight, and 16.5 to 39.1
  percent of transactions are refused undecided. A refused transaction is not a
  slow decision; it is no decision.
- **The stream does both.** The in-process consumer decided every transaction,
  in order, at a p99 of 5.50 ms on the same host. The consumer reads one
  ordered partition by construction, which is the whole of ADR 8's argument,
  and here it is measured.

Two things this does not settle. The HTTP load client is Python, so which side
limits the single connection, client or endpoint, is not separated; `PLAN.md`
names k6 for this load, and a client outside Python would settle it. And the
endpoint here writes nowhere; with a durable write per request it would pay a
flush per decision where the consumer pays one per batch, which can only widen
the gap.

## Before the idle machine: the untuned baseline

What the first measurement said, kept because the host costs above are
measured against it:

| Stream | p50 | p95 | p99 |
|---|---|---|---|
| In-process | 1.40 [1.30, 1.50] | 3.57 [2.30, 4.84] | 17.47 [1.12, 33.82] |
| Redpanda | 59.30 [27.60, 91.00] | 110.00 [48.92, 171.07] | 150.29 [50.91, 249.67] |

From the `-untuned.json` artefacts: no fine-grained timer, the load producer
in the scorer's thread, and the port forwarder in the path.

## Second attempt, 2026-09-18: what it established, and what it could not

All eight reports are in `docs/provisional/2026-09-18/`, with a note saying
why they are there and not here. In short: the runs were gated on one named
job on the build machine finishing, and another project's backtest started
two minutes before the first run and held 40 to 60 percent of the CPU
throughout. No tail and no throughput figure from them is a result. Two
comparisons are large and structural enough to survive it, and one question is
left open.

**1. The forwarder is the scorer's flush, not only the probe's.** The same load
test, run from a Linux container on the broker's own Docker network instead
of from the Windows host:

| Where the scorer ran | Flush p50, mean of 5 runs | Records per batch |
|---|---|---|
| Windows host, through the port forwarder | 55.22 ms (4 of 5 runs about 65 ms, 1 run 4.80 ms) | 115.8 |
| Container on the broker's network | about 2 ms in every run (1.70 to 2.93) | 25 to 49 |

ADR 9 found the forwarder with a probe; this is the whole scorer showing the
same thing. The batch size falls with it, which is the per-batch ceiling
(above) relaxing when the fixed cost falls.

**2. Withdrawn the same evening: "the in-process stream's tail was mostly the
Windows host."** Kept here rather than edited over, as `docs/leak-caught.md`
keeps its corrections. The claim was that the in-process stream's p99 of
323.12 ms on the Windows host against 17.54 ms in a Linux container, both
measured beside the other job, showed a Windows-specific tail. Measured on an
idle machine a few hours later, the two are the same: p99 5.50 ms on Windows
and 5.93 ms in the container (the section below). The difference was the other
job, which bore harder on one process than on the other; a comparison between
two measurements that each shared the CPU is not a comparison. The claim also
said the Windows p99 "has never been below about 100 ms since the first run",
which was false on its face: the untuned baseline's p99 was 17.47 ms.

**3. Open: stalls inside the network.** Inside the broker's network the median
end to end over the last tenth of each run is 23 to 34 ms, flush p99 is at
most 18 ms and checkpoint p99 at most 10 ms, and yet p95 ranges from 133 ms to
845 ms across runs. The stalls are mid-run and are neither the commit nor a
standing queue. With the CPU shared they may simply be the other job. One
candidate that is not: the broker's health check runs `rpk` inside its
container every five seconds, on a broker started with `--smp=1
--overprovisioned`, and a stall every five seconds would produce a p95 of this
shape. It is an A/B test (health check on and off, shuffled) for an idle host,
not something to conclude from these runs. Tested that night on an idle machine: it is
not the health check (the table under "End-to-end figures" above).

**What the HTTP runs show, and do not.** At 1,000 per second offered, the
endpoint decided every transaction over one connection but completed only
about 290 per second, so the offered load queued behind it. Over two, four
and eight connections the engine refused 25.8, 28.7 and 10.5 percent of
transactions as arriving after later ones (409). The refusal share is a
matter of order, not of CPU, and is the structural half of Rule C candidate 3:
more connections buy throughput only by delivering out of order, and the
engine will not score out of order. The achieved rates are not published:
a throughput ceiling is exactly what a shared CPU lowers, and on the idle
machine every configuration reached about 1,000 a second (992 on average over
one connection, 1,000 over more), so the 287 to 488 measured here was the other
job, and which side binds
(the Python client or the endpoint) is not separated by these runs. `PLAN.md`
names k6 for the HTTP load; a client outside Python would answer the second
question.
