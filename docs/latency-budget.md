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

## End-to-end figures

**Provisional, and not yet the published result.** The runs that would fill
this table were taken while another project on the same machine was training a
model, and the tails show it: the in-process backend, which touches no network
and no broker, produced a 99th percentile between 20 ms and 288 ms across five
runs of the same configuration. A p50 survives that; a p95 or p99 does not.

What is here is the untuned baseline, kept because it is what the first
measurement said and because the timer artefact above is measured against it:

| Stream | p50 | p95 | p99 |
|---|---|---|---|
| In-process | 1.40 [1.30, 1.50] | 3.57 [2.30, 4.84] | 17.47 [1.12, 33.82] |
| Redpanda | 59.30 [27.60, 91.00] | 110.00 [48.92, 171.07] | 150.29 [50.91, 249.67] |

Milliseconds, mean across five runs with a 95 percent t interval, from the
`-untuned.json` artefacts: no fine-grained timer, the load producer in the
scorer's thread, and the port forwarder in the path. All three costs above are
inside those numbers. They are a starting point, not a result.

The table is completed by re-running the two load tests on an idle host, which
is the one outstanding piece of week 4.
