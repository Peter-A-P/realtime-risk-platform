# 9. The latency budget is measured end to end, and the host's own costs are measured apart from the platform's

- Status: accepted, 2026-09-17. The budget and the method are settled. The
  numbers under it are the local development host's; the published figure is
  the live stack's, in week 7.
- Date: 2026-09-17
- Deciders: the build session.

## Context

`PLAN.md` section 2.4 sets the budget: a decision in under 50 ms at the 99th
percentile, at a sustained thousand transactions a second, measured end to end
from the moment a transaction is handed to the stream to the moment its
decision is handed back. It names the five hops the time is divided into
(`ingest`, `features`, `model`, `decision`, `persist`) and requires a
confidence interval on every figure.

The first measurements on the development host, a Windows 11 laptop running
the broker under Docker Desktop, came out at about 60 ms at the median and
130 ms at the 99th percentile, while the sum of every hop the scorer is
responsible for stayed under 0.4 ms at the 99th percentile. So somewhere
between 99 and 99.7 percent of the measured latency was not the platform, and
the budget could be neither claimed nor abandoned until it was known what it
was.

Four rounds of measurement found three separate costs that belong to the
measuring host rather than to the platform, and one that belongs to the
platform's design. Two of the three were, at first, mistaken for platform
properties, and one of them was mistaken twice.

## Decision

### The budget stands as `PLAN.md` states it, and the published figure is the live stack's

Nothing here changes the budget or the way it is divided. What changes is
where it may be claimed from: the local stack cannot support a claim about the
50 ms budget, for the reasons below, and the number that goes in the README's
results table comes from the live Linux stack in week 7. The local stack's job
is to say where time goes, and it has now done that.

### Every host cost is measured apart, published, and named

The rule this ADR exists to fix in place: **a cost that belongs to the
measuring host is measured on its own and reported on its own, never
subtracted quietly and never left inside a platform figure.** Each of the
three below has an artefact under `docs/` and a way to reproduce it.

#### 1. The process's timer resolution (about 44 ms)

Windows gives each process a timer whose resolution defaults to about 15.6 ms,
and since Windows 10 version 2004 that resolution is per process. Every wait
the process makes, including the ones inside the Kafka client, rounds up to
the next tick. Measured on this project's own load test, the time from
producing a record to its acknowledgement was 48 ms at the median without a
finer timer and 3.7 ms with one, against the same broker with nothing else
changed.

`verdict/scoring/timing.py:fine_grained_timers` asks for a 1 ms timer, the
load test holds one for every run and reports whether it was granted, and
`docs/latency-week4-*-untuned.json` keeps the measurement made without one so
that the difference cannot be mistaken for a platform property. On Linux,
where the live stack runs, there is nothing to ask for and the call does
nothing.

#### 2. The load producer sharing the scorer's interpreter (about 60 ms)

The load test drove the scorer from a thread beside it, and therefore behind
the same interpreter lock. Send to receive through Redpanda, with no scorer in
the loop at all, was 8.5 ms at the median with the producer in its own process
and about 70 ms with it in the scorer's, against the same broker with the same
settings at the same rate; over twelve shuffled runs the producer's own
process drew the slow behaviour once and the shared thread five times out of
six. The same lock, held by a pacing loop that never slept, put waits of over
100 ms into the `ingest` hop of the in-process backend, which has no network
in it whatsoever.

So on a broker the load producer now runs in its own process
(`loadtest.send_from_a_subprocess`), joined to the scorer's timings by event
id on a clock that a test checks is the same counter in both processes; and
the pacing loop yields rather than spinning. The in-process backend keeps its
producer in-process, because its broker is an object in that process, and its
figure is a ceiling and says so.

#### 3. Docker Desktop's port forwarder on Windows (about 41 ms, on some connections)

The scorer's flush, which waits for a batch's decisions to be acknowledged
before their transactions are checkpointed, cost either about 6 ms or about
47 ms. Which one was fixed for the life of a producer connection, redrawn when
a new producer opened, and unchanged by every client setting tried: acks,
idempotence, Nagle, linger, and the topic's partition count all made no
difference, and the two modes appeared under each of them.

`verdict/stream/probe.py` opens producers in turn and reports the median flush
per connection. From the Windows host, 9 of 12 connections drew about 47.7 ms
and 3 drew about 7.1 ms. Run inside the broker's own Docker network, against
the same broker at the same moment, 10 of 10 connections drew about 4.0 ms,
with a 95th percentile under 8 ms. The container IP is not routable from the
host, so the forwarder is not avoidable from Windows; it is measured and
reported instead.

Both artefacts are kept: `docs/latency-week4-flush-host.json` and
`docs/latency-week4-flush-in-network.json`. The probe is in the repository and
runs from the command line so that the comparison can be repeated; it is a
measuring instrument and is not part of the platform.

### The platform's own cost is the per-batch durability cycle, and it is a throughput limit before it is a latency one

What is left after the three above is the scorer's own design, and it is worth
stating plainly because it is the thing that will actually bound the live
stack.

The scorer consumes a batch, decides every record in it, flushes the decisions,
and then checkpoints the transactions; ADR 8 requires that order, because a
checkpoint that ran ahead of a durable decision would lose decisions on a
crash. The deciding is cheap: features, model, rules and hand-off together stay
under 0.4 ms at the 99th percentile. The durability cycle is not: a flush and a
synchronous offset commit cost tens of milliseconds together, **per batch,
whatever the batch holds**.

That fixed cost sets a throughput ceiling, and the ceiling is what produces the
latency. Batches grow until the scorer's throughput meets the offered rate, so
the queue depth, and with it the end-to-end latency, settles at whatever it
must be for that to happen. A rate sweep on the local host showed exactly this
shape, with the settling point tracking the flush cost rather than the rate.
It follows that:

- **A latency percentile alone cannot show it.** So every run now reports the
  median end to end over its first and last tenth (`backlog`), and the mean
  batch size (`records_per_batch`). Alike early and late means the consumer
  kept up; a larger `late_p50` is the depth of a queue that stood, and the
  batch size is what the fixed cost was amortised over.
- **The remedies are known and are not week 4's.** Committing offsets less
  often than every batch removes the coordinator round trip from most batches
  and widens the redelivery window after a crash; overlapping the next batch's
  decisions with the current batch's flush removes the serialisation at the
  cost of one batch of checkpoint lag. Both preserve "a checkpoint never runs
  ahead of a decision". Neither is adopted here: they trade a property the
  platform advertises for latency, and that trade should be made against a
  measurement from the live stack, not against a figure that is mostly a
  Windows port forwarder.

## Consequences

- The README's results table carries the local figures labelled as the local
  host's, with the three host costs named, and the 50 ms claim is not made
  from them. The live-window figure fills the table in week 7.
- Every load-test report says where its producer ran, whether it held a fine
  timer, what its batches held, and whether a queue stood. A report without
  those is not comparable to one with them.
- `docs/latency-budget.md` holds the numbers, the intervals, and the method.
- The one local measurement that is clean, because it crosses no forwarder and
  shares no interpreter, is the in-network flush: about 4 ms. That is the best
  available local evidence for what the live stack's stream hop will cost, and
  it is an indication, not a result.

## What was tried and rejected

Recorded because the project publishes what did not work (Rule C).

- **Tuning the Kafka client.** `linger.ms=0`, `fetch.wait.max.ms=5`, both
  together, `acks=1` without idempotence, and a single partition were each run
  three or more times in shuffled order. Every one of them showed both the
  fast and the slow mode, in about the same proportion as the shipped
  settings. The first, unshuffled pass had suggested that `linger.ms=0` and
  `fetch.wait.max.ms=5` were large improvements; they were run order.
- **The broker's write caching.** Turning `write_caching_default` on was tried
  early, on the theory that the cost was the single broker's fsync. Shuffled
  repeats showed the same two modes with it on and off, and the broker was put
  back to its default.
- **Nagle's algorithm on the client socket.** Disabled with
  `socket.nagle.disable`; both modes still appeared. Whatever the delay is, it
  is not on the socket this process opens.
- **Reaching the broker without the forwarder.** The container's address on
  the Docker bridge is not routable from the Windows host, so there is no host
  measurement that avoids it. The in-network run is the comparison instead.

## Sources

- Microsoft, *timeBeginPeriod function*, and the Windows 10 version 2004
  change to per-process timer resolution.
- librdkafka's `CONFIGURATION.md` for every client setting named above.
- Apache Kafka protocol documentation for the produce and offset-commit paths
  the flush and the checkpoint use.
