# Real-Time Fraud and Risk Decisioning Platform

Every transaction scored in under 50 milliseconds, at sustained thousands per second,
before the money moves, and a system that keeps working as fraud patterns shift, without
the training-versus-production mismatch that quietly breaks most deployed models. The
platform around the model is what organisations are actually missing, and this one runs
live where a hiring manager can watch it.

**Status: building, and the real data now runs through the pipeline.** The plan is in
[PLAN.md](PLAN.md): build first, then run live at risk.peterparker.ca. Nothing is scored
yet, so the
headline tables below are still empty, and they stay empty until the thing they describe
has actually run.

This is a platform, not a fraud model. The model is the least interesting part.

## Result

Not yet measured. The build fills the first table; the live window fills the second.

**Platform properties (replay and load tests)**

| Decision latency p50 / p95 / p99 ms (95% CI) | Hop breakdown | Online/offline parity | Leak caught (commit, PR-AUC inflation) | Rollback drill (s, 5 runs) | Queue: $ caught per analyst-hour, expected loss vs score (95% CI) |
|---|---|---|---|---|---|
| _not yet_ | | | | | |

**What has been measured: the synthetic live track**

The event generator only. Nothing here is a platform latency or throughput figure, because
nothing is being scored yet. Build laptop, Windows 11, Python 3.13.15; five runs of 500,000
events; 95 percent intervals.

| Measurement | Result | Against |
|---|---|---|
| Events generated and written to the raw log | 12,219 /s (8,790 to 15,648) | a live rate of 1,000 /s |
| Events generated, nothing written | 19,577 /s (8,146 to 31,008) | the same |
| Fraud share produced | 3.06% (2.85 to 3.26) | a 3% target |

The intervals are wide because a laptop is a noisy machine, and because the raw-log write
rather than the generator is what binds: a single continuous run of a million events is the
slowest of the lot, at 6,580 /s. Details in [docs/generator.md](docs/generator.md).

**What has been measured (real-data track, offline)**

The public competition data mapped onto the platform's events and replayed through the
feature engine. Counts from the file; build desktop, Python 3.13.5.

| Measurement | Result |
|---|---|
| Transactions replayed, in their own time order | 590,540 over 182 days |
| Rows whose card could be identified | 88.7% (203,467 cards); the rest have no visible history |
| Features that exist on this track | 6 of 16: card velocity and amounts. No device, merchant or session is in the file ([ADR 17](docs/adr/0017-real-data-event-mapping.md)) |
| Point-in-time check over the replay | 0 violations in 67,920 comparisons: every row of a 2% hash sample of cards (5,386), against the definition recomputed from each card's history |

A count of violations is not a rate, so it carries no interval: it is zero for the sample
checked, and the sample is fixed by a hash of the card identifier rather than chosen.

**What has been measured: latency, and where the time goes**

Not the latency figure, which is the live stack's and comes later. The first local runs
put a decision at about 60 ms while every step the scorer takes stayed under 0.4 ms at
the 99th percentile, so the work went to finding the other 59. Three of the four costs turn
out to belong to the measuring host, and each is measured on its own rather than
subtracted quietly ([ADR 9](docs/adr/0009-latency-budget.md),
[docs/latency-budget.md](docs/latency-budget.md)).

| Cost | Measured as | Whose |
|---|---|---|
| The process's timer resolution | produce to acknowledgement 48.44 ms at p50 on a default Windows timer, 3.74 ms holding a 1 ms one | the host's |
| The load producer in the scorer's own interpreter | send to receive 8.64 ms at p50 from its own process, 70.57 ms from a thread beside the scorer; 12 shuffled runs | the harness's |
| Docker Desktop's port forwarder on Windows | flush 47.72 ms at p50 on 9 of 12 host connections and 7.10 ms on the other 3, against 3.99 ms on 10 of 10 connections from inside the broker's network | the host's |
| Flush and checkpoint, once per batch | tens of ms per batch whatever the batch holds, which is a throughput ceiling before it is a latency one | **the platform's** |

The scorer's own work, per hop at p99: features 0.340 ms, model 0.003 ms, rules 0.065 ms,
hand-off 0.048 ms.

End to end on the build laptop, measured on an idle machine at 1,000 transactions a
second with the stand-in model; milliseconds, mean of the per-run figure with a 95
percent interval. Local figures, not the live claim:

| Path | Runs | p50 | p95 | p99 | Decided |
|---|---|---|---|---|---|
| In-process stream (no broker) | 5 | 1.16 (1.14 to 1.17) | 1.98 (1.95 to 2.01) | 5.50 (3.48 to 7.52) | all |
| Redpanda, from inside its Docker network | 20 | 11.94 (11.36 to 12.53) | 24.21 (16.33 to 32.09) | 52.81 (36.27 to 69.34) | all |

With the forwarder out of the path, the median and the 95th percentile sit well inside 50
ms and the 99th does not reliably: 13 of 20 runs came in under it, and the rest lost it to
stalls on the broker's side that are not yet explained (the broker's health check was
tested and ruled out). From the Windows host, each run's figure depends on which path the
forwarder gave its connections, so it is reported by mode, not averaged.

**The transport comparison.** The same load into the synchronous HTTP endpoint, with no
broker on either side: over one connection it decided every transaction but ran at its
limit (p99 96 to 132 ms in four runs, and one run fell a second behind and stayed there);
over two, four or eight connections its p99 dropped to between 6 and 49 ms, and it refused
16.5 to 39.1 percent of transactions undecided, because they arrived after later ones and
the engine will not score out of order. The stream consumer decided every one, in order,
at 5.50 ms. That is the argument for scoring from a stream, measured
([docs/latency-budget.md](docs/latency-budget.md)).

**One approach tried and rejected: tuning the Kafka client.** The 47 ms looked like a
client setting, and `linger.ms=0` and `fetch.wait.max.ms=5` each appeared to fix it in a
first pass. Run three or more times each in shuffled order, every setting tried, including
`acks=1` without idempotence, disabling Nagle, a single partition, and the broker's write
caching, showed both the fast and the slow behaviour in about the same proportion as the
shipped settings. The first result was run order. What the setting sweep could not do, the
comparison against the same client inside the broker's network did in one run.

**Live window**

| Sustained events/s | Uptime % | Interruptions recovered | Drift triggers / retrains approved | Champion vs challenger PR-AUC (95% CI) | Cost per million events |
|---|---|---|---|---|---|
| _not yet_ | | | | | |

## What this does not do

- It does not chase model accuracy. One well-tuned gradient-boosting champion and one
  honest neural challenger; the interesting numbers are about the platform.
- The live stream is synthetic. Real fraud data has no streaming timestamps and no
  volume, so the real data (a public competition set) runs through the same pipeline
  offline, and every number says which track it came from.
- It does not touch real payment systems, card networks or personal data.
- The public competition data is used under its own terms, which permit non-commercial
  use and forbid redistribution. None of it, and nothing derived from it row by row, is in
  this repository or on the live stack. [docs/data.md](docs/data.md) records the clauses.
- It runs in one region with at-least-once delivery and idempotent decisions. The
  architecture decision records say what changes at ten times the scale.

## What exists so far

The event stream, the test that judges every feature, and the sixteen features it now
judges, plus the mapping that puts the real data through all three.

| Piece | Where | Note |
|---|---|---|
| Wire schema, versioned and closed | [verdict/events/schema.py](verdict/events/schema.py) | A transaction carries no label, no score and no feature. A test asserts it |
| Entity graph: cards, devices, merchants | [verdict/events/generator/entities.py](verdict/events/generator/entities.py) | Fraud is a property of a graph, not a row |
| Three fraud patterns | [verdict/events/generator/scenarios.py](verdict/events/generator/scenarios.py) | Card testing, account takeover, merchant collusion |
| Sealed regime schedule | [verdict/events/generator/regimes.py](verdict/events/generator/regimes.py) | Design public, development schedule public, live realisation sealed until the live window ends |
| Raw event log | [verdict/events/rawlog.py](verdict/events/rawlog.py) | Three files. Ground truth is kept out of the transaction log, and a test reads the bytes to prove it |
| Replay, in time order | [verdict/events/replay.py](verdict/events/replay.py) | Refuses an out-of-order log rather than sorting it quietly |
| Real data onto events | [verdict/events/ieee_cis_events.py](verdict/events/ieee_cis_events.py) | A card is issuer, product, billing region and account start day. No device or merchant is invented to fill the schema, which is at version 2 so it can say so |
| Sampled replay check | [verdict/features/replay_check.py](verdict/features/replay_check.py) | The point-in-time check over a replay too large to check in full, sampling cards rather than rows |
| The scorer | [verdict/scoring/consumer.py](verdict/scoring/consumer.py) | A stream consumer, not an HTTP service ([ADR 8](docs/adr/0008-consumer-scoring.md)). Duplicates are stopped before the feature engine can count them twice, and transactions are checkpointed only after their decisions are on the stream. The model is a stand-in until the real one is trained, and says so in every decision. A record it cannot decide (not a transaction, a newer schema, late in event time) goes to a dead-letter topic with its bytes untouched, where one such record used to stop it for good ([failure modes](docs/failure-modes.md)) |
| The scorer as a service | [verdict/scoring/service.py](verdict/scoring/service.py), [verdict/observe/metrics.py](verdict/observe/metrics.py) | `verdict score` runs the consumer until stopped, and a stop never splits a batch: what was consumed is decided and checkpointed first. It serves Prometheus metrics on localhost: decisions by action and model, each hop, flush and checkpoint per batch, batch size, duplicates, records set aside, and the time of the last decision. It drains the per-batch timings the load test keeps, which in a service running for months would have grown by about a gigabyte a day |
| One decision core, two transports | [verdict/scoring/core.py](verdict/scoring/core.py), [http_api.py](verdict/scoring/http_api.py) | The stream consumer and the HTTP endpoint share every line of the decision, so comparing them compares transports. HTTP has to refuse what the stream never delivers: a transaction older than one already scored |
| Rollback flag and shadow | [verdict/scoring/flags.py](verdict/scoring/flags.py) | The champion is read from a pointer on every event, so a rollback takes effect on the next one; a bad pointer is refused and scoring carries on. A challenger scores in shadow on the same features, and a challenger that fails cannot touch a decision |
| Training sets | [verdict/models/dataset.py](verdict/models/dataset.py), [inputs.py](verdict/models/inputs.py) | A set at a cutoff holds only labels that had arrived by it; the week before is left out and counted, never read as legitimate. The model's inputs are built in one order for both training and serving, and a test holds the training matrix to the scorer's vector row for row |
| Promotion gate | [verdict/models/promote.py](verdict/models/promote.py) | Non-inferiority on the interval bound, never the point estimate, from a paired bootstrap on the labelled shadow window; labels that had not arrived are not evidence; a challenger that declines everything is refused. It writes the pull request's evidence table and never promotes ([ADR 11](docs/adr/0011-shadow-and-promotion.md)) |
| Review queue | [verdict/review_queue/ranking.py](verdict/review_queue/ranking.py) | Expected-loss ranking against score ranking at fixed analyst capacity, simulated a day at a time, paired by day ([ADR 13](docs/adr/0013-queue-ranking.md)). No policy can read the label, and a test proves the prices cannot reorder the queue |
| Drift monitors and trigger | [verdict/drift/](verdict/drift/) | PSI and KS per feature and on the score, against a fixed reference, with the "no history" sentinel binned on its own. Thresholds are published conventions fixed before any drift was seen; a retraining request needs the same quantity drifted two days running ([ADR 12](docs/adr/0012-drift-thresholds-and-approval.md)) |
| Load test | [verdict/scoring/loadtest.py](verdict/scoring/loadtest.py) | Sends at a fixed rate and times every decision per hop, with intervals across runs. On a broker the load producer runs in its own process, joined to the scorer's timings by event id, because in a thread beside the scorer it was most of what the test measured. Every run says where its producer ran, whether a queue stood, and what its batches held. Its first published numbers wait for a quiet machine |
| HTTP load client | [verdict/scoring/httpload.py](verdict/scoring/httpload.py) | The other half of Rule C candidate 3. Starts the endpoint in its own process, offers transactions at a fixed rate over a stated number of connections, and reads the endpoint's own `Server-Timing` back so the round trip splits into hops and transport. It reports how long a transaction waited for a free connection, and what the endpoint refused |
| Stream parity | [verdict/stream/parity.py](verdict/stream/parity.py) | ADR 3's check that the interface is a fact: the same replay through each stream, every served feature, every transaction as received and every decision read back compared exactly with a run that uses no stream. Shown failing on a planted one-cent change before it was trusted, and not failing on a stream that delivers everything twice, because the scorer's ledger is what makes that safe |
| Flush probe | [verdict/stream/probe.py](verdict/stream/probe.py) | A measuring instrument, not the platform: times the scorer's flush on a series of fresh connections, from the host and from inside the broker's network. It is what found the 41 ms the host's port forwarder adds to some connections ([ADR 9](docs/adr/0009-latency-budget.md)) |
| Stream interface | [verdict/stream/](verdict/stream/) | Produce, consume, checkpoint ([ADR 3](docs/adr/0003-stream-choice.md)). In-process and Redpanda implementations held to one set of contract tests: order per key, redelivery without a checkpoint, resume after one, no rewind, unknown topics refused |
| **The feature engine** | [verdict/features/engine.py](verdict/features/engine.py) | Written here, not taken off the shelf: Bytewax has no Python 3.13 wheels ([ADR 4](docs/adr/0004-aggregation-engine.md)). Serves each event before observing it, and holds it back until time moves on |
| Windowed aggregations | [verdict/features/aggregators.py](verdict/features/aggregators.py) | Bounded state, amortised constant time, checked against brute force with property-based tests |
| One write path | [verdict/features/sinks.py](verdict/features/sinks.py) | The same value reaches both stores from one call. Parity 100% on a replay |
| **The leakage test** | [verdict/store/leakage.py](verdict/store/leakage.py) | Written before the first feature. Two checks, and three planted leaks that prove it can fail |
| Sixteen feature definitions | [verdict/store/features.py](verdict/store/features.py) | A feature is a specification, not code: card velocity, device and merchant entity-graph counts, session aggregates. The window is `[t - w, t)`, and an event is never part of its own features |
| Feature store | [verdict/store/repo.py](verdict/store/repo.py) | Feast, generated from the definitions, push sources rather than materialisation |
| Local stream stack | [deploy/compose/docker-compose.yml](deploy/compose/docker-compose.yml) | Redpanda, three topics created explicitly, auto-creation off and checked at start-up. Its first run found a start-up flag that Redpanda v24.3 rejects |
| The live stack | [deploy/terraform/](deploy/terraform/), [deploy/live/compose.yml](deploy/live/compose.yml) | One region, one spot instance, no inbound port: the dashboard leaves through a tunnel and a person arrives through Session Manager ([ADR 14](docs/adr/0014-live-in-one-region.md)). Everything is tagged, and `deploy/down.sh` asks the cloud, not Terraform's state, what is left after a teardown. Not yet running |
| What the platform keeps | [verdict/history/](verdict/history/) | Every decision staged with the features it was served before its transaction is checkpointed; once labels are in, every reviewed or declined row and a published sample of the rest, each weighted so totals come out right ([ADR 18](docs/adr/0018-history-is-a-weighted-sample.md)). On a replay the weighted PR-AUC and decision cost match the full data's, and read without the weights they do not |
| The live feeds | [verdict/live/feed.py](verdict/live/feed.py) | The generator played in real time: each transaction at its event time, each label a week later, from two runs of one deterministic stream. Each feed saves its place, and a run restored after a spot replacement continues byte for byte, sending at least once and skipping nothing ([ADR 15](docs/adr/0015-spot-and-recovery.md)) |
| The public dashboard | [verdict/observe/dashboard.py](verdict/observe/dashboard.py) | Grafana, anonymous and read-only behind the tunnel, provisioned from code. A test checks every panel's query against the metrics the platform exports |
| Decisions 1 to 15, 17 and 18 | [docs/adr/](docs/adr/) | Platform not model; two tracks; stream choice; aggregation engine (amended); feature store; computed once; leakage test first; scoring as a consumer; the latency budget and what the host costs; labels arrive late and nothing reads them early; shadow and promotion; drift thresholds; queue ranking; the live stack's shape; surviving a spot replacement; what a card, device and moment are on the real data; history as a weighted sample |

489 tests, `ruff` and `mypy --strict` clean. The broker tests skip, with a reason, where no broker is running.

**The leakage test caught a real leak on the day the first features were written**, which
is what it was written a week earlier for. Two transactions sharing a timestamp saw each
other, because a window is `[t - w, t)` and excludes anything at `t`. Replayed over the
public competition data with the unfixed engine, it serves a wrong value on 65 rows of
590,540, about one in nine thousand: invisible and permanent. The episode is in
[docs/leak-caught.md](docs/leak-caught.md) with two corrections kept in place. The first
published measurement described the wrong stream and overstated the impact by three orders
of magnitude; a later one grouped by a column that is not a card and counted rows the leak
cannot touch.

**The first run of the local stream stack found two faults in a Compose file that had never
run**: a start-up flag the broker rejects, and a topic job that failed on every restart after
the first. Both are fixed, and the stream contract tests now run against the live broker.

## How it works

See [PLAN.md](PLAN.md). Streaming features are computed once by a single dataflow and
written to both the online store and the offline store, so training and serving cannot
disagree; a parity test proves it daily. A point-in-time test, written before the first
feature existed, recomputes features from the raw log and fails on any leakage. A stream
consumer scores each event with an ONNX model and records timing per hop against a
published latency budget. A challenger runs in shadow; promotion needs a non-inferiority
result and a person; rollback is a flag, drilled and timed. Drift monitors open a
retraining pull request with the evidence; merging it is the approval. The review queue is
ranked by expected loss, and the evaluation shows what that buys per analyst-hour. The
live stack runs in one AWS region on a spot instance with Redpanda as the stream, the same
broker the local Docker Compose stack runs, because AWS documents Kinesis's delivery delay
as larger than the whole latency budget ([ADR 3](docs/adr/0003-stream-choice.md)). Sixteen architecture decision records
explain every choice with its public sources.

## Part of a portfolio

One of fifteen projects. This is the systems project: scale, latency
and operations evidence, where the others are about measurement, causal inference,
retrieval, fine-tuning and compliance.

## How this was built

Design, methodology, evaluation choices and judgement are Peter Parker's. AI coding
assistants (Claude Code) were used for implementation and drafting, the way a senior
engineer uses them in 2026. Every number in the results tables is reproducible from this
repository with one command, and that reproducibility is the evidence that matters.
