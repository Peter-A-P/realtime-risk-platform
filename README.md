# Real-Time Fraud and Risk Decisioning Platform

Every transaction scored in under 50 milliseconds, at sustained thousands per second,
before the money moves, and a system that keeps working as fraud patterns shift, without
the training-versus-production mismatch that quietly breaks most deployed models. The
platform around the model is what organisations are actually missing, and this one runs
live where a hiring manager can watch it.

**Status: building, weeks 1 to 3 of 9 done, and the real data now runs through the
pipeline.** The plan is in [PLAN.md](PLAN.md): a nine-week
build, then three months live at risk.peterparker.ca. Nothing is scored yet, so the
headline tables below are still empty, and they stay empty until the thing they describe
has actually run.

This is a platform, not a fraud model. The model is the least interesting part.

## Result

Not yet measured. The build fills the first table; the live window fills the second.

**Platform properties (replay and load tests)**

| Decision latency p50 / p95 / p99 ms (95% CI) | Hop breakdown | Online/offline parity | Leak caught (commit, PR-AUC inflation) | Rollback drill (s, 5 runs) | Queue: $ caught per analyst-hour, expected loss vs score (95% CI) |
|---|---|---|---|---|---|
| _not yet_ | | | | | |

**What has been measured (week 1, synthetic live track)**

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

**Live window, Apr 5 to Jun 30 2027**

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

Weeks 1 to 3 of nine: the event stream, the test that judges every feature, and the
sixteen features it now judges, plus the mapping that puts the real data through all three.

| Piece | Where | Note |
|---|---|---|
| Wire schema, versioned and closed | [verdict/events/schema.py](verdict/events/schema.py) | A transaction carries no label, no score and no feature. A test asserts it |
| Entity graph: cards, devices, merchants | [verdict/events/generator/entities.py](verdict/events/generator/entities.py) | Fraud is a property of a graph, not a row |
| Three fraud patterns | [verdict/events/generator/scenarios.py](verdict/events/generator/scenarios.py) | Card testing, account takeover, merchant collusion |
| Sealed regime schedule | [verdict/events/generator/regimes.py](verdict/events/generator/regimes.py) | Design public, development schedule public, live realisation sealed until Jul 1 2027 |
| Raw event log | [verdict/events/rawlog.py](verdict/events/rawlog.py) | Three files. Ground truth is kept out of the transaction log, and a test reads the bytes to prove it |
| Replay, in time order | [verdict/events/replay.py](verdict/events/replay.py) | Refuses an out-of-order log rather than sorting it quietly |
| Real data onto events | [verdict/events/ieee_cis_events.py](verdict/events/ieee_cis_events.py) | A card is issuer, product, billing region and account start day. No device or merchant is invented to fill the schema, which is at version 2 so it can say so |
| Sampled replay check | [verdict/features/replay_check.py](verdict/features/replay_check.py) | The point-in-time check over a replay too large to check in full, sampling cards rather than rows |
| The scorer | [verdict/scoring/consumer.py](verdict/scoring/consumer.py) | A stream consumer, not an HTTP service ([ADR 8](docs/adr/0008-consumer-scoring.md)). Duplicates are stopped before the feature engine can count them twice, and transactions are checkpointed only after their decisions are on the stream. The model is a stand-in until week 5, and says so in every decision |
| One decision core, two transports | [verdict/scoring/core.py](verdict/scoring/core.py), [http_api.py](verdict/scoring/http_api.py) | The stream consumer and the HTTP endpoint share every line of the decision, so comparing them compares transports. HTTP has to refuse what the stream never delivers: a transaction older than one already scored |
| Rollback flag and shadow | [verdict/scoring/flags.py](verdict/scoring/flags.py) | The champion is read from a pointer on every event, so a rollback takes effect on the next one; a bad pointer is refused and scoring carries on. A challenger scores in shadow on the same features, and a challenger that fails cannot touch a decision |
| Load test | [verdict/scoring/loadtest.py](verdict/scoring/loadtest.py) | Sends at a fixed rate and times every decision per hop, with intervals across runs. Its first published numbers wait for a quiet machine |
| Stream interface | [verdict/stream/](verdict/stream/) | Produce, consume, checkpoint ([ADR 3](docs/adr/0003-stream-choice.md)). In-process and Redpanda implementations held to one set of contract tests: order per key, redelivery without a checkpoint, resume after one, no rewind, unknown topics refused |
| **The feature engine** | [verdict/features/engine.py](verdict/features/engine.py) | Written here, not taken off the shelf: Bytewax has no Python 3.13 wheels ([ADR 4](docs/adr/0004-aggregation-engine.md)). Serves each event before observing it, and holds it back until time moves on |
| Windowed aggregations | [verdict/features/aggregators.py](verdict/features/aggregators.py) | Bounded state, amortised constant time, checked against brute force with property-based tests |
| One write path | [verdict/features/sinks.py](verdict/features/sinks.py) | The same value reaches both stores from one call. Parity 100% on a replay |
| **The leakage test** | [verdict/store/leakage.py](verdict/store/leakage.py) | Written before the first feature. Two checks, and three planted leaks that prove it can fail |
| Sixteen feature definitions | [verdict/store/features.py](verdict/store/features.py) | A feature is a specification, not code: card velocity, device and merchant entity-graph counts, session aggregates. The window is `[t - w, t)`, and an event is never part of its own features |
| Feature store | [verdict/store/repo.py](verdict/store/repo.py) | Feast, generated from the definitions, push sources rather than materialisation |
| Local stream stack | [deploy/compose/docker-compose.yml](deploy/compose/docker-compose.yml) | Redpanda, three topics created explicitly, auto-creation off and checked at start-up. Its first run found a start-up flag that Redpanda v24.3 rejects |
| Decisions 1 to 8, and 17 | [docs/adr/](docs/adr/) | Platform not model; two tracks; stream choice; aggregation engine (amended); feature store; computed once; leakage test first; scoring as a consumer; what a card, device and moment are on the real data |

336 tests, `ruff` and `mypy --strict` clean. The broker tests skip, with a reason, where no broker is running.

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
live stack runs in one AWS region on a spot instance with Kinesis as the stream; locally,
the same code runs on Docker Compose with Redpanda. Sixteen architecture decision records
explain every choice with its public sources.

## Part of a portfolio

One of fifteen projects built over twelve months. This is the systems project: scale, latency
and operations evidence, where the others are about measurement, causal inference,
retrieval, fine-tuning and compliance.

## How this was built

Design, methodology, evaluation choices and judgement are Peter Parker's. AI coding
assistants (Claude Code) were used for implementation and drafting, the way a senior
engineer uses them in 2026. Every number in the results tables is reproducible from this
repository with one command, and that reproducibility is the evidence that matters.
