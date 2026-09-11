# Real-Time Fraud and Risk Decisioning Platform

Every transaction scored in under 50 milliseconds, at sustained thousands per second,
before the money moves, and a system that keeps working as fraud patterns shift, without
the training-versus-production mismatch that quietly breaks most deployed models. The
platform around the model is what organisations are actually missing, and this one runs
live where a hiring manager can watch it.

**Status: planning.** Nothing has run yet. The plan is in [PLAN.md](PLAN.md): a nine-week
build from Feb 1 2027, live Apr 5 to Jun 30 2027 at risk.peterparker.ca, torn down Jul 1.

This is a platform, not a fraud model. The model is the least interesting part.

## Result

Not yet measured. The build fills the first table; the live window fills the second.

**Platform properties (replay and load tests)**

| Decision latency p50 / p95 / p99 ms (95% CI) | Hop breakdown | Online/offline parity | Leak caught (commit, PR-AUC inflation) | Rollback drill (s, 5 runs) | Queue: $ caught per analyst-hour, expected loss vs score (95% CI) |
|---|---|---|---|---|---|
| _not yet_ | | | | | |

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
- It runs in one region with at-least-once delivery and idempotent decisions. The
  architecture decision records say what changes at ten times the scale.

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
