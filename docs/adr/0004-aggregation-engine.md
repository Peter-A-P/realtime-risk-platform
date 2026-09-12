# 4. Bytewax for the streaming aggregations

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

The platform's central claim is that features are computed once and served
both online and offline (`PLAN.md` section 2.2). That claim is about a single
dataflow with two sinks, and the engine that runs it decides how believable
the claim is.

The features are keyed aggregations over time windows: velocity counts and
sums per card, device and merchant over several windows; entity-graph
quantities such as how many cards a device has been seen with; session
aggregates. They are ordinary streaming operations. The hard parts are
latency at the tail, watermarks and late events, and being able to run the
identical code on a laptop and on one small instance in `ca-central-1`.

## Options

1. **Apache Flink.** The reference implementation for this class of problem,
   with the most rigorous event-time and watermark semantics. It also brings
   a JVM cluster, a job manager and task managers, and either Kinesis Data
   Analytics or a self-managed deployment on the live instance. Both the
   operational weight and the cost are out of proportion to a one-shard
   stream on a four-core instance, and neither fits the CA$223 live budget.
2. **Kafka Streams or ksqlDB.** Same objection with a different accent, and
   it binds the local implementation to Kafka precisely where ADR 3 needs the
   engine not to care which stream it is reading.
3. **Spark Structured Streaming.** Micro-batch, so the sub-50 ms decision
   path would have to live outside it anyway, at which point there are two
   engines and the "computed once" claim is gone.
4. **Bytewax.** A Python dataflow library on a Rust runtime (Timely
   Dataflow). Runs as a process, not a cluster. Stateful operators with
   recovery, event-time windows with watermarks, and the feature code is the
   same Python the training pipeline imports.
5. **Hand-rolled consumer with in-process state.** Fewest dependencies, and
   it means writing windowing, watermarks and recovery, which is where the
   subtle leakage bugs live. The point of this project is not to write a
   worse stream processor.

## Decision

Option 4, Bytewax.

The dataflow lives in `features/dataflow.py` and writes through one sink
module to both stores: Redis for online serving, Parquet for the offline
store. There is no second implementation in SQL for training, and a feature
that cannot be produced by this dataflow does not exist.

The dataflow is expressed as keyed aggregations over event-time windows,
deliberately, so that the shape of the code does not depend on Bytewax. If
the engine ever has to change, what moves is the operator names.

## Consequences

- Python is in the hot path for feature computation. The latency budget in
  week 4 measures this and publishes the per-hop breakdown; if feature
  computation turns out to be the tail, the fix is recorded as a measurement
  rather than assumed now.
- Scaling out is not free. One process on one instance is the live design,
  which matches one Kinesis shard and the ASG of one. ADR 15 records what a
  spot interruption does to in-flight state; the online store is rebuilt by
  replaying the stream.
- Watermarks and late events must be tested, not trusted. Two tests hold this
  down: out-of-order events within the watermark produce the same features as
  in-order delivery, and the leakage test recomputes features from the raw
  log using only events strictly before the label event's time.
- Bytewax is a smaller project than Flink, with a smaller community and a
  faster-moving API. The pin in `pyproject.toml` is a major version with a
  comment, and the dataflow avoids anything exotic.
- The deferred door stays open: because the features are keyed aggregations
  over event-time windows, moving to Flink later is a rewrite of one module,
  not of the platform. `PLAN.md` section 11 records this.

## Sources

- Bytewax documentation: Python dataflow API, stateful operators, event-time
  windowing and recovery, built on Timely Dataflow.
  https://docs.bytewax.io/
- Timely Dataflow, the Rust runtime underneath.
  https://github.com/TimelyDataflow/timely-dataflow
- Apache Flink event-time and watermark semantics, the reference this design
  is measured against.
  https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
- Amazon Managed Service for Apache Flink pricing, for the comparison that
  rejected option 1.
  https://aws.amazon.com/managed-service-apache-flink/pricing/
- Feast documentation on online and offline stores sharing one definition,
  the pattern this dataflow implements directly.
  https://docs.feast.dev/
