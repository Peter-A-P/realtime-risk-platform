# 4. Bytewax for the streaming aggregations

- Status: **amended 2026-09-12, in week 3.** The reasoning below stands; the
  chosen engine does not. Bytewax cannot be installed on this project's
  Python, so the dataflow is written here instead. See "Amendment" at the
  end, which is the decision now in force.
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

---

## Amendment, 2026-09-12 (week 3): the dataflow is written here

### What forced it

Bytewax publishes no wheels for Python 3.13. Not on Windows, not on Linux,
not for any version up to its latest, 0.21.1, whose wheels stop at cp312.
Installing it on 3.13 falls back to a source build that needs a Rust
toolchain.

The portfolio's engineering standard is Python 3.13. So this is not a
platform inconvenience: the engine chosen above cannot be installed on the
project's own Python anywhere.

### Options, once that was known

1. **Pin the project to Python 3.12 and keep Bytewax.** Nothing written so
   far needs 3.13, so the code cost is nil. It changes the portfolio's stated
   standard for this project, and puts a second Python on every machine that
   builds it.
2. **Install Rust and build Bytewax from source.** Keeps both the engine and
   3.13. It makes a Rust toolchain a build requirement on the laptop, in CI
   on every run, and on the live spot instance, and makes a source build of a
   native extension part of the recovery path during the live window.
3. **Quix Streams.** Pure Python, installs on 3.13, actively maintained. It
   is Kafka-only, so the Kinesis path from ADR 3 would need a second
   implementation, which breaks the one-dataflow-two-transports property that
   ADR 3 and the parity test exist to protect.
4. **Write the stateful consumer here**, which is option 5 above, previously
   rejected.

### Decision

Option 4. `verdict/features/` holds the engine: `aggregators.py` for the
windowed aggregations and `engine.py` for the per-entity state and the
ordering.

The original objection to this option was that windowing, watermarks and
recovery are where the subtle bugs live, and that the point of this project
is not to write a worse stream processor. That objection was right, and what
changed is not the difficulty but the safety net. The leakage test and the
parity test were both written in week 2, **before** this choice was forced,
against a definition of each feature that is independent of how it is
computed. The engine is therefore checked against a brute-force
implementation on every run, on every feature, on real generated traffic.

That is not a theoretical reassurance. The test caught a real leak in this
engine within hours of it being written, at a boundary that the off-the-shelf
engine would also have had to get right: two events sharing a timestamp.
`docs/leak-caught.md` records it, including the measurement that it would
have corrupted 2.75 percent of one feature's values on the real-data track,
where timestamps are whole seconds.

### Consequences

- The scope of what is written here stays deliberately narrow: keyed
  aggregations over event-time windows, and nothing else. No dataflow graph,
  no operator algebra, no distributed shuffle. If the platform ever needs
  those, that is a signal to revisit rather than to extend.
- Aggregations are amortised constant time per event: a running total with
  subtracting eviction, a monotonic deque for sliding extremes, a multiset
  for distinct counts. They are checked against brute force with
  property-based tests, because a boundary bug here is exactly the failure
  this project exists to prevent.
- State is bounded by pruning entities whose windows have all emptied. A
  memory leak found by the same test: the "seconds since last" aggregator
  originally kept its timestamp after it fell out of the window, so no card
  that had ever transacted was ever dropped. Over 87 live days that is one
  retained aggregator per card seen.
- Recovery is now this project's problem rather than the library's. The
  online store is rebuilt by replaying the stream after an instance
  replacement, which is what ADR 15 will cover; the engine holds no state
  that cannot be rebuilt that way.
- What is given up is real: Bytewax's recovery, its scaling story, and the
  keyword on the technical line. The parity test between the Redpanda and
  Kinesis paths is unaffected, because it never depended on the engine.
- The door stays open. The features are specifications, not code
  (`verdict/store/features.py`), so the thing that would have to be
  rewritten to move to Flink or a future Bytewax is one module, not the
  platform.

### Sources

- Bytewax on PyPI: wheels for the current release, 0.21.1, cover cp38 to
  cp312. https://pypi.org/project/bytewax/#files
- Bytewax's build requirements: Rust and Cargo for a source install.
  https://github.com/bytewax/bytewax
- Quix Streams, the pure-Python alternative considered.
  https://quix.io/docs/quix-streams/introduction.html
- Sliding-window maximum with a monotonic deque, the standard technique used
  in `aggregators.py`.
  https://en.wikipedia.org/wiki/Sliding_window_protocol#Sliding_window_maximum
