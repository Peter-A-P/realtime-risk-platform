# 3. Redpanda locally, Kinesis live, behind one `Stream` interface

- Status: accepted; **an open question added 2026-09-18**, pending Peter (see the end)
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

The platform needs a stream in two places that have opposite constraints.

On the build laptop it needs a stream that starts in seconds, runs beside a
dozen other containers, and costs nothing. During the three live months it
needs a managed stream that survives a spot-instance replacement without
losing events, bills predictably, and is the portfolio's worked example of
running something on a hyperscaler.

The live stack runs entirely in AWS `ca-central-1`, because pulling a
thousand events a second out of a cloud to a virtual machine somewhere else
costs more in egress than the compute does (`PLAN.md` section 2.8). So the
live stream is an AWS service. The question is which, and what the local
stack runs instead.

## Options

1. **Kafka locally and MSK live.** The most faithful pairing, and the most
   expensive: MSK has no meaningful free tier and its smallest broker
   configuration would consume most of the CA$223 live budget on its own.
   Kafka on the laptop also wants a quorum and a heap to match.
2. **Redpanda locally and Redpanda self-hosted live.** One implementation
   everywhere. But then the live stack is a container on an instance, not a
   managed service, which removes the AWS example the portfolio wants and
   makes the spot-interruption story harder rather than easier.
3. **Redpanda locally, Kinesis live, behind one interface.** Two
   implementations, one contract, and a test that asserts the same replay
   produces identical features through both paths.
4. **Kinesis everywhere, including locally, via LocalStack.** Removes the
   second implementation but adds a third: LocalStack's Kinesis is neither
   the real service nor a fast local broker, and a latency measurement taken
   against it describes LocalStack.

## Decision

Option 3.

- Locally: Redpanda, single node, Kafka protocol, in `deploy/compose`. Topics
  are created explicitly; auto-creation is off, so a typo fails loudly
  instead of creating its own silent topic.
- Live: Amazon Kinesis Data Streams, one provisioned shard, records
  aggregated into PUT units so the bill is shard-hours rather than records.
- Both sit behind `stream/base.py`: `produce`, `consume`, `checkpoint`. The
  scorer, the dataflow and the generator know only that interface.
- `stream/parity.py` runs the same replay through both paths and asserts the
  features that come out are identical. Without that test the interface is a
  claim, not a fact.

## Consequences

- Two implementations have to be maintained, and they differ in ways the
  interface must hide: Kafka partitions against Kinesis shards, offsets
  against sequence numbers, consumer groups against a lease table. Those
  differences are the reason `checkpoint` is in the interface at all.
- Delivery is at least once on both sides, so decisions must be idempotent by
  event id. That is recorded in ADR 8 and tested under duplicate delivery.
  Exactly-once end to end is explicitly out of scope.
- One provisioned shard supports one megabyte per second of ingest and a
  thousand records a second before aggregation. At the target rate of 1,000
  events a second at about 200 bytes an event, that fits with headroom, and
  aggregation gives more. If the load test in week 8 says otherwise, the
  reserve in the budget buys a second shard and the ADR is amended with the
  measurement that forced it.
- A local measurement is not a live measurement. Latency numbers are reported
  per path, and the published p99 is the one from the live stack.
- If the design has to grow beyond one region or beyond at-least-once, the
  interface is where that lands. `PLAN.md` section 11 records what would
  change.

## Open question, 2026-09-18: Kinesis cannot carry the ingest hop PLAN.md budgets

Found while starting the Kinesis implementation, before any of it was built,
and recorded here rather than decided, because the answer changes the
project's one-line claim.

`PLAN.md` section 2.4 measures latency "from the event's ingest timestamp to
the decision timestamp" and gives the ingest hop 5 ms of a 50 ms budget. AWS
documents the time from `PutRecord` to a consumer receiving the record as the
*message propagation delay*, and its own figures are:

| Consumer | Average propagation delay, per AWS |
|---|---|
| Shared throughput, polling `GetRecords` (5 calls per second per shard) | about 200 ms with one consumer |
| Enhanced fan-out, pushed over HTTP/2 by `SubscribeToShard` | typically about 70 ms |

Those are averages; a 99th percentile is higher. So on Kinesis the stream
alone exceeds the whole 50 ms budget on average, whichever consumer is used,
before the scorer does anything. For comparison, the same flush measured
inside the local broker's network in ADR 9 was 3.99 ms.

The options, none taken yet:

1. **Keep Kinesis, and move where the clock starts** to the scorer receiving
   the record. The 50 ms claim then covers features, model, rules and the
   durable write, and Kinesis's propagation is published beside it as AWS's.
   Cheapest; the one-liner has to say "once it reaches the scorer".
2. **Replace Kinesis on the live stack with Redpanda on the instance** (option
   2 above, rejected at the time). The end-to-end claim survives as written,
   on the evidence of ADR 9's in-network measurement. Costs the managed-AWS
   example and makes a spot replacement a broker recovery, not just a
   consumer restart.
3. **Kinesis with enhanced fan-out, and a budget set from what it measures.**
   Honest and managed, but the headline becomes a number in the low hundreds
   of milliseconds, and enhanced fan-out adds a consumer-shard-hour and a
   per-GB retrieval charge to the budget in `PLAN.md` section 6.

Until this is decided, week 7's Kinesis client is not written; what every
option needs (the parity test between stream implementations, the teardown
check, the budget alarms) is.

Sources: Amazon Kinesis Data Streams developer guide, *Develop enhanced
fan-out consumers with dedicated throughput* (the propagation-delay table),
https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html ; and
*Quotas and limits* (five `GetRecords` transactions per second per shard),
https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html

## Sources

- Amazon Kinesis Data Streams quotas and limits: one shard supports 1 MB/s or
  1,000 records/s ingest.
  https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
- Kinesis Producer Library record aggregation, which is what makes the bill
  shard-hours rather than per record.
  https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html
- Amazon Kinesis Data Streams pricing, `ca-central-1`.
  https://aws.amazon.com/kinesis/data-streams/pricing/
- Amazon MSK pricing, for the comparison that rejected option 1.
  https://aws.amazon.com/msk/pricing/
- Redpanda documentation: single-binary broker, Kafka API compatibility.
  https://docs.redpanda.com/
