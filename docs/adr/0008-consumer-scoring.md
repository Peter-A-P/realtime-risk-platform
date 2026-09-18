# 8. Scoring is a stream consumer, at least once, with idempotent decisions

- Status: accepted
- Date: 2026-09-14
- Deciders: the build session, within `PLAN.md` section 2.4 as written

## Context

`PLAN.md` section 2.4 says the scorer consumes the stream rather than serving
HTTP, fetches features, scores with the champion, applies decision rules,
writes the decision and times every hop. It leaves open the things that only
appear once the code exists: what happens to a transaction delivered twice,
when progress is acknowledged, where features come from in the scorer's own
process, and what the feature engine needs from the stream's ordering.

## Decision

`verdict/scoring/consumer.py`. For each transaction, in order:

1. **Check the ledger of decided event ids.** A duplicate is acknowledged and
   skipped. It is not served, not scored, and not observed by the engine.
2. Serve features from the feature engine, which serves before it observes.
3. Score with the model behind the `Model` interface.
4. Apply the rules; build a `DecisionEvent` naming the rule that decided.
5. Hand the decision to the `decisions` topic, keyed by card.

After each batch: `flush` until the decisions are acknowledged, then
`checkpoint` the transactions. Never the other way round.

### Delivery is at least once; decisions are idempotent by event id

ADR 3 already rules out exactly-once end to end. Two consequences are made
concrete here:

- **Duplicates are stopped before the engine, not after the model.** A
  redelivered transaction that reached the engine would be counted twice in
  every window it falls in. That is a feature bug, and no amount of
  deduplication downstream of the model would undo it. The ledger is checked
  first, and `tests/test_scoring.py` asserts that a duplicate leaves the next
  event's velocity count unchanged.
- **Checkpoints follow durable decisions.** If the scorer dies after
  deciding and before checkpointing, the transactions are redelivered. If it
  dies before its decisions were acknowledged, they are redelivered too,
  because the checkpoint did not run. A transaction is never acknowledged
  without its decision on the stream. The test makes `flush` fail and asserts
  a restarted scorer decides the whole batch.

The ledger is in memory and bounded (a million ids, about twenty minutes at the
live rate). Across a restart it is empty, and a transaction redelivered then
is decided again. The decision carries the same `event_id`, so a consumer of
`decisions` sees one decision twice rather than two decisions, which is the
idempotency the plan asks for. The second decision may differ from the first,
because the engine's state was also lost. That is the larger problem, and it
is the next point.

### Engine state after a restart

The engine's windows live in the scorer's process. After a restart they are
empty, and card features read "no history" until a day of traffic has
refilled them. The plan's answer (section 2.8) is to rebuild the online state
by replaying the stream, and that is week 7's recovery work, when the live
stack exists to recover. Until then a restart is a known degradation, stated
here rather than discovered.

### Features are served from the engine in process

The engine is the one computation of every feature (ADR 6). Serving from it
in the scorer's process gives exactly the values the stores hold, with no
network hop. The dual sink still writes both stores. The alternative, reading
the online store for every event, costs a read the engine already has the
answer to, and ADR 5 measured that read at 0.76 ms at p50 once warm and about
42 ms on the first call. If the scorer and the engine are ever separated, the
read comes back, and the budget in ADR 9 has a hop for it.

### The transaction topic has one partition

The engine refuses an event older than one it has already processed
(`LateEventError`), because folding it in would corrupt windows that have
moved past it. A consumer reading several partitions interleaves them, and
events a few milliseconds apart in event time arrive out of order. So:

- The live stream is one Kinesis shard (ADR 3), which is totally ordered.
- The local `transactions` topic now has one partition to match, down from
  four. The compose topics job checks the count and refuses an existing topic
  with the wrong one.
- One partition carries the live rate with room: Redpanda's single partition
  and one Kinesis shard (1,000 records a second before aggregation) both
  exceed 1,000 events a second at about 200 bytes.

Scaling past one partition needs a reorder buffer: hold each event until the
watermark, the latest event time seen less an allowed lateness, has passed
it. The hold time is added to every decision's latency, so the allowed
lateness is a budget line, not a tuning knob. That is deferred until a
measurement says one partition is not enough. The chaos test for out-of-order
events in week 8 exercises the same path.

## Consequences

- `verdict/scoring/` depends on `verdict.stream.base` and nothing below it; a
  test parses the module's imports to keep it that way.
- A poison record (one that does not decode) stops the scorer with an error.
  Deliberately, for now: the week 8 chaos scenario `poison_event` decides what
  should happen, with the behaviour observed first.
- The decisions topic keeps four partitions, keyed by card. Nothing reads it
  in time order.

## Addendum, 2026-09-15: one decision core, and when the ledger records

The decision moved out of the consumer into `verdict/scoring/core.py`, which
the HTTP endpoint (`http_api.py`) now shares. Rule C candidate 3 compares the
two, and that comparison is only about transport if the decision is the same
code on both sides; a test feeds identical events to both and compares scores,
actions and rules.

Moving it exposed an ordering fault in the first version. The ledger recorded
an event after its decision was handed to the stream. But the engine has
already folded the event into its windows once it has served it, so if the
write then failed and the event came back, the engine would have counted it
twice. The ledger now records an event immediately after the engine serves it.
The cost is that a retried event whose decision never landed is refused as a
duplicate: a lost decision, which a replay recovers, rather than a corrupted
window, which nothing recovers. `tests/test_scoring.py` makes the decision
write fail and asserts the retry is refused without reaching the engine.

The HTTP endpoint differs from the consumer in the three ways its module
docstring lists (a lock, 409 for out-of-order transactions, a flush per
request), and those differences are the evidence the comparison is for.

## Addendum, 2026-09-18: a record that cannot be decided is set aside

**The fault.** A record the scorer could not decide raised out of `poll`
before the batch's checkpoint. The scorer stopped; on restart it resumed from
the last checkpoint, read the same record, and stopped again. One malformed
message, one record from a producer on a newer schema, or one transaction
late in event time was a permanent outage, and nothing behind it was ever
decided. It was reproduced before it was fixed: three restarts, three
identical stops on the same record.

**The decision.** Three kinds of record are set aside rather than raised:

| Reason | What it is |
|---|---|
| `undecodable` | Not UTF-8, not JSON, or not a valid transaction |
| `unknown-schema-version` | A transaction this build does not know how to read |
| `late` | A transaction behind the engine's clock, which the engine refuses rather than corrupt its windows (the section above on one partition) |

Each goes to the `dead-letter` topic with its bytes untouched (base64, since
they may not be text), the reason, the error's text, and where it came from,
so it can be inspected and, once the fault is fixed, replayed. A dead letter
is written before the batch's flush and so is durable before the checkpoint
that passes it, the same guarantee a decision has.

**A run of them stops the scorer.** One bad record is a bad record; fifty in a
row is a producer deployed ahead of the scorer, or a clock gone wrong
upstream, and setting the whole stream aside quietly would be worse than
stopping. After `MAX_CONSECUTIVE_DEAD_LETTERS` (50, a placeholder: far more
than one fault produces, far fewer than a systemic one would, and 50 ms of
stream at the live rate) the scorer raises `DeadLetterRunError` and says what
it last saw. Any other exception while deciding is a bug, would affect every
record alike, and still stops the scorer at once, as before.

**What it costs.** A transaction set aside gets no decision. On a payment path
that means the caller's own timeout and default apply. A fallback decision
(for example, review everything that could not be scored) was considered and
not taken here: it would be a decision made without features, recorded on the
decisions topic beside real ones, and whether that is better than no decision
is a product question with no answer in a public problem statement. The
count of each reason is on the scorer (`dead_letters`), the load test reports
it per run, and a run with any is not comparable to one without.

**Parity follows.** `stream/parity.py` counts what the scorer set aside, and a
path that decided fewer transactions than the reference reports them as
missing. A stream that reorders is therefore caught by parity as a set of
missing decisions, where before it stopped the check with an exception.

## Options not taken

- **Deduplicate on the decisions topic only.** Cheap, and it lets the engine
  double-count; see above.
- **Exactly-once transactions from consume to produce.** Kafka supports it and
  Kinesis does not, so it would break ADR 3's one interface, and it would
  put transaction coordination on the latency path.
- **One engine per partition, with transactions keyed by card.** Correct for
  card features and wrong for device and merchant features, which see many
  cards and would be split across engines.

## Sources

- Apache Kafka documentation, "Message Delivery Semantics": at-least-once as
  commit-after-process, and the transactional alternative.
  https://kafka.apache.org/documentation/#semantics
- Amazon Kinesis Data Streams: ordering within a shard, and per-shard limits.
  https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html
- Akidau et al., "The Dataflow Model", VLDB 2015: watermarks and allowed
  lateness, the model for the reorder buffer deferred here.
  https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/
