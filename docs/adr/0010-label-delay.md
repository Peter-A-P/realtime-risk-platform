# 10. A label is its own event, arrives seven days late, and nothing may read it early

- Status: accepted, 2026-09-18. **Written after the fact.** The decision was
  built in weeks 1 and 2 (the schema, the generator, the real-data mapping)
  and relied on since (the leakage test, the promotion gate, the review
  queue), without the record this repository says a decision needs. This is
  that record, and it names the one rule that no code enforces yet.
- Date: 2026-09-18
- Deciders: the build session.

## Context

Whether a card transaction was fraud is not known when it is decided. It
becomes known later, when a cardholder disputes it or an investigation closes
it, and in life that takes days to months. A platform that trains, evaluates
or promotes on outcomes has to respect that the outcome did not exist at the
moment the decision was made, or every figure it reports is inflated by
information nobody had.

There are three ways that goes wrong, and each has a different victim:

- **Features that read the label**, or the label's time, as if it were known
  at decision time. The model looks better offline than it can be live. This
  is the leak `PLAN.md` section 2.3 exists to catch.
- **Evaluation on labels that had not arrived.** A shadow comparison on the
  last week of traffic, run today, uses outcomes that in life would not exist
  for another week.
- **Training on labels that had not arrived.** A model trained at a cutoff
  that includes rows whose outcomes came in after the cutoff.

`PLAN.md` section 2.5 sets the simulation: "labels arrive with a simulated
seven-day delay, as they do in life".

## Decision

### A label is a separate record, on its own topic, joined by event id

`LabelEvent` (`verdict/events/schema.py`) carries `event_id`, `label_time`,
`is_fraud` and `recovered_cents`. A transaction carries no label, no score and
no feature, and a schema test asserts it
(`tests/test_schema.py::test_a_transaction_carries_no_outcome`). Labels
travel on their own `labels` topic (one partition, thirty days' retention,
`deploy/compose`), and the join
is by event id, so a real feedback feed would slot in behind the same record
without anything downstream changing.

`recovered_cents` is on the label rather than the transaction because it is
also an outcome: how much of a fraud was clawed back is known no earlier than
the fraud itself. The review queue's expected loss (ADR 13) reads it.

### The delay is a constant seven days, on both tracks

`label_time` is the transaction's event time plus seven days: in the
generator (`GeneratorConfig.label_delay_days`, default 7.0) and in the
real-data mapping (`ieee_cis_events.LABEL_DELAY`), where the competition
file's `isFraud` becomes a label seven days after the row's own time.

A constant is a simplification, and it is chosen on purpose. Real delays are
a distribution with a long tail, and a platform that got the rules below
right for a constant delay gets them right for any delay, because every rule
is written against each row's own `label_time`, never against "seven days".
What a constant buys is determinism: whether a label had arrived at a given
moment has one answer, which a test can check exactly. A realistic delay
distribution would add realism to the simulation and no property to the
platform, and its shape would need a public source this project does not
have.

### Four rules, each written against `label_time`

1. **Features never read it.** The leakage test's label-shift check
   (`verdict/store/leakage.py`, `check_label_shift_invariance`) moves every
   label time earlier and asserts no feature value moves: a feature that
   changed was using the label time as its as-of point.
2. **Promotion counts a row only if its label had arrived.** `evaluate` in
   `verdict/models/promote.py` keeps a shadow row only when `label_time` is at
   or before the moment of evaluation, and the evidence table says how many
   rows it left out and why (ADR 11).
3. **The review queue reads a label only when an item is reviewed.** The
   simulation (`verdict/review_queue/ranking.py`) ranks and schedules on
   scores and amounts alone, and opens the label to count what a review
   caught (ADR 13).
4. **Training at a cutoff uses only labels that had arrived by the cutoff.**
   A model trained as of time T sees rows whose `label_time` is at or before
   T, which means its most recent seven days of transactions are unlabelled
   and excluded, not labelled with hindsight. **No code enforces this yet**,
   because no training pipeline exists yet; it is set here so that the
   pipeline is built to it, and the training code carries a test of it as
   the promotion gate does of rule 2.

## Consequences

- **Every evaluation has a seven-day blind edge.** The newest week of
  traffic cannot be judged. A shadow window must run at least seven days
  longer than the evidence it is meant to produce, and a retraining request
  (ADR 12) cannot become a promotion decision in under seven days, whatever
  the drift monitors say.
- **Drift monitoring is unaffected,** because it watches features and scores,
  which exist at decision time, and never labels (ADR 12).
- **On the real-data track, any split of the 182 labelled days by time** (the
  project uses the competition's labelled file only, `docs/data.md`) must
  drop the seven days before the cutoff from training, or the model is
  trained on outcomes that at the cutoff did not yet exist.
- **A real label feed would change the delay, not the rules.** The join is by
  event id and every rule reads the row's own `label_time`, so a feed with a
  long-tailed delay slots in with no code change and no weaker guarantee.

## Options not taken

- **The label on the transaction, flagged as not for use.** One field away
  from a leak that no test could distinguish from correct use, since the value
  would be present at decision time.
- **Label time equal to event time.** The simplest simulation and the one that
  makes every rule above untestable, because nothing would ever be early.
- **A delay drawn from a distribution.** More realistic, no platform property
  served, and a shape that would have to be invented.

## Sources

- `PLAN.md` sections 2.3 (the leakage test) and 2.5 (the seven-day delay).
- `docs/data.md`, for the real-data file: 590,540 labelled transactions over
  182 days, which the seven-day delay is applied to.
