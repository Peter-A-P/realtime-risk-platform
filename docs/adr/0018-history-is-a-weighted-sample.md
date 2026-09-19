# 18. History is a labelled, weighted sample, and every estimate reads the weight

- Status: accepted, 2026-09-19. Closes the open question in ADR 14.
- Date: 2026-09-19
- Deciders: Peter Parker (keep a day on the stream, keep a sample for the
  record); the build session (the design below)

## Context

ADR 14 measured what the live rate costs in bytes: 367 per transaction and
142 per label on the wire, about 5.2 billion transactions over the sixty-day
window, and about 1.9 TB of transactions alone. No disk this project can
afford holds that, and nothing it publishes needs every row.

What does need history, and what each needs:

| Reader | Needs | From which rows |
|---|---|---|
| Promotion gate (ADR 11) | Champion and challenger scores, labels, amounts, over a shadow window | Enough frauds and enough legitimate rows to estimate PR-AUC and decision cost with an interval |
| Review queue evaluation (ADR 13) | Every reviewed transaction, its score, amount and label | All of the reviewed ones, exactly |
| Retraining (week 6) | Features as served, and labels | A sample, weighted, is standard practice for a rare class |
| Drift monitors (ADR 12) | A day of each feature and the score | A day's rows, before any sampling: drift is judged daily |
| Replay after a spot replacement (ADR 15, to come) | The last 24 hours of transactions | The transactions topic |

The one fact that shapes the design: **a label arrives seven days after its
transaction** (ADR 10). Keeping every fraud means knowing which rows were
fraud, which is not known for a week, so whatever is decided at decision time
has to be kept, in some form, for at least that long.

## Options

1. **A disk for everything.** About two terabytes of gp3, about US$176 a
   month. Rejected in ADR 14.
2. **Keep a fixed fraction of every row at decision time.** Simple, but a 1
   percent sample of a stream that is 3 percent fraud keeps too few frauds
   to promote on inside a week, and too few reviewed rows to evaluate the
   queue at all.
3. **Stage everything compactly for eight days, then sample by outcome.**
   Every decision is staged with its features the moment it is made. Once a
   day's labels are all in, the day is reduced to every reviewed or declined
   row and a hash sample of the rest, stratified by the label, each row
   weighted by the inverse of its keep probability. Chosen.

## Decision

**Topics keep a day.** `transactions`, `labels`, `decisions` and `shadow` are
buffers with a 24-hour retention (`deploy/live/compose.yml`). A day of
transactions is also the longest feature window, which is what a replacement
instance replays. `dead-letter` keeps thirty days: it is read by a person.

**The scorer stages every decision** (`verdict/history`, `StreamScorer(history=...)`):
the transaction's fields, the sixteen features it was served, the
champion's decision, and the shadow's score and action. Rows go to an Arrow
IPC file per hour and are written **before the batch's decisions are flushed
and its transactions checkpointed**, so a checkpointed transaction always has
its row. A redelivery after a crash is staged twice and kept once.

**The label collector spools labels** by their own label time, and
checkpoints only after they are written. A label it cannot read is counted
and skipped: a stopped collector would lose every label after it once the
topic's day had passed.

**An hourly compactor seals and finalises** (`verdict history compact`).
Sealing turns a closed hour into one zstd Parquet file. Finalising a day waits
until the day's end plus seven days plus six hours of grace; it counts only
labels whose label time is at or before that moment, and keeps:

| Stratum | Which rows | Keep probability | Weight |
|---|---|---:|---:|
| acted | the champion reviewed or declined | 1 | 1 |
| fraud | approved, and labelled fraud | 0.10 | 10 |
| legit | approved, and labelled legitimate | 0.01 | 100 |

The draw is a hash of the event id with a fixed salt, not a random number: a
replay reproduces the sample exactly, and a duplicate cannot be drawn twice.
A candidate row with no arrived label is counted in the day's manifest and
not kept; it is evidence of a lost label, not a legitimate transaction. Each
day gets a manifest (`kept/<day>.json`) with the counts at every step, the
rates, the weights, the weighted estimates of the day's transactions and
frauds, and the SHA-256 of the kept file, so a published number can name the
data it came from.

**Every estimate reads the weight.** The promotion gate now carries a weight
per row: precision and recall count weighted rows, decision cost sums
weighted costs, and the bootstrap resamples within each weight so a resample
keeps the sample's design. With every weight 1, as on the offline track, it
is draw for draw the unweighted computation, and the existing tests pass
unchanged. The fraud count that gates a verdict counts rows, not weights. The
queue evaluation needs no weight at all: every reviewed row is kept.

**Drift is judged before sampling.** When the monitors are wired to the live
stack, they read a whole day of staged rows, which exist for eight days, not
the kept sample.

## Evidence

`tests/test_history.py`, on a synthetic day of 20,000 decisions sampled at
test rates (fraud 0.5, legitimate 0.1): the weighted estimates of the day's
transaction count (within 5 percent), frauds (10 percent), PR-AUC (0.03) and
decision cost (10 percent) match the same quantities computed on every row,
and the same sample read **without** its weights misses PR-AUC by more than
0.03. The test was shown failing with the weights set to 1. The other tests
show a day refused before its labels could have arrived, a late fraud label
not counted as legitimate, a duplicate kept once, the scorer's rows present
after a batch that failed before its checkpoint (and failing when staging is
moved after it), and a file cut short mid-batch yielding every whole batch
before the cut.

## Sizes

Measured on 20,000 generated events run through the scorer on 2026-09-19: a
staged row is 284 bytes in its hourly IPC file and 29.5 bytes sealed as zstd
Parquet; a label is 7.2 bytes sealed. Those features came from windows that
were still filling, which compresses better than a steady state will, so the
sizes below allow 50 percent more:

| Store | Retention | Estimate |
|---|---|---:|
| `transactions` topic | a day | 32 GB |
| `decisions`, `shadow`, `labels` topics | a day | 56 GB uncompressed; about 14 GB if their producers compress with zstd |
| Staged decisions, sealed | eight days | 31 to 42 GB |
| Labels, sealed | eight days | 5 GB |
| Kept sample | the window | up to 20 GB, set mostly by how many decisions the rules review |

That is about 105 to 115 GB with compression on the three topics off the
transaction path, so the data volume is **150 GB** (ADR 14 said 100). The
producers take a `compression` setting (`RedpandaStream`, `verdict score
--compression`), off by default so every latency figure measured so far
describes the configuration it names; zstd is switched on for the live stack
once the load test has measured what it costs.

## Consequences

- The scorer does more on its hot path: a dictionary per decision and one
  record batch per poll. Its cost is unmeasured until the load test runs
  with and without a spool, which is CPU work and waits for a quiet machine.
- Eight days of staged decisions are on disk at any time, holding features
  for every transaction. They are synthetic, as everything on the live stack
  is, and are deleted as each day is finalised.
- If the label collector falls more than a day behind, labels are lost: the
  topic has deleted them. Its lag becomes a metric and an alert with the
  dashboard; until then it is a line in `docs/failure-modes.md`.
- A kernel crash, as opposed to a spot interruption, can lose the last
  batches of staged rows whose transactions were already checkpointed. No
  fsync is paid on the latency path for that; the manifest's counts make any
  such loss visible as fewer staged rows than decisions.
- Rates are fixed before go-live and published with every manifest. Changing
  them mid-window is allowed only at a day boundary, because a day's weights
  are one set.
- The acted stratum's size is set by the rules, not the sample: with the
  placeholder rules every transaction of 5,000 dollars or more is reviewed.
  The dry run measures it before the rates are sealed.

## Sources

- Horvitz and Thompson (1952), "A generalization of sampling without
  replacement from a finite universe", Journal of the American Statistical
  Association 47(260), the inverse-probability weight used here.
- King and Zeng (2001), "Logistic regression in rare events data", Political
  Analysis 9(2): sampling on the outcome with weights for a rare class.
- Apache Arrow IPC streaming format, and why a stream file needs no footer.
  https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
- Apache Parquet compression codecs (zstd).
  https://parquet.apache.org/docs/file-format/data-pages/compression/
- librdkafka configuration, `compression.type`.
  https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
