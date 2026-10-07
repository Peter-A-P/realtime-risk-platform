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
  record batch per poll. **Measured on 2026-09-19** (`docs/latency-budget.md`,
  "The cost of staging history"): about 0.1 ms at p50 in process and about
  1 ms through the broker; zstd adds nothing measurable. The 99th percentile
  is not settled: two of ten staged runs held a stall of 0.8 or 1.4 s, and
  that is re-measured on the live instance.
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

## Addendum, 2026-09-22: finalising streams an hour

The dry run's compactor was killed by the kernel at 9.3 GB sealing a backlog,
because sealing read each hour whole; sealing was made to stream
(`docs/STATE.md`). Finalising had the same shape and had not been reached on
any track yet: a day is first final eight days after it starts, so the live
window's eighth day would have been its first run at scale. It had two
costs, and the second was the larger:

- **Every staged hour read whole**, and each of the up to eight label hours
  it joins against read whole too.
- **A set of every event id in the day**, for finding duplicates, held until
  the day was done. Measured at about 100 bytes an id, that is about 8.7 GB
  by the day's last hour at 1,000 a second.

On one staged hour at a time, the committed code peaked at 803 MB above its
baseline for a million rows and 1,519 MB for two million, about 2.7 GB for a
live hour of 3.6 million; with the id set, about 11 GB by the end of a day,
on the 16 GB instance that also holds the engine and the broker. The
compactor would have been killed on every attempt, and the staged hours it
never deleted would have filled the volume.

Now, with the same rows kept (the same SHA-256 on both versions, on the same
hours):

- **Two passes over each hour, neither holding it.** The first reads only the
  event ids and computes each row's draw, eight bytes a row; the second
  streams the rows and keeps only the candidates. Labels are streamed and
  filtered a batch at a time.
- **Duplicates per hour, found by their draw first.** A row is filed by its
  transaction's event time, and a redelivery carries the same event, so both
  copies land in the same hour. Only rows whose draw repeats within the hour
  are compared by id.
- **Sealed hours are read without Arrow's pre-buffering**, which read ahead
  several row groups: 265 MB against 102 MB in Arrow's pool for a million
  rows. Sealing reads through the same function and gains the same.

`verdict history footprint` measures it (`docs/finalise-footprint.json`):
about 194 MB per million staged rows plus 193 MB, so **about 890 MB for a
live hour**, and nothing carried from one hour to the next. Those rows have
about four percent reviewed or declined; the candidates, and so most of what
finalising holds, grow with that share, which the dry run measures before
the rates are sealed. `tests/test_history.py` shows a day finalised to the
same rows sealed or unsealed, in one batch or many, with duplicates split
across batches, and fails if finalising reads any hour whole.

## Addendum, 2026-10-07: sealing and finalising run apart, each with a limit

The live window's first day (2026-09-29, from 19:11Z) became finalisable at
2026-10-07T06:00Z, the first finalise ever run on the live stack. The run
that started then had not ended four hours later: no run of the compactor
finished after 06:00Z, `DayNotFinalised` fired at 07:01Z and
`HistoryUnsealed` at 09:16Z. Why it did not end is not known from outside
the instance; it was not failing, since no failed run was counted. Two
properties of the compactor turned one slow run into two problems:

- **Sealing ran in the same process, before finalising**, and the parent
  started the next run only when the last ended. Nothing was sealed while
  the finalise ran, at about a gigabyte an hour of unsealed staged rows.
- **A run had no time limit**, and a run that never ends is never counted
  as failed, so `CompactionFailing` could not fire. The alerts that did fire
  named the symptoms, not the run.

Decided:

1. **Two loops, side by side, one per step.** `verdict history compactor`
   runs `history compact --no-finalise` and `history compact --no-seal`,
   each still a process per run, every five minutes after the last of the
   same step ended.
2. **A day is finalisable only once every hour it reads is sealed**
   (`compact.is_sealed_for`, over `compact.hours_read`). A seal settles the
   hour's Parquet file and then deletes its folder; a reader that looked for
   the file before it existed and for the folder after it was gone would
   miss the hour, and for labels that would finalise a day short of labels
   for good. With this rule the two steps never read and write the same
   hour. The cost: a day is ready about seventy minutes after its labels are
   all in rather than at once, because the last label hour it reads starts
   at that moment. `as_of` is unchanged, so the rows kept are the same.
3. **Every run has a time limit** (`--seal-timeout`, 30 minutes;
   `--finalise-timeout`, two hours), several times what a healthy run needs.
   A run past it is killed and counted as `outcome="timeout"`; the loop goes
   on. `CompactionTimedOut` tells of one for three hours.
4. **The run in hand is timed** (`verdict_history_compact_run_seconds`,
   with the limit published beside it). A process blocked in the kernel
   cannot be killed until the kernel lets it go, so a kill can fail;
   `CompactionStuck` fires fifteen minutes past the limit.

The run counter gained a `step` label, so `CompactionFailing` now says which
step is failing. `tests/test_alerts.py` runs each new rule through
`promtool` and shows a finalise that never ends while seal runs go on.

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
