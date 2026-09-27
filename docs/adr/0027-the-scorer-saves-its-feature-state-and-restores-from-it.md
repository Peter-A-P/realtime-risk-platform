# 27. The scorer saves its feature state as it runs, and a replacement restores from it

- Status: accepted, 2026-09-26. Closes the scorer's half of ADR 15.
- Date: 2026-09-26
- Deciders: Peter Parker (a saved snapshot and a short replay, over a whole
  day's replay or accepting thin features, on 2026-09-25); the build session
  (how the snapshot is taken, and its evidence)

## Context

The feature engine's windows live in the scorer's process. A scorer that
starts with an empty engine serves every card "no history" until its windows
refill, and the day-long ones take a day (ADR 8, ADR 15). That was stated as
a known gap, and the plan's answer was to rebuild the state by replaying the
stream (`PLAN.md` section 2.8).

The dry run made it a go-live question. AWS reclaimed the instance eleven
times in 72 hours, eight of them on 2026-09-25 alone, across four instance
types (`docs/dry-run-report.json`). Latency survived it, which is why ADR 25's
numbers looked fine. Decision quality did not: at a reclaim every few hours
the scorer was almost never on full history, so the live window's shadow,
drift and queue evidence would be measured on features thinner than the
model was trained on. Peter ruled out an on-demand instance on cost the same
week.

Three ways to rebuild, put to Peter on 2026-09-25 with what each costs:

1. **Save the engine to the data volume as it runs**, and on a replacement
   load the last save and replay only the records after it. Minutes to
   recover; the risk is the save's own cost.
2. **Replay the topic's whole day on start.** Nothing new to save, but the
   engine takes about 18,000 events a second without serving
   (`decode + observe`, measured 2026-09-26), so a day of 86 million is about
   80 minutes per replacement, at eight replacements a day.
3. **Accept it and say so**, marking a day after each replacement as out of
   the evidence. At the dry run's rate, most of the window.

Peter chose the first.

## Decision

**The scorer saves its engine to `/data/engine` a slice at a time, on its own
thread, between batches, a pass every fifteen minutes, and a replacement
restores the last complete pass and replays the records after it before it
decides anything** (`verdict/scoring/recovery.py`, `verdict score
--engine-snapshot`).

**Why slices on the scorer's thread, not one copy.** The engine is millions
of small Python objects. Pickling them in one go took 13 s per gigabyte of
engine (measured 2026-09-26), a minute and a half at the live size with
every decision waiting behind it. A forked child would not hold the scorer,
but reading an object in CPython writes its reference count, so the child
copies every page it pickles; with a 7.5 GB engine on a 16 GB instance that
is the out-of-memory killer. So each step pickles entities for at most 2 ms,
then waits at least 8 ms before the next, so a pass never takes more than a
fifth of the thread; and the file's final `fsync`, which on a pass of a few
hundred megabytes held the thread for 278 ms, runs on a thread of its own,
since the system call does not hold the interpreter's lock.

**Why a pass taken over minutes is still exact.** A pass begins at a stream
position: the last record before the events the engine is still holding
back (it holds an event until time moves past it: the fix the leakage
test forced, `docs/leak-caught.md`).
Every slice records how many records after that position the scorer had
handled when it was taken, and which events were still held back then.
Restoring, the replay reads every record after the position again, through
the group's checkpoint, and for each entity folds in only the events its
slice had not already taken. An entity with no slice (new since the pass
began, or pruned before its turn) takes every event after the position,
which is its whole history or, for one pruned, history that had all
expired. The event ids decided just before the position are saved too, so a
record sent twice across the save is still counted as a redelivery. Records
that were set aside or turned away the first time are set aside or turned
away again, in the same order.

**It fails towards starting cold, never towards wrong state.** No save, a
save written by other feature code (the fingerprint covers the engine's and
the aggregators' source), one cut short, or one older than the day the topic
keeps: each starts the scorer with an empty engine, as before, and says why
in its log and in `verdict_engine_restored`.

## Evidence

- `tests/test_recovery.py`: a scorer stopped at points spread through several
  passes, including between deciding a batch and checkpointing it, is
  restored and serves every later event exactly the features an
  uninterrupted scorer served. The stream carries events sharing a
  timestamp, records sent twice, and one that does not decode. Six faults
  planted in the save and the restore were each caught: never skipping an entity's saved
  events, ignoring the held-back events, starting the pass at the last record
  rather than before the held-back ones, saving no ledger, and miscounting
  the records a slice holds by two or by one too many. Miscounting by one too
  few passes, and is not a fault: the last record handled is always either
  held back, and so named in the slice, or not an event.
- The same file holds each way a save cannot be used to starting cold, and a
  pass in progress to leaving the last complete one in place.
- `tests/test_stream.py`: the reread and the group's checkpoint, against the
  in-process stream and against Redpanda.
- End to end against the local broker on 2026-09-26: `verdict score` saving
  every 2 s, killed without warning, then started again: "restored 18,554
  entities ... and replayed 67,501 records in 5.6s", no duplicates.
- Measured locally on 2026-09-26, at a tenth of the live engine (198,157
  entities, 764 MB): steps 2.1 ms at p50, 2.4 ms at p99, at most about 11 ms;
  beginning a pass 20 ms and writing its header 6 ms, once each per pass;
  7.9 s of pickling a pass and a 207 MB file; a restore in 5.6 s. Copying
  the engine's entity list in recency order cost 49 ms against 7 ms for
  first-seen order, so the save uses first-seen order.

## Consequences

- At the live size, by those figures: about 80 s of pickling a pass, spread
  over about seven minutes; a file of about 2 GB, two on the volume while a
  pass is written; about a minute to load and a replay of up to about 22
  minutes of records at about 18,000 a second, a minute or two more. That
  time is added to each reclaim's recovery window (ADR 25) and ends its day
  of thin features. The first reclaim after the roll is the check.
- The decisions behind a step wait up to 2 ms, and one poll in a pass waits
  20 ms. The live report's latency figures after the roll include both.
- A restored engine is in first-seen order, not recency order, so it prunes
  less promptly until its entities are seen again, at most a day: memory,
  never a feature.
- Any change to `engine.py` or `aggregators.py` starts the next scorer cold
  once, since saved state is only read back into the code that pickled it.
- A save that stops is an alert, `EngineSnapshotStale`, after an hour
  without a complete pass (`docs/failure-modes.md`).
- ADR 15's scorer half, ADR 8's "engine state after a restart" and `PLAN.md`
  section 2.8 now point here.

## Sources

- Python, `pickle`: https://docs.python.org/3/library/pickle.html
- Python, `os.fsync` and the global interpreter lock around system calls:
  https://docs.python.org/3/library/os.html#os.fsync
- Instagram Engineering, "Dismissing Python Garbage Collection at
  Instagram" (2017), on copy-on-write pages written by reference counting
  after `fork`:
  https://instagram-engineering.com/dismissing-python-garbage-collection-at-instagram-4dca40b29172
- Apache Kafka, consumer positions, committed offsets and watermark
  offsets, as the reread uses them:
  https://kafka.apache.org/documentation/#consumerapi
- ADR 4 (the engine), ADR 8, ADR 15, ADR 18 (a day
  on each topic), ADR 25.

## Addendum, 2026-09-27: merchants and sessions are replayed, not saved

**Found on the instance.** Peter saw the dashboard's p99 jump to about 80 ms
for a few minutes several times an hour. Per-minute p99 over three hours:
median 24.7 ms, and about 210 to 230 ms in the minute after each pass began
(:04, :19, :34, :49), 40 to 140 ms for a few minutes after. py-spy, sampling
the scorer through one pass at 500 Hz, found the save's steps near their 2
ms budget except a few in its first minute that ran 60 to 274 ms. The pass
file's own sizes named them: of 493,071 entities (2.3 GB pickled), the 12
largest were merchants, the largest 4.4 MB, and every one of the 252 over 200
kB was a merchant. A busy merchant's one-hour windows hold an entry per
transaction as Python objects, and a step cannot split an entity, so every
decision behind it waited. The test at a tenth of the live size had no such
merchant.

**Decision.** A kind whose every feature has an exact window no longer than
an hour (`REPLAY_REACH`) is not saved at all: its state is exactly that
window's events, which the topic keeps for a day. Merchants (three one-hour
features) and sessions (two half-hour ones) qualify; cards and devices, with
day-long bucketed features, do not. The scorer notes where the stream stood
every 30 s of event time (`StreamScorer.marks`); a pass records the latest
position more than an hour before the engine's time (`replay_from`) and
waits if the scorer has not yet seen an hour. A restore reads from there:
before `after`, only the replayed kinds take each event, then everything as
before. The file's format moves to 2, so the first scorer on this code
starts cold once.

**Evidence.** `tests/test_recovery.py`: the existing tests, with merchants and
sessions now rebuilt by replay, serve exactly what an uninterrupted scorer
served; a new test runs a stream past an hour so the replay must start
mid-stream, and fails when the replay starts at the pass's start instead.
The saved ledger's joining the duplicates set at `after` is not exercised by
any test stream (it matters only for a record sent again whose original is
older than the replayed hour); it is the same rule as before.

**Cost.** A restore replays about an hour more of records for the two
kinds, about 3.6 million at the live rate, which adds roughly a minute and a
half to each replacement's recovery, measured on the next one.

