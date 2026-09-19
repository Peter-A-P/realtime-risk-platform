# 15. Surviving a spot replacement: the feeds resume exactly; the scorer's rebuild is open

- Status: accepted in part, 2026-09-19. The feeds' recovery is decided and
  built. The scorer's recovery (rebuilding its feature windows after a
  replacement) is **open**, and so is a finding about the engine's memory at
  the live rate that bears on it.
- Date: 2026-09-19
- Deciders: the build session, within `PLAN.md` section 2.8 as amended

## Context

The live stack is one spot instance in a group of one (ADR 14). A spot
interruption terminates it with two minutes' notice; the group launches a
replacement, which attaches the data volume and starts the stack again. Two
kinds of process have state that a restart would otherwise lose:

- **the feeds**, which play the generator in real time: without their place
  in the stream, a restart would either start the stream again from the
  window's start or skip to "now" and leave a hole;
- **the scorer**, whose feature engine holds up to 24 hours of windows per
  card, device, merchant and session in memory.

Until this record the live stack had no feed at all, because the generator
could not be resumed: it was a Python generator function, whose state cannot
be saved, and regenerating from the window's start to find the place would
take hours by day thirty.

## Decision: the feeds

**The generator is a resumable run.** `GeneratorRun`
(`verdict/events/generator/driver.py`) holds the random state, the planned
attacks, the counters and any records built but not yet handed out, and can
be snapshotted and restored. The refactor is byte for byte: a digest of the
first 30,000 records (events, labels and ground truth) is identical before
and after it, and `tests/test_live_feed.py` restores runs snapshotted at
several points, including mid-step, and compares every byte of what follows.
A snapshot names a hash of its configuration and refuses to continue under
another, because a different seed, rate, start or schedule from the same
place is not the same stream.

**Two feeds, one stream** (`verdict/live/feed.py`, `verdict live
transactions | labels`). The transaction feed sends each transaction when the
wall clock reaches its event time; the label feed runs the same
configuration from the same start and sends each label at its label time,
seven days later. A week of labels at the live rate is about 600 million
records, which no process could hold; a second run of a deterministic
generator costs a few percent of a core. **Neither stores ground truth**:
after the reveal, anyone can regenerate every record from the secret.

**Each feed saves its place** on the data volume after a flush, at most every
30 seconds, atomically (a temporary file, then a rename). After a
replacement it restores the place and sends everything from the first record
it had not saved past: **at least once**, so up to about 30 seconds of records
are sent twice, which the scorer's ledger and history's finalising already
absorb (ADR 8, ADR 18). Records whose time passed while the instance was
down are sent as fast as the generator allows, in order, until the feed has
caught up; **none are skipped**, because the sealed schedule is graded on the
stream as generated and a skipped transaction would still have its label a
week later. The dashboard shows a feed's lag while it catches up.

**A fresh start deep into a window is refused.** With no saved place and a
window more than an hour old, a feed stops and says so, instead of replaying
days. Starting fresh then needs `--from-start`, said on purpose.

**The live window runs on the sealed schedule only after checking it.** In
`sealed` mode a feed reads the secret (from SSM at `/verdict/schedule-secret`,
put there by Peter at sealing, through the boot script to a file only the
feeds' user can read) and the committed hashes (`docs/sealed-schedule.json`,
carried by Terraform), and refuses to start unless the secret, the derived
schedule and `regimes.py` all match. A dry run uses the public development
schedule, `dev`. Terraform refuses `sealed` without the committed hashes.

The window's start is a Terraform variable (`window_start`), fixed for the
window, because every restart continues from it.

## Open: the scorer's state after a replacement

The scorer starts a replacement with an empty engine, so every card has "no
history" until its windows refill: up to 24 hours of decisions on features
that are wrong in a known direction. The plan was to replay the
`transactions` topic, which keeps a day, through the engine before scoring
(ADR 4, ADR 18). That is not built, and a measurement taken on 2026-09-19 says
it needs rethinking first:

**The feature engine's memory, measured on 2026-09-19** (`verdict
engine-footprint`, `docs/engine-footprint.json`: 800,000 events at the live
population, resident memory sampled every 50,000 and fitted against entities
and events). **About 4,700 bytes per tracked entity and about 900 bytes per
event while it is inside the windows.** Six of the sixteen features hold 24
hours; at 1,000 events a second that is about 33 GB for a full day of
windows, against an instance of 4 GB. A first look had put it at 8.5 kB per
event, before the per-entity part was separated out.

**Decided 2026-09-19 (ADR 20): hourly buckets for the day-long windows and
a 16 GB instance; the engine measures about 6 GB at the live rate.** The
original analysis follows.

So the scorer cannot hold 24 hours of windows at the live rate on the live
instance, and its recovery cannot be designed until that is decided. The
options, for Peter, with what each costs:

1. **Windows kept as time buckets** (for example one-minute buckets for the
   24-hour features): bounded per entity whatever the rate. The definition
   of each 24-hour feature changes to "at one-minute resolution", the
   reference evaluation and the leakage test are taught the same definition,
   and the engine's aggregators are rewritten and re-tested. The most work,
   and the only option that keeps both the rate and the instance.
2. **A lower live rate**, about 100 a second: about 3 GB, still tight
   beside the broker. Changes the one-line claim.
3. **A memory-optimised instance** (32 GB or more): the rest unchanged, at
   several times the instance cost.
4. **Shorter windows** (1 hour instead of 24): a change to the feature set
   the plan and the leakage test are built on.

## Consequences

- The live stack now has traffic: the feeds, the scorer, the label collector
  and the compactor all run from the one image (`deploy/live/compose.yml`).
- The stream after a replacement is the committed stream, so the sealed
  schedule still grades what ran.
- Duplicates after a replacement are expected and visible (the dashboard's
  duplicates panel), never silent.
- A generator snapshot is a pickle. It is written and read only on the
  stack's own data volume; nothing restores a snapshot from anywhere else.

## Sources

- Amazon EC2 spot instance interruptions and the two-minute notice.
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html
- Python `pickle`, and why only trusted data may be unpickled.
  https://docs.python.org/3/library/pickle.html
- NumPy `Generator` and bit generator state, which pickles exactly.
  https://numpy.org/doc/stable/reference/random/generator.html
