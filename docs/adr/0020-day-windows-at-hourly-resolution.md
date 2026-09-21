# 20. Day-long windows at hourly resolution, and 16 GB for the instance

- Status: accepted, 2026-09-19. Closes the memory question in ADR 15.
- Date: 2026-09-19
- Deciders: Peter Parker (hourly buckets and a 16 GB instance, over minute
  buckets, once the numbers were in); the build session (the design below)

## Context

ADR 15 measured the feature engine at about 4,700 bytes per entity and 900 per
event held in a window, which put a day of 24-hour windows at 1,000
transactions a second near 33 GB, on a 4 GB instance.

Minute buckets were proposed first, then checked before building and
withdrawn: a bucket saves memory only when an entity has more events in a
window than there are buckets, and at 1,000 a second over 200,000 cards a
card has about 430 transactions a day, fewer than the day's 1,440 minutes.

## Decision

**The six day-long features have an hourly resolution.** Their window is
`[floor(t - 24h), t)`, the far edge rounded down to the hour since the Unix
epoch: the last 24 to 25 hours, whole hours at the far end. The near edge is
unchanged, strictly before `t`, so nothing about point-in-time correctness
moves. `FeatureSpec.resolution` carries it; `FeatureSpec.window_start` is the
one place the edge is computed, and `events_in_window`, the reference
evaluation and the leakage test use it. It is a definition, not an
approximation: the engine computes it exactly, and the tests say so.

The features: `card_txn_count_24h`, `card_amount_mean_24h`,
`card_amount_max_24h`, `card_seconds_since_last`,
`card_distinct_merchants_24h`, `device_distinct_cards_24h`. Every window of an
hour or less stays exact.

**The engine holds those features in buckets.** Because every event in one
hour leaves the window at the same moment, a bucket's summary answers exactly
what its events would (`verdict/features/aggregators.py`, `Bucketed*`):

- count, sum and mean: an `array` column per quantity, one entry per bucket;
- maximum and minimum: a monotonic sequence with at most one entry per
  bucket;
- distinct count: each value's latest bucket and, per bucket, how many values
  were last seen there, with values interned. A first version kept a set per
  bucket and held a card's merchants up to 25 times over.

**The instance is an r7i.large**: 2 vCPU and 16 GB, spot price US$0.040 to
0.055 an hour in `ca-central-1` on 2026-09-19 (0.040 in `ca-central-1d`),
against US$0.037 to 0.040 for the c6a.large. The group may fall back to an
r6i.large or r5.large, both 2 vCPU and 16 GB on x86.

**Amended 2026-09-21, after the first hours of the dry run: the instance is
an m7i.xlarge, m6i.xlarge or m5.xlarge**, 4 vCPU and 16 GB on x86 (Peter).
Two vCPUs are one physical core. With the four latency faults the dry run
found fixed, the scorer (about 0.85 of a vCPU at the live rate), the broker
(0.35), the feed (0.2) and the feed's saves still overloaded it: load average
near 3, and on an r5.large 13 percent of 309,249 decisions over a second.
The same image on an m6i.xlarge decided 99.95 percent of 617,040 inside
50 ms and none over 250 ms, and drained a 130,000-transaction backlog after
a replacement in about two minutes (`docs/latency-budget.md`, "The first
hours on the live stack"). Memory is unchanged, so the m family, not r.
Spot in `ca-central-1d` that day: m5.xlarge US$0.063, m7i.xlarge 0.087,
m6i.xlarge 0.089 an hour, about US$59 to 78 a month with the volume.
`PLAN.md` section 6 carries the cost. `PLAN.md` fixed a fallback for this
case in advance, 4 vCPU and a 45-day window; Peter raised the budget
instead (`verdict-monthly` to 130 a month, which AWS holds as US$130),
so the window stays sixty days.

## Evidence

- `tests/test_aggregators.py`: every bucketed aggregation against the
  resolved definition by scan, 300 generated cases, a 60-minute window at
  15-minute resolution so buckets leave constantly; shown failing with the
  far edge left unrounded. A bucketed day holds at most 25 entries however
  many events arrive.
- `tests/test_engine.py`: three days of sparse traffic, every feature the
  engine serves on every event against the definition recomputed from raw
  events; shown failing with the far edge left unrounded.
- The existing leakage and parity tests pass unchanged against the new
  definition.
- **Memory, measured** (`verdict engine-footprint --steady`,
  `docs/engine-footprint-steady.json`): the scaled configuration (population
  and rate divided by fifty, per-entity rates unchanged) run 24 hours of
  stream time, sampled every two hours. It levels off at about 119 MB, which
  is about **6 GB at the live rate** (state is per entity, so it scales by
  fifty). The exact windows would have been about 33 GB, and the first
  bucketed version with a set per bucket about 15 GB.

## Consequences

- 6 GB for the engine leaves about 10 GB on the instance for the broker (1
  GB), the two feeds, Prometheus, Grafana and the operating system. The dry
  run measures the whole instance, not only the engine.
- The live window is about CA$15 more than on the c6a.large (`PLAN.md`
  section 6).
- A day-long feature can now include up to 59 minutes more history than a
  strict 24 hours. Every model is trained on features from the same engine,
  so training and serving agree; the only readers who must know are those
  comparing these features with some other system's "last 24 hours".
- The synthetic champion is retrained on the new features with the harder
  generator (ADR 21).

## Sources

- Python `array` module, typed arrays of machine values.
  https://docs.python.org/3/library/array.html
- `sys.intern`. https://docs.python.org/3/library/sys.html#sys.intern
- Amazon EC2 R7i instances. https://aws.amazon.com/ec2/instance-types/r7i/
