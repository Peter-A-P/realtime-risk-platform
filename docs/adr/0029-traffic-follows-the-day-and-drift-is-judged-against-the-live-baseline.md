# 29. Live traffic follows the day, and drift is judged against the live window's own baseline

- Status: accepted, 2026-09-28
- Date: 2026-09-28
- Deciders: Peter Parker (that the live rate should vary through the day as
  card traffic does, 750 to 1,250 a second rather than a flat 1,000); the
  build session (the cycle's shape, and the change to the drift reference
  the check for it turned up)

## Context

Two things, the second found while checking the first.

**The live rate was flat.** The feeds played the generator at a constant
1,000 transactions a second, Poisson arrivals around a fixed mean. Real card
traffic is quiet overnight and busiest in the afternoon and evening, and a
flat line is one of the plainest signs a stream is synthetic. Peter asked
for it to vary.

**The drift reference did not describe the live stream.** Before shipping
the cycle, it had to be shown that the monitors (ADR 12, ADR 23, ADR 28)
would not read the daily rhythm itself as drift. `verdict drift-cycle-check`
judged a fresh stream against the reference the models job had built, the
champion's training window (the scaled synthetic configuration's first seven
days), on days 2 to 5 of a six-day replay under another seed, before any
regime change, once with the cycle and once flat as the control. **Both
flagged six features every day**, the flat control as much as the cycle
(`docs/drift-another-seed-flat.json`, `docs/drift-another-seed-cycle.json`):
merchant transactions per hour at a PSI up to 0.59 and a KS statistic up to
0.19, merchant and card amounts, device amounts, merchant and card distinct
counts, against thresholds of 0.25 and 0.10. The cycle added only the
champion's score, on three days of four, at a KS of 0.105.

The seed draws the population: every card's and merchant's typical amount,
which merchants are busy, how devices are shared. A reference is one
population's distributions, and any other population differs by more than
the thresholds allow. The live stream is another population: the full one
(200,000 cards, 4,000 merchants) where the champion's scaled one has 4,000
and 80. ADR 23 had judged the replay against a reference from the same
stream, which is why this never showed. On the live window it would have
opened a false retraining request within days of go-live, cycle or no cycle.

## Decision

**1. The live feeds follow a daily cycle** (`GeneratorConfig.daily_cycle`,
`LIVE_DAILY_CYCLE`): the rate swings 25 percent either side of 1,000 a
second on a cosine through the day, lowest about 08:00 UTC and highest about
20:00 UTC, times an hourly wobble (a standard normal fixed by the seed and
the hour, spread 4 percent, interpolated between hours) so no two days are
the same shape. Every gap, legitimate and attack, is divided by the moment's
multiplier, so the day's mean is the nominal rate, the fraud share is the
schedule's at every hour, and a run restored from a snapshot continues the
same stream, since the multiplier depends only on the moment and the seed.
With no cycle, the default, the stream is the flat one bit for bit: every
model, replay and committed hash built on it stands. `regimes.py` is not
touched, so the seal holds.

**2. The drift reference is the live window's own first full days.** Two
consecutive days on full features (no cold start within 25 hours, ADR 28),
each with at least 20 of its hours staged, both ending within seven days of
the window's start, which the public design of the schedule guarantees are
the first regime (`MIN_REGIME_DAYS`) at baseline levels (a fraud multiplier
of 1, no amount or online-share shift; its attack mix is the sealed one,
which is then what later regimes are judged against). Judging starts the
day after. With go-live's cold start, that is the window's second and third
whole days, judged from the fourth. If no such pair fits, the job fails
every pass and says why, and `ModelsJobFailing` emails. After a promotion
the reference is rebuilt from the two full days after the change, with no
deadline, since the champion's score is one of the quantities.

A live day is judged on 0.06 percent of its transactions, about 52,000, the
sample ADR 23's replays judged a day on; the reference on twice that share
of each of its two days.

This replaces ADR 12's "fixed reference: the champion's training window"
for the live window. The replays of ADR 23 and 24 keep theirs; there the
reference and the judged days are one stream.

## Evidence

- `docs/drift-another-seed-*.json`: the check above.
- `tests/test_generator.py`: the busiest three hours carry 1.45 to 1.9 times
  the quietest three (1.67 by design); two days hold the flat stream's count
  within 3 percent; the fraud share at the peak and at the trough differ by
  under 2 points; a run restored mid-cycle continues the same stream; no
  cycle is the flat stream.
- `tests/test_drift_live.py`: the reference is the first two days on full
  features, skipping days near a cold start and days too sparse to be the
  baseline, waiting for days not yet sealed, refusing when none fit in the
  first regime and with no deadline after a promotion; it is the same hash
  draw of its days; judging starts after it.

## Consequences

- **The first drift the live window can report is on its fourth day**, and
  the first regime change the schedule can make comes on its seventh at the
  earliest, so nothing the plan asks of the monitors is lost.
- **The monitors' first days are their own check.** Days 4 to 6 of the
  window are the first regime by design; a flag there is the platform's
  fault, not the stream's, and nothing it could start (a candidate needs
  three finalised days, a week and more away) reaches a person before it
  can be looked at.
- The Sep 29 check on the development stream, planned before this was
  found, is not needed: it would have tested the reference this replaces.
- Latency and throughput figures are unaffected: the peak is 1,250 a second
  against the 4,000 measured (ADR 8's addendum of 2026-09-27).

## Sources

- ADR 12 (drift thresholds), ADR 23 (the replayed monitors), ADR 27 (cold
  starts), ADR 28 (the live job).
- `verdict/events/generator/regimes.py`, `MIN_REGIME_DAYS` and
  `derive_schedule`: the first regime's guaranteed length and baseline
  levels, public before sealing.
