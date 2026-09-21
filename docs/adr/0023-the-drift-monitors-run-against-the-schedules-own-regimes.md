# 23. The drift monitors are run against the schedule's own regimes

- Status: accepted, 2026-09-20
- Date: 2026-09-20
- Deciders: the build session (how to run ADR 12's monitors over a stream,
  and what the run is allowed to be told)

## Context

ADR 12 set the drift monitors and their thresholds: PSI at or above 0.25, a
KS statistic at or above 0.10 with a p-value below 0.01, at least 500 values
in a day, a reference that is fixed to the champion's training window, and a
retraining request only after two consecutive days of the same quantity
drifting. The values are credit-scoring conventions, fixed before any drift
was seen here.

All of it was tested against constructed windows: given a shifted
distribution the monitors report drift, and given two drifted days the
trigger opens a request. None of it had been run over a stream, so two
questions had no answer. Does the platform notice a real regime change, and
how long does it take. A monitor that never fires and a monitor that fires
every day are both useless, and constructed windows cannot tell them apart.

## Decision

**The run is given a stream and nothing else.** `verdict drift-report`
replays the generator through the scorer's own engine, scores every
transaction with the shipped champion, and judges each day against the fixed
reference. The development schedule's regime days are printed beside the
firings so the delay can be read, and are not an input to any decision. The
thresholds are ADR 12's and were not revisited after seeing these results.

**The reference is the champion's training window**, as `monitors.py`
requires: the days before the champion's cutoff, from this same stream.
Nothing is ever judged against yesterday, because a rolling reference
absorbs a slow drift one step at a time and never reports it, which is the
shape two of the four regimes take.

**Each day is judged on a hash-drawn sample.** PSI and a KS statistic need a
distribution, not every row, and a day holds 1.7 million transactions. Three
percent is kept, about 52,000 values per quantity against a 500 minimum, by
the same salted draw ADR 18 samples history with, so the sample is
deterministic, uniform across the day, and the same transactions feed every
quantity.

**The trigger is asked once per day, as the scheduled job would ask it.**
Handed all the days at once it would report a request opening on the last
day of the run. The report therefore names the day a request would really
have opened, and carries the two days of evidence behind it.

## Evidence

`docs/drift-report.json`. Fifty days of synthetic stream, 86 million
transactions; reference of 375,281 values per quantity from the seven days
before the cutoff; 43 days judged.

| Regime, and what it moves | Starts | First day flagged | What flagged |
|---|---|---|---|
| baseline | 2027-01-01 | never, 7 clean days | nothing |
| card-testing-wave, fraud 2.1x, online share +0.05 | 2027-01-15 | **2027-01-15** | device and merchant distinct cards, merchant count and mean amount |
| amount-drift-no-fraud-change, log-amount +0.3 | 2027-01-31 | **2027-01-31** | the five amount features, and the score |
| takeover-season, fraud 1.4x, online share +0.15 | 2027-02-15 | **2027-02-15** | merchant count and distinct cards return, amounts stay |

**The first retraining request opens on 2027-01-16**, one day after the
first regime change, on `device_distinct_cards_1h`,
`device_distinct_cards_24h`, `merchant_amount_mean_1h`,
`merchant_distinct_cards_1h` and `merchant_txn_count_1h`, carrying
2027-01-15 and 2027-01-16 as its evidence. One day is the floor the rule
sets: two consecutive days of the same quantity.

Three things in that table matter more than the detection itself.

**The seven baseline days flagged nothing.** Untuned thresholds against a
fixed reference produced no false alarm on the quiet stretch, which is what
gives the firings meaning.

**Every regime was caught on its first full day.** Not eventually, and not
after the effect had grown.

**The drifted set changes with the regime rather than growing.** On
2027-01-31 it switches from the entity-graph features to the amount
features; on 2027-02-15 the merchant features return while the amounts stay.
The middle regime is the one worth the whole exercise: it shifts what the
model is shown without changing how much fraud there is, so a fraud-rate
alarm would see nothing at all. The monitors caught it, and the champion's
score drifted with it, which is the argument for watching the score and not
only the inputs.

**36 of the 43 days report drift, and that is not 36 alarms.** The reference
is fixed until a new champion is promoted, so once the stream has moved the
monitors go on saying so, correctly, until it is retrained. The trigger
opens one request, because a request already open suppresses the next. A
reader who wants an alarm per day wants a rolling reference, and ADR 12
explains why this does not have one.

## Consequences

- Week 6's drift evidence exists: `docs/drift-report.json`, reproducible
  with `verdict drift-report`, whose defaults are the published run.
- What remains of week 6 is the retraining job the request should start, and
  the pull request that carries the evidence to a human. Nothing promotes
  itself, so the request is the end of the automated path, not the
  beginning of a deployment.
- The monitors will be run again on the live window against the **sealed**
  schedule, where nobody knows the regime days in advance. This run is the
  rehearsal that says the instrument works; it is not the live result.
- The run costs about five hours for fifty days, and reports each day as it
  is judged. Two earlier attempts were silent for hours and both were lost,
  one to a serialisation bug at the final line and one to a death with no
  output at all. A job this long must say where it has got to.

## Sources

- ADR 12, the thresholds and the fixed reference, with their sources.
- Siddiqi (2006), the PSI conventions ADR 12 cites.
- ADR 18, the hash draw the daily sample reuses.
- ADR 19 and ADR 21, the champion and the stream this is run on.
