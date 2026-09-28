# 12. Drift is judged against a fixed reference by stated conventions, and a person decides

- Status: accepted, 2026-09-15. Written ahead of week 6: the monitors and the
  trigger need no model. The retraining job and its pull request are week 6's.
- Date: 2026-09-15
- Deciders: the build session. **Amends `PLAN.md` section 2.6**, which named
  Evidently; see "Why not Evidently".

## Context

`PLAN.md` section 2.6: PSI and KS per feature and on the score distribution,
daily; two consecutive days above threshold open a retraining job, which trains
a candidate, runs it in shadow, and opens a pull request with the evidence;
merging is the approval; nothing retrains itself into production.

Two things make drift monitoring on this project harder than usual, and both
shape the decision.

- **The live schedule of regime shifts is sealed** (ADR 2). The monitors are
  graded after Jul 1 2027 against shifts their author could not see. That only
  means something if the thresholds were not tuned to the public development
  schedule either.
- **At a thousand events a second a day holds 86 million events.** Any
  difference between two such samples has a vanishing p-value. A test
  statistic used as a significance test flags every day.

## Decision

`verdict/drift/`.

### Statistics (`stats.py`)

- **PSI** with bins fixed from the reference window's deciles. Repeated
  quantiles collapse, so a count feature with three values gets three bins.
  **`NO_EVENTS` has its own bin**: a day on which far more cards arrive with no
  history has drifted, and folding the sentinel into the lowest bin would hide
  it. Empty bins are floored at a share of 0.0001, which caps one empty bin's
  contribution near 0.69 against a ten percent reference share.
- **Two-sample KS**: the largest gap between empirical distribution functions,
  with the asymptotic Kolmogorov p-value and Stephens' small-sample correction.
  A test checks the p-values are calibrated under no drift.

### Monitors (`monitors.py`)

- **The reference is fixed**: the champion's training window, replaced only
  when a promoted champion brings a new one. A rolling reference absorbs a slow
  drift step by step and never reports it.
- **Every feature and the champion's score** are monitored. The score catches a
  shift the model is sensitive to even when no single feature moved far; the
  features say where a shift came from.
- **Thresholds, from convention, fixed before any drift was seen here:**

| Quantity | Threshold | Source |
|---|---|---|
| PSI | 0.25 or above is drift; 0.10 to 0.25 is moderate and triggers nothing | The credit-scoring rule of thumb (Siddiqi 2006) |
| KS | statistic 0.10 or above, and p below 0.01 | The statistic decides; the p-value only stops a small day flagging on noise |
| Minimum values | 500 per quantity per day | Below it a day is insufficient: neither drifted nor clean |

None of these may be adjusted by looking at how they behave on the development
schedule's regimes. If they prove wrong, the change is an amendment to this
record made before the sealed schedule is revealed, with the reason, and the
live-window report says so.

### Trigger (`trigger.py`)

- **The same quantity, drifted on two consecutive calendar days.** Different
  quantities on consecutive days are two days of noise. A missing day, or a day
  too small to judge, breaks the run.
- **No new request while one is open.** A persistent shift is one request.
- **The output is a retraining request with its evidence table.** It does not
  train, and the candidate it asks for reaches production only through the
  promotion gate (ADR 11) and a merged pull request.

## Why not Evidently

`PLAN.md` named Evidently. It was not used, and the plan is amended in the same
commit:

- The two statistics are a few lines each, and a reviewer can check them
  against the tests in `tests/test_drift.py`: a hand-worked PSI, a hand-worked
  KS statistic, the Kolmogorov tail at its published 5 and 1 percent points,
  and p-value calibration under no drift.
- The sentinel bin is a requirement specific to this platform's features, and
  would have had to be arranged around a library's binning rather than written
  into it.
- The project already carries Feast's dependency tree. A monitoring library
  whose report generation is its main value adds weight for a use that needs
  two numbers per quantity per day.

What is given up is Evidently's HTML reports and its wider catalogue of tests.
The live dashboard is Grafana (week 7), which reads these numbers directly.

## Consequences

- Drift numbers in the live-window report are reproducible from this
  repository's code alone, with no library version to pin against.
- The thresholds are public now, before go-live, and cannot quietly move.
- The week 6 retraining job consumes `RetrainRequest`. Until a model exists it
  has nothing to train, so the end-to-end "retraining PR on a forced shift"
  test in the plan's week 6 row is still to come.

## Sources

- Siddiqi, *Credit Risk Scorecards: Developing and Implementing Intelligent
  Credit Scoring*, Wiley, 2006: the population stability index and its 0.10
  and 0.25 conventions.
- Press, Teukolsky, Vetterling and Flannery, *Numerical Recipes*, 3rd edition,
  Cambridge University Press, 2007, section 14.3: the two-sample KS statistic
  and its asymptotic p-value with the small-sample correction.
- Stephens, "Use of the Kolmogorov-Smirnov, Cramer-Von Mises and Related
  Statistics Without Extensive Tables", Journal of the Royal Statistical
  Society B, 1970. https://doi.org/10.1111/j.2517-6161.1970.tb00821.x
- Evidently documentation, for the option not taken.
  https://docs.evidentlyai.com/

## Addendum, 2026-09-28: the live window's reference is its own first days

On the live window the fixed reference is no longer the champion's training
window but the window's own first two days on full features, inside its
guaranteed first regime (ADR 29): a reference from another population of the
same generator flagged six features every day. The thresholds are
unchanged.
