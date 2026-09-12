# 7. The leakage test is written before the first feature, and never weakened

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

A feature leaks when its value at time `t` depends on something that happened
at or after `t`. Two forms account for almost all of it:

- **The window includes the event being scored.** A velocity count written as
  `<=` instead of `<`. In production the decision has to be made before the
  event exists anywhere, so the feature cannot be computed at all; offline it
  computes beautifully and is one higher for exactly the rows the model is
  trying to find.
- **The training join uses the label time.** Labels arrive seven days after
  the transaction. If the entity dataframe carries `label_time` where it
  should carry `event_time`, every feature on every row gains a week of
  hindsight.

What makes these dangerous is not that they are subtle. It is that every
instrument a team normally has points the wrong way:

- Offline metrics **improve**. A leak is indistinguishable from a good
  feature until production.
- A holdout split does not help: the leak is in the holdout too.
- Production monitoring does not fire. The model still returns scores. It
  merely underperforms, quietly, for as long as it is deployed.
- Cross-validation does not help, and time-series cross-validation only helps
  if the split respects the same boundary the feature got wrong.

By the time anyone suspects, the model has been retrained several times on
the same leak and the drift monitors have been tuned around its absence in
production.

## Options

1. **Review features carefully when they are written.** This is what most
   teams do. It relies on the reviewer holding the window convention in their
   head at the moment they read the diff.
2. **Detect leakage statistically:** flag features with suspiciously high
   individual AUC, or an importance that collapses in production. Catches
   some leaks late, produces false alarms on genuinely strong features, and
   cannot say what is wrong.
3. **Write a structural test before any feature exists, and make adding a
   feature without it impossible in practice.** Recompute from the raw log
   using only what was knowable, and compare.

## Decision

Option 3, with three commitments that matter more than the code.

**The test is written first.** `verdict/store/leakage.py` exists in week 2,
and `FEATURE_SET` is empty. The test runs green over nothing, which is the
point: week 3 cannot add the first feature without the test already standing
there.

**The test is never weakened to make a feature pass.** This is in the
repository's own instructions to itself and is repeated here because it is
the commitment most likely to be quietly broken at the worst moment, in the
middle of week 3 when a feature is nearly working. A failing leakage test
means the feature is wrong. Raising the tolerance, skipping an entity or
sampling less until it passes is the same as deleting it.

**Both checks exist, because one is not enough.**

- *Point-in-time*: recompute every feature from the raw log as of each row's
  event time, using events strictly before it, and compare with what the
  store served.
- *Label-shift invariance*: move the label times earlier and assert nothing
  about the features moves. This catches what the first check cannot: a
  shared convention error, where the serving path and the reference make the
  same mistake and compare equal. Here a value is compared against itself
  computed under a different label time, and there is no shared convention to
  hide behind.

The tests are themselves tested against three planted leaks: a window that
includes the current event, a window that reaches forward, and a training
join on the label time. Each must come back red and name the feature. A
leakage test nobody has watched fail is an assumption, not a test.

## Consequences

- The window convention is stated once, in
  `verdict/store/features.py`, and asserted: `[t - w, t)`, half-open,
  strictly before `t`. An event is never part of its own features.
- An empty window returns a negative sentinel rather than zero or null. Zero
  is a real value a count can take, and a null becomes a decision made later
  by whoever is least aware of it.
- The reference evaluation must stay brute-force and obvious even when it is
  slow. Optimising it would make it resemble the thing it is checking, which
  is how both sides end up sharing a bug. ADR 6 covers why it is not a
  forbidden second implementation.
- The check is sampled, not exhaustive, on anything larger than a small
  replay, because it is quadratic in the worst case. Sample size is reported
  with every result.
- If the test catches a real leak during week 3, that commit and the offline
  PR-AUC inflation the leak would have produced are recorded and published.
  If no real leak occurs, the README says that plainly rather than planting
  one to have a story.

## Sources

- Kaufman, Rosset and Perlich, "Leakage in Data Mining: Formulation,
  Detection, and Avoidance", ACM TKDD 2012. The standard treatment, including
  why leakage survives ordinary validation.
  https://dl.acm.org/doi/10.1145/2382577.2382579
- Breck et al., "The ML Test Score", IEEE Big Data 2017: training-serving
  skew and point-in-time correctness as explicit production tests.
  https://research.google/pubs/pub46555/
- Feast documentation on point-in-time joins and why the entity dataframe's
  timestamp is the join key that decides correctness.
  https://docs.feast.dev/getting-started/concepts/point-in-time-joins
