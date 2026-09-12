# 6. Features are computed once, and the reference evaluation is not a second computation

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

Training-serving skew is the failure this project is built around. It happens
when the features a model was trained on are not the features it is served,
and the usual cause is banal: the training features were computed by one
piece of code, over a warehouse, in SQL or pandas, and the serving features
were computed by a different piece of code, in the stream, months later, by
someone reading the first one and reimplementing it.

Nothing catches that. The two implementations agree on the easy cases, which
are most cases, and disagree on window boundaries, late events, null
handling, time zones and the treatment of the current row. The model is
trained on one distribution and served another, and the only symptom is that
it works less well than it did offline, which is attributed to drift.

So: one computation. One Bytewax dataflow computes every streaming
aggregation and writes the result to both sinks, online and offline, from the
same value in the same pass (ADR 4 and ADR 5). A feature that cannot be
produced by that dataflow does not exist.

That rule immediately raises a question the leakage test forces into the
open. `verdict/store/features.py` contains `evaluate_spec`, which computes a
feature directly from raw events by brute force. Is that a second
implementation, and does it break the rule?

## Options

1. **Treat the reference evaluation as a violation and delete it.** The rule
   stays absolute. The leakage test then has nothing to compare the served
   value against except itself, which makes it a test that cannot fail, which
   is worse than no test at all because it certifies whatever it is pointed
   at.
2. **Have the dataflow call the reference evaluation.** One piece of code
   genuinely, but it means rescanning history per event, which cannot be done
   at a thousand events a second, and it means the test compares a function
   with itself again.
3. **Name the distinction precisely: one definition, two executions, and the
   test holds them together.** The specification is the definition. The
   brute-force evaluation applies it directly. The dataflow applies it
   incrementally, keeping state per entity. The leakage and parity tests
   assert they agree.

## Decision

Option 3, stated as the rule this repository actually follows:

> There is one **definition** of each feature, in `FEATURE_SET`. Nothing
> trains on or serves from anything but the dataflow's output. The
> brute-force evaluation exists only to check the dataflow, and a
> disagreement between them means the dataflow is wrong.

The practical tests of whether something is a forbidden second
implementation:

- **Does anything train on it?** No. The training set is built from the
  offline store, which the dataflow wrote.
- **Does anything serve from it?** No. Serving reads the online store, which
  the dataflow wrote.
- **Does it restate what a feature means?** No. It consumes the same
  `FeatureSpec` objects the dataflow does; the window, the aggregation and
  the field are read from the specification in both cases.

A second implementation in SQL for training would fail all three. The
reference evaluation fails none.

## Consequences

- The feature set is data, not code: entity, aggregation, field, window. That
  constrains what a feature can be, deliberately. A feature needing arbitrary
  Python is a feature that cannot be checked this way, and it needs its own
  ADR before it is added.
- The leakage test is a real test, because the two executions are genuinely
  independent: one filters a list, the other maintains and evicts state. The
  week 2 tests already exercise that pairing, with the incremental side
  standing in for the dataflow until week 3 replaces it.
- The brute-force evaluation is far too slow to serve, by design, and it is
  used on samples rather than on whole days of traffic. The leakage check
  therefore samples, and the sample size is reported with its result.
- `FEATURE_SET` is empty until week 3. The tests run against it as it stands,
  so they keep working as it fills rather than needing to be rewritten.
- The one-way door this closes: if the dataflow ever needs a feature the
  specification language cannot express, the honest move is to extend the
  language, not to add a bespoke feature outside it. Anything computed
  outside the specification cannot be checked by the leakage test, and an
  unchecked feature is exactly what this project says breaks systems.

## Sources

- Breck et al., "The ML Test Score: A Rubric for ML Production Readiness and
  Technical Debt Reduction", IEEE Big Data 2017. Training-serving skew as a
  testable production criterion rather than a code-review preference.
  https://research.google/pubs/pub46555/
- Sculley et al., "Hidden Technical Debt in Machine Learning Systems",
  NeurIPS 2015, on entanglement and duplicated pipelines.
  https://papers.nips.cc/paper/5656-hidden-technical-debt-in-machine-learning-systems
- Feast documentation on a single feature definition serving both the online
  and offline paths.
  https://docs.feast.dev/getting-started/concepts/feature-view
