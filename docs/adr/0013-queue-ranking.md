# 13. The review queue is ranked by expected loss, and the comparison is simulated

- Status: accepted, 2026-09-15. Written ahead of week 6 because the ranking and
  its evaluation need no trained model to be built and tested. The prices are
  placeholders until week 6 states the assumptions they stand for.
- Date: 2026-09-15
- Deciders: the build session, within `PLAN.md` section 2.7 as written
- **Note, 2026-09-19 (ADR 18):** live history keeps every reviewed row at
  weight 1, so the queue evaluation reads exact rows and needs no weights.
  Anything that reads approved rows from history must use their weight.

## Context

`PLAN.md` section 2.7: expected loss is the probability of fraud times the
amount times one minus the expected recovery, less the review cost. At a fixed
analyst capacity, replay labelled data and report money caught per
analyst-hour under expected-loss ranking against score ranking, with
intervals. Rule C candidate 2 is the same comparison seen from the other side:
ranking by score is the approach expected to be rejected.

The plan fixes the formula and the headline number. It leaves open how the
queue is simulated, what counts as caught, how the interval is formed, and
what the prices actually change.

## Decision

`verdict/review_queue/ranking.py`. The package is `review_queue`, not the
plan's `queue`, which shadows the standard library's module.

- **Simulated a day at a time.** An analyst team works a shift; a queue carried
  across six months would be measuring a backlog, not a policy.
- **Hourly steps.** At the end of each hour the team reviews up to its
  capacity from the items waiting, highest priority first.
- **Items expire.** An item that has waited longer than `max_wait` (four hours
  by default) leaves unreviewed and catches nothing: card fraud not acted on
  within hours has already cost the money. Waiting is measured at the hour's
  end, so a `max_wait` under an hour is refused rather than silently expiring
  everything.
- **Caught money** is a reviewed fraud's amount less what chargeback would
  have recovered anyway. A reviewed legitimate transaction catches nothing.
- **No policy reads the label.** A policy sees score, amount and prices; the
  label is read only after an item is reviewed, to count what the review
  found. A test gives a policy two items that differ only in their label and
  asserts identical priorities.
- **The interval** is a bootstrap over days of the paired per-day difference.
  Both policies see the same arrivals, capacity and labels each day.

### What the prices change, stated so nobody tunes them for the wrong reason

At fixed capacity, expected-loss ranking orders items exactly as probability
times amount does. The recovery rate multiplies every saving by the same
factor and the review cost subtracts the same amount from each, so neither
reorders the queue; a test asserts the ordering is identical across three very
different price settings. The prices change the money reported, and, when week
6 wires this into the rules, whether an item is worth reviewing at all: an
item whose expected loss is below zero is not worth a review at any capacity.

### What the probability is

The model's score, read as a probability. Score ranking does not care whether
that reading is true; expected-loss ranking does. The stand-in model is not
calibrated, so no result is published from it. Week 5's champion is calibrated
on held-out data before the evaluation runs, and the result table names the
model whose scores it used.

## Consequences

- The published table is money caught per analyst-hour for both policies,
  their paired difference with its interval, the capacity, the prices, the
  wait limit, the model, and the track. Real data is the natural track for it:
  182 days of labelled transactions give 182 paired days.
- With equal amounts the two policies are the same policy, and a test asserts
  the difference is exactly zero. On data where amounts vary, expected-loss
  ranking should win; the synthetic test shows it can, and the real data will
  say by how much, or whether it does not.
- The simulation ignores analyst skill, partial reviews and the time a review
  takes varying with the transaction. Those belong in the result's limitations,
  not in the model.

## Options not taken

- **Continuous-time simulation.** More faithful to minutes of wait, and not
  needed to compare two orderings of the same arrivals. The hourly step is
  stated and enforced.
- **A carried-over backlog across days.** Would let one busy day contaminate
  the next and turn the paired comparison into a comparison of backlogs.
- **Rank by expected loss including false-positive friction** (the customer
  cost of reviewing a legitimate transaction). Worth adding when a cost for it
  can be stated from a public source; until then it would be a number invented
  here.

## Sources

- Bahnsen, Aouada, Stojanovic and Ottersten, "Example-Dependent Cost-Sensitive
  Decision Trees", Expert Systems with Applications, 2015: why fraud decisions
  should weigh each transaction's amount rather than count errors.
  https://doi.org/10.1016/j.eswa.2015.04.042
- Efron and Tibshirani, *An Introduction to the Bootstrap*, 1993: resampling
  whole days as the unit, the block that keeps a day's dependence intact.
