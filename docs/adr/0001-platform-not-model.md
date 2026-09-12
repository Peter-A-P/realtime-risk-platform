# 1. This is a platform, not a fraud model

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

The obvious shape for a fraud project is a model: take a public competition
dataset, engineer features, tune a gradient-boosting model, report an AUC
that beats some baseline. That project has been built thousands of times and
its result cannot be checked by a reader, because the number depends on a
split nobody else can reproduce and on decisions nobody wrote down.

It also answers the wrong question. Models that fail in production rarely
fail because the offline metric was too low. They fail because the features
computed at serving time are not the features the model was trained on, or
because a feature was computed using information that did not exist at the
moment it claims to describe, or because the pattern moved and nothing
noticed for six weeks, or because the thing that was supposed to catch it
retrained itself on the wrong data.

This project is being built to produce evidence about scale, latency and
operations. That evidence is not a model score.

## Options

1. **A fraud model with a good score.** Cheap, fast, well understood. It
   proves feature engineering. It proves nothing about running anything.
2. **A platform whose model is deliberately ordinary.** Costs most of the
   nine weeks on the parts around the model: feature computation, parity,
   latency, shadow deployment, drift, approval, queueing, teardown. The score
   is reported honestly and is not the point.
3. **Both, sequenced.** Build the platform, then spend the remaining time on
   the model. In practice this is option 1 with extra steps, because model
   work expands to fill whatever time it is given.

## Decision

Option 2. The model is one well-tuned gradient-boosting champion and one
honest neural challenger, timeboxed to week 5 of a nine-week build. Any model
work after that week needs its own architecture decision record saying which
platform property it serves.

The README's first line says platform, not model, and the results tables lead
with latency, parity, throughput and queue economics. The model's numbers
appear, with their intervals, in their place.

## Consequences

- The headline numbers are p99 decision latency, sustained throughput,
  online/offline parity, the leak the point-in-time test caught, and money
  caught per analyst-hour. Every one of those is checkable by a stranger
  running the repository.
- A reader looking for a state-of-the-art fraud score will not find one. The
  README says so in its own words, in the "What this does not do" section.
- The comparison that does get made is the one champion/challenger exists
  for: a tabular deep-learning challenger against the gradient-boosting
  champion, promoted only on a non-inferiority result plus a human.
- The risk this accepts is that "the platform is the point" reads as an
  excuse for a weak model. The defence is the real-data track: the same
  pipeline scores a public competition dataset and publishes its result with
  a confidence interval, so the model is visible and merely unglamorous.

## Sources

- Sculley et al., "Hidden Technical Debt in Machine Learning Systems",
  NeurIPS 2015. The observation that the model is a small box in the middle
  of a large diagram, and that the surrounding system is where the debt is.
  https://papers.nips.cc/paper/5656-hidden-technical-debt-in-machine-learning-systems
- Breck et al., "The ML Test Score: A Rubric for ML Production Readiness and
  Technical Debt Reduction", IEEE Big Data 2017. Training-serving skew and
  feature-store consistency as explicit, testable production criteria.
  https://research.google/pubs/pub46555/
- Paleyes, Urma and Lawrence, "Challenges in Deploying Machine Learning: A
  Survey of Case Studies", ACM Computing Surveys 2022.
  https://arxiv.org/abs/2011.09926
