# 33. A replayed verdict, on recorded live days

- Status: accepted, 2026-10-10
- Date: 2026-10-10
- Deciders: Peter Parker (judge the candidate on recorded days rather than
  wait two weeks for a labelled live shadow week; 2026-10-10); the build
  session (the replay, and its limits)

## Context

The window's first candidate, `candidate-2026-10-09-1`, was merged as the
shadow model and rolled at 2026-10-10T00:47Z (ADR 32). The gate judges a
shadow model on `SHADOW_DAYS` (7) finalised days of its live scores (ADR 11,
28), and a day is finalised only once its labels have arrived, a week after
the transactions. Its first verdict could not come before about 2026-10-24,
five days before the window ends, while the champion it would replace acts
on 19% of transactions at 5.3% precision (`docs/live-decision-quality.json`).

Waiting for the live shadow week buys one thing the gate cannot otherwise
see: the candidate scoring inside the scorer, on the features the scorer
serves as it runs. Everything else the gate reads is already recorded. A
kept row holds the features the scorer served for that transaction, the
score the champion gave it live, its label once it arrives and its weight
(ADR 18). The candidate's pull request compared the two models on later rows
of its own training days, unweighted; that sample keeps every row the
champion acted on, so it overstates the champion's errors, and it is no
substitute for the gate.

## Decision

**The models job asks the gate for a replayed verdict** on the newest
candidate that beat the incumbent and ships in the image (its pull request
merged), once `REPLAY_DAYS` (3) finalised days follow the last day it was
fitted on:

- the candidate scores those days' kept rows, from the recorded features,
  with the same ONNX file and runtime the scorer uses;
- the champion's score is the one it gave live, from the same rows;
- the same gate (`promote.evaluate`) judges them, weighted, with its margins,
  its fifty-fraud floor and its latency budget, the hop timed on the
  instance;
- one pull request carries the verdict, titled and introduced as replayed,
  with the days it read. Merging an eligible one is the approval to promote,
  and `verdict flag set` moves the pointer, exactly as for a live verdict.

It is asked once per candidate. The live shadow keeps running, and the live
verdict follows on its own week; it now leaves out rows the shadow model
decided itself, which after a promotion are no evidence about it.

The replay is checked against the scorer itself. On the first sealed hour
after the roll (2026-10-10T00, synthetic live track), the scorer scored
208,140 rows with the candidate in shadow; scored again from their recorded
features with the same file, all 208,140 agree exactly (largest difference
0.0). The replay reproduces the hop the scorer runs, and the features are
the scorer's own by construction.

## Consequences

- The replayed verdict can be asked once 2026-10-04 is finalised, about
  2026-10-12T06:00Z, rather than about 2026-10-24.
- A promotion on a replayed verdict rests on the scorer's recorded features
  and the candidate's offline hop; the scorer running the candidate is shown
  by the live shadow from 2026-10-10, not by the verdict.
- The window's report says which model decided which days, and which
  verdict promoted the second one.

## Sources

- `verdict/models/live.py` (`replayed_rows`, `replay_due`, `REPLAY_DAYS`),
  `verdict/live/models_job.py` (`_replayed_verdict`),
  `tests/test_models_live.py`.
- ADR 11 (shadow and promotion), ADR 18 (history as a weighted sample),
  ADR 28 (the live models job), ADR 32 (a request opened by hand).
