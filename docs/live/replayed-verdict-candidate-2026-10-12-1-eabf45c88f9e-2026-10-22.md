### A replayed verdict, on 2026-10-13 to 2026-10-14 (ADR 33)

`candidate-2026-10-12-1-eabf45c88f9e` was not fitted on these days. Their kept rows hold the features the scorer served and the score the champion gave live; the candidate scored the same rows here, and the promotion gate judged both, weighted as history keeps them (ADR 18). The model hop below was timed here, not in the scorer. The candidate also runs in shadow in the scorer, and the gate's live verdict follows a week of labelled shadow scores later.

### Shadow evidence: candidate-2026-10-12-1-eabf45c88f9e against candidate-2026-10-09-1-3f05ac05258b

Verdict: **eligible for promotion**. Eligible is not promoted; merging this pull request is the approval.

Labelled rows: 599,639, of which frauds: 199,586. Rows excluded because their label had not arrived: 0.

| Measure | Champion | Challenger | Difference (95% CI) | Margin |
|---|---:|---:|---|---|
| PR-AUC | 0.1069 | 0.1279 | +0.0210 (+0.0193 to +0.0230) | lower bound at least -0.0100 |
| Decision cost, $ | 150,384,059.61 | 150,461,704.41 | +77,644.80 (-52,922.64 to +199,121.65) | upper bound at most 2% of champion |
| Challenger model hop p99, ms | | 0.037 | | at most 3.0 |

Paired bootstrap, 1,000 resamples, seed 20270405, percentile intervals.

### After merging

Move the pointer on the instance: `verdict flag set candidate-2026-10-12-1-eabf45c88f9e`. The previous champion's file stays in the image, so `verdict flag rollback` returns to it in one event.
