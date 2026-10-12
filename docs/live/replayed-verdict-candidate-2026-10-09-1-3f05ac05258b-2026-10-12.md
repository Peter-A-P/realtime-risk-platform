### A replayed verdict, on 2026-10-02 to 2026-10-04 (ADR 33)

`candidate-2026-10-09-1-3f05ac05258b` was not fitted on these days. Their kept rows hold the features the scorer served and the score the champion gave live; the candidate scored the same rows here, and the promotion gate judged both, weighted as history keeps them (ADR 18). The model hop below was timed here, not in the scorer. The candidate also runs in shadow in the scorer, and the gate's live verdict follows a week of labelled shadow scores later.

### Shadow evidence: candidate-2026-10-09-1-3f05ac05258b against champion-8d960d985749

Verdict: **eligible for promotion**. Eligible is not promoted; merging this pull request is the approval.

Labelled rows: 599,079, of which frauds: 200,432. Rows excluded because their label had not arrived: 0.

| Measure | Champion | Challenger | Difference (95% CI) | Margin |
|---|---:|---:|---|---|
| PR-AUC | 0.0616 | 0.4504 | +0.3888 (+0.3750 to +0.4013) | lower bound at least -0.0100 |
| Decision cost, $ | 533,215,673.50 | 347,454,362.12 | -185,761,311.38 (-187,530,240.28 to -183,942,763.94) | upper bound at most 2% of champion |
| Challenger model hop p99, ms | | 0.156 | | at most 3.0 |

Paired bootstrap, 1,000 resamples, seed 20270405, percentile intervals.

### After merging

Move the pointer on the instance: `verdict flag set candidate-2026-10-09-1-3f05ac05258b`. The previous champion's file stays in the image, so `verdict flag rollback` returns to it in one event.
