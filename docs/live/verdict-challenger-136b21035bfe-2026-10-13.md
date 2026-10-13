### Shadow evidence: challenger-136b21035bfe against champion-3f05ac05258b

Verdict: **eligible for promotion**. Eligible is not promoted; merging this pull request is the approval.

Labelled rows: 600,292, of which frauds: 200,622. Rows excluded because their label had not arrived: 0.

| Measure | Champion | Challenger | Difference (95% CI) | Margin |
|---|---:|---:|---|---|
| PR-AUC | 0.0597 | 0.0595 | -0.0002 (-0.0008 to +0.0005) | lower bound at least -0.0100 |
| Decision cost, $ | 1,125,739,546.29 | 936,056,845.91 | -189,682,700.39 (-193,032,520.12 to -186,161,345.12) | upper bound at most 2% of champion |
| Challenger model hop p99, ms | | 0.364 | | at most 3.0 |

Paired bootstrap, 1,000 resamples, seed 20270405, percentile intervals.

### After merging

Move the pointer on the instance: `verdict flag set challenger-136b21035bfe`. The previous champion's file stays in the image, so `verdict flag rollback` returns to it in one event.
