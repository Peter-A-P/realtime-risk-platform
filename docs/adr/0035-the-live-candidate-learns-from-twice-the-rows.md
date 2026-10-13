# 35. The live candidate learns from twice the rows

- Status: rejected, 2026-10-13, on the test fit's evidence (see Outcome)
- Date: 2026-10-13
- Deciders: Peter Parker (800,000 frauds, 2026-10-13); the build session

## Context

The retrained champion scores a PR-AUC of 0.45 on the live stream. A
diagnostic on 2026-10-12 (`docs/model-ceiling.json`, `docs/model-ceiling.py`;
test day 2026-10-04, weighted, synthetic live track) fitted the live
candidate's own recipe on more or less of the kept history:

| Training frauds | Test PR-AUC (95% CI) |
|---|---|
| 25,000 | 0.237 (0.226 to 0.251) |
| 100,000 | 0.345 (0.331 to 0.363) |
| 400,000, the job's cap | 0.458 (0.444 to 0.473) |
| 800,000 | 0.495 (no interval: the run was stopped) |

The same 400,000 frauds drawn from one, three or five days, and two other
tunings, moved it by 0.02 at most, and a depth-10 model scored 0.687 on its
own training rows and 0.448 on the test day. The cap, set when the instance
had 16 GB (ADR 28), was the binding limit; the kept history holds about a
million frauds a day.

The diagnostic ran its fits on two threads, and from about 23:20Z the share
of decisions over 50 ms rose from its usual 0.1 to 0.3% to 3 to 6%, and to
17% during the larger fits, until they were stopped at about 00:10Z. The
job's own one-thread fits at 400,000 frauds (2026-10-09 and 12) left it at
0.1 to 0.3%.

## Decision

`models/live.TARGET_ROWS` is 800,000 frauds and 4,000,000 legitimate rows. The
job still fits on one thread at the lowest priority. A test fit at that size
was run on the instance before the next candidate, watched for latency (see
Consequences).

This is model work after the model's week (CLAUDE.md). The platform property
it serves: the live retraining path learns from the data it already keeps,
rather than from a cap sized for a smaller machine.

## Consequences

- Expected gain about +0.04 PR-AUC; better features, not more rows, are
  what would move it further, and they wait for the end of the window, since
  designing them now against the stream would weaken the sealed experiment.
- A fit takes about twice as long and holds about twice the memory.

## Outcome: rejected

The test fit ran on 2026-10-13 from 00:29Z, by the job's own path
(`training_table` then `retrain`, one thread, lowest priority), on 4,798,065
rows, and was stopped at 00:47Z by a guard that stops it when decisions over
50 ms pass 2% in two consecutive two-minute windows. In two-minute windows
from 00:12 to 00:50Z the share of decisions over 50 ms averaged 1.18% that
night, with windows up to 3.6%, against 0.17% over the same minutes on each of
the two nights before; available memory fell from 10 to 5 GB as it built its
table. The likely cause is memory more than processor: the fit's copies of
the table push the broker's page cache out. `TARGET_ROWS` is back at 400,000
frauds. The gain was about +0.04 PR-AUC; a fit that holds fewer copies of the
table, or runs off the instance, would be needed to take it, and the
scorer's latency is the platform's first claim.

## Sources

- `docs/model-ceiling.json`, `docs/model-ceiling.py`, `verdict/models/live.py`.
- ADR 18 (history as a weighted sample), ADR 28 (the live models job).
