# 35. The live candidate learns from twice the rows

- Status: accepted, 2026-10-13
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

## Sources

- `docs/model-ceiling.json`, `docs/model-ceiling.py`, `verdict/models/live.py`.
- ADR 18 (history as a weighted sample), ADR 28 (the live models job).
