# 34. A drift candidate learns from the drift, or it answers nothing

- Status: accepted, 2026-10-12
- Date: 2026-10-12
- Deciders: Peter Parker (two drifted days to learn from and two replayed
  days to judge on, 2026-10-12); the build session (the rules below)

## Context

On 2026-10-10 and 11 the amount means per card (24 hours), per device and
per merchant (1 hour) drifted (PSI 0.43 to 0.80, synthetic live track), and
the monitors opened a request on 2026-10-12. The models job fitted a
candidate at once, on the finalised days, the latest of which was
2026-10-03, and its training ended on 2026-10-02: it had never seen a
drifted transaction, and its own pull request (#3) said so. It still beat
the incumbent, the scaled-population champion that barely separates fraud
(ADR 32), so the request closed as answered. Peter closed #3 unmerged.

Two things let that happen. A candidate was fitted as soon as a request
opened, before any drifted day's labels had arrived; ADR 24 had found that
first candidate blind to the drift and said only that the gate makes it
safe. And a request closed on any candidate that beat the incumbent, which
against a weak incumbent says nothing about the drift. With the request
closed, the monitors would open another on the next two drifted days, and
the job would fit another blind candidate on it.

The fit also tested on the last 30% of its table's time, so drifted days at
the end of the table were tested on, not learned from.

## Decision

- **A drift request's candidate waits for the drift.** It is due once
  `DRIFT_DAYS` (2) finalised days from the start of the run of drift and one
  more are in (`models/live.candidate_due`). The run starts where the
  consecutive drifted days start, walking back past a request on the same
  run that already closed (`models_job.drift_began`).
- **It learns from the drifted days.** It is fitted on every day of its table
  but the last and tested on the last (`models_job.train_until`); the replayed
  verdict (ADR 33) then judges it on later days.
- **Only a candidate that saw the drift answers it.** A request closes as
  answered only when its candidate beat the incumbent and its training
  reached the first drifted day (`retrain`, `run_pass`).
- **The replayed verdict reads `REPLAY_DAYS` = 2 days**, not 3 (ADR 33). A
  live day is about a hundred million transactions, about three million of
  them fraud; the gate's table keeps 200,000 frauds and 400,000 legitimate
  rows from whatever days it reads. A second day is for a day that is odd,
  not for the count.

Requests opened by hand (ADR 32) are unchanged.

## Consequences

- For the drift that began on 2026-10-10: the monitors open a request about
  2026-10-14 if it continues; the candidate is fitted once 2026-10-12 is
  finalised (about 2026-10-20T06:00Z) on days to 2026-10-11 and tested on
  2026-10-12; after its pull request is merged and the image rolled, the
  replayed verdict reads 2026-10-13 and 14 (finalised about 2026-10-22T06:00Z).
  A promotion is possible about seven days before the window ends.
- A candidate learns from two drifted days among up to twelve earlier ones.
  If that is not enough to beat the incumbent the request stays open, and the
  next candidate is fitted three newly finalised days later (`NEW_DAYS`).

## Sources

- `verdict/models/live.py`, `verdict/live/models_job.py`,
  `verdict/models/retrain.py`, `tests/test_models_live.py`.
- ADR 12 (drift monitors), ADR 24 (retraining stops at a pull request),
  ADR 28 (the live models job), ADR 32, ADR 33.
