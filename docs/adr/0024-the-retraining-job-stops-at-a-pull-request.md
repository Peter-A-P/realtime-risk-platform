# 24. The retraining job stops at a pull request

- Status: accepted, 2026-09-21
- Date: 2026-09-21
- Deciders: Peter Parker (that a candidate which cannot see the drift is safe,
  because it has to beat the incumbent to move anything); the build session
  (what the job does, where it stops, and what it reports)

## Context

ADR 12's trigger opens a retraining request after two consecutive days of the
same quantity drifting, and ADR 23 showed it doing so one day after a real
regime change. Nothing acted on the request. `PLAN.md` week 6 asks for the
rest: a retraining job, and a pull request that carries the evidence to a
person. The rule it has to keep is the repository's: **nothing promotes
itself.**

## Decision

**The job fits, compares, writes a pull request, and stops.** `verdict retrain`
rebuilds the request from a saved drift report, fits a candidate exactly as
the champion was fitted (same parameters, same split in time, no label that
had not arrived), exports it, scores it against the incumbent on rows neither
was trained on, and writes a JSON report and a pull request body. It never
moves the champion pointer, never writes a flag, and never runs the promotion
gate, because the gate is judged on a labelled **shadow** window (ADR 11) and
a new candidate has none. `tests/test_retrain.py` asserts the pointer file is
byte for byte unchanged after a run. The pull request says what it is: merging
it accepts the candidate as the challenger to run in shadow, and a second pull
request carrying the gate's verdict is what moves the pointer.

**It reports whether the candidate has seen the drift, from the transactions.**
Drift is flagged a day after it starts (ADR 23); a label takes a week to
arrive (ADR 10). So the first candidate a request can build is usually fitted
entirely on the old regime. Peter's point, and it is right: that is safe,
because a candidate that has not seen the shift cannot beat the incumbent and
nothing moves. It is reported anyway, so that a losing candidate is not a
puzzle to the person reading the pull request, and so the request is asked
again once the shifted days' labels are in. Coverage is judged by the latest
transaction the candidate was fitted on, not by its cutoff: on the synthetic
track the two are a week apart, and the first version of this check compared
the cutoff and would have reported a blind candidate as trained on the shift.

## Evidence

The ADR 23 drift run's request opened on 2027-01-16, one day into the
card-testing wave. Twenty days of stream were replayed into a separate working
directory (35,998,419 served, 3,168,838 kept), so the champion's own table stays
reproducible. The incumbent is the shipped `champion-8d960d985749`.

| Candidate | Last day trained on | Tested on | Champion | Candidate | Candidate minus champion |
|---|---|---|---|---|---|
| Built when the request opened (`docs/retrain.json`) | 2027-01-14, the day before the drift | days 14 to 20 | 0.2654 | 0.2630 | **-0.0024 (-0.0031 to -0.0015)** |
| Built once three drifted days' labels had arrived (`docs/retrain-later.json`) | 2027-01-17 | days 17 to 20 | 0.2613 | 0.8238 | **+0.5626 (+0.5584 to +0.5662)** |

Test PR-AUC, 95 percent bootstrap intervals, synthetic track, offline replay.

Three findings, in the order they matter.

**The drift did real damage.** The champion scores 0.84 on the stream it was
fitted for (ADR 19) and 0.27 once the card-testing wave arrives. The regime
doubles the fraud rate, which on its own would lift PR-AUC, so this is a
collapse, not a base-rate effect. The monitors' alarm on day one was an alarm
about something that mattered.

**The first candidate cannot help, and the gate stops it.** Fitted on
transactions up to the day before the drift, it is a rebuild of the old
regime and loses by an interval that excludes zero. Nothing is promoted.

**Once labels from the shifted days arrive, retraining recovers.** Three days
of drifted transactions, labelled, take the candidate to 0.82, back to where
the champion was before the drift, and it beats the incumbent by 0.56. The
earliest such candidate is as of 2027-01-24: the drift began on 2027-01-15,
so recovery by retraining comes about nine days after it, of which one is
detection, three are the drifted days the model needs to see, and the rest is
the label delay.

## Consequences

- **The week after a drift is carried by everything except the model.**
  Between the alarm and the first useful candidate, the champion runs at
  about a third of its PR-AUC and no retrain can change that. That is the
  case for the rules and for the review queue's expected-loss ranking (ADR 22)
  doing their job in exactly that window, and it is worth watching on the live
  dashboard as its own panel rather than inferring.
- **The request must be asked again.** A request whose first candidate lost
  has not been answered, and the trigger's rule that an open request
  suppresses the next would then keep the platform silent about a stream that
  is still drifted. So a request closes only when it has been answered
  (`drift/trigger.resolve`, added the same day): **answered** when a candidate
  beats the incumbent by an interval that excludes zero, **drift ended** after
  two consecutive calendar days on which every quantity it named was judged
  and none drifted, the same bar it took to open, and **still open**
  otherwise. The two reports above record it: the first candidate leaves the
  request `still-open`, the later one `answered`.
- The job never runs the promotion gate. The second pull request, with the
  gate's verdict on a labelled shadow window, is produced by the live stack,
  which is where a shadow window exists.
- `compare_models` and the job take the split point as an argument, because a
  later retrain tested on the earlier split's rows would include rows it was
  trained on.

## Sources

- ADR 10, the label delay and the rule against training on labels that had not
  arrived.
- ADR 11, the promotion gate and the shadow window it is judged on.
- ADR 12 and ADR 23, the trigger and the run that fired it.
- ADR 19 and ADR 21, the champion and the stream.
