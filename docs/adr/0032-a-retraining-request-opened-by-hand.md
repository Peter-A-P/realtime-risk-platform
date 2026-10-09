# 32. A retraining request opened by hand, for a model wrong from the first day

- Status: accepted, 2026-10-08
- Date: 2026-10-08
- Deciders: Peter Parker (retrain on the live window's history through the
  platform's own path, rather than leave the champion and report it;
  2026-10-08); the build session (the request by hand, and its limits)

## Context

`verdict history quality` (`verdict/history/quality.py`) over the restarted
window's first two finalised days (`docs/live-decision-quality.json`,
synthetic live track, weighted as ADR 18 keeps it) found that the champion
barely separates fraud on the live stream. Fraud was 2.97% of transactions;
the champion acted on 19.0% of them (declined 10.3%, reviewed 8.6%), and
what it declined was 5.8% fraud, what it reviewed 4.7%, what it approved
2.4%: acted precision 5.3% and recall 33.9%, about 1.8 times what acting at
random on as many would catch. Transactions it scored 0.98 on average were
5.8% fraud.

Offline the same model scored a test PR-AUC of 0.84 (ADR 19). It was trained
on the scaled synthetic configuration, 4,000 cards and 80 merchants, and the
live stream is the full one, 200,000 cards and 4,000 merchants, whose
feature values (a merchant's transactions an hour above all) it never saw.
ADR 29 found the same mismatch for the drift reference and fixed it by
judging the live window against its own first days. That fix is also why
nothing could catch this: a model wrong from the first day is, to monitors
whose reference is the first days, simply how the stream looks.

The platform's answer to a model that has stopped fitting the stream is a
retrained candidate, a pull request, a week in shadow and the promotion gate
(ADR 11, 24, 28). Only a drift request starts it.

## Decision

**A person may open a retraining request**, with a reason and no drift:
`verdict models-request --state ... --reason ...`. It is the same request
the monitors open, with the reason where their evidence would be, so it
takes the same path and nothing else changes:

- the models job fits a candidate on the window's finalised history once
  `models/live.candidate_due` says so (three finalised days, and three more
  for each further candidate), with the champion's parameters, on one thread;
- it opens a pull request whose title says the request was opened by hand
  and whose body carries the reason, the offline comparison and what is
  missing; merging it makes the candidate the shadow model, by hand;
- after a week of labelled shadow scores the gate opens a second pull
  request with its verdict, and only a person moves the pointer.

**A request opened by hand closes only when a candidate beats the
incumbent.** No drift opened it, so no end of drift can close it
(`drift.trigger.resolve`). Only one request is open at a time; one opened by
hand is refused while another is open, and while it is open the monitors
open none of their own, as for any open request.

The champion and the rules are unchanged until the gate's verdict is merged.
This is model work after the model's week, which `CLAUDE.md` allows with an
ADR naming the platform property it serves: here, the promotion path run end
to end on live traffic, which until now it has been only in replays.

## Consequences

- The window's first candidate is fitted when the third day is finalised
  (2026-10-01, finalised about 2026-10-09T06:00Z), on the days finalised by
  then; the earliest gate verdict is a week of shadow later, leaving about a
  week of the window on a promoted model if it wins. The window's report
  covers the champion before and, if promoted, after.
- The candidate learns from the kept sample, whose acted stratum is the
  current champion's choices kept whole; the weights make the training set
  stand for the stream (ADR 18), so the candidate is not taught to imitate
  the champion it replaces.
- What the request does not fix: the offline numbers in the README describe
  the scaled population, and stay as measured; the live report says which
  model decided which days.

## Sources

- `docs/live-decision-quality.json`, `verdict/history/quality.py`,
  `tests/test_quality.py`.
- ADR 11 (shadow and promotion), ADR 18 (history as a weighted sample),
  ADR 19 (the champion), ADR 24 (retraining stops at a pull request), ADR 28
  (the live models job), ADR 29 (the live drift reference).
