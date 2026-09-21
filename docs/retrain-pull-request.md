## Retraining candidate challenger-efd837c03dbf

Incumbent champion: `champion-8d960d985749`. Candidate: `challenger-efd837c03dbf`.

**This pull request does not promote anything.** Merging it accepts the candidate as the challenger to run in shadow. The champion pointer moves only on a second pull request carrying the promotion gate's verdict on a labelled shadow window (ADR 11), which does not exist yet and cannot until the candidate has scored live traffic beside the champion.

### Why this was built

### Drift on 2 consecutive days, to 2027-01-16

A retrained candidate is requested. It runs in shadow and is promoted only through the promotion gate and a merged pull request.

| Quantity | Day | Values | PSI | KS statistic | KS p-value | Status |
|---|---|---:|---:|---:|---:|---|
| device_distinct_cards_1h | 2027-01-15 | 55,395 | 0.080 | 0.113 | 0 | drifted |
| device_distinct_cards_1h | 2027-01-16 | 55,598 | 0.115 | 0.139 | 0 | drifted |
| device_distinct_cards_24h | 2027-01-15 | 55,395 | 0.425 | 0.268 | 0 | drifted |
| device_distinct_cards_24h | 2027-01-16 | 55,598 | 1.495 | 0.520 | 0 | drifted |
| merchant_amount_mean_1h | 2027-01-15 | 55,395 | 0.134 | 0.106 | 0 | drifted |
| merchant_amount_mean_1h | 2027-01-16 | 55,598 | 0.171 | 0.123 | 0 | drifted |
| merchant_distinct_cards_1h | 2027-01-15 | 55,395 | 0.168 | 0.145 | 0 | drifted |
| merchant_distinct_cards_1h | 2027-01-16 | 55,598 | 0.172 | 0.154 | 0 | drifted |
| merchant_txn_count_1h | 2027-01-15 | 55,395 | 0.173 | 0.147 | 0 | drifted |
| merchant_txn_count_1h | 2027-01-16 | 55,598 | 0.184 | 0.153 | 0 | drifted |

### The candidate, offline

Fitted as of 2027-01-21T23:59:59.029905+00:00 on 1,959,184 rows (748,078 frauds; 0 left out because their labels had not arrived), 1,810 trees. Both models scored on the same 1,209,654 later rows, which neither was trained on.

| Measure | Champion | Candidate | Difference (95% CI) |
|---|---:|---:|---|
| Test PR-AUC | 0.2654 | 0.2630 | -0.0024 (-0.0031 to -0.0015) |

Candidate model hop, single row: 0.148 ms at p50, 0.224 ms at p99.

### What is missing

- A labelled shadow window, which the promotion gate needs and this does not have.
- The gate's verdict on it, with its margins and latency check.
- A person who has read both.

**The candidate has not seen the drift.** The latest transaction it was fitted on is from 2027-01-14, and the first drifted day is 2027-01-15, 1 days later. Drift is reported a day after it starts and a label takes a week to arrive, so the first candidate a request can produce is fitted entirely on the old regime. It is a rebuild, not an answer. The promotion gate makes that safe, since a candidate that has not seen the shift will not beat the incumbent; it is said here so a losing result is not a puzzle, and so the request fires again once the shifted days' labels arrive.
