## Retraining candidate challenger-6b09560385ee

Incumbent champion: `champion-8d960d985749`. Candidate: `challenger-6b09560385ee`.

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

Fitted as of 2027-01-24T23:59:58.994223+00:00 on 2,563,087 rows (1,092,170 frauds; 0 left out because their labels had not arrived), 1,444 trees. Both models scored on the same 605,751 later rows, which neither was trained on.

| Measure | Champion | Candidate | Difference (95% CI) |
|---|---:|---:|---|
| Test PR-AUC | 0.2613 | 0.8238 | +0.5626 (+0.5584 to +0.5662) |

Candidate model hop, single row: 0.135 ms at p50, 0.229 ms at p99.

### What is missing

- A labelled shadow window, which the promotion gate needs and this does not have.
- The gate's verdict on it, with its margins and latency check.
- A person who has read both.

The candidate was fitted on transactions up to 2027-01-17, which reaches the first drifted day (2027-01-15), so it has seen the shifted stream.

The drift request is **answered**: this candidate beats the incumbent by an interval that excludes zero. It still reaches the pointer only through shadow and the gate.
