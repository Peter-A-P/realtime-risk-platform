# The sealed schedule, revealed

The live window's fraud schedule was derived from a secret and sealed on
2026-09-27T13:27Z, before the first window opened: the hashes of the secret, of the
derived schedule and of `verdict/events/generator/regimes.py` were committed as
[docs/sealed-schedule.json](../sealed-schedule.json) in commit 85d3d07, and neither file
has changed since. The secret was kept where the build's own AWS identity was denied it.
On 2026-11-07, after the stack was shut down, `verdict schedule verify --reveal` was run
with the secret and reported:

    verified: secret, derived schedule and regimes.py all match the commitment

The derived schedule is [schedule-revealed.json](schedule-revealed.json). Its canonical
JSON (sorted keys, no spaces) hashes to the committed `schedule_sha256`, `a2aae908...`, so
anyone can check that this is the schedule committed before go-live:

    python -c "import json,hashlib; d=json.load(open('docs/live/schedule-revealed.json')); print(hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':')).encode()).hexdigest())"

## When each shift came, and when the monitors saw it

Day zero is the window start, 2026-09-29T19:11Z (the feed's `--start`). Times are UTC.
The monitors judge whole UTC days against a reference built from two full days, first
the window's own (2026-09-30 and 10-01) and, after each promotion, the two days after it
([ADR 29](../adr/0029-traffic-follows-the-day-and-drift-is-judged-against-the-live-baseline.md)).
They were never told the schedule. Flags are from the archived
`state/models/drift/days/*.json` and `requests.jsonl`.

| Regime | Starts | What it changes against regime 1 | First day flagged | Retraining request opened |
|---|---|---|---|---|
| regime-1 | 2026-09-29 19:11 (day 0) | the baseline | never: 10-03 to 10-09 all clean | none from the monitors (one opened by hand on 10-09, ADR 32) |
| regime-2 | 2026-10-09 18:11 (day 9.96) | fraud 0.58x, attacks 2.3x as intense, log-amount -0.19, online share +0.02 | 2026-10-10, its first full day | 2026-10-12 03:02, on 10-10 and 10-11 |
| regime-3 | 2026-10-19 14:11 (day 19.79) | fraud 2.5x, log-amount -0.32, online share +0.19 | 2026-10-20, its first full day | 2026-10-22 03:17, on 10-20 and 10-21 |
| regime-4 | 2026-10-27 04:11 (day 27.38) | fraud 3.0x, log-amount -0.14, online share +0.08 | 2026-10-27, the same day | 2026-10-29 03:40, on 10-27 and 10-28 |
| regime-5 | 2026-11-03 18:11 (day 34.96) | fraud 2.0x, attacks 2.3x, log-amount +0.15, online share -0.08 | already flagging since 10-27; two more features joined on 11-03 and 11-04 | none new: the 10-28 request was still open |
| regime-6 | 2026-11-12 07:11 (day 43.5) | | after teardown | |
| regime-7 | 2026-11-20 20:11 (day 51.04) | | after teardown | |

The schedule covers sixty days so that its length did not say how long the window was;
regimes 6 and 7 never ran.

**What this shows.** Every regime change inside the thirty-day window was flagged on its
first full UTC day, and no day was flagged without a change since the reference: the
seven regime-1 days, and the regime-2 and regime-3 days judged after each promotion's
rebuilt reference (10-15 to 10-19, 10-25 and 10-26), were all clean. Regimes 2 and 3
began in the evening and the afternoon, and the part day they began on was not flagged;
regime 4 began at 04:11 and was flagged that day. Each request opened two days after
its regime's first full day, the floor the rule sets (two consecutive drifted days, asked
once a day after the day is sealed).

**What it does not show.** Regime 5 began in the run-on week, while the monitors were
already flagging regime 4 against a regime-3 reference, so its arrival cannot be told
apart from the shift before it: `card_amount_sum_1h` joined the drifted features on 11-03
and the two session features on 11-04, which is consistent with it but is not a
detection. Three changes are a small sample: this is evidence that the monitors fire on
shifts of this size within a day, not a measured false-alarm rate.

Synthetic live track.
