# Chaos captures: the raw signals of every fault

`verdict chaos run` reports what a fault did (`docs/chaos/`, `docs/failure-modes.md`).
`verdict chaos capture` makes the same runs and keeps what an operator would have seen
while they happened: logs, metrics, and, in a file of its own, which fault it was. It was
added on 2026-10-17 at Peter's request, for project 10 in the same portfolio, which learns
to group failure symptoms by their cause and needs runs whose cause is known.

It runs against the local stack (`deploy/compose`) and never the live one.

## What a campaign makes

Every fault in `verdict/chaos/faults.py`, five times each with its duration or intensity
varied, and two control runs with nothing done, in a shuffled order:

| Fault | Done to | What it is | Varied |
|---|---|---|---|
| `pause` | broker | the broker's container frozen (`docker pause`) | 15 to 60 s; from inside a scorer batch or not |
| `throttle` | broker | the broker held to a twentieth of a CPU | the same |
| `scorer-stall` | scorer | the scorer stops inside a batch while the stream keeps coming | 15 to 60 s |
| `duplicates` | producer | a share of records sent twice | share 0.05 to 1 |
| `out-of-order` | producer | records reversed within blocks of the stream | blocks of 0.2 to 5 s |
| `poison` | producer | a share of records cut off mid-JSON | share 0.002 to 0.5 |
| `schema-change` | producer | a share of records with `amount_cents` renamed `amount` | the same |
| `clock-skew` | producer | event times moved ahead or back | -300 s to +1 day |
| `none` | nothing | the control | |

Each run sends 90,000 transactions at 1,000 a second with the fault from the tenth second,
waits for them to be decided, and keeps 20 seconds of quiet on each side.

## What a run keeps

    <run_id>/manifest.json             the ground truth, apart from the signals
    <run_id>/logs/harness.jsonl        the scorer's process output
    <run_id>/logs/broker.jsonl         the broker container's log
    <run_id>/prometheus/series.json    every series, query_range at the 1 s scrape interval
    <run_id>/prometheus/metadata.json  each metric's type and HELP text
    <run_id>/prometheus/scrape.json    the scrape config, its targets, and the window

Log records are JSON lines with `ts` (UTC), `service`, `severity` and `message`; a
broker-client line also has `client` and `facility`, and a traceback is one record with
`exception_type` and the full `traceback`. The manifest has the run id, the fault and its
parameters, when it began and ended (UTC), the components it targeted, the commit, the host,
the host's CPU load before and after (the machine is shared with other projects), and the
harness's own report.

**Signals are as the platform emits them.** The scorer logs almost nothing: a healthy run's
`harness.jsonl` is empty, and a broker fault shows as the Kafka client's own error lines
and, if the scorer dies, its traceback. Nothing was added to make faults easier to tell
apart. **There are no traces**: the platform has no OpenTelemetry, and adding it would be a
change to its design. **No alerts**: the local Prometheus evaluates no rules, and the live
rules wait minutes, longer than a run.

Captures are large (6 to 10 MB a run, mostly series) and are kept outside git, under
`chaos-captures/`.
