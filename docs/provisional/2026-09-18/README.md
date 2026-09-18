# Provisional: measured on a shared machine, 2026-09-18

**These are not results.** They are kept because the comparisons drawn from
them in `docs/latency-budget.md` are structural, and a finding with no
artefact behind it cannot be checked.

What happened: the runs were gated on one named process on the build machine
finishing. That process finished at 20:06 UTC, and another project's job
(`finishline backtest --hierarchical --weather`) started at 20:07 and ran
through every run here, from 20:09 to about 20:50, holding 40 to 60 percent
of the CPU. The gate should have been the machine's load, not a process name,
and the CPU reading the gate logged came back empty, so the fault was not
caught until afterwards.

What survives a busy machine and what does not:

- **A median, mostly.** The p50s here are probably close to what an idle host
  gives; they are not published as such.
- **A 95th or 99th percentile, no.** Every tail here may be the other job.
- **A throughput ceiling, no.** The HTTP runs' achieved rates (287 to 488 per
  second) were measured with the CPU shared, and a ceiling is exactly what a
  shared CPU lowers.
- **Large structural differences, yes, with care.** The scorer's flush at a
  median of about 2 ms inside the broker's network against about 55 ms through
  the Windows port forwarder; and the share of HTTP transactions the engine
  refused as out of order once more than one connection carried them. Neither
  is a small effect a busy CPU could produce.

| File | What |
|---|---|
| `latency-week4-memory.json` | In-process stream, Windows host |
| `latency-week4-redpanda.json` | Redpanda from the Windows host, through the port forwarder |
| `latency-week4-memory-in-network.json` | In-process stream, in a Linux container |
| `latency-week4-redpanda-in-network.json` | Redpanda from a container on the broker's own network |
| `latency-week4-http-c{1,2,4,8}.json` | The HTTP endpoint at 1,000 per second offered, over 1, 2, 4 and 8 connections |

All at 1,000 transactions per second offered, 20,000 per run, five runs, the
first 1,000 of each excluded, the week 4 stand-in model.
