# 36. The window's record outlives the stack

- Status: accepted, 2026-10-23
- Date: 2026-10-23
- Deciders: Peter Parker (all three: the dashboard served again from the
  archived metrics, the window day by day on the demo site, and the kept
  history on the build machine, 2026-10-23); the build session (how)

## Context

Teardown is part of done (`down.sh`), and `terraform destroy` deletes the
data volume. Everything the live window kept is on it: Prometheus, every
metric since before go-live (98 MB on 2026-10-23, 75-day retention);
history's kept sample (every reviewed or declined decision, a tenth of
approved frauds, a hundredth of approved legitimate ones, about 1.3 GB a
day); and the models job's state. The public dashboard at
risk.peterparker.ca is Grafana on the same instance, so it ends with it.
The committed records (`docs/live/`) carry the verdicts and candidates,
not the window's operation. Without a step before teardown, the thirty
days could be shown afterwards only as the final report's few numbers.

## Decision

Three things, so the window can be seen after the stack is gone.

1. **The dashboard, served again from its own data.** `deploy/archive/
   fetch.sh --to DIR [--final]` streams the Prometheus directory, the kept
   history and the job's state from the volume through a private bucket
   (`aws_s3_bucket.archive`, only the instance may write, objects expire
   after thirty days, `force_destroy` so `down.sh` still leaves nothing) to
   the build machine, checks each archive, and empties the bucket.
   `--final` stops Prometheus first, so its copy is exactly what it wrote.
   `deploy/archive/serve.sh DIR` runs the same Prometheus and Grafana
   versions locally with no scrapes, and the same dashboard
   (`verdict observe grafana --window-start --window-end`), which then opens
   on the whole window, never refreshes, and takes every rate over
   Grafana's interval rather than a fixed five minutes, since a five-minute
   rate drawn at a thirty-day zoom shows one sample of each step. Retention
   is a century: it is measured back from the newest block.
2. **The window day by day, in the repository and on the site.**
   `verdict observe daily` reads the same metrics into one row a UTC day:
   decisions, the busiest minute, mean, p50, p99 and p99.9, the share over
   50 ms, the actions, the models that decided, dead letters, and minutes
   with no decisions. It is committed as `docs/live/daily.json` and drawn on
   the demo site. Every decision is counted, so the counts carry no
   sampling interval; percentiles are interpolated within the histogram's
   buckets, and each row says which bucket its p99 fell in.
3. **The kept history, on a machine of Peter's.** It is synthetic, so the
   data terms (`docs/data.md`) do not reach it, and it is what any analysis
   after the window would start from. About 30 GB by the window's end;
   `fetch.sh --to` takes any directory, another disk included.

## Consequences

- The deploy identity needs the bucket too: `deploy/aws/iam/archive-policy.json`, a
  policy of its own because the deploy policy is within 60 characters of IAM's
  6,144 limit, attached by Peter as the other two were.
- One more resource in the account while the stack runs. It holds data
  only between a fetch's upload and its download, a few hours at most.
- The archived dashboard is local, not public. The public record is the
  demo site's day-by-day chart and the repository.
- Prometheus also kept the dry runs and the failed first window before the
  restart (ADR 31): the archive opens on the window's own start, read from
  the volume's `window-start`, and the earlier days stay reachable by
  zooming out.
- A copy taken while Prometheus runs may end with a torn write-ahead log,
  which Prometheus repairs on start, losing at most its last few minutes;
  the final fetch is taken with it stopped.
- The labels of the window's last week arrive a week after it, so its last
  days are finalised only if the stack runs past the thirtieth day. That is
  Peter's decision, with the move to OVH (ADR 31); the archive takes
  whatever is there when it runs.
