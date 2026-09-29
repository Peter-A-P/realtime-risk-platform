# 31. The live window restarts: thirty days, on demand, with 32 GB

- Status: accepted, 2026-09-29
- Date: 2026-09-29
- Deciders: Peter Parker (stop the failed window; restart on a machine AWS
  cannot take back, with 32 GB, for thirty days, and then either stop or move
  to OVH for several months; accept the cost; 2026-09-29); the build session
  (the fixes, and how a new window starts clean)

## Context

The live window opened on 2026-09-28 at 08:51 UTC on one spot instance with
16 GB in `ca-central-1d` (ADR 14, ADR 20). In its first 28 hours AWS
reclaimed the instance thirteen times, four of them between 11:50 and 12:40
UTC on the second day, across four instance types, so the allocation
strategy of ADR 20's third amendment could not help: the zone was short. The
platform came back by itself from twelve of the thirteen, each in 7 to 13
minutes. The thirteenth broke it, through two faults the storm found:

- **A restore that outgrew the machine.** The reclaims kept interrupting the
  scorer's saves, so the replacement at 12:42 had more than two hours of
  records to replay, and the restore ran the machine out of memory. It froze
  (tunnel and SSM with it), the scorer was killed and restarted into the
  same restore, and decisions stopped from 12:40 (ADR 27, addendum of
  2026-09-29).
- **State files left empty by a hard stop.** Terminating the frozen instance
  left the transactions feed's saved place and the alert relay's state at
  zero bytes: each had been replaced by rename without an fsync. The feed
  would not start without its place, so no transaction was sent after 13:28,
  and the scorer, restored on a new instance with swap, ran dry at 14:04.

And the machine was too small regardless of reclaims. In the 72-hour dry run
the scorer held about 7.5 GB and on the window's first day about 9 GB; on the
second, with a full day of day-long windows built up, 13 GB or more before
the restore and about 15 GB an hour after it, with 2 GB more for the broker,
the feeds and the dashboard, on 16 GB. How much of that growth is the day's
windows filling and how much is something else is not yet explained; a local
measurement puts a restore's own overhead well under the difference.

## Decision

1. **The failed window is stopped** (the group at zero, the data volume
   kept) and recorded as it happened: this ADR, ADR 27's addendum,
   `docs/STATE.md`, and the live window's report will say it.
2. **A new window of thirty days** starts at a fresh go-live, on the same
   sealed schedule: the secret has not been revealed, `regimes.py` is
   unchanged and still hashes to the commitment, and the schedule was derived
   for sixty days, of which the new window runs the first thirty. The secret
   is revealed the day after the thirty days end. At day thirty Peter decides
   between stopping and moving the stack to OVH for several months; moving
   carries the data volume's contents, so the stream would continue.
3. **On demand, not spot**: `var.on_demand`, the group's whole capacity on
   demand, which AWS does not reclaim for capacity or price.
4. **32 GB, 4 vCPU**: r6i.xlarge first, then r7i.xlarge and r5.xlarge.
   r6a.xlarge, cheaper, is not offered in `ca-central-1d`, where the data
   volume is.
5. **Every state file is replaced durably** (`verdict/durable.py`): data
   synced before the rename and the directory after it, so a machine stopped
   at any moment leaves the old file or the new one. The feed's place, the
   alert relay's state and outbox, the drift and models jobs' state, history
   hours and days and their manifests, the champion pointer and the scorer's
   saved state all go through it. The alert relay treats state it cannot read
   as none, since an alert told twice is better than none.
6. **Swap stays**, 8 GB at swappiness 60, as the margin for a restore.
7. **A new window starts clean at boot.** The boot script keeps the window's
   start on the data volume; a boot for a different start clears the last
   window's broker data, saved state, feeds' places, history, models job and
   alerts told before any service starts. `deploy/go-live.sh` no longer clears
   a running instance over SSM, which needed one to be up and reachable.

## Cost

On demand in `ca-central-1`, r6i.xlarge is about US$0.28 an hour, about
CA$275 a month; with the data volume, about CA$295 for the thirty days, where
spot at 16 GB was about CA$90 a month. Estimated, not from the price list,
which the build identity cannot read. The account's monthly budget alarm,
set outside Terraform, will be exceeded and should be raised to match.

## Consequences

- The live window's report covers thirty days, and the first window's
  28 hours are reported beside it as what happened, not dropped.
- What makes the scorer's memory grow past the dry run's is still to be
  explained; the dashboard gets the scorer's resident memory so it is watched
  in the new window rather than found by the next outage.
- `PLAN.md`'s sixty days, the README and the demo site now say thirty.

## Sources

- Amazon EC2 Auto Scaling, instances distribution and on-demand allocation:
  https://docs.aws.amazon.com/autoscaling/ec2/userguide/allocation-strategies.html
- Amazon EC2 on-demand pricing: https://aws.amazon.com/ec2/pricing/on-demand/
- `rename(2)` and `fsync(2)`: a rename is atomic, not durable, without an
  fsync of the file and its directory.
  https://man7.org/linux/man-pages/man2/fsync.2.html
- ADR 14, 15, 20, 25, 26, 27.
