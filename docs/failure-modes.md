# Failure modes

One section per failure: what was done to the platform, what it did, what was
changed, and the test that holds the change in place. `PLAN.md` lists the
failures to be tried (a store going away, the stream throttling, consumer lag,
clock skew, a poison event, duplicates, events out of order, a schema change).
They are added here as they are tried, in the order they were found, and a
section is not removed when its fault is fixed: the observed behaviour before
the fix is the evidence that the fix was needed.

## A record the scorer cannot decide (poison event, schema change, out of order)

**Found:** 2026-09-18, reading the scorer's code for something else, before any
live traffic. Not yet exercised on the live stack.

**What was done.** Three records the scorer cannot decide, each put on the
transaction topic between two good ones: bytes that are not JSON, a
transaction with `schema_version` 3 (a producer deployed ahead of the
scorer), and a transaction earlier in event time than the one before it.

**What it did.** Each raised out of the scorer's `poll` before the batch was
checkpointed. The scorer stopped. On restart it resumed from the last
checkpoint, read the same record, and stopped again: three restarts, three
identical stops at the same record, and nothing behind it decided. One bad
message was a permanent outage of the whole decision path, and the only way
out would have been an operator moving the group's offset by hand past a
record they had not seen.

**What changed.** A record that cannot be decided for one of those three
reasons is set aside: it goes to the `dead-letter` topic with its bytes
untouched and the reason, is flushed with the batch, and is checkpointed past
like any decided record. A run of fifty in a row stops the scorer instead,
because that is a fault upstream rather than a bad record, and setting a whole
stream aside quietly is worse than stopping. Any other error while deciding
still stops the scorer at once. ADR 8's addendum of 2026-09-18 records the
decision and its cost: a transaction set aside gets no decision.

**Held in place by** `tests/test_scoring.py`: one malformed record set aside
and a restarted scorer finding nothing left to do; a newer schema version set
aside with its reason; a late transaction set aside with no decision while the
next one is decided; a run of records past the limit stopping the scorer; and
a good record resetting the count. `tests/test_stream_parity.py` holds a
reordering stream to the reference and reports the late events as missing
decisions.

**Still open.** Replaying the dead-letter topic once a fault is fixed is a
manual step with no tool yet, and nothing alarms on the dead-letter count.
Both belong with the dashboards and alarms, on the live stack.

## Known before it happens: history the platform could lose (ADR 18)

Not yet tried, so there is no observed behaviour; recorded now because the
design chose to accept them, and each needs a chaos run before go-live.

- **The label collector falls more than a day behind.** The labels topic
  keeps a day, so labels older than that are gone. They show up a week later
  as candidates with no label in that day's manifest, and those rows are not
  kept. Since 2026-09-22 the collector's lag is a metric and an alert at two
  hours (`LabelCollectorBehind`, below).
- **A kernel crash, not a spot interruption.** The scorer stages rows without
  an fsync, so a kernel crash can lose the last batches of rows whose
  transactions were checkpointed. A spot interruption is a clean shutdown
  and flushes them. Visible as fewer staged rows than decisions for the hour.
- **The compactor does not run.** Staged hours pile up unsealed at about 284
  bytes a row instead of 30, about a gigabyte an hour. Since 2026-09-22 the
  age of the oldest unsealed hour is a metric and an alert at three hours
  (`HistoryUnsealed`), and failing runs and a day not finalised are alerts of
  their own (below).

## Known before it happens: a spot replacement (ADR 15)

Not yet tried on the instance. What the design says happens, to be checked by
a timed interruption drill before go-live:

- **The feeds** restore their saved place and resend up to 30 seconds of
  records, then catch up on whatever came due while they were down. The
  duplicates are visible on the dashboard and absorbed by the scorer's ledger
  and history's finalising. A fresh start with no saved place, more than an
  hour into the window, is refused rather than replaying the window.
- **The scorer** starts with empty feature windows and serves "no history"
  until they refill. Its rebuild by replay is not built, and the engine's
  memory at the live rate is the first thing to measure (ADR 15).

## Alerts, and what to do about each (ADR 26)

Prometheus on the instance evaluates the rules in `deploy/live/compose.yml`;
each one that fires is emailed once, again every six hours while it keeps
firing, and once when it stops. The email names its section here. Every
command below runs on the instance, reached with `aws ssm start-session
--profile verdict --region ca-central-1 --target <instance id>`, and the
compose command is the boot script's: `docker compose -f
/opt/verdict/compose.yml --env-file /etc/verdict/stack.env --env-file
/etc/verdict/tunnel.env`.

None of these has fired on the live stack yet. When one does, what it
showed and what was done go into a section above, like every other failure.

### LabelCollectorBehind

The label collector has written labels more than two hours older than the
labels feed has sent, for fifteen minutes. The labels topic keeps a day:
past that, labels are gone and their rows are lost from the kept sample
(ADR 18). Look at `docker logs --tail 100 verdict-labels`. A collector that
is running but slow is working off a backlog and needs nothing unless the
gap keeps growing; one that is restarting in a loop needs its error fixed
within the day. Losing some labels costs rows from the sample, which the
day's manifest reports as unlabelled; it does not stop the platform.

### HistoryUnsealed

An hour of staged decisions or labels ended more than three hours ago and is
still unsealed. Either compaction runs are failing (`CompactionFailing` will
usually be firing too) or the compactor is not running. Unsealed staged rows
take about a gigabyte an hour of the data volume against about a tenth of
that sealed. Look at `docker logs --tail 100 verdict-compactor` and the
kernel's log, `dmesg | grep -i oom`; the compactor was killed for memory
once, on 2026-09-22 (`docs/STATE.md`).

### DayNotFinalised

A day has had all its labels for over an hour and is not final. Finalising a
day at the live rate takes minutes, so this is a finalising run failing each
time it tries. Every day waiting keeps eight days of staged rows on the
volume longer. The compactor's log has the error; `verdict history
footprint` is the instrument if memory is the suspect (ADR 18's addendum).

### CompactionFailing

Three or more compaction runs exited with an error in half an hour. The
compactor starts a new process every five minutes, so it will keep trying;
what matters is why. `docker logs --tail 200 verdict-compactor`, and exit
code 137 in it means the kernel killed the run.

### ScorerStopped

The scorer has decided nothing for fifteen minutes. That is the platform
down, and every minute of it counts against the availability figure (ADR
25). `docker ps` to see whether it is running, then `docker logs --tail 100
verdict-scorer`. A run of fifty records it cannot decide stops it on
purpose (the first section above); the dead-letter topic has them.

### TargetDown

A service Prometheus scrapes (`job` in the email says which) has been
unreachable for ten minutes: stopped, restarting in a loop, or hung. A spot
replacement takes about four minutes and is not this. `docker ps -a`, then
its log.

### PrometheusUnreachable

The alerts relay has not been able to read Prometheus for ten minutes, so no
other alert can fire until it can. Alerts already emailed are not called
resolved while Prometheus cannot be seen. `docker logs --tail 50
verdict-prometheus`; its data is on the volume and survives a restart.

If no email comes at all, the stack may be gone rather than quiet: the
auto-scaling group could not find spot capacity, or the host's sender
(`systemctl status verdict-alert-send`) has stopped. The dashboard's "Since
the last decision" panel is the check that needs nothing on the instance.
