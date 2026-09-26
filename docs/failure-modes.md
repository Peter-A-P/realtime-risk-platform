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

## The broker stops answering (the plan's redis_down and stream_throttle)

**Found:** 2026-09-22, on purpose, against the local broker. The plan names
Redis going away; there is no Redis on the decision path, because the
features live in the scorer's process (ADRs 4 and 8), so the store that can
go away under the scorer is the broker.

**What was done.** `verdict chaos run` (`verdict/chaos/faults.py`) sends
generated transactions at the live rate, 1,000 a second, through fresh
topics on the local Redpanda container, runs the real scorer as the service
runs it and under the live stack's restart policy, and freezes the broker's
container with `docker pause` for 15 seconds, from inside a scorer batch:
after a transaction is decided and before its decision is flushed.

**What it did** (`docs/chaos/pause-15s-before.json`). The scorer's flush
gave up after 10 seconds and raised `132 records still undelivered after
10.0s`; the service stopped on it, 20 seconds into the run. The restart
policy started a second scorer, which began cold: empty feature windows,
every card served "no history", which live is up to a day of thin features
after a broker blip of seconds. The 132 transactions of the batch that had
not been checkpointed were decided a second time. The same freeze at a
wall-clock moment usually misses a batch in flight and was ridden out
(`pause-15s-between-batches-before.json`), so this is a fault that is rare
per outage and all but certain over a sixty-day window. A standalone
producer and consumer showed the same split: the producer gave up at 10
seconds; the consumer rode out 15 and 60.

**What changed.** A producer waits for a broker that has stopped answering,
for ten minutes (`RIDE_OUT_SECONDS`, `verdict/stream/base.py`): the flush's
default, librdkafka's delivery timeout and the checkpoint's retry deadline.
Nothing is decided while the broker is away whether the scorer waits or
restarts, and waiting keeps its windows. The feeds and the label collector
use the same client, so they wait too, where before a feed would have
stopped and resent up to 30 seconds of the stream from its saved place. ADR
8's addendum of 2026-09-22 records it.

**After** (`docs/chaos/pause-15s-after.json`, `pause-60s-after.json`, 90,000
transactions each): through 15 and 60 second freezes inside a batch, one
scorer throughout, every transaction decided exactly once, none set aside.
Decisions fell at most 15.3 and 62.7 seconds behind their sends and were
back within a second of them 6.0 and 19.4 seconds after the broker returned:
a backlog of 60,000 cleared in about 19 seconds, roughly four times the
live rate.

**Throttled, not frozen** (`throttle-60s-after.json`): the broker held to
a twentieth of one CPU for 60 seconds. Decisions were never more than 1.0
second behind and nothing was lost or doubled. At this rate that was not
enough starvation to hurt; a harder throttle is the next thing to try, not
a result claimed here.

**Held in place by** `tests/test_stream.py`: the producer is configured with
the ride-out and its flush waits it, every stream implementation defaults
to the same patience, and a checkpoint retries as long as a flush waits.
The experiment itself needs Docker and the local broker, and reruns with
`verdict chaos run --fault pause --seconds 15`.

## The scorer stalls while the stream keeps coming (consumer lag)

**Found:** 2026-09-22, on purpose; on the live stack before that, by a spot
reclaim.

**What was done.** The scorer stopped for 60 seconds inside a batch, as a
long pause in the process or a very slow model would stop it, while
transactions kept arriving at 1,000 a second (`verdict chaos run --fault
scorer-stall --seconds 60`).

**What it did** (`docs/chaos/scorer-stall-60s-after.json`). It carried on
from where it stopped: 60.0 seconds behind at worst, back within a second
of the stream 16.0 seconds after it resumed, all 90,000 decided exactly
once. On the dry run a spot reclaim does the same for about four minutes,
and the catch-up ran at about 2,200 a second (`docs/STATE.md`).

**What changed.** Nothing: the lag is reported, not hidden. The dashboard
separates latency while serving from the catch-up (ADR 25), and decisions
made during a catch-up are as old as they are.

## Duplicates

**What produces them.** Delivery is at least once. A scorer that stops
between flushing a batch's decisions and checkpointing its transactions is
given the batch again when it restarts, and its ledger of decided events is
in memory, so it decides them again: the 132 in the broker experiment's
before run. A feed restarted after a spot replacement resends up to 30
seconds of records from its saved place.

**What happens to them.** A duplicate within one scorer's life is caught by
the ledger before the feature engine sees it, so no window counts it twice
(`tests/test_scoring.py`). One decided twice across a restart appears twice
on the decisions topic, whose readers key by event id, and is staged twice,
which finalising a day removes (`tests/test_history.py`, including a
duplicate split across batches). A feed's resent transaction arrives behind
the restarted scorer's clock and is set aside as `late` rather than decided
twice; that is reasoned from the code, not yet counted on the live stack,
where the dead-letter panel will show it after each replacement.

**What changed.** The broker fix above removes the commonest cause, a
scorer or feed stopping over a slow broker. The rest is the at-least-once
design (ADR 8) and stays.

## The host's clock jumps (clock skew)

**What was done.** In process, with the feed's clock under the test's
control (`tests/test_live_feed.py`): an hour forward, and two minutes back.
The feed is the only part of the platform that reads the wall clock to
decide anything; the feature engine and the scorer work in event time.

**What it did.** Forward: an hour's records fell due at once and went out
5,000 at a time, flushed between, in event order, none skipped or sent
twice, with the feed's lag metric reporting the hour until it had caught
up. Back: the feed sent nothing until the clock passed where it had been,
and nothing twice. Its place is in the stream, not in the clock.

**What it costs.** A forward jump is a burst the scorer works through like
any catch-up. A backward jump is a silence as long as the jump; over fifteen
minutes `ScorerStopped` fires. The event-to-decision metric subtracts event
time from the wall clock, so it is wrong by the jump while the clock is; the
scorer's own hop timings use a monotonic clock and are not.

**What changed.** Nothing. The instance keeps time with the Amazon Time Sync
Service, and a step of seconds is the realistic case.

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

## A spot replacement (ADR 15, ADR 25, ADR 27)

**What was done to it:** nothing; AWS did it. Eleven times in the 72-hour dry
run (2026-09-22T16:53Z to 2026-09-25T16:53Z, `docs/dry-run-report.json`),
eight of them on 2026-09-25, every one with AWS's two-minute notice recorded
on the volume by the spot watcher.

**What it did:** each time, about two to four minutes with no decisions (the
notice, a launch, a boot), then a catch-up at about twice the live rate, and
decisions on time again seven to eleven minutes after the notice. No stop was
the platform's own. The feeds restored their saved place and resent up to 30
seconds of records, which the scorer's ledger turned away. **The scorer
started every time with empty feature windows**: every card "no history",
and the day-long features a day from full. At a reclaim every few hours it
was almost never on full history, which would have left the live window's
shadow, drift and queue evidence measured on features thinner than the
model was trained on.

**What was changed:** ADR 27. The scorer saves its feature state to the data
volume a slice at a time between batches, a pass every fifteen minutes, and
a replacement restores the last complete pass and replays the records after
it before deciding (`verdict/scoring/recovery.py`). Measured locally on
2026-09-26 at a tenth of the live engine's size: steps of 2.1 ms at p50 and
at most about 11 ms, 7.9 s of pickling a pass, a restore in 5.6 s. At the
live size that is about 80 s of pickling spread over about seven minutes,
and about a minute to restore plus the replay, which lengthens each
reclaim's recovery by that much and ends the day of thin features.

**The test:** `tests/test_recovery.py` stops a scorer at points spread through
several passes, including between deciding a batch and checkpointing it,
restores another, and holds every feature it serves to an uninterrupted
scorer's; six deliberate faults in the save and the restore were each
caught. A save
that cannot be used (none, other feature code, cut short, older than the
topic keeps) starts cold and says why. Not yet seen on the instance: the
first replacement after the roll is the check, in the scorer's log and the
`verdict_engine_restored` metric.

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

### EngineSnapshotStale

The scorer has not finished a save of its feature state for over an hour
(ADR 27); a pass normally finishes minutes after it starts, every fifteen.
The scorer is still deciding, so this costs nothing until the next
replacement, which will replay further than it should or, if the last save
is older than the day the topic keeps, start with empty windows. `docker logs
--tail 100 verdict-scorer`, `ls -la /data/engine` (a `.partial` file that is
not growing means a pass has stopped), and `df -h /data`: a full volume stops
a save before it stops anything else.

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
