# 25. Latency while serving and availability are two numbers

- Status: accepted, 2026-09-22
- Date: 2026-09-22
- Deciders: Peter Parker (report the two separately, rather than one number
  over every minute or one with the bad minutes dropped); the build session
  (the rule that separates them, and its evidence)

## Context

The live stack is one spot instance (ADR 14). On 2026-09-22, the dry run's
second day, AWS reclaimed it five times in eleven hours (`docs/STATE.md`).
Each reclaim is about four minutes with no decisions (two minutes' notice,
then a launch and a boot), then a catch-up at about twice the live rate, in
which every decision is minutes old. The dashboard's p99 read about five
minutes through each one.

`PLAN.md` asks for decision latency with confidence intervals across daily
windows, and separately for throughput, uptime and the count of instance
replacements. It does not say which minutes the latency figure covers. Over
every minute, a p99 reports the recovery from AWS taking the machine as
the scorer's latency, which is not what the scorer does. With those minutes
dropped and nothing else said, it hides that decisions stopped. Either
would mislead the reader the figure is for.

## Decision

**Two numbers, under rules fixed before the live window starts.**

1. **Latency while serving**: decision latency, p50, p95 and p99, over every
   minute of the window outside the recovery window of a spot reclaim, with
   a 95 percent t interval across daily figures.
2. **Availability**: every minute counted. Uptime (minutes with decisions),
   each reclaim with its recovery time and the decisions it made late, and
   every stop that was not a reclaim, listed beside them. The latency over
   every minute is published next to the figure above, so the difference is
   visible rather than inferred.

**Only a reclaim AWS announced is left out, with AWS's notice as the
evidence.** The boot script starts a watcher that polls the instance
metadata for `spot/instance-action` and, when the notice appears, writes it
with its own timestamp to `/data/interruptions/<instance>.json` on the data
volume, where it outlives the instance; every boot is appended to
`boots.jsonl` there too. A crash, a hang, an out-of-memory kill, a deploy, or
anything else the platform did to itself has no notice and stays in the
latency figure.

**A recovery window starts at the notice and ends on throughput, never on
latency**, so the rule cannot pick its own answer: at the first run of two
minutes in which the transaction feed is at most one second behind its
clock and the scorer decides no more than 1.1 times what the feed sends. A
scorer working off a backlog decides faster than the feed sends; once it
decides what arrives, it has caught up, and every minute from there counts,
however slow. A notice after which the stream never falters within fifteen
minutes excludes nothing.

`verdict/observe/availability.py` holds the rule; `verdict observe report`
reads the minutes from Prometheus and the notices from the volume and
writes both numbers as JSON. It runs on the instance, on the stack's
network:

    docker run --rm --network verdict_default \
        -v /data/interruptions:/data/interruptions:ro "$VERDICT_IMAGE" \
        observe report --start ... --end ... --out /dev/stdout

## Evidence

`tests/test_availability.py`: a reclaim's window runs from its notice to two
caught-up minutes and no further; the same stop with no notice is left in,
and listed as the platform's own; a slow minute after catching up stays in;
one caught-up minute in the middle of a catch-up does not end it; minutes
Prometheus has nothing for are kept as stopped minutes, since a replacement
is exactly a hole in the scrape; every excluded minute still counts in
uptime; the quantile matches Prometheus's `histogram_quantile`.

## Consequences

- The dashboard shows both: the p99 over every minute, and a p99 only while
  caught up, by the same throughput rule. The dashboard cannot read the
  notices, so its second panel leaves out every catch-up, reclaim or not,
  and its description says so; the published figure is the report's.
- The latency histogram gains 60 s and 120 s buckets, so a catch-up of a few
  minutes reads as a few minutes.
- Prometheus keeps 75 days rather than 15, so the report can be computed
  over the whole window after it ends.
- The README's live table carries both numbers.
- The reclaims of 2026-09-22 predate the watcher and have no notices. They
  are dry-run history, not a published figure, and the report would count
  them against the platform, which is the direction a rule like this should
  fail in.
- A reclaim still costs the scorer its feature windows (ADR 15, open): after
  one, decisions are on-time but made on thin history for up to a day. That
  is a quality cost, not a latency one, and neither number here hides it.

## Sources

- Amazon EC2, spot instance interruption notices, the `spot/instance-action`
  metadata item and its two-minute warning.
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html
- Prometheus, `histogram_quantile`.
  https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile
- ADR 14 (the single spot instance), ADR 15 (recovery), ADR 20 (the instance).

## Addendum, 2026-09-28: a catch-up read as an hour

On the live window's first day the dashboard's event-to-decision percentiles
showed about an hour after each spot replacement. The waits were not an hour.
The histogram had one bucket from 300 to 3,600 seconds, and
`histogram_quantile` places a quantile inside its bucket by linear
interpolation, so every catch-up longer than five minutes read as somewhere
up to an hour: 3,567 s at the p99 and 1,950 s at the p50 in the minute after
the 18:16 replacement. The exact mean over the same minute, the histogram's
sum over its count, was 475 s, falling to 3 s five minutes later; over the
day's six replacements the worst minute's mean was 722 s. Decisions stopped
for 8 to 13 minutes each time (the two-minute notice, the replacement's
launch and boot, and about four minutes restoring the feature state) and the
backlog was worked off within six minutes of resuming.

The buckets now include 600, 900, 1,200, 1,800 and 7,200 s, so a ten-minute
catch-up reads as ten minutes, and the dashboard's event-to-decision panel
draws the exact mean beside the percentiles. The availability report's
latency while serving leaves reclaim catch-ups out, so its figures were not
affected; its every-minute p99 was, and is read from the finer buckets from
the image that carries them.
