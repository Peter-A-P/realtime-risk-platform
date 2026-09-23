# 26. Alerts reach a person by email, and no container holds a credential to send them

- Status: proposed, 2026-09-22; accepted when Peter applies it
- Date: 2026-09-22
- Deciders: Peter Parker (asked for the label collector's lag and the unsealed
  history's age to alert, on 2026-09-22); the build session (the channel, the
  rules and their thresholds)

## Context

Until now nothing on the live stack told anyone anything. The dashboard shows
what is happening to whoever is looking; nobody is looking at 3 a.m.
`docs/failure-modes.md` named two losses that happen quietly and for good: a
label collector more than a day behind (the labels topic keeps a day, ADR
18), and a compactor that stops working (unsealed staged rows at about a
gigabyte an hour). The dry run's first night showed the second is real: the
compactor was killed by the kernel every run for hours, and the only sign was
the instance's memory. Its runs were a shell loop, which reported nothing.

Sending anything from the instance needs a credential. The instance's role is
reachable only from the host: IMDSv2 with a hop limit of 1 keeps every
container away from it, so that a compromise of the public-facing ones
(Grafana, the tunnel) does not hand over a role that can read `/verdict/*`,
including, on the live window, the sealed schedule's secret.

## Options

1. **Alertmanager with SMTP.** The standard shape. Needs an SMTP account and
   its password on the instance: a new credential to keep, rotate, and keep
   out of the repository.
2. **Alertmanager publishing to SNS.** No password, but it signs with the
   instance's role, so the Alertmanager container would need the metadata
   service: a hop limit of 2 for every container, or host networking for it.
3. **Grafana alerting.** The dashboard is anonymous with login disabled on
   purpose; alerting there means SMTP again and state inside Grafana.
4. **Prometheus rules, a relay container that writes messages to files, and
   a loop on the host that publishes them to SNS.** No new credential; only
   the host's few lines ever sign anything.

## Decision

**Option 4.**

- **Prometheus evaluates six rules** (`deploy/live/compose.yml`,
  `alert-rules`): `LabelCollectorBehind` (the collector's latest label time
  more than two hours behind the labels feed's, for 15 minutes),
  `HistoryUnsealed` (an ended hour unsealed for over three hours),
  `DayNotFinalised` (a day ready to finalise for over an hour),
  `CompactionFailing` (three failed runs in half an hour), `ScorerStopped`
  (no decision for fifteen minutes) and `TargetDown` (a scrape target gone
  for ten minutes, longer than a spot replacement's four).
- **New metrics make the first four possible.** The labels feed reports the
  label time it has sent up to; the collector the label time it has written
  up to. The compactor becomes a parent process (`verdict history
  compactor`) that still runs each compaction in a process of its own, so a
  run's memory is given back, and reports how each run ended, the age of the
  oldest unsealed hour, and the days waiting to be finalised. The last two
  are read from the spools when Prometheus scrapes, so they stay true while
  the runs are failing.
- **The relay** (`verdict observe alerts`, `verdict/observe/alerts.py`) polls
  Prometheus's alerts API a minute at a time and writes a message file when
  an alert starts, every six hours while it continues, and when it stops.
  What it has told is on the data volume, so a spot replacement neither
  repeats nor forgets. Prometheus unreadable for ten minutes is an alert of
  its own, and nothing is called resolved while it cannot be seen.
- **The host publishes** each file to the `verdict-alerts` SNS topic with
  the instance's role (`sns:Publish` on that topic only) and deletes it. The
  email subscription's address is a Terraform variable passed from the
  environment, never committed: the repository is public.
- **Each alert has a section in `docs/failure-modes.md`** saying what it
  means and what to do, and the email names it.

Thresholds are set from what the platform does when well, measured or
designed: an hour seals about ten minutes after it ends (labels hours after
the next hour's labels arrive, so about an hour); finalising a day takes
minutes; a spot replacement is about four minutes without decisions.

## Evidence

`tests/test_alerts.py`: every rule reads a metric the platform registers; the
rules, run through Prometheus's own `promtool test rules` in the pinned
image, fire on a collector that stops and not while it keeps up or before
labels start, fire on a day stuck for 70 minutes and not 50, and do not fire
on a four-minute replacement (shown failing when a threshold was loosened);
the relay tells an alert once when it starts and once when it stops, repeats
it after six hours, survives a restart, keeps two targets' alerts apart,
ignores pending alerts, does not resolve on a failed poll and raises its own
alert after ten; the spool metrics read the spools; every run is counted by
how it ended. `promtool check config` passes on the Prometheus configuration.
`terraform validate` passes.

## Consequences

- **Cost:** SNS email is free for the first thousand a month; the topic and
  subscription cost nothing standing. Both are tagged and go with `down.sh`.
- **Peter's steps before it works:** attach the updated deploy policy
  (`deploy/aws/iam/deploy-policy.json`, SNS on `verdict-alerts` only), set
  `TF_VAR_alert_email`, apply, and confirm the subscription from the email
  AWS sends. Until the subscription is confirmed alerts are published to
  nobody.
- **What it cannot see:** the whole instance gone. If the auto-scaling group
  finds no spot capacity, nothing is left to send an alert. The dashboard's
  "Since the last decision" panel shows it, and an alarm outside the
  instance (CloudWatch on the group's in-service count) would email it; not
  built, since a replacement has always arrived within minutes so far.
- **The user data is nearly full:** about 400 bytes under the 16 KB limit
  with the test's 2 KB margin. The next addition to the boot script should
  gzip the whole user data, which cloud-init accepts, rather than squeeze.
- **Dead letters do not alert yet.** Whether a spot replacement's resent
  records are set aside as late is not measured, and an alert on every one
  would email five times a day in the dry run's conditions. The dry run's
  metrics decide it.

## Sources

- Prometheus, alerting rules and the `/api/v1/alerts` endpoint.
  https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/
  https://prometheus.io/docs/prometheus/latest/querying/api/#alerts
- Prometheus, unit testing for rules (`promtool test rules`).
  https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/
- Amazon SNS, email notifications and pricing.
  https://docs.aws.amazon.com/sns/latest/dg/sns-email-notifications.html
  https://aws.amazon.com/sns/pricing/
- Amazon EC2, instance metadata options and the hop limit.
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-options.html
- ADR 14 (the stack), ADR 18 (what is kept), ADR 25 (availability).
