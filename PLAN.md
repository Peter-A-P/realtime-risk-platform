# Plan: Real-Time Fraud and Risk Decisioning Platform

**Written:** 2026-09-06. **Status:** building. Week 1 was built on 2026-09-12, about twenty
weeks ahead of its slot, after a dependency check found nothing in the portfolio blocking
it (the plan's own sequence has 01, 08 and 09 independent).

**Build:** nine weeks. The slot was Feb 1 to Apr 4 2027; the build started early, so the
week numbers below are the schedule and the dates are not. **Live:** Apr 5 to Jun 30 2027
as planned, torn down Jul 1, **but the go-live date is now an open decision**: starting
five months early moves the live window unless the built platform waits, and the live
window is what the AWS account timing in the plan repository's action 9 was arranged
around (a free-plan account closes itself six months after opening). That decision is due
before week 7, which is the first week that needs an AWS account, and it belongs in the
plan repository's STATUS, not here. Nothing before week 7 costs anything: the build months
run on the laptop.

**Package:** `verdict`. **Fed by:** nothing in the portfolio; 01, 02 and 03 exist by then and
their habits carry over. **Feeds:** 10 reads this platform's traces as one of its production
signals; the write-up is one of the three under Rule E.

This project calls no language model, so the 04 gateway is not on its path, and the 03 gate
measures language-model systems, so it is not used here either. Every number below comes
from the platform's own load tests, replay evaluations and live-window telemetry, with
bootstrap confidence intervals.

> **Employer boundary, before anything else.** This is the project nearest to systems built
> inside the Government of Newfoundland and Labrador. It is built from public data and
> public problem statements only: card-transaction fraud as posed by a public competition,
> plus a synthetic stream whose design is published. No internal architecture, features,
> thresholds, prompts or code, and no government, benefits or claims scenario of any kind.
> Open question 02 in the plan repository was closed on 2026-09-06 on exactly this basis.
> If any part of the build starts to resemble the internal system, the substitute is a
> supply-chain or energy-grid anomaly platform, which proves the same platform properties
> with no overlap. Every architecture decision record cites its public sources.

---

## 1. What this produces

A platform that scores every transaction before the money moves, at sustained thousands of
events per second and under fifty milliseconds at the 99th percentile, and keeps working
as fraud patterns shift: features computed once and served identically online and
offline, a test that fails on time leakage, shadow deployment with a rollback drill, drift
monitors behind a human approval gate, and a review queue ranked by expected loss. Three
months live behind a public dashboard, on one command.

**It is a platform, not a fraud model.** The model is the least interesting part and the
README says so. Training-serving skew, not AUC, is what breaks these systems in
production.

The numbers a stranger can check:

| Number | What it shows |
|---|---|
| End-to-end decision latency p50, p95, p99 in milliseconds at the sustained live rate, with the budget broken down by hop (ingest, feature lookup, model, decision, persist), 95% CIs across daily windows over the live months | The sub-50 ms claim, and where the time goes |
| Sustained throughput in events per second over 87 live days, with uptime percent and the count of instance replacements | Thousands per second is a measurement, not a Compose file |
| Online/offline parity: features identical between the online store and an offline recompute on a daily sample (must be 100% within stated tolerance), and the number of parity violations the test caught during development | The skew that breaks production ML, made visible |
| Point-in-time correctness: the leakage test, the commit where it caught a real leak, and the offline PR-AUC inflation that leak would have produced | The number that says why the test exists |
| Champion (XGBoost) against challenger (FT-Transformer): PR-AUC and expected loss on the held-out real data and on the shadow window, with CIs, and the promotion decision | The tabular deep-learning comparison, run where champion/challenger is for |
| Drift: PSI and KS by feature and on the score distribution across the live months; triggers fired; approvals given or refused; performance before and after each retrain | Drift-triggered retraining as a record, not a diagram |
| Review queue: money caught per analyst-hour under expected-loss ranking against score ranking at fixed queue capacity, on replayed labelled data, with CIs | Why the queue is ranked by dollars, not probability |
| Rollback drill: seconds from decision to the previous champion serving, measured five times | Rollback as a rehearsed procedure |
| Cost per million events from the actual cloud bills | The scale claim priced |
| Real-data track: PR-AUC with CIs on the public competition data through the same pipeline | The platform works on real transactions, not only on the generator |

## 2. Design decisions

### 2.1 Two tracks: real data offline, synthetic data live

The public competition data (IEEE-CIS) is real card-transaction fraud with rich features
but no timestamps at streaming resolution and no volume. A synthetic generator gives
volume and scheduled pattern shifts but no real fraud. So the project runs two tracks
through one pipeline and says which number came from which:

| Track | Data | What it proves |
|---|---|---|
| Real-data offline | IEEE-CIS, replayed in its own time order through the feature store and training pipeline | The leakage test, parity, champion/challenger and queue ranking on real fraud |
| Synthetic live | The `verdict` generator: an entity graph of cards, devices and merchants, fraud scenarios (card testing bursts, account takeover, merchant collusion), and a sealed schedule of regime shifts | Throughput, latency, drift detection, retraining, operations, cost |

The generator's regime schedule is sealed before go-live and revealed on Jul 1, so the
drift monitors are graded against shifts they could not have been tuned to.

**Amended 2026-09-12, in week 1, in the same commit as the code.** The plan said the
schedule was "hashed and committed before go-live and revealed on Jul 1". Those two
things cannot both be true of a schedule that sits in the repository in the clear: it
would be committed, but it would not be sealed, because the person writing the drift
monitors would have read it. Three things are now kept apart, and `docs/generator.md`
and ADR 2 record why:

- **The design is public.** `events/generator/regimes.py` publishes the kinds of shift
  and the range each parameter may take. A test asserts every derived schedule stays
  inside those ranges.
- **A development realisation is public.** `DEV_SCHEDULE` is fixed, readable and used
  throughout the build, and it contains the case that catches a naive monitor: a regime
  that moves the amount distribution while the fraud rate holds still.
- **The live realisation is sealed.** It is derived from a secret this repository does
  not contain. `verdict schedule seal` commits three hashes before go-live: of the
  secret, of the derived schedule, and of `regimes.py` itself. On Jul 1 2027 the secret
  is published and `verdict schedule verify --reveal` re-derives the schedule and checks
  all three, which is what makes the live drift numbers checkable by a stranger rather
  than merely asserted.

### 2.2 Features are computed once

**Amended 2026-09-12, in week 3, in the same commit as the code.** The engine below was
Bytewax. It publishes no wheels for Python 3.13 on any platform, and the portfolio
standard is 3.13, so the dataflow is written in `verdict/features/` instead: windowed
aggregations with bounded state and per-entity keying. ADR 4 carries the options, the
decision and what it gives up. Everything else in this section is unchanged, because none
of it depended on which engine ran the dataflow.

Streaming aggregations (velocity windows per card, device and merchant; entity-graph
degree and shared-device counts; session features) are computed by one dataflow
and written to two sinks: Redis for online serving and Parquet for the offline store.
There is no second implementation in SQL for training. This is the design that removes
training-serving skew at the source; the parity test exists to prove it stayed removed.

### 2.3 The leakage test is written before the first feature

Point-in-time correctness is enforced by a test, written in week 2 before any feature
exists: for a sample of training rows it recomputes every feature from the raw event log
using only events strictly before the label event's time and compares with what the store
served; a second test shifts label times earlier and asserts feature values do not change.
The plan expects the first velocity window to get a boundary wrong; when the test catches
it, that commit and the PR-AUC inflation it would have caused are recorded in an ADR. If no
real leak occurs, the plan says so rather than planting one.

### 2.4 Scoring is a stream consumer, not an HTTP hop

The scoring service consumes the feature-complete event from the stream, fetches the
entity features from Redis, runs the champion model exported to ONNX, applies the decision
rules, writes the decision, and emits timing per hop. Latency is measured from the event's
ingest timestamp to the decision timestamp on the same host clock. A synchronous HTTP
endpoint exists for the demo and for the load test that compares the two designs (Rule C
candidate 3). The latency budget is set in week 4 from the first measurement and
published; provisional split: ingest 5 ms, feature fetch 5 ms, model 3 ms, decision and
persist 5 ms, headroom to 50 ms for the tail.

### 2.5 Shadow first, promotion by evidence, rollback by flag

The challenger scores every event alongside the champion; only champion decisions act.
Promotion requires, on the labelled shadow window (labels arrive with a simulated seven-day
delay, as they do in life): non-inferior PR-AUC and expected loss with the interval shown,
latency within budget, and a human approval. Rollback is a configuration flag read on every
event; the drill flips it and measures the time to the previous champion serving.

### 2.6 Drift triggers a candidate, a person promotes it

Evidently computes PSI and KS per feature and on the score distribution daily. Two
consecutive days above threshold open a retraining job; the job trains a candidate, runs
it in shadow, and opens a pull request with the evidence tables. Merging the pull request
is the approval gate. Nothing retrains itself into production.

### 2.7 The queue is ranked by expected loss

Expected loss is probability of fraud times amount times one minus expected recovery, less
review cost. At a fixed analyst capacity per hour, the evaluation replays labelled data and
reports money caught per analyst-hour under expected-loss ranking against score ranking. A
high-probability twelve-dollar fraud is not worth an analyst's time; a medium-probability
forty-thousand-dollar one is.

### 2.8 The live stack runs in one AWS region

The managed stream is Amazon Kinesis (the portfolio's AWS example). Pulling a thousand
events per second from Kinesis to a VPS on another cloud would cost more in egress than
the compute, so the whole live stack, stream, consumers, stores, dashboard, runs in
`ca-central-1` on one spot instance behind an auto-scaling group of size one, with state on
a separate EBS volume reattached at boot and the online store rebuilt by replaying the
stream after a replacement. Spot interruptions are counted and reported as part of uptime;
recovering from them on one command is part of the evidence. Locally and for interviews the
same stack runs on Docker Compose with Redpanda in place of Kinesis behind one `Stream`
interface, and a parity test asserts both paths produce identical features on the same
replay. Records are aggregated into Kinesis PUT units so the cost is shard-hours, not
records.

### 2.9 Architecture decision records are the first deliverable

Every decision above, and every one that follows, is an ADR in `docs/adr/` in MADR form:
context, options, decision, consequences, public sources. Architect interviews probe for
exactly this artefact, and almost no portfolio contains it. At least fourteen are planned
(section 4).

### 2.10 Out of scope, on purpose

- Model optimisation beyond a well-tuned champion and one honest challenger. Every hour on
  AUC is an hour not spent on the platform properties that make this distinctive.
- Graph neural networks, sequence models on transaction histories, and other model
  research. Named in Deferred.
- Any language model. No triage assistant, no explanation generator.
- Multi-region, exactly-once end to end, or Flink. One region, at-least-once with
  idempotent decisions, and the windowed aggregation engine in `verdict/features/` rather
  than a distributed one; the ADRs say what changes at ten times the scale.
- Real payment rails, card networks, PCI scope. Synthetic PANs only; the real data is
  already tokenised by its publisher.

## 3. Data

| Source | Size | What it gives | Access and terms |
|---|---|---|---|
| IEEE-CIS Fraud Detection | About 590k transactions, 400+ features, 3.5% fraud, six months in relative time | The real-data track: leakage test, parity, champion/challenger, queue evaluation | Kaggle competition data; downloaded by the user with their own credentials, never redistributed; competition rules read and recorded in `docs/data.md` |
| Sparkov transaction generator | Generator | Reference for realistic merchant categories and amount distributions | Public repository; licence recorded before use |
| `verdict` generator (own) | Unlimited; 1,000 events per second sustained in the live months | The live track: entity graph, fraud scenarios, sealed regime schedule, deterministic seeds | Written here, published with the repository |
| ULB credit-card fraud (PCA features) | 285k transactions | A second real-data check of the champion/challenger comparison only | ODbL; loader verifies checksum |

Nothing raw is committed; loaders verify checksums; every licence is recorded.

## 4. Architecture

```
verdict/
  events/      schema.py (pydantic, JSON on the wire, versioned), generator/ (entities, scenarios,
               regimes.py with the sealed schedule, seeds), replay.py (raw log in time order),
               ieee_cis.py (ingest, verify, inspect), ieee_cis_events.py (IEEE-CIS rows onto events)
  stream/      base.py (Stream protocol: produce, consume, checkpoint), redpanda.py, kinesis.py
               (record aggregation and deaggregation), parity.py (same replay, both paths, identical features)
  features/    aggregators.py (windowed aggregations, bounded state, amortised constant time),
               engine.py (per-entity state; serves each event before observing it, and holds an event back
               until time moves on so same-instant events cannot see each other), sinks.py (one write path
               to both stores), verify.py (parity, and the served-value record the leakage check reads)
  store/       feast/ (feature repo), retrieval.py (point-in-time joins), leakage_test.py (the test in 2.3)
  models/      train.py (XGBoost champion), challenger.py (FT-Transformer, PyTorch), export.py (ONNX),
               registry.py (MLflow), promote.py (non-inferiority on the shadow window)
  scoring/     consumer.py (stream consumer scorer with per-hop timers), rules.py (decision rules),
               core.py (the decision, shared by both transports), flags.py (champion pointer, read per event),
               http_api.py (sync endpoint for demo and comparison; shadow scoring lives in core.py)
  drift/       monitors.py (Evidently PSI, KS; daily), trigger.py, retrain.py, approval.py (opens the PR with evidence)
  queue/       expected_loss.py, simulate.py (fixed capacity replay), evaluate.py (money caught per analyst-hour, CIs)
  observe/     OpenTelemetry spans and Prometheus metrics; Grafana provisioning
  chaos/       redis_down, stream_throttle, consumer_lag, clock_skew, poison_event, duplicates, out_of_order, schema_change
  cli.py       verdict up | down | replay | loadtest | parity | drift-report | queue-eval | rollback-drill
deploy/
  compose/     local stack: Redpanda, Redis, Postgres, MLflow, Prometheus, Grafana, the services
  terraform/   AWS: Kinesis stream, spot ASG of one, EBS, IAM, CloudWatch budget alarms, security groups
  up.sh, down.sh   one command each; down leaves nothing billable
docs/
  adr/         0001-platform-not-model, 0002-two-tracks, 0003-stream-choice, 0004-aggregation-engine,
               0005-feature-store, 0006-features-computed-once, 0007-leakage-test-first,
               0008-consumer-scoring, 0009-latency-budget, 0010-label-delay, 0011-shadow-and-promotion,
               0012-drift-thresholds-and-approval, 0013-queue-ranking, 0014-live-in-one-region,
               0015-spot-and-recovery, 0016-teardown-and-repeatability,
               0017-real-data-event-mapping (added in week 4; numbered after the planned set)
  failure-modes.md   the chaos results, observed behaviour and fix, one section per failure
  data.md, latency-budget.md, runbook.md
loadtest/      rate ramps with the generator; k6 for the HTTP comparison; results with CIs
```

### Tests that matter

The leakage test (2.3); the parity test (online against offline recompute, and Redpanda
against Kinesis); idempotent decisions under duplicate delivery; out-of-order events within
the watermark produce the same features as in-order; the rollback flag is honoured within
one event; the promotion function refuses when the interval crosses the margin; the
generator is deterministic for a seed; `down.sh` leaves no billable resource (asserted
against the AWS API).

## 5. Week by week

| Dates | Built | Done when |
|---|---|---|
| Week 1 (built 2026-09-12) | Repository, event schema, generator with entities, scenarios and the regime schedule (hash committed); Redpanda Compose; raw event log; ADRs 1 to 4 | **Done**, except the Compose stack, which is written but unrun: Docker is not installed on the build laptop. 12,219 events/s generated and logged (8,790 to 15,648, five runs of 500,000), against a target of 1,000; hashes in `docs/generator-hashes.json`; ADRs 1 to 4 written |
| Week 2 (built 2026-09-12) | Feast repository and offline store; IEEE-CIS replay in time order; **leakage test written, no features yet**; training pipeline skeleton; ADRs 5 to 7 | **Done, except the IEEE-CIS replay.** The leakage test runs green on the empty feature set and red on each of three planted leaks (window includes the current event, window peeks forward, training join uses the label time); Feast repository generated from the feature definitions, with push and point-in-time retrieval proven end to end; raw-log replay refuses an out-of-order log; ADRs 5 to 7 written; 149 tests. The IEEE-CIS replay is still outstanding, but no longer blocked on the terms: those were read and recorded on 2026-09-12 (`docs/data.md`, quoting sections 7.A, 7.B and 8.B). Non-commercial use is permitted and results may be published; the data and any row-level derivative may not be, which is now enforced by `.gitignore` and `tests/test_data_terms.py`, and which puts the real-data track permanently off the live AWS stack. The archive was downloaded on 2026-09-12 and `verdict data inspect` answered the open questions from the file rather than from memory of a 2019 schema: 590,540 transactions over 182 days at 0.0376 events per second, 3.499% fraud, whole-second timestamps, device data on 24.4% of rows, and **no merchant identifier at all**, so three of the sixteen features cannot be computed on this track and the honest response is to report per track which exist. **The row-to-event mapper was built on 2026-09-14** (`verdict/events/ieee_cis_events.py`, ADR 17), which closes the week. Measured rather than assumed, the file holds no device either: its identity columns describe configurations shared by thousands of purchasers. So a card is issuer, product, billing region and account start day (88.7% of rows, 203,467 cards), there is no device or merchant, the wire schema moves to version 2 so an event can say so, and 6 of the 16 features exist on this track. The point-in-time check ran over the whole replay with 0 violations in 67,920 comparisons on a 2% sample of cards. The training pipeline skeleton moves to week 3 |
| Week 3 (built 2026-09-12) | Dataflow: velocity, entity-graph, session features; dual sink; parity test; first leak caught (expected) and recorded | **Done.** 16 features computed by the engine written here (ADR 4 amended: Bytewax has no Python 3.13 wheels); parity 100% online against offline through real Feast; the leakage test passes on all 16 features against a brute-force recomputation. **The predicted leak happened and was caught**: two events sharing a timestamp saw each other. Measured against the competition data once it arrived, that is 312 rows of 590,540, a twentieth of one percent, slightly enriched for fraud. `docs/leak-caught.md` records it, and carries a correction: the first published version of the measurement described a synthetic stream truncated to seconds and overstated the real impact by three orders of magnitude. The offline PR-AUC inflation is measured in week 5, when a model exists to measure it with |
| Week 4 (started 2026-09-14) | Stream-consumer scorer, ONNX champion, decision rules, per-hop timers; HTTP endpoint for comparison; first local load test; latency budget published; ADRs 8, 9 | p99 and hop breakdown in `docs/latency-budget.md`. **In progress.** Built so far: the `Stream` protocol (`produce`, `consume`, `checkpoint`) with an in-process and a Redpanda implementation, held to one contract suite that runs against the live broker; and the Compose stack's first real runs, which found two faults and fixed them. Then the scorer (`verdict/scoring/`, ADR 8): duplicates stopped before the engine, checkpoints after durable decisions, per-hop timers, placeholder rules, and a stand-in model behind the model interface, because the ONNX champion is week 5's. The `transactions` topic moves to one partition to match the one live shard, since the engine needs time order. The load test runs on both streams. Then, on 2026-09-15, while another project's training held the CPU: the decision moved into one core shared by the consumer and a new HTTP endpoint (`http_api.py`), so Rule C candidate 3 compares transports rather than two scorers; and two week 5 pieces that need no model were built early, the champion pointer read per event with its rollback (`flags.py`, the plan's "rollback flag honoured within one event" test) and shadow scoring on the same features with its own `shadow` topic. Not yet: the first published measurement and ADR 9, which wait for a machine not shared with another project's training job, and the comparison run itself |
| Week 5 | Champion training on IEEE-CIS and on replay; FT-Transformer challenger; shadow scoring; promotion function; rollback flag and drill; ADRs 10, 11 | Champion/challenger table with CIs; drill timed five times |
| Week 6 | Drift monitors, trigger, retraining job, approval pull request; expected-loss queue and evaluation; ADRs 12, 13 | Queue evaluation table; a retraining PR opened end to end on a forced shift |
| Week 7 | Kinesis path with aggregation; Redpanda/Kinesis parity; Terraform for the live stack; AWS budget alarms (plan repository action 9); Grafana dashboards; ADRs 14, 15 | Stack up and down on AWS on one command each; parity across paths |
| Week 8 | Chaos tests and `failure-modes.md`; load test on the live instance; cost per million events; hardening; ADR 16 | Every chaos scenario has observed behaviour and a fix; loadtest results with CIs |
| Week 9 | 72-hour dry live run; README; runbook; ADR review | Dry run clean; go-live checklist ticked |
| **Go-live** | **Go-live at 1,000 events per second**, dashboard public at risk.peterparker.ca. Date open: see the header | |
| Apr 5 to Jun 30 | Weekly check; monthly report to the plan repository's STATUS; regime shifts land on the sealed schedule; retraining PRs reviewed; interruptions recovered | 87 days of telemetry |
| Jul 1 | `down.sh`; schedule revealed; live-window report; Rule E write-up drafted | Nothing billable remains; report published |

First to drop if behind: the entity-graph features (velocity and session stay); then the
FT-Transformer challenger is replaced by a retrained XGBoost challenger so shadow and
promotion are still demonstrated, with the FT-Transformer returning during the live window
if time allows (rev. 4 placed it here deliberately). Neither is in the definition of done as
written. The leakage test, parity, latency budget, shadow, drift gate, queue and ADRs are
not droppable.

The build overlaps 06 for the first week of April by design; the sequence gives 09 nine
weeks and 06 starts when 09 is live.

## 6. Cost

Prices as of 2026-09-06, AWS `ca-central-1` public list prices; re-checked in March. The
build months run on the laptop and cost nothing.

| Item | Basis | US$ | CA$ |
|---|---|---:|---:|
| Spot instance, 4 vCPU 8 GB class, 87 days | About US$0.055 per hour | 115 | 155 |
| Kinesis, 1 provisioned shard plus aggregated PUT units, 87 days | Shard-hours dominate; 1,000 events per second at about 200 bytes fits one shard with headroom | 35 | 47 |
| EBS gp3, 50 GB, 3 months | | 12 | 16 |
| Egress (dashboard viewers, exports) | A few GB | 3 | 4 |
| **Live window** | | **165** | **223** |
| Interview re-runs after teardown | A few hours each, on demand | 10 | 14 |
| Reserve for a second shard or a larger instance if the load test says so | | | 100 |
| **Project total against the CA$350 line** | | | **337** |

The live window runs over the CA$210 "live throughput demo" line by about CA$13 and takes
it from the rest of 09's CA$350, which no longer needs a share of the shared VPS because
the live stack runs on AWS. The reserve buys, in order: a second shard, a larger instance,
a longer live window (a budget decision that goes to STATUS, not this plan). Actuals go in
STATUS monthly; `down.sh` runs on Jul 1 whatever the state of the write-up.

## 7. Handover

Nothing in the ten imports `verdict`. What carries forward: the ADR set as the reference
for how platform decisions are written in the rest of the portfolio; the traces and metrics
as one of 10's production signals; the write-up under Rule E. `v1.0.0` is tagged at
go-live; the live-window report is `v1.1.0` on Jul 1.

## 8. Risks

| Risk | Handling |
|---|---|
| Largest build in the plan, most likely to overrun | Nine weeks with a drop order (section 5); 01, 02 and 03 exist before it; five months of smaller projects follow to absorb slip; go-live can move a week without touching the live budget |
| Employer boundary | Public data and public problem statements only; card-transaction fraud, never a government scenario; ADRs cite public sources; if the design drifts toward the internal system, stop and substitute the supply-chain or energy-grid anomaly platform. Rule G applies to every commit |
| Live-month cost overrun | Budget alarms at 50, 80 and 100 percent of the line before the first resource exists; `down.sh` asserted to leave nothing billable; teardown on Jul 1 is in the calendar |
| Spot interruptions | Counted and reported; ASG of one with state on EBS and the online store rebuilt by replay; if interruptions exceed one a week the instance type changes and the ADR records why |
| p99 misses 50 ms | Measured in week 4 before anything is optimised; per-hop breakdown shows where; the honest number is published whichever side of 50 it lands |
| No real leak for the leakage test to catch | Say so. The test and the planted fixture remain; the ADR records that the design prevented the class of error |
| Synthetic fraud is too easy or too hard | Scenario difficulty is tuned in week 1 to a champion PR-AUC in the range the real data shows; the parameters are published |
| IEEE-CIS terms constrain use | Rules read and recorded in week 1; if redistribution or derived-work limits bite, the real-data track moves to the ULB set and says so |
| The model becomes the project | The README's first line says platform, not model; model work is timeboxed to week 5 |
| The dashboard invites abuse (public URL under load) | Read-only Grafana behind a rate limit; the scoring path is not publicly writable |

## 9. Rule C candidates

1. **Computing features twice, batch SQL for training and streaming for serving.** Build the
   SQL version of three velocity features and compare with the streaming values on a day's
   replay. Expected: window-boundary and late-event differences on a measurable share of
   rows, and a PR-AUC gap between offline and shadow. That share is the evidence for 2.2.
2. **Ranking the review queue by score.** Already in the queue evaluation: money caught per
   analyst-hour under score ranking against expected-loss ranking, with CIs.
3. **Synchronous HTTP scoring per event.** Load the HTTP endpoint at the live rate and
   compare p99 and error rate with the stream consumer at the same rate on the same host.
   Expected: the HTTP path's tail grows with connection handling long before the model or
   the store are the bottleneck.

Whichever produces the clearest evidence becomes `docs/rejected.md`; the others stay as
sections in the ADRs.

## 10. Definition of done

- [ ] Point-in-time correctness test exists, was written before the first feature, and has caught at least one real leak (or the plan says none occurred)
- [ ] Online and offline features verified identical by automated test; Redpanda and Kinesis paths verified identical on the same replay
- [ ] p99 under 50 ms at the live rate, with the per-hop breakdown published and CIs across daily windows
- [ ] Sustained throughput, uptime and interruption count over 87 live days published
- [ ] Shadow deployment demonstrated; promotion by non-inferiority with the interval shown; rollback drill timed five times
- [ ] Drift monitors fired retraining behind a pull-request approval gate at least once, on a shift from the sealed schedule
- [ ] Review queue ranked by expected loss; money caught per analyst-hour against score ranking, with CIs
- [ ] Champion against FT-Transformer challenger reported on real and shadow data (or the substitution stated)
- [ ] Real-data track PR-AUC with CIs through the same pipeline
- [ ] Cost per million events from actual bills
- [ ] Sixteen architecture decision records with public sources
- [ ] Failure-mode analysis with observed behaviour and fix per scenario
- [ ] Public dashboard live Apr 5 to Jun 30 at risk.peterparker.ca; stack up and down on one command; nothing billable after Jul 1
- [ ] One rejected approach documented with evidence (Rule C)
- [ ] Repository public at go-live, `v1.0.0` tagged; live-window report tagged `v1.1.0`

## 11. Deferred

| Deferred | Kept so the door stays open |
|---|---|
| Graph neural network or sequence-model challengers | The challenger interface takes any model that exports to ONNX; the entity graph is already a feature source |
| Flink or a managed streaming engine | The dataflow is expressed as windows and keyed aggregations; ADR 4 lists what changes |
| Exactly-once end to end | Decisions are idempotent by event id; ADR 8 records the at-least-once choice |
| Multi-region and active-active | Region is a Terraform variable; nothing else assumes one |
| A longer or repeated live window | `up.sh` brings it back for interviews; extending it is a budget decision for STATUS |
| Real-time labels from a feedback loop | Label delay is simulated at seven days; the join is by event id, so a real feed would slot in |
