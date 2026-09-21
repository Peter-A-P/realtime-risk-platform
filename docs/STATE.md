# Where this project is, and what to know before touching it

**Written 2026-09-13; updated after the move to a new machine, and on 2026-09-14 after the
real-data mapper.** Read this first, then `PLAN.md`. It exists so that a
session starting cold knows everything a session that had been here all along
would know: what is built, what was decided and why, what is waiting on a
person, and the handful of things that will waste an hour if nobody mentions
them.

Everything here is a fact about the project as it stands. Where something is a
judgement or an open question, it says so.

---

## 1. Status in one paragraph

Weeks 1 to 3 of a nine-week build are done, and the real-data track now runs
through the pipeline. The event stream, the synthetic generator with a sealed
regime schedule, the point-in-time leakage test, sixteen features computed by
an engine written here, and one write path into both stores, with parity at
100 percent. The competition data is downloaded, its terms are recorded, its
checksums are committed, and since 2026-09-14 its rows map onto the platform's
events (ADR 17): 590,540 events replayed, 6 of 16 features on that track, and
the point-in-time check clean over a 2 percent sample of cards. The wire
schema is at version 2. Week 4 has started: the `Stream` interface exists with
in-process and Redpanda implementations, tested against the running broker,
and the scorer consumes it (ADR 8) with a stand-in model and a load test.
Week 4's latency work found where the time goes and published that (ADR 9,
`docs/latency-budget.md`): three of the four costs belong to the measuring
host, and the end-to-end figure still waits for a quiet machine (section 11).
502 tests; `ruff`, `ruff format` and
`mypy --strict` all clean. Nothing has been scored yet, so the README's headline tables are
still empty and stay that way until they are real.

The build started on 2026-09-12, about twenty weeks ahead of its Feb 2027
slot, after a check that nothing in the portfolio blocks it.

**On 2026-09-18 Peter dropped the calendar and asked for go-live as soon as
possible.** Decided that day, and recorded in `PLAN.md` (header, 2.8, 6, 10),
ADR 3 and `CLAUDE.md`:

- **Go-live when the full definition of done (PLAN.md section 10) is met.**
  Nothing is deferred into the live window, including the chaos tests and the
  72-hour dry run.
- **The live window is sixty days.** The sealed schedule is derived over 60
  days, revealed and the stack torn down the day after. Every "Jul 1 2027" in
  the repository means that day now.
- **The live stream is Redpanda on the instance** (ADR 3, option 2). No
  Kinesis client is written.
- **AWS is the account project 04 already uses, shared as an account only.**
  09's Terraform and scripts live here; every resource is tagged
  `project=verdict`; the budget and the teardown check are scoped to that tag.
  About CA$236 of the CA$350 line (PLAN.md section 6).

**On 2026-09-19 Peter closed ADR 14's storage question: topics keep a day,
and history is a sample.** ADR 18 and `verdict/history/`: the scorer stages
every decision with its features before it checkpoints; a collector spools
labels; an hourly compactor seals hours and, once a day's labels are all in,
keeps every reviewed or declined row and a tenth of approved frauds and a
hundredth of approved legitimate rows, each with its weight. The promotion
gate reads the weight. The data volume is 150 GB. **Peter also asked that
CPU-heavy work (training, load tests) wait for his green light**: another
project uses the machine. Section 11 has the list and the estimate.

---

## 2. The machine, and what it cannot do

**The build moved machines on 2026-09-13**, from a managed work laptop to
Peter's personal desktop. Weeks 1 to 3 were built on the laptop; the
measurements in section 8 were taken there and say so.

| Fact | Consequence |
|---|---|
| Windows 11 Home, not domain-joined. ASUS ROG Strix G15CK, Intel i5-10400F (6 cores, 12 threads) | A personal machine. No corporate proxy |
| **No administrator rights** in the Claude session | Anything needing elevation (installers, `wsl --install`, BIOS) is Peter's to run. Ask; do not look for a way around it |
| Python 3.13.5 at `C:\Users\peter\AppData\Local\Programs\Python\Python313\python.exe`, on PATH as `python` | No Anaconda. Do not search for conda |
| The project venv is `.venv` in the repository root, rebuilt 2026-09-13 | Run everything as `.venv/Scripts/python.exe -m ...`. Bare `python` has no dependencies. `pip install` failed twice mid-download on a TLS record error and succeeded on the third try: retry before diagnosing |
| The repository is under **OneDrive** | The competition data is kept outside it, at `C:\Dev\POCs Dev\09-realtime-risk-platform\data\ieee-fraud-detection` (the folder was renamed from `POCs` to `POCs Dev` on 2026-09-14; check it is still there). Set `VERDICT_IEEE_CIS_DIR` to that path and every `verdict data` command finds it (section 9). A `.lnk` shortcut to it sits under the repository's `data/`, which git ignores |
| **Docker Desktop 4.90.0** on WSL2, engine 29.7.2, 12 CPUs and 7.7 GB to the VM | Not on PATH in a fresh shell: prepend `C:\Program Files\Docker\Docker\resources\bin`. Needed Intel VMX turned on in the BIOS; Windows still reports `VirtualizationFirmwareEnabled: False` once the hypervisor owns it, which is not a fault |
| **Terraform 1.16.3** at `C:\Users\peter\bin\terraform.exe`, installed per user from the checksummed release zip | Set `TF_DATA_DIR` to `C:/Dev/POCs Dev/09-realtime-risk-platform/terraform-data` before `init`: the AWS provider is hundreds of megabytes and the repository is under OneDrive. The lock file carries Windows and Linux hashes |
| **AWS CLI v2** at `C:\Program Files\Amazon\AWSCLIV2\aws.exe`, profile `verdict` (IAM user `verdict-bootstrap`, policy `deploy/aws/iam/bootstrap-policy.json`, region `ca-central-1`) | A shell started before the install has no `aws` on PATH: call it by full path. From Git Bash, set `MSYS_NO_PATHCONV=1` or it rewrites `/aws/service/...` and `/verdict/...` parameter names into Windows paths |
| The old laptop: domain-joined to PSNL.CA, a TLS proxy, no Docker possible | Only relevant if work returns to it. Python HTTPS failed there where the browser worked |

The full test suite takes about **2 minutes** here with Docker stopped (220
fast tests in 88 s, the 3 slow ones in 21 s) and **3.5 minutes** with the stack
running, which takes memory from the host; it was about 8 on the laptop. The slow markers hold
the generator throughput measurement and the two end-to-end Feast tests.

---

## 3. What is built, file by file

```
verdict/
  events/
    schema.py        Wire schema: versioned, closed, frozen. A transaction carries no
                     label, no score, no feature. Money is integer cents.
    rawlog.py        Append-only JSONL, three files. Ground truth is kept out of the
                     transaction log and a test reads the bytes to prove it.
    replay.py        Replays the raw log in event-time order. Refuses an out-of-order
                     log rather than sorting it quietly.
    ieee_cis.py      Ingest, inspect and verify the competition archive. Downloads
                     nothing; stores no Kaggle credential.
    ieee_cis_events.py  Competition rows onto events: the card key, no device or
                     merchant, the 2017-12-01 clock, decimal cents. ADR 17.
    generator/
      entities.py    Cards, devices, merchants and the links between them.
      scenarios.py   Card testing, account takeover, merchant collusion.
      regimes.py     The regime schedule. SEALED: see section 5.
      driver.py      Merges legitimate traffic and attacks into one time-ordered stream.
  features/
    aggregators.py   Sliding windows with bounded state, amortised constant time.
    engine.py        Per-entity state. Serves each event before observing it, and holds
                     an event back until time strictly moves on.
    sinks.py         One write path to both stores. Offline first, then online.
    verify.py        Parity between the stores, and the served-value record the leakage
                     check reads.
    replay_check.py  The point-in-time check over a replay too big to check in full:
                     samples entities by hash and checks every row of each.
  stream/
    base.py          The Stream protocol: produce, consume, checkpoint. Positions are
                     opaque; delivery is at least once; order is per key.
    memory.py        In process, shaped like Kafka: a broker object and client handles.
    redpanda.py      confluent-kafka. Idempotent producer; consumers assigned, not
                     subscribed, so no rebalance delay lands in a latency number.
  scoring/
    core.py          Decider: the one decision, shared by both transports. Ledger records
                     an event when the engine sees it. Optional shadow model, timed apart.
    consumer.py      The stream scorer on top of Decider: flush then checkpoint per batch.
                     Needs a one-partition topic.
    http_api.py      FastAPI endpoint on the same Decider. Serialised by a lock; 409 for a
                     duplicate or an out-of-order transaction; Server-Timing header.
    flags.py         Champion pointer file, read per event (one stat), atomic writes,
                     rollback; a bad pointer is refused and scoring continues.
  models/
    inputs.py        MODEL_INPUTS: the sixteen features then the amount, in one order;
                     `vector` at decision time and `matrix` for training, held equal.
    dataset.py       ADR 10 rule 4: `at_cutoff` keeps rows whose labels had arrived and
                     counts the rest; `replay_table` serves features through the scorer's
                     own engine path for the offline track.
    promote.py       The promotion gate: paired bootstrap on labelled shadow rows, PR-AUC
                     and decision cost, non-inferiority on the bound. Never promotes.
  drift/
    stats.py         PSI (reference deciles, NO_EVENTS in its own bin) and two-sample KS.
    monitors.py      Fixed reference; daily verdict per feature and score; thresholds are
                     conventions and must not be tuned against any schedule.
    trigger.py       Same quantity drifted two consecutive days, none open: RetrainRequest.
  history/           ADR 18. What the platform keeps.
    sampling.py      The strata, the rates, and the draw: a salted hash of the event id.
    spool.py         Hourly Arrow IPC files, appended per batch; sealed to zstd Parquet.
    records.py       The staged row (transaction, 16 features, champion, shadow) and label.
    labels.py        The label collector: consume, spool by label time, then checkpoint.
    compact.py       Seal closed hours; finalise a day after its labels (7 days + 6 h);
                     draw before joining; one row per event; write, then delete.
  review_queue/
    ranking.py       Expected loss, the hourly day simulation, score vs expected-loss
                     comparison paired by day. Named so it does not shadow stdlib queue.
    model.py         The Model protocol and StandInModel: fixed weights, NOT trained.
    rules.py         Placeholder thresholds, until week 6's expected loss.
    timing.py        Hops, percentiles, the t interval used for every rate and latency.
    loadtest.py      Paced producer thread, scorer thread, per-hop report.
  store/
    features.py      FEATURE_SET: the sixteen features, as specifications not code.
                     Also the brute-force reference evaluation.
    leakage.py       The point-in-time test. Two checks. Never weakened.
    repo.py          Generates the Feast repository from the specifications.
    retrieval.py     Point-in-time training sets. Refuses to carry the label time.
  cli.py             verdict generate | loadtest | serve | flag show/set/rollback |
                     schedule show/hash/seal/verify |
                     data ingest/manifest/verify/inspect/events/check
```

Not yet written: `models/` training and export (week 5), `drift/retrain.py`
and `approval.py` (week 6), `chaos/` beyond the first failure mode, Grafana
provisioning in `observe/`, and the scorer, generator and dashboard in the live
compose file. `deploy/terraform/` was written on 2026-09-18 (ADR 14) and
validated and planned against the account, 15 resources, **but never applied**:
it waits on the deploy policy (section 6) and on Peter's go. `stream/kinesis.py` will
not be written (ADR 3, option 2); `stream/parity.py`, `observe/metrics.py`,
`drift/` and `review_queue/` exist.

`deploy/compose/docker-compose.yml` **ran for the first time on 2026-09-13**:
Redpanda healthy, `transactions` (4 partitions), `labels` (1) and `decisions`
(4) created, the console answering on `localhost:8080`, the Kafka listener on
`localhost:19092`. Its first run failed: `--set=redpanda.auto_create_topics_enabled=false`
is not a `redpanda start` flag in v24.3, and the broker exited on it. Auto-creation
is now set in the cluster bootstrap file, and the topics job fails unless it
reads back `false`. Producing to a missing topic was checked by hand to fail
with `UNKNOWN_TOPIC_OR_PARTITION`.

**The `transactions` topic now has one partition (ADR 8).** A volume created
before that holds a four-partition topic, and the topics job refuses it with
`transactions has 4 partitions, expected 1`: run `down -v` and `up` again.

**Its second run found a second fault (2026-09-14):** the topics job exited 1
whenever the volume already held the topics, so every `up` after the first
failed. It now creates only missing topics and checks the partition count of
existing ones. Verified on an existing volume, a fresh one and a forced rerun.
If Docker Desktop is not running (it does not start on login), start it with
`Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"`; no admin
rights needed.

---

## 4. The decisions that are already made

Twenty-three ADRs, in `docs/adr/`: 1 to 15 and 17 to 24. Read them before reopening anything they cover.

| ADR | Decision | Note |
|---|---|---|
| 1 | Platform, not model | Model work is timeboxed to week 5 |
| 2 | Two tracks: real data offline, synthetic live | Every number says which track |
| 3 | **Amended 2026-09-18: Redpanda locally and live**, one `Stream` interface | Interface written 2026-09-14 with in-process and Redpanda implementations. Kinesis dropped: AWS documents its propagation delay as larger than the whole 50 ms budget. A spot replacement is now a broker recovery, which becomes a chaos scenario |
| 4 | **Amended.** The aggregation engine is written here | Bytewax has no Python 3.13 wheels, on any platform, up to 0.21.1. Peter chose this over pinning Python to 3.12, adding a Rust toolchain, or a Kafka-only engine |
| 5 | Feast: registry, point-in-time join, online read | Push sources, not materialisation. **Its first online read after start-up costs about 42 ms against a 5 ms budget hop: the scorer must warm the store** |
| 6 | Features computed once; the reference evaluation is not a second computation | One definition, two executions, and the test holds them together |
| 7 | The leakage test is written first and never weakened | It has already caught three real faults |
| 8 | The scorer is a stream consumer: at least once, duplicates stopped before the engine, checkpoint after durable decisions, features served from the engine in process | **`transactions` has one partition** because the engine needs time order; more needs a reorder buffer whose hold time is latency |
| 9 | The latency budget stands as PLAN.md 2.4 states it; every cost that belongs to the measuring host is measured on its own and published, never subtracted quietly; the 50 ms figure is claimed from the live stack, not from here | Three host costs found and measured: the process's timer resolution, the load producer sharing the scorer's interpreter, and Docker Desktop's port forwarder (about 41 ms on some connections, fixed per connection, unchanged by every client setting tried). The platform's own cost is the per-batch flush and checkpoint, which is a throughput ceiling before it is a latency one |
| 10 | A label is its own event on its own topic, joined by event id, arriving a constant seven days late on both tracks; features never read label time, promotion counts only arrived labels, the queue opens a label only on review | **Written after the fact** (2026-09-18): the decision was built in weeks 1 and 2 without its record. Rule 4, training only on labels that had arrived by the cutoff, **is enforced since 2026-09-19** by `models/dataset.py`, with a test shown failing when broken |
| 11 | Shadow on the champion's own features, timed apart, unable to break scoring; promotion only if the interval bound clears the margin, on labels that had arrived, with at least 50 frauds; rollback by a pointer read per event | Margins and prices are placeholders until the champion's variability is measured |
| 12 | Drift: PSI and KS against a fixed reference; PSI 0.25, KS statistic 0.10 with p below 0.01, 500 values minimum; same quantity two consecutive days | **Amends PLAN.md 2.6**: no Evidently. **The thresholds must not be tuned against the development schedule**, or the sealed schedule grades nothing |
| 13 | The queue ranks by expected loss; simulated a day at a time in hourly steps with expiry; paired by day | At fixed capacity the prices cannot reorder the queue, and a test says so. Scores must be calibrated before a result is published |
| 14 | The live stack: its own VPC in `ca-central-1d`, no ingress rule, one spot instance (r7i.large, r6i.large or r5.large, 16 GB, since ADR 20) in a group of one, a data volume that outlives it, ECR for the image, SSM Session Manager for a person, a Cloudflare Tunnel for the dashboard; the budget and the tunnel token outside Terraform | Its storage question (history does not fit on a disk at 1,000 events a second) was **closed by ADR 18** on 2026-09-19; the data volume is 150 GB |
| 18 | History is a labelled, weighted sample: topics keep a day; every decision staged with its features before the checkpoint; days finalised seven days and six hours after they end; every reviewed or declined row kept, approved frauds at 0.10, approved legitimate at 0.01, each with weight 1 / rate; the promotion gate reads the weight | Rates are fixed before go-live and change only at a day boundary. The scorer's added cost on the hot path is **unmeasured** until the load test runs with and without a spool |
| 15 | Spot recovery. The feeds resume exactly: `GeneratorRun` snapshots on the data volume every 30 s, restores byte for byte, sends at least once and never skips; a fresh start deep in a window is refused; `sealed` checks the secret against the commitment before running | **Open:** the scorer's rebuild after a replacement, and the engine's memory at the live rate (about 4.7 kB per entity and 900 B per held event, measured; 24 hours at 1,000 a second is about 33 GB against 4 GB) |
| 19 | Champion and challenger: tracks replayed through the scorer's own engine path; split in time at 70 percent; XGBoost with fixed parameters and early stopping; an FT-Transformer challenger; ONNX export refused unless it scores as the fitted model does; PR-AUC with stratified bootstrap intervals; only synthetic-trained models ship | Superseded on the synthetic track by ADR 21: after the generator was made harder the champion scores 0.8427 (0.8393 to 0.8464) and the challenger loses by -0.0415 (-0.0437 to -0.0392), so the gate refuses it on both tracks. The record also carries the model hop that was measured on a busy machine and the cap it nearly bought |
| 20 | The six day-long features at hourly resolution: window `[floor_hour(t - 24h), t)`, a definition the reference and leakage test share; bucketed aggregators in arrays, distinct by latest bucket; instance r7i.large (16 GB) | Engine about 6 GB at the live rate, measured at scale (`docs/engine-footprint-steady.json`); the whole instance is measured in the dry run |
| 21 | The synthetic fraud is much harder: attacks take their cards' own sessions, card testing is slow and spread, takeovers spend like the owner and often from a known device, collusion is gentle and uses front merchants; legitimate traffic gains big tickets, new devices, shared terminals and busy merchants | Champion on the synthetic track falls from 0.9996 to 0.8427 (0.8393 to 0.8464), no single feature above 0.05. Done before sealing, as it had to be |
| 22 | The review queue is measured on the queue the platform would hold: the stream through the scorer's own engine, the shipped champion and rules, arrivals unsampled, and nothing counted from the champion's training window | Expected loss beats score ranking by $97.46 an hour (48.03 to 150.44) over thirteen unseen days. The first run, inside the training window, said 221.69: more than double |
| 23 | The drift monitors are run over a replayed stream against the schedule's own regime days, which they are never told: reference fixed to the champion's training window, each day judged on a hash-drawn 3 percent, the trigger asked once a day | Seven baseline days flagged nothing; all three regime changes caught on their first full day; the first retraining request opens one day after the first change, the floor the rule sets |
| 24 | The retraining job fits a candidate, compares it to the incumbent on rows neither saw, writes a pull request, and stops: it never moves the pointer or runs the gate. It reports whether the candidate has seen the drift, judged by the transactions and not the cutoff | Under the card-testing wave the champion falls from 0.84 to 0.27. The candidate built when the alarm fires loses (-0.0024); one built once three drifted days' labels arrive recovers to 0.82 (+0.5626), about nine days after the drift. A request now closes only when answered or when the drift has stopped for two consecutive days (`drift/trigger.resolve`) |
| 17 | On the real data a card is `card1` to `card6`, `addr1` and the account start day; there is no device or merchant; the clock starts 2017-12-01 | **Accepted by Peter on 2026-09-19**, all three choices as written: taken by the build session on 2026-09-14 with the measurements in the ADR. Numbered 17 because the plan already assigns 8 to 16. It moved the wire schema to version 2 |

### Plan amendments made in the same commits as the code

- **PLAN.md 2.1**, the sealed schedule (week 1). The plan said the schedule
  was "hashed and committed before go-live and revealed on Jul 1", which
  cannot both be true of a schedule sitting in the repository in the clear.
- **PLAN.md 2.2 and ADR 4**, the engine (week 3).
- **PLAN.md section 5, week 2 row, and section 4**, the real-data mapper and
  ADR 17 (2026-09-14).

---

## 5. Things that will break if you do not know them

**The regime schedule is sealed.** `verdict/events/generator/regimes.py` is
frozen: a test asserts its source hash against `docs/generator-hashes.json`.
Editing it is a deliberate act that belongs in the same commit as an
explanation. The live schedule is derived from a secret the repository does
not hold; `verdict schedule seal` commits three hashes before go-live and the
secret is published on Jul 1 2027.

**The committed hashes.** `docs/generator-hashes.json` pins the wire schema,
the dev schedule, the reference entity graph and the `regimes.py` source. If
a test fails against it, the code changed. Update the file deliberately, in
the same commit, with the reason in the message. Do not paste the new hash in
to make a test pass.

**The leakage test is never weakened.** Not the tolerance, not the sample
size, not by excluding an entity. A failure means the feature is wrong.

**The engine's ordering is the point-in-time guarantee.** `serve` then
`observe`, and observation is deferred until a strictly later timestamp
arrives. Reordering those, or removing the buffer, reintroduces the leak in
`docs/leak-caught.md`.

**The competition data may not be redistributed.** `data/` is ignored in full,
including derived per-row feature stores, and `tests/test_data_terms.py` asks
git itself to confirm it. The real-data track therefore never runs on the live
AWS stack, because the live window is public. `docs/data.md` has the clauses.

**Nothing promotes itself.** A model reaches production only through a merged
pull request carrying the shadow evidence. That is week 5 onward, but it is a
standing rule.

**`mypy` here is not the `mypy` CI runs.** This machine is Windows and CI is
Linux, and mypy analyses only the branch for the platform it is asked about.
`fine_grained_timers` calls `ctypes.WinDLL`, which exists on Windows and
nowhere else; a clean local run passed it and CI failed on it. Before pushing,
run both:

    .venv/Scripts/python.exe -m mypy verdict tests
    .venv/Scripts/python.exe -m mypy --platform linux verdict tests

Platform-specific code goes in `if sys.platform == "win32": ... else: ...`,
never behind an early return: mypy prunes the branch it is not checking, but
with `warn_unreachable` on it still reports whatever follows a return as
unreachable.

---

## 6. What is waiting on a person

| # | Item | Who | When it bites |
|---|---|---|---|
| 1 | ~~The go-live date~~ **Decided 2026-09-18**: as soon as the definition of done is met; sixty-day window (section 1) | | |
| 2 | ~~Docker~~ **Done 2026-09-13**, on the personal desktop. The Redpanda stack runs (section 3) | | |
| 3 | **Kaggle forum posting.** Rule 8.B asks that publicly shared competition code be posted to the competition's own forum. Arguably spent since 2019, cheap to honour, and it publishes under Peter's name | Peter | Go-live |
| 4 | ~~A least-privilege AWS identity for 09~~ **Done 2026-09-18**; the deploy policy `verdict-deploy` attached by Peter on 2026-09-19. **Nothing is applied without Peter's go** | | |
| 7 | ~~The storage question in ADR 14~~ **Decided 2026-09-19** (ADR 18): a day on each topic, and a weighted sample for the record. The original item: **The storage question in ADR 14.** Keep a sample of legitimate traffic, with weights, instead of every row; or pay for disk; or lower the rate | Peter | Before go-live, and before week 6's queue evaluation reads the live history |
| 5 | ~~Review ADR 17~~ **Done 2026-09-19**: Peter accepted all three choices (the card key, no device, the 2017-12-01 reference date) as written. Week 5 can train on the real data | | |
| 6 | ~~Kinesis and the 50 ms budget~~ **Decided 2026-09-18: Redpanda on the instance** (ADR 3). The original item: **Kinesis cannot meet the 50 ms budget as PLAN.md 2.4 measures it.** AWS documents about 200 ms average propagation for a polling consumer and about 70 ms with enhanced fan-out, before the scorer starts. Three options in ADR 3's open question: start the clock at the scorer, run Redpanda on the live instance instead, or keep Kinesis and publish what it measures. The first changes the one-liner's wording, the second the AWS story, the third the headline number | Peter | Before week 7's Kinesis client is written; nothing earlier depends on it |

**Before sealing:** the docstrings in `regimes.py` say "Jul 1 2027". That
file is hashed, so changing the text is a deliberate edit with
`docs/generator-hashes.json` updated in the same commit and the reason in the
message, done once, before `verdict schedule seal --window-days 60`.

**`risk.peterparker.ca`** (decided 2026-09-18): the zone is on Cloudflare,
and the dashboard is served from the live instance through a **Cloudflare
Tunnel**, the way `coach.peterparker.ca` is served, not from an Azure Static
Web App like 01, 02 and 08. The dashboard shows live telemetry that exists
only on the instance; a tunnel needs no inbound port, no Elastic IP and no
DNS change when a spot replacement arrives, and it keeps the live stack in
one cloud with one teardown. The tunnel token lives in SSM Parameter Store at
`/verdict/cloudflare-tunnel-token`, put there by Peter, never in the
repository. The public hostname route is added at go-live. ADR 14 records
this with the Terraform.

**AWS access, phase 1** (2026-09-18): `deploy/aws/iam/bootstrap-policy.json`
is the policy for the `verdict-bootstrap` IAM user: read-only account facts in
`ca-central-1`, budgets named `verdict-*`, and parameters under `/verdict/`.
It cannot create any compute. The phase 2 policy, for `terraform apply`, is
written from the Terraform's own resource list when that exists. The account
must be on AWS's **paid plan** before the live stack runs: a free-plan account
closes when its credits run out, and this account also holds 04. **Done
2026-09-18**: Peter upgraded the account, created the user and profile, and
stored the tunnel token (a `SecureString`, tagged `project=verdict`).

Checked from this machine the same day: the profile works; it is refused an
instance launch (dry run), IAM reads, a parameter outside `/verdict/`, any
region but `ca-central-1`, and a budget not named `verdict-*`. EC2 quotas in
`ca-central-1`: 32 vCPU of standard spot, 16 of on-demand, so no increase is
needed. The account already has 04's budget, US$150 a month account-wide with
email alerts, which stays as the backstop.

**`verdict-monthly` exists (2026-09-18):** US$60 a month, filtered to the tag
`user:project$verdict`, email alerts to the same address as 04's budget at 50,
80 and 100 percent actual and 100 percent forecast.

**It counts nothing until the `project` cost allocation tag is active**, and
the tag is not listed yet. The only tagged resources so far (the parameter,
the IAM user) are free, and the billing console lists a tag key only once it
appears in billing data, so it may not show until the first billable tagged
resource runs. Hence a rule for the go-live script, to be built with the
Terraform: the stack may come up for the dry run, which puts the tag into
billing data, and Peter activates it then; **the live window does not start
(schedule sealed, generator at the live rate) unless `ce
list-cost-allocation-tags` shows `project` as Active**. Until then 04's account-wide US$150 budget is the
only alarm that sees 09's spend.

**Spot prices are higher than PLAN.md section 6 assumed**: 4 vCPU, 8 GB spot in
`ca-central-1` was US$0.080 to 0.097 an hour on 2026-09-18 (c5, c6i, c6a, c7i;
cheapest c7i.xlarge in 1d), against the US$0.055 the plan priced in. The
section 6 amendment carries the new figure. **Decided the same day: sixty days on a c6a.large**
(2 vCPU, 4 GB, US$0.037 to 0.040 an hour), about CA$115 for the window with ADR 14's 100 GB volume; fallback
4 vCPU for 45 days if the live load test fails the budget, decided before sealing.
`verdict-monthly` is US$60 accordingly (expected about US$50 a month).

Nothing else is blocked. Weeks 4, 5 and 6, including the broker-backed latency
measurement, can be built on this machine.

**The dry run, 2026-09-21.** Peter gave the go, and approved the budget as it
stood (US$60 a month on the r7i.large estimate). After a policy fix Peter
re-applied (`CreateInsideOwnVpc`, 4c528d8, with the schedule-secret deny of
c9217fb, which is confirmed working: the build identity is refused the
parameter with an explicit deny), the whole stack came up and has streamed
the development schedule at 1,000 a second since **2026-09-21T18:57:00Z**, the
window start every redeploy keeps (`C:/Dev/POCs Dev/09-realtime-risk-platform/dry-run-start.txt`).

It decided nothing inside 50 ms at first. Four causes found and fixed the same
evening, each with tests (`docs/latency-budget.md`, "The first hours on the
live stack"): the consumer waited out its 0.1 s timeout on every batch
(6c5e696); the engine's sweep walked every entity (fbcf5ca); Python's full
collection walked the feature state (38e948b); the feed stopped the stream
for 3.3 s to save its place (4bef6b1, 40a1dce, 3c0ced1).

**What is left is the instance, and it is Peter's call on cost.** Two vCPUs
are one physical core; scorer, broker and feed overload it (load average near
3) and on an r5.large 13 percent of decisions still took over a second. The
same image on an **m6i.xlarge** (4 vCPU, 16 GB) decided 99.95 percent of
617,040 inside 50 ms and none over 250 ms, and drained a 130,000 backlog in
two minutes. **The stack is running on it now as a trial**, through a
command-line `instance_types` override that is not committed; the repository
still says r7i.large, r6i.large, r5.large. Spot in ca-central-1d that day:
m5.xlarge US$0.063, m7i.xlarge 0.087, m6i.xlarge 0.089 an hour, so about
US$59 to 78 a month with the volume, against a US$60 budget. If approved:
amend ADR 20, set `instance_types` to the m-family xlarge list, raise
`verdict-monthly` (Peter, console), and restart the 72-hour clock on it.

Operating notes: roll a new image without replacing the instance by editing
`VERDICT_IMAGE` in `/etc/verdict/stack.env` over SSM and running `docker
compose ... up -d` (the scorer restarts cold, ADR 8). `py-spy` is installed
on the current instance for profiling.

---

## 7. The real-data track, as measured

Downloaded 2026-09-12 to `data/raw/ieee-fraud-detection/` on the laptop, which
is what `verdict data` commands default to. Downloaded again 2026-09-13 to
`C:\Dev\POCs\09-realtime-risk-platform\data\ieee-fraud-detection`, outside
OneDrive, and `verdict data verify --directory` there passes against
`docs/ieee-cis-checksums.json`: the same bytes.

| | |
|---|---|
| Transactions | 590,540 across 182 days |
| Rate | 0.0376 events per second |
| Timestamps | `TransactionDT`, whole seconds, offset from an unstated reference |
| Fraud | 3.499% |
| Identity file coverage | 24.4% of rows, but it identifies configurations, not devices |
| Merchant identifier | **None** |
| Card, as ADR 17 defines it | 88.7% of rows linkable; 203,467 cards; median 1 transaction, 99th percentile 19 |
| Features on this track | 6 of 16 (`features_on_track()`) |

**The absence of a merchant is a design constraint, not a gap to fill.** Three
of the sixteen features are merchant-keyed and cannot be computed on this
track. The agreed response is to report per track which features exist.
Promoting `ProductCD`, a five-valued product category, into a merchant would
make those features a measurement of a party invented here.

**The mapper answered the three open questions on 2026-09-14** (ADR 17). A card
is issuer, product, billing region and account start day; a row that cannot
form that key gets an identifier of its own, so it has no history rather than
a borrowed one. There is **no device**: the identity columns describe
configurations shared by thousands of purchasers, which contradicts what this
section used to say. The clock starts at 2017-12-01T00:00Z by published
convention. The mapped log is written by `verdict data events` to
`data/raw/ieee-cis-events/` (ignored), in 68 seconds.

Consequence worth knowing before week 5: the real data can show a card behaving
unlike itself, and nothing else. Card testing and merchant collusion are graph
patterns, and the graph is not in the file.

---

## 8. Measurements taken so far

On the build laptop unless marked, all with intervals where they are rates. Nothing
here is a platform latency or throughput figure: nothing is scored yet.

| Measurement | Result | Source |
|---|---|---|
| Generator, written to the raw log | 12,219 events/s (8,790 to 15,648) | `docs/week1-measurement.json` |
| Generator, nothing written | 19,577 events/s (8,146 to 31,008) | same |
| Fraud share produced | 3.06% (2.85 to 3.26) against a 3% target | same |
| Feast online read, steady state | p50 0.76 ms, p99 1.76 ms | ADR 5 |
| Feast online read, first call | about 42 ms | ADR 5 |
| Online/offline parity | 100% on a replay | `tests/test_parity.py` |
| Leak impact on the real data, grouped by `card1` | 312 rows of 590,540 (0.053%) | `docs/leak-caught.md` |
| Leak impact on the real data, ADR 17 card, unfixed engine over every card (build desktop) | 65 rows of 590,540 (0.011%) served a wrong value; 324 values; 45 card-instants | `docs/leak-caught.md` |
| Point-in-time check, real replay, 2% of cards (build desktop) | 0 violations, 67,920 comparisons, 5,386 cards, 104 s | `verdict data check`, ADR 17 |
| Real replay mapped to events (build desktop) | 590,540 events, 68 s | `verdict data events` |
| Scorer hops at p99, through Redpanda | features 0.340, model 0.003, rules 0.065, hand-off 0.048 ms | `docs/latency-week4-redpanda-untuned.json` |
| Produce to acknowledgement, default Windows timer vs 1 ms | 48.44 ms vs 3.74 ms at p50 | ADR 9 |
| Send to receive, producer in its own process vs in the scorer's thread | 8.64 ms vs 70.57 ms at p50, 12 shuffled runs | ADR 9 |
| Flush, from the Windows host | 47.72 ms at p50 on 9 of 12 connections, 7.10 ms on 3 | `docs/latency-week4-flush-host.json` |
| Flush, from inside the broker's Docker network | 3.99 ms at p50 on 10 of 10 connections | `docs/latency-week4-flush-in-network.json` |
| End to end, in-process stream, idle machine | p50 1.16, p95 1.98, p99 5.50 ms, 5 runs | `docs/latency-week4-memory.json` |
| End to end, Redpanda inside its network, idle machine | p50 11.94, p95 24.21, p99 52.81 ms, 20 runs | `docs/latency-week4-redpanda-in-network-healthcheck-*.json` |
| HTTP endpoint, 1 / 2 / 4 / 8 connections at 1,000/s | decided 100 / 83.5 / 61.4 / 60.9 percent | `docs/latency-week4-http-c*.json` |

**Two of these were published wrong and then corrected.** The leak's impact was
first measured on a synthetic stream truncated to seconds and reported as if
it described the competition data, overstating it by three orders of
magnitude. Then the sampled check that remeasured it keyed served values by
card and instant, so a burst's events were all compared with the last one's
value: 110 rows, where 65 is right. `docs/leak-caught.md` carries the correction in place rather than
edited over. If a number here ever looks too good, that document is the
precedent for what to do about it.

---

## 9. How to run things

```bash
# everything, from the repository root
.venv/Scripts/python.exe -m pytest                 # 390 tests, 2.5 to 4 minutes with the broker up
.venv/Scripts/python.exe -m pytest -m "not slow"   # about 90 seconds
.venv/Scripts/python.exe -m ruff format .
.venv/Scripts/python.exe -m ruff check .
.venv/Scripts/python.exe -m mypy verdict tests

# the generator
.venv/Scripts/python.exe -m verdict.cli generate --out data/raw/dev --events 100000

# the sealed schedule
.venv/Scripts/python.exe -m verdict.cli schedule hash

# the competition data, which on this machine lives outside the repository
export VERDICT_IEEE_CIS_DIR="C:/Dev/POCs Dev/09-realtime-risk-platform/data/ieee-fraud-detection"
.venv/Scripts/python.exe -m verdict.cli data verify
.venv/Scripts/python.exe -m verdict.cli data inspect
.venv/Scripts/python.exe -m verdict.cli data events    # the mapped log, into data/
.venv/Scripts/python.exe -m verdict.cli data check     # point-in-time check, about 2 minutes

# the stream contract against the broker (skips without one)
.venv/Scripts/python.exe -m pytest tests/test_stream.py -m broker

# the local stream stack (Docker is not on PATH in a fresh shell; see section 2)
docker compose -f deploy/compose/docker-compose.yml up -d --wait
docker compose -f deploy/compose/docker-compose.yml down -v
```

---

## 10. The other two repositories

| Repository | Path | What it is |
|---|---|---|
| This project | `POCs/09-realtime-risk-platform` | `Peter-A-P/realtime-risk-platform`, private until it has a result |
| The plan | `POCs/ml-portfolio-plan` | Private. `STATUS.md` is the first thing to read on any machine. Update it when this project starts, ships or spends money |
| The public site | `POCs/peterparker.ca` | Public. `projects.yaml` holds the card and the log |

**The site rule.** Whenever a project's status changes in the plan's
`STATUS.md`, the same session adds an entry to `log:` in the site's
`projects.yaml`, sets the card's `status` in the same edit, and pushes; the
push deploys. Site words: `planned`, `building`, `shipped`, `live`. Those log
pushes are pre-authorised. Anything else on the site is not. 09 is already
`building`, so no site change is due until it ships.

Run the site's own tests before pushing it: `.venv/Scripts/python.exe -m
pytest` in that repository, which checks card layout against the real fonts.

**Noticed and not acted on:** the site build warns that
`compliant-ai-gateway` is public while `projects.yaml` still says `building`.
That is project 04's status flip, not this project's, and it needs someone to
decide what 04 claims.

---

## 11. What comes next

**CPU-heavy work waits for Peter's green light** (2026-09-19): another project
uses this machine, and every latency figure here has been spoiled once by a
shared CPU. Estimated on this desktop (i5-10400F, 6 cores, no GPU), in the
order it would run:

| Work | Why it needs the CPU | Estimate |
|---|---|---|
| Engine memory at steady state: a few million events through the feature engine at the live population | ADR 15's open question; may decide the instance size or the window design | 10 minutes |
| Load test with and without the history spool, memory and Redpanda, and with zstd on the decisions producer | ADR 18's cost on the hot path, and whether zstd is safe to switch on live | 45 minutes, idle machine required |
| Features for training: replay the competition data and about 5 million synthetic events through the engine | Training reads the engine's features, never a second computation | 20 to 30 minutes |
| Gradient-boosting champion on each track, with its interval | Week 5 | 20 to 40 minutes |
| The leak's PR-AUC inflation: the same training on the unfixed engine | README's "Leak caught" column | 20 to 40 minutes |
| FT-Transformer challenger, on CPU | Week 5; the heaviest item by far | 2 to 3 hours, can run overnight |
| Rollback drill, five timed runs | Definition of done | 10 minutes |

About 4 to 5 hours in all: a first session of about 2 hours (everything but
the challenger), then the challenger on its own. Package installs (XGBoost,
PyTorch CPU, ONNX Runtime) are downloads, not CPU, and can happen any time.

**Built on 2026-09-19 without the CPU**, while it was needed elsewhere: the
training-set code (ADR 10 rule 4 enforced), the platform image and its push
script, and the scorer, label collector, compactor and Prometheus in the live
compose file. Then the same day: **the live feeds** (ADR 15), a resumable
generator played in real time, transactions at their event time and labels a
week later, each saving its place on the data volume; and **the public
dashboard** (ADR 14's addendum), Grafana provisioned from code, anonymous and
read-only, every query checked against the exported metrics.

**A finding that may block go-live, needing the CPU to settle:** the feature
engine retained about 8.5 kB per event over its first 60,000 events at the
live population (`tracemalloc`). Part of that is per-entity setup that stops
growing, but at 1,000 events a second over 24-hour windows even a few hundred
bytes per event is tens of gigabytes, on a 4 GB instance. The steady-state
figure needs a few million events through the engine, about 10 minutes of
CPU, and is now **first** on the CPU list; what follows it (bucketed windows,
a smaller population, a lower rate, or a larger instance) is ADR 15's open
question and Peter's call on cost.

**Peter's steps before go-live, from this work:** (1) in Cloudflare, the
tunnel's public hostname `risk.peterparker.ca` must point to
`http://grafana:3000`, not `localhost:3000`; (2) at sealing, put the secret
in SSM with `aws ssm put-parameter --profile verdict --region ca-central-1
--name /verdict/schedule-secret --type SecureString --value ... --tags
Key=project,Value=verdict`, as with the tunnel token, and commit
`docs/sealed-schedule.json`.

**CPU session, 2026-09-19** (Peter's green light; projects 06 and 08 were
running jobs too, so nothing timing-sensitive was measured):

- **Engine memory: decided and built** (ADR 20, Peter): hourly buckets for
  the day-long features and an r7i.large (16 GB); measured about 6 GB at the
  live rate. The first measurement, kept for the record:
- **Engine memory** (`docs/engine-footprint.json`): about 4,700 bytes per
  entity and 900 per held event; a day of 24-hour windows at 1,000 a second
  is about 33 GB against a 4 GB instance. **Blocks go-live; Peter's call**
  among the options in ADR 15.
- **Real-track champion** (`docs/champion-real.json`, ADR 19): test PR-AUC
  0.0750 (0.0715 to 0.0789), base rate 0.035; model hop p99 0.10 ms.
- **Real-track challenger** (`docs/challenger-real.json`): FT-Transformer
  0.0492 (0.0473 to 0.0514); minus the champion -0.0258 (-0.0288 to
  -0.0230); hop p99 0.48 ms. 810 s to fit.
- **Leak inflation** (`docs/leak-inflation.json`, `docs/leak-caught.md`):
  none measurable; the leak changes 4 of 152,415 test rows.
- **Synthetic champion and challenger** (`docs/champion-synthetic.json`,
  `docs/challenger-synthetic.json`, ADR 19 and ADR 21): on the harder
  generator, champion 0.8427 (0.8393 to 0.8464) against a base rate of
  0.030, challenger 0.8012 (0.7978 to 0.8050), the paired difference
  -0.0415 (-0.0437 to -0.0392), so the gate refuses the challenger on this
  track as on the real one. Hops p99 0.22 ms and 0.37 ms. Both ship in
  `verdict/models/artifacts/` (`champion-8d960d985749`,
  `challenger-136b21035bfe`), so the dry run exercises the pair the live
  stack will carry.
- **A latency number taken on a busy machine cost most of an evening.** The
  synthetic champion measured 3.047 ms p99 while project 12 held the CPU at
  72 percent, which is over budget, so it was capped at 700 trees for
  -0.007 PR-AUC. Idle, the same model is 0.22 ms. The cap was reverted;
  `MAX_ROUNDS` is 2,000. Check total CPU before and after every timing, and
  sanity-check against a known figure: the real track's 374 trees at
  0.101 ms made 3 ms for 1,496 trees impossible on its face. Written up in
  ADR 19 and `docs/latency-budget.md` rather than quietly corrected.
- **Refitting does not reproduce a model id.** The version is the SHA-256 of
  the ONNX bytes; XGBoost's parallel histogram build is not bit-stable, so
  the same command gives a new id and a PR-AUC that agrees to 7 decimals.
  Provenance, not reproducibility, is what the hash buys.
- **Then, on an idle machine** (06 stopped by Peter, 08 idle): the
  **rollback drill**, five runs, 5.8 ms (5.4 to 6.2) from flag to the old
  champion deciding, none by the rolled-back model after it
  (`docs/rollback-drill.json`); and **the cost of staging history**
  (`docs/latency-history-cost.json`, `docs/latency-budget.md`): about 0.1 ms
  at p50 in process and 1 ms through the broker; zstd costs nothing
  measurable, so it can go on live. The p99 is not settled: two of ten
  staged broker runs stalled for 0.8 and 1.4 s. Re-measure on the instance.
- Training tables and models live at
  `C:/Dev/POCs Dev/09-realtime-risk-platform/models/`, outside OneDrive and
  outside git; `verdict train replay | champion | challenger | leak`
  rebuild them.

In the order the plan sets, with nothing blocked except where noted:

1. ~~The IEEE-CIS row-to-event mapper~~ **Done 2026-09-14**, ADR 17.
2. **Week 4, in progress.** Done: the `Stream` protocol, in-process and
   Redpanda implementations, contract tests on the live broker; the scorer,
   rules, stand-in model, per-hop timers and load test (ADR 8); on
   2026-09-15 the shared decision core, the HTTP endpoint, and two week 5
   pieces that need no model: the rollback flag and shadow scoring, with a
   `shadow` topic in the compose file. An existing local volume needs the
   topics job run again (`docker compose ... run --rm topics`) to create it.
   Also on 2026-09-15, ahead of their weeks: the promotion gate (ADR 11) and
   the review queue's ranking and evaluation (ADR 13). Both found a flaw in
   their own first draft before any test ran against real data, and both
   flaws are in the ADRs: a decision cost that let a decline-everything model
   cost nothing, and an expiry check that expired every item before review
   when the wait limit was under an hour. Then the drift monitors and trigger
   (ADR 12), which amend PLAN.md 2.6 by not using Evidently.
   On 2026-09-17 the latency work ran: the untuned baselines were taken
   first, as the plan requires, and are kept
   (`docs/latency-week4-{memory,redpanda}-untuned.json`). Then the 59 ms that
   was not the scorer was tracked down. **ADR 9 and `docs/latency-budget.md`
   are written**; three of the four costs belong to the measuring host (the
   process's timer resolution, the load producer sharing the scorer's
   interpreter, and Docker Desktop's port forwarder), and the fourth is the
   platform's own per-batch flush and checkpoint, which is a throughput
   ceiling before it is a latency one. The load test changed with it: on a
   broker the producer now runs in its own process, joined by event id; the
   pacing loop yields instead of spinning; and every run reports where its
   producer ran, whether a queue stood, and what its batches held.
   `verdict/stream/probe.py` and `verdict flush-probe` are the instrument
   that found the forwarder, and it is meant to be run twice, once from the
   host and once from inside the broker's network, which the module's
   docstring gives the command for.
   On 2026-09-18 the HTTP half of Rule C candidate 3 was built:
   `verdict/scoring/httpload.py` and `verdict http-loadtest`. It starts the
   endpoint in its own process, offers the same generated events at the same
   rate over a stated number of connections, and reports the same statistics
   as the stream test plus two of its own: how long a transaction waited for
   a free connection, and what the endpoint refused. A fresh endpoint runs per
   run, on its own port, because the ledger and the engine's windows are per
   process and every run sends the same transactions.
   **A second attempt on 2026-09-18 was also taken on a shared machine**: the
   runs were gated on project 12's `sun_fit.py` finishing, and its
   `finishline backtest` started two minutes before the first run and held
   40 to 60 percent of the CPU throughout. The reports are kept in
   `docs/provisional/2026-09-18/` with a note, and `docs/latency-budget.md`
   has a section on what survives: inside the broker's network the scorer's
   flush is about 2 ms against about 55 ms through the forwarder; the
   in-process stream's Windows tail (p99 323 ms) looked like 17.5 ms in a
   Linux container (**withdrawn that evening**: on an idle machine both are
   about 5.5 ms; the gap was the other job); HTTP over 2, 4 and 8 connections refused 25.8, 28.7 and 10.5
   percent as out of order. Open: mid-run stalls inside the network (p95 133
   to 845 ms) that are neither the commit nor a queue; the broker's
   five-second `rpk` health check on `--smp=1 --overprovisioned` is the
   candidate to A/B on an idle host. `verdict loadtest` now takes
   `--bootstrap`, and the container command in `docs/latency-budget.md` is
   checked. **Gate the next attempt on total CPU, not a process name.**
   **Done on an idle machine, 2026-09-18 23:20 to 23:57 UTC** (Peter stopped
   the other jobs; the watcher waited for five minutes under 15 percent CPU
   and logged it around every run; no other Python job ran). The end-to-end
   table in `docs/latency-budget.md` is filled: in-process stream p50 1.16,
   p99 5.50 ms; Redpanda inside its network, 20 runs, p50 11.94, p95 24.21,
   p99 52.81 ms (13 of 20 runs under 50 ms at p99, the rest losing it to
   broker-side stalls whose cause is not known); from the Windows host, two
   modes by forwarder connection. The broker's health check was A/B tested
   in ABBA blocks and **ruled out** as the cause of the stalls. The transport
   comparison is done too: HTTP over one connection decides everything but
   runs at its limit (p99 96 to 132 ms, one run in five fell a second
   behind); over two or more it refuses 16.5 to 39.1 percent as out of order.
   The HTTP client's "offered" time was fixed first (it timed from the
   scheduled slot, and the pacing loop sends up to 2 ms early, so waits came
   out negative). Open: what stalls the broker's side of the flush in about a
   third of runs; and a non-Python HTTP load client (k6, as PLAN.md names) to
   separate client from endpoint on the single connection.

   A functional check on 2026-09-18, at 500/s on a busy machine and **not a
   result**, showed the shape the comparison is likely to have, and it is
   structural rather than load-dependent: one connection decided 100 percent
   of transactions; two or more decided about three quarters, the rest
   refused with 409 as arriving after later ones; and adding connections made
   throughput worse rather than better, 500/s at one, two and four
   connections falling to 395/s at eight, because the endpoint serialises on
   the engine's lock and extra connections only add queueing. That is ADR 8's
   argument for a stream consumer, measured.

   Also on 2026-09-18, ahead of week 7: `verdict/stream/parity.py`, the parity
   check ADR 3 names, in `tests/test_stream_parity.py`. Its first draft
   compared only effects, and its own planted fault (one cent on one
   transaction) showed that a changed event only surfaces in a later event's
   window; it now also compares each transaction as received, by digest.
   Starting the Kinesis client turned up a larger problem, now item 6 in
   section 6 and an open question in ADR 3: AWS's own documented propagation
   delay (about 200 ms polling, about 70 ms with enhanced fan-out) is larger
   than the whole 50 ms budget. The client is not written until Peter picks
   an option.

   Also on 2026-09-18, a real defect fixed: one record the scorer could not
   decide (not a transaction, a newer schema version, or late in event time)
   raised before the checkpoint, so every restart read it again and stopped
   again, a permanent outage from one message. Reproduced, then fixed: such a
   record now goes to a `dead-letter` topic (added to the compose topics job;
   **an existing local volume needs the topics job run again**) and a run of
   fifty in a row stops the scorer instead. ADR 8's addendum and the first
   section of `docs/failure-modes.md`, which PLAN.md's week 8 row names, carry
   it.

   Then the scorer as a service, which did not exist: nothing ran the stream
   consumer except the load test. `verdict score` (`scoring/service.py`) polls
   until stopped and checks the stop only between batches, so a batch in hand
   is always decided and checkpointed; `observe/metrics.py` serves Prometheus
   metrics on localhost:9108 by default. Building it found a leak: the
   scorer's per-batch timing lists, kept for the load test's percentiles,
   never shrank, about a gigabyte a day at the live rate. The service drains
   them into histograms (`CommitStats.drain`). `prometheus-client` is now a
   direct pin. No Prometheus or Grafana container yet; a scrape of
   `127.0.0.1:9108/metrics` is the check.

   Note the local stack cannot support the 50 ms claim whatever the machine is
   doing: the forwarder alone costs more than the budget on most connections,
   which is why the figure is the live stack's.
3. **Week 5:** champion on IEEE-CIS and on replay, FT-Transformer challenger,
   shadow scoring, promotion function, rollback drill. ADRs 10 and 11. This is
   also where the leak's offline PR-AUC inflation gets measured, by training
   twice; the unfixed engine is kept in `tests/test_engine.py` for that.
4. **Week 6:** drift monitors, the retraining pull request, the expected-loss
   queue. ADRs 12 and 13.

The week-by-week table in `PLAN.md` section 5 is the authority, and it now
records what was actually done for weeks 1 to 3.
