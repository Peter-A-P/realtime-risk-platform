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
404 tests; `ruff`, `ruff format` and
`mypy --strict` all clean. Nothing has been scored yet, so the README's headline tables are
still empty and stay that way until they are real.

The build started on 2026-09-12, about twenty weeks ahead of its Feb 2027
slot, after a check that nothing in the portfolio blocks it.

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
    promote.py       The promotion gate: paired bootstrap on labelled shadow rows, PR-AUC
                     and decision cost, non-inferiority on the bound. Never promotes.
  drift/
    stats.py         PSI (reference deciles, NO_EVENTS in its own bin) and two-sample KS.
    monitors.py      Fixed reference; daily verdict per feature and score; thresholds are
                     conventions and must not be tuned against any schedule.
    trigger.py       Same quantity drifted two consecutive days, none open: RetrainRequest.
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

Not yet written: `stream/kinesis.py` and `stream/parity.py` (week 7),
`models/` training and export (week 5), `drift/retrain.py` and `approval.py` (week 6), `drift/`, `queue/`, `observe/`, `chaos/`,
`deploy/terraform/`.

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

Thirteen ADRs, in `docs/adr/`: 1 to 9, 11 to 13, and 17. Read them before reopening anything they cover.

| ADR | Decision | Note |
|---|---|---|
| 1 | Platform, not model | Model work is timeboxed to week 5 |
| 2 | Two tracks: real data offline, synthetic live | Every number says which track |
| 3 | Redpanda locally, Kinesis live, one `Stream` interface | Interface written 2026-09-14 with in-process and Redpanda implementations; Kinesis is week 7 |
| 4 | **Amended.** The aggregation engine is written here | Bytewax has no Python 3.13 wheels, on any platform, up to 0.21.1. Peter chose this over pinning Python to 3.12, adding a Rust toolchain, or a Kafka-only engine |
| 5 | Feast: registry, point-in-time join, online read | Push sources, not materialisation. **Its first online read after start-up costs about 42 ms against a 5 ms budget hop: the scorer must warm the store** |
| 6 | Features computed once; the reference evaluation is not a second computation | One definition, two executions, and the test holds them together |
| 7 | The leakage test is written first and never weakened | It has already caught three real faults |
| 8 | The scorer is a stream consumer: at least once, duplicates stopped before the engine, checkpoint after durable decisions, features served from the engine in process | **`transactions` has one partition** because the engine needs time order; more needs a reorder buffer whose hold time is latency |
| 9 | The latency budget stands as PLAN.md 2.4 states it; every cost that belongs to the measuring host is measured on its own and published, never subtracted quietly; the 50 ms figure is claimed from the live stack, not from here | Three host costs found and measured: the process's timer resolution, the load producer sharing the scorer's interpreter, and Docker Desktop's port forwarder (about 41 ms on some connections, fixed per connection, unchanged by every client setting tried). The platform's own cost is the per-batch flush and checkpoint, which is a throughput ceiling before it is a latency one |
| 11 | Shadow on the champion's own features, timed apart, unable to break scoring; promotion only if the interval bound clears the margin, on labels that had arrived, with at least 50 frauds; rollback by a pointer read per event | Margins and prices are placeholders until the champion's variability is measured |
| 12 | Drift: PSI and KS against a fixed reference; PSI 0.25, KS statistic 0.10 with p below 0.01, 500 values minimum; same quantity two consecutive days | **Amends PLAN.md 2.6**: no Evidently. **The thresholds must not be tuned against the development schedule**, or the sealed schedule grades nothing |
| 13 | The queue ranks by expected loss; simulated a day at a time in hourly steps with expiry; paired by day | At fixed capacity the prices cannot reorder the queue, and a test says so. Scores must be calibrated before a result is published |
| 17 | On the real data a card is `card1` to `card6`, `addr1` and the account start day; there is no device or merchant; the clock starts 2017-12-01 | **Pending Peter's review**: taken by the build session on 2026-09-14 with the measurements in the ADR. Numbered 17 because the plan already assigns 8 to 16. It moved the wire schema to version 2 |

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
| 1 | **The go-live date.** It was Apr 5 2027. Starting twenty weeks early unfixes it, and the AWS account timing in the plan's action 9 was arranged around it (a free-plan account closes itself six months after opening) | Peter | Before week 7, the first week that needs an AWS account. Nothing before then costs anything |
| 2 | ~~Docker~~ **Done 2026-09-13**, on the personal desktop. The Redpanda stack runs (section 3) | | |
| 3 | **Kaggle forum posting.** Rule 8.B asks that publicly shared competition code be posted to the competition's own forum. Arguably spent since 2019, cheap to honour, and it publishes under Peter's name | Peter | Go-live |
| 4 | AWS budget alarms before the first resource exists | Peter | Week 7 |
| 5 | **Review ADR 17.** Three choices about the real data, each reversible in one function: the card key, no device, the reference date. The one most worth a second opinion is having no device at all | Peter | Before week 5 trains on the real data |
| 6 | **Kinesis cannot meet the 50 ms budget as PLAN.md 2.4 measures it.** AWS documents about 200 ms average propagation for a polling consumer and about 70 ms with enhanced fan-out, before the scorer starts. Three options in ADR 3's open question: start the clock at the scorer, run Redpanda on the live instance instead, or keep Kinesis and publish what it measures. The first changes the one-liner's wording, the second the AWS story, the third the headline number | Peter | Before week 7's Kinesis client is written; nothing earlier depends on it |

Nothing else is blocked. Weeks 4, 5 and 6, including the broker-backed latency
measurement, can be built on this machine.

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
   **Still waiting on a quiet machine:** the end-to-end table in
   `docs/latency-budget.md`, which needs
   `verdict loadtest --stream memory --out docs/latency-week4-memory.json` and
   the same with `--stream redpanda`, on an idle host; and the transport
   comparison, which is `verdict http-loadtest --rate 1000 --connections N`
   for N of 1, 2, 4 and 8 beside the stream run at the same rate. The runs
   taken on 2026-09-17 were discarded rather than published: project 12 was
   training a model, and the in-process backend, which touches no network at
   all, gave a p99 between 20 ms and 288 ms across five runs of one
   configuration. A p50 survives that; a p99 does not.

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
