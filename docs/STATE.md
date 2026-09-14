# Where this project is, and what to know before touching it

**Written 2026-09-13, updated the same day after the move to a new machine.** Read this first, then `PLAN.md`. It exists so that a
session starting cold knows everything a session that had been here all along
would know: what is built, what was decided and why, what is waiting on a
person, and the handful of things that will waste an hour if nobody mentions
them.

Everything here is a fact about the project as it stands. Where something is a
judgement or an open question, it says so.

---

## 1. Status in one paragraph

Weeks 1 to 3 of a nine-week build are done, plus the real-data track's terms
and ingest. The event stream, the synthetic generator with a sealed regime
schedule, the point-in-time leakage test, sixteen features computed by an
engine written here, and one write path into both stores, with parity at 100
percent. The competition data is downloaded, its terms are recorded, and its
checksums are committed. 223 tests; `ruff`, `ruff format` and `mypy --strict`
all clean. Nothing has been scored yet, so the README's headline tables are
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
| The repository is under **OneDrive** | The competition data is kept outside it, at `C:\Dev\POCs\09-realtime-risk-platform\data\ieee-fraud-detection`. Pass `--directory` to `verdict data` commands (section 9) |
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
  store/
    features.py      FEATURE_SET: the sixteen features, as specifications not code.
                     Also the brute-force reference evaluation.
    leakage.py       The point-in-time test. Two checks. Never weakened.
    repo.py          Generates the Feast repository from the specifications.
    retrieval.py     Point-in-time training sets. Refuses to carry the label time.
  cli.py             verdict generate | schedule show/hash/seal/verify |
                     data ingest/manifest/verify/inspect
```

Not yet written: `stream/` (the `Stream` protocol, Redpanda and Kinesis),
`scoring/`, `models/`, `drift/`, `queue/`, `observe/`, `chaos/`,
`deploy/terraform/`.

`deploy/compose/docker-compose.yml` **ran for the first time on 2026-09-13**:
Redpanda healthy, `transactions` (4 partitions), `labels` (1) and `decisions`
(4) created, the console answering on `localhost:8080`, the Kafka listener on
`localhost:19092`. Its first run failed: `--set=redpanda.auto_create_topics_enabled=false`
is not a `redpanda start` flag in v24.3, and the broker exited on it. Auto-creation
is now set in the cluster bootstrap file, and the topics job fails unless it
reads back `false`. Producing to a missing topic was checked by hand to fail
with `UNKNOWN_TOPIC_OR_PARTITION`.

---

## 4. The decisions that are already made

Seven ADRs, in `docs/adr/`. Read them before reopening anything they cover.

| ADR | Decision | Note |
|---|---|---|
| 1 | Platform, not model | Model work is timeboxed to week 5 |
| 2 | Two tracks: real data offline, synthetic live | Every number says which track |
| 3 | Redpanda locally, Kinesis live, one `Stream` interface | Interface not yet written |
| 4 | **Amended.** The aggregation engine is written here | Bytewax has no Python 3.13 wheels, on any platform, up to 0.21.1. Peter chose this over pinning Python to 3.12, adding a Rust toolchain, or a Kafka-only engine |
| 5 | Feast: registry, point-in-time join, online read | Push sources, not materialisation. **Its first online read after start-up costs about 42 ms against a 5 ms budget hop: the scorer must warm the store** |
| 6 | Features computed once; the reference evaluation is not a second computation | One definition, two executions, and the test holds them together |
| 7 | The leakage test is written first and never weakened | It has already caught three real faults |

### Plan amendments made in the same commits as the code

- **PLAN.md 2.1**, the sealed schedule (week 1). The plan said the schedule
  was "hashed and committed before go-live and revealed on Jul 1", which
  cannot both be true of a schedule sitting in the repository in the clear.
- **PLAN.md 2.2 and ADR 4**, the engine (week 3).

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

---

## 6. What is waiting on a person

| # | Item | Who | When it bites |
|---|---|---|---|
| 1 | **The go-live date.** It was Apr 5 2027. Starting twenty weeks early unfixes it, and the AWS account timing in the plan's action 9 was arranged around it (a free-plan account closes itself six months after opening) | Peter | Before week 7, the first week that needs an AWS account. Nothing before then costs anything |
| 2 | ~~Docker~~ **Done 2026-09-13**, on the personal desktop. The Redpanda stack runs (section 3) | | |
| 3 | **Kaggle forum posting.** Rule 8.B asks that publicly shared competition code be posted to the competition's own forum. Arguably spent since 2019, cheap to honour, and it publishes under Peter's name | Peter | Go-live |
| 4 | AWS budget alarms before the first resource exists | Peter | Week 7 |

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
| Device (identity) coverage | 24.4% of rows |
| Merchant identifier | **None** |

**The absence of a merchant is a design constraint, not a gap to fill.** Three
of the sixteen features are merchant-keyed and cannot be computed on this
track. The agreed response is to report per track which features exist.
Promoting `ProductCD`, a five-valued product category, into a merchant would
make those features a measurement of a party invented here.

Open design questions for the row-to-event mapper, which is the next piece of
real-data work:

- **What is a card?** `card1` is the obvious proxy; `card1`+`addr1` is
  tighter. Neither is a card. Whatever is chosen goes in an ADR.
- **What is a device?** Only on the 24.4 percent of rows with identity data,
  from `DeviceInfo` and `id_3x`. Absent is a value the platform already has a
  sentinel for (`NO_EVENTS`, a negative number, never zero or null).
- **What is the event time?** The offset needs a reference datetime. Any fixed
  one works as long as it is published and never changed.

---

## 8. Measurements taken so far

All on the build laptop, all with intervals where they are rates. Nothing
here is a platform latency or throughput figure: nothing is scored yet.

| Measurement | Result | Source |
|---|---|---|
| Generator, written to the raw log | 12,219 events/s (8,790 to 15,648) | `docs/week1-measurement.json` |
| Generator, nothing written | 19,577 events/s (8,146 to 31,008) | same |
| Fraud share produced | 3.06% (2.85 to 3.26) against a 3% target | same |
| Feast online read, steady state | p50 0.76 ms, p99 1.76 ms | ADR 5 |
| Feast online read, first call | about 42 ms | ADR 5 |
| Online/offline parity | 100% on a replay | `tests/test_parity.py` |
| Leak impact on the real data | 312 rows of 590,540 (0.053%) | `docs/leak-caught.md` |

**One of these was published wrong and then corrected.** The leak's impact was
first measured on a synthetic stream truncated to seconds and reported as if
it described the competition data, overstating it by three orders of
magnitude. `docs/leak-caught.md` carries the correction in place rather than
edited over. If a number here ever looks too good, that document is the
precedent for what to do about it.

---

## 9. How to run things

```bash
# everything, from the repository root
.venv/Scripts/python.exe -m pytest                 # 223 tests, 2 to 3.5 minutes
.venv/Scripts/python.exe -m pytest -m "not slow"   # about 90 seconds
.venv/Scripts/python.exe -m ruff format .
.venv/Scripts/python.exe -m ruff check .
.venv/Scripts/python.exe -m mypy verdict tests

# the generator
.venv/Scripts/python.exe -m verdict.cli generate --out data/raw/dev --events 100000

# the sealed schedule
.venv/Scripts/python.exe -m verdict.cli schedule hash

# the competition data, which on this machine lives outside the repository
.venv/Scripts/python.exe -m verdict.cli data verify --directory "C:/Dev/POCs/09-realtime-risk-platform/data/ieee-fraud-detection"
.venv/Scripts/python.exe -m verdict.cli data inspect --directory "C:/Dev/POCs/09-realtime-risk-platform/data/ieee-fraud-detection"

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

1. **The IEEE-CIS row-to-event mapper**, finishing week 2's real-data half.
   Needs the three decisions in section 7, each of which wants an ADR.
2. **Week 4:** the `Stream` protocol and an in-process implementation, the
   stream-consumer scorer with per-hop timers, decision rules, the HTTP
   endpoint for the comparison in Rule C candidate 3, and
   `docs/latency-budget.md`. ADRs 8 and 9. The published p99 waits on a real
   broker; everything else does not.
3. **Week 5:** champion on IEEE-CIS and on replay, FT-Transformer challenger,
   shadow scoring, promotion function, rollback drill. ADRs 10 and 11. This is
   also where the leak's offline PR-AUC inflation gets measured, by training
   twice; the unfixed engine is kept in `tests/test_engine.py` for that.
4. **Week 6:** drift monitors, the retraining pull request, the expected-loss
   queue. ADRs 12 and 13.

The week-by-week table in `PLAN.md` section 5 is the authority, and it now
records what was actually done for weeks 1 to 3.
