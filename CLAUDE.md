# Working notes for Claude Code

This repository is the Real-Time Fraud and Risk Decisioning Platform, package `verdict`: a
streaming feature store with point-in-time correctness, a stream-consumer scorer under a
published latency budget, shadow deployment, drift-triggered retraining behind an approval
gate, and an expected-loss review queue. The plan is in [PLAN.md](PLAN.md).

The slot was a Feb to Apr 2027 build with a live window from Apr 5 2027. **The build
started on 2026-09-12, about twenty weeks early, and on 2026-09-18 the calendar was
dropped**: go-live is as soon as the plan's full definition of done is met, the live
window is sixty days, and the live stream is Redpanda on an EC2 instance in the AWS
account project 04 uses (shared account only, every resource tagged `project=verdict`).
Weeks 1 to 3 are done.

## Read first

- **[docs/STATE.md](docs/STATE.md): start here.** Where the build has got to, what was
  decided and why, what is waiting on Peter, and the handful of facts about this machine
  that will otherwise cost an hour. It is written for a session starting cold.
- [README.md](README.md): what this is and the current result tables.
- [PLAN.md](PLAN.md): the design. Do not deviate from it silently; if something in it turns
  out wrong, change the plan in the same commit as the code and say why in the commit
  message, and write or amend the architecture decision record.
- `docs/adr/`: every architecture decision, with its public sources.

Keep `docs/STATE.md` current as the build moves. It is the handover, so a week that ends
without updating it is a week whose context lives only in one session's memory.

## Engineering standard

- Python 3.13. Typed throughout; `mypy --strict` and `ruff` clean in CI.
- Tests that fail meaningfully: the leakage test, the parity tests, idempotency under
  duplicates, out-of-order handling, the rollback flag, the promotion refusal.
- `pyproject.toml` with pinned major versions and a comment saying why for each pin.
- Docs ship in the same commit as the change. A decision without an ADR is not decided.
- Never commit credentials, cloud keys, or anything from `.env`. Terraform state is not
  committed.

## Rules specific to this repository

- **Public data and public problem statements only.** Card-transaction fraud as posed by a
  public competition plus the repository's own synthetic generator. No government,
  benefits or claims scenario, no internal feature names, thresholds or architecture from
  anywhere. If a design starts to resemble an employer system, stop and raise it.
- **Features are computed once.** No second implementation of any feature for training.
  A feature that cannot be produced by the dataflow does not exist.
- **The leakage test is never weakened to pass.** A failing leakage test means the
  feature is wrong.
- **The regime schedule is sealed.** `events/generator/regimes.py` is hashed before go-live
  and not edited until the secret is revealed, the day after the sixty-day live window.
- **Nothing promotes itself.** A model reaches production only by a merged pull request
  that carries the shadow evidence.
- **Teardown is part of done.** `down.sh` must leave nothing billable; the test asserts it
  against the cloud API.
- **Run Python through the project venv**: `.venv/Scripts/python.exe -m ...`. Bare `python`
  is the system interpreter and does not have the dependencies.
- **The competition data may not be redistributed.** `data/` is ignored in full, including
  anything derived from it row by row, and the real-data track never runs on the live AWS
  stack. `docs/data.md` carries the clauses; `tests/test_data_terms.py` enforces them.
- **The model is timeboxed.** Model work is week 5 of the build. Improvements after that
  need an ADR saying what platform property they serve.
- **Every reported number carries a confidence interval** and says which track (real data
  offline, synthetic live) it came from.
- **Plain punctuation** in everything written here: no em-dashes or other typographic
  dashes, straight quotes only.

## What goes in the README

The README opens with the one-liner, the results tables and the honest limitation, before
any installation instructions. Load tests and the live-window report fill the tables; do
not hand-edit them. Record one approach that was tried and rejected, with the evidence,
once the work has produced it.
