# Go-live checklist

The live window starts when every line of `PLAN.md` section 10 that can be
met before it is met, and runs sixty days (`CLAUDE.md`). This is the list,
in order, with who does each step and how it is checked. `deploy/go-live.sh`
is step 4 as one command.

## 1. Before: what must already be true

| Item | Who | Checked by |
|---|---|---|
| The schedule is sealed: `docs/sealed-schedule.json` committed, its source hash matching `regimes.py` | Peter sealed, build committed (85d3d07) | `verdict schedule verify` works only with the secret; the feed checks the secret against the commitment at start and refuses a mismatch |
| The secret is in SSM at `/verdict/schedule-secret`, and in Peter's password manager | Peter | `go-live.sh` checks the parameter exists; the build identity cannot read it |
| The `project` cost allocation tag is active | Peter, done 2026-09-22 | `go-live.sh` checks it |
| Alerts reach Peter | Peter confirmed the subscription, 2026-09-27 | An alert email has arrived |
| The live stack runs the image to go live on: saved feature state (ADR 27), the shadow challenger (ADR 11), the models job (ADR 28) | build | the dashboard, and `docker ps` on the instance |
| The drift reference is sound: the models job judges the dry run's first clean baseline day (development schedule, 2026-09-29, judged about 03:00Z on 2026-09-30) and flags no quantity. A reference that disagreed with the live stream would show on every day and in most quantities, so one day decides it | build | `/data/models/drift/days/` on the instance; a flag here would mean the reference and the live stream disagree, and go-live waits |
| A restore from saved state has happened on a real reclaim | build, done 2026-09-27 (511,519 entities, 96 s) | the scorer's log |
| The load test on the live instance, with the champion, the shadow and history on | build | `docs/loadtest-live.json`, and the README's table |
| The GitHub token for the models job is in SSM at `/verdict/github-token` (fine-grained, this repository only, contents and pull requests read and write, expiring after the window) | Peter | the models job's log says pull requests will be opened |

## 2. The repository, the moment before

- `docs/STATE.md` current; the README's tables filled only from committed
  measurements; `ruff`, `mypy --strict` and the tests clean in CI.
- The image pushed: `deploy/push-image.sh` prints its tag.

## 3. Peter's go

Nothing below runs without it.

## 4. The switch: `deploy/go-live.sh --image TAG`

It refuses unless step 1's first three rows hold, sets the window's start a
quarter of an hour ahead, applies the sealed launch template (Terraform shows
the plan and asks), stops the stack, clears everything the dry run left that
could reach the window (the scorer's saved state, the feeds' places, history,
the models job's state, the topics and their consumer groups), replaces the
instance, and waits for the new one's feed to start on the sealed schedule.
Kept: Prometheus's data, the champion pointer, the alerts' state, the spot
notices.

## 5. After, the same hour

| Item | Who |
|---|---|
| Decisions flowing at the live rate; the feed's log names the sealed schedule | build |
| `docs/STATE.md`: the window's start, its end sixty days later, the reveal the day after | build |
| The repository made public, `v1.0.0` tagged | build, on Peter's word |
| The Kaggle forum post (competition rule 8.B) | Peter |

## 6. During the window

The models job judges each day and opens pull requests; Peter merges or
closes them, and the build rolls an image after a merged candidate and moves
the pointer after a merged eligible verdict. `verdict observe report` can be
run at any time for the numbers so far.

## 7. After the window

`verdict observe report` over the sixty days, committed and tagged `v1.1.0`;
the cost per million events from the bill; the secret revealed and
`verdict schedule verify` run in public; `deploy/down.sh` and its output
committed (ADR 16); the GitHub token deleted and revoked.
