# 28. Drift, retraining and the promotion gate run on the live stack, and end at pull requests

- Status: accepted, 2026-09-27
- Date: 2026-09-27
- Deciders: Peter Parker (the job opens pull requests itself, with a token
  scoped to this repository, rather than emailing for a person to run the
  retrain; 2026-09-27); the build session (the job's shape, its guards, and
  its evidence)

## Context

The definition of done asks that the drift monitors fire retraining behind a
pull-request approval gate at least once, **on a shift from the sealed
schedule**, and that promotion is shown by non-inferiority on a shadow
window. Everything that does this was built and tested on replays (ADR 11,
12, 23, 24), and none of it ran on the live stack: the scorer ran no shadow
model, nothing judged a live day, and nothing fitted or opened anything.
Checked against the plan on 2026-09-27, that was a go-live gap.

Two ways to close it were put to Peter: run everything on the instance and
let it open the pull requests with a GitHub token scoped to this repository,
or let the instance only detect drift and email, with the retrain run by
hand on the build machine. He chose the first: it keeps the automated path
exactly as ADR 24 states it, ending at a pull request, and a retrain does not
wait for anyone to be at a desk.

## Decision

**One job, `verdict models-job`, runs beside the scorer and does three things
an hour** (`verdict/live/models_job.py`):

1. **Judges each finished day of the live window** from the scorer's staged
   decisions: the same 3 percent hash draw, monitors, thresholds and
   trigger the replay used, against a fixed reference, the champion's own
   training window rebuilt on the instance from the code in its image
   (`verdict/drift/live.py`). A day is judged three hours after it ends,
   once none of its hours is still being written; the state (a report per
   day, the open request) is on the data volume.
2. **Fits a candidate** while a request is open, once three days of the
   window are finalised, and again each time three more are (ADR 24: the
   first candidate is usually blind to the drift, and the one that helps is
   fitted after the drifted days' labels arrive). It fits on the latest
   fourteen finalised days of history with the champion's parameters on one
   thread, compares it with the champion on later rows neither saw, and
   opens a pull request that adds the model file under its own name and
   makes it the scorer's shadow model.
3. **Runs the promotion gate** on the shadow model once it has a week of
   finalised shadow scores, and opens a pull request with the verdict: the
   first verdict on each shadow model, and again if it later turns eligible.

**What it never does:** merge, deploy, write the champion pointer, or replace
a model file. Merging a candidate's pull request is the approval to run it in
shadow, and a person rolls the image; merging an eligible verdict is the
approval to promote, and a person runs `verdict flag set`. Model files are
only ever added, so no file the pointer might name is replaced and a
rollback can always find its model.

**Four guards, each for a way the obvious version would be wrong:**

- **A day the scorer spent on thin features is not judged.** A cold start
  serves card counts too low for up to 25 hours, which reads as drift and is
  the platform's own doing. The scorer now writes every start, and whether
  it restored its state (ADR 27), to `/data/engine/starts.jsonl`; a day
  within 25 hours of a cold start is reported as too thin to judge, which the
  trigger already treats as breaking a run and not as recovery.
- **The shadow model is read from history, not configured.** The job takes
  the version the scorer ran on the latest finalised day, so no setting of
  the job's can disagree with the scorer's.
- **Tables are bounded by another weighted sample.** A live day keeps
  millions of rows and the instance holds a 7.5 GB scorer. Past 400,000
  frauds and 2,000,000 legitimate rows a table keeps a hash-drawn share of
  each label with its weight scaled to match, which is the case-control
  sampling history already uses and is unbiased for everything here, all of
  which reads the weight.
- **A request that closes cannot reopen on the same run of days.** After a
  close only days after it count towards the next request.

**The token** is a fine-grained GitHub token scoped to this repository, with
contents and pull requests read and write, kept in SSM at
`/verdict/github-token` and read by the boot script into a root-only
environment file. Without it the job writes each pull request's body and
files under `/data/models/work/` and opens nothing, so the stack runs either
way.

**The shadow scores only what history could keep** (ADR 11's addendum of the
same day), about a seventh of decisions, which is all the gate reads.

**Three alerts** (ADR 26): `DriftRequestOpened` in the hour a request opens,
`ModelPullRequestOpened` when the job opens one, `ModelsJobFailing` on three
failed passes in four hours; `docs/failure-modes.md` has a section for each.

## Evidence

- `tests/test_drift_live.py`: a day is judged only once it is over, settled
  and sealed, and only from the window's first whole day; two drifted days
  open a request that survives a restart without reopening; a request closes
  when the drift ends or a candidate beats the incumbent, and the same run
  does not reopen it; a day within 25 hours of a cold start is never read as
  drift, and a restored start leaves it to be judged; a day's values are the
  same hash draw the replay used; the reference is the stream before the
  cutoff and nothing after.
- `tests/test_models_live.py`: a thinned table stands for the same weighted
  totals; the gate reads only rows the shadow model scored; a candidate is
  fitted only when there is something new to fit on; a verdict is told once
  and again only when it turns eligible; a pass with an open request fits on
  one thread and opens a pull request carrying the model file under its own
  name, its report, and the one changed line of the compose file, touches no
  other model file and no pointer, and fits nothing on the next pass; the
  gate's pull request carries no code. A fake GitHub records every call.
- Run for real on a small history with a planted signal: a candidate fitted
  on one thread, exported to ONNX, compared with the shipped champion and
  its pull request written, in 6.3 s.
- The image with XGBoost's CPU-only build is 1.38 GB against 1.03 GB.

## Consequences

- **The live drift result is the platform's own, on a schedule nobody has
  read**: the first request on the sealed schedule is the result the plan
  asks for, and the pull requests are its record.
- **Deploying a merged candidate is by hand**, an image roll; nothing deploys
  itself any more than it promotes itself.
- **The reference is rebuilt when the champion changes**, since the score is
  one of the quantities; a promotion restarts the drift baseline, which the
  job does on its next pass.
- **The first pass on a new instance builds the reference** from the
  synthetic stream, about an hour at the lowest priority; it is kept on the
  volume after that.
- The image is 350 MB larger, which lengthens each spot replacement's pull.

## Sources

- GitHub REST API, git database (blobs, trees, commits, references) and pull
  requests: https://docs.github.com/en/rest/git and
  https://docs.github.com/en/rest/pulls/pulls
- GitHub, fine-grained personal access tokens:
  https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
- XGBoost, the `xgboost-cpu` package: https://pypi.org/project/xgboost-cpu/
- ADR 11, 12, 18, 23, 24, 26, 27.
