# 11. Shadow first, promotion by non-inferiority, rollback by flag

- Status: accepted, 2026-09-15. Written ahead of week 5 because none of it
  depends on which models exist; the margins are placeholders until the
  champion's own variability is measured, and are revisited then.
- Date: 2026-09-15
- Deciders: the build session, within `PLAN.md` section 2.5 as written
- **Addendum, 2026-09-19 (ADR 18):** live history is a weighted sample, so
  every shadow row carries a weight and the gate uses it. See the addendum
  before Sources.

## Context

`PLAN.md` section 2.5: the challenger scores every event alongside the
champion and only champion decisions act. Promotion needs, on the labelled
shadow window, non-inferior PR-AUC and expected loss with the interval shown,
latency within budget, and a human approval. Rollback is a flag read on every
event. The rule in `CLAUDE.md` is sharper still: nothing promotes itself; a
model reaches production only by a merged pull request carrying the shadow
evidence.

That leaves the mechanics open: what "non-inferior" is computed on, which
labels count, what stops a challenger gaming the measure, and how a flag read
on every event avoids becoming a latency cost or a way to break scoring.

## Decision

### Shadow (`verdict/scoring/core.py`)

- The challenger is given **the features the champion was served**, the same
  dictionary, so every comparison is row for row on identical inputs.
- Its would-be decision goes to its own topic, `shadow`, as a `ShadowEvent`
  naming the champion's version and action beside its own. It never reaches
  `decisions`.
- It is **timed apart**: after the champion's decision exists, so the
  champion's hops never include it, and its own time is reported for the
  latency condition below.
- **It cannot break scoring.** An exception in the challenger is counted
  (`shadow_failures`) and the champion's decision is written regardless.

### The gate (`verdict/models/promote.py`)

- **Rows**: transactions the champion decided and the challenger scored,
  joined to labels. **Only labels that had arrived by the moment of
  evaluation count.** A label arrives seven days after its transaction, and a
  gate that read later labels would judge promotion on outcomes nobody had.
  This is the leakage test's rule applied to the decision about models.
- **PR-AUC** as average precision over distinct thresholds, so the listing
  order of tied scores cannot move it.
- **Decision cost** as the expected-loss comparison: fraud approved at its
  amount, one review cost per review, and a share of each declined legitimate
  transaction's amount as lost business, all under the same rules. The last
  term is not optional. Without it, a challenger that declines everything
  costs nothing, and a test builds exactly that challenger and asserts it is
  refused.
- **Intervals**: a paired bootstrap, both models on the same resampled rows,
  95 percent percentile intervals on the difference, seeded.
- **Non-inferiority on the bound, not the estimate.** Eligible only if the
  lower bound of the PR-AUC difference is above minus its margin, and the
  upper bound of the cost difference is below its margin as a share of the
  champion's cost. A test holds a challenger whose point estimate is inside
  the margin and whose interval is not, and asserts refusal.
- **Enough evidence first.** Below 50 labelled frauds no interval is computed
  and the gate refuses. At 50, the standard error of a recall of 0.5 is about
  0.07, which is already as wide as a promotion decision can use.
- **Latency**: the challenger's shadow p99 against the model hop's budget.
- **The output is a verdict and a Markdown table**, for the pull request. The
  function never writes the champion pointer. "Eligible" is not "promoted".

### Rollback (`verdict/scoring/flags.py`)

- The champion is a pointer file, `data/flags/champion.json`, naming the
  champion and the previous one. The scorer's model source checks it **on
  every event**, by one `stat` call, and parses it only when the file's
  identity, modification time or size changes.
- Writes are atomic (temporary file, then `os.replace`). A scorer reading
  mid-write sees the old pointer or the new one.
- A pointer that does not parse, or names a model the scorer lacks, is
  **refused and counted, and scoring continues** on the last good champion.
- `verdict flag set` refuses an unknown model before writing. `verdict flag
  rollback` swaps champion and previous.

## Consequences

- The drill in week 5 times flag write to first decision by the previous
  champion, five times. The test that a flip takes effect on the next event
  already runs against the real decider.
- The margins (0.01 PR-AUC, 2 percent of cost) and the placeholder prices
  (500 cents a review, 10 percent of a falsely declined amount) are stated in
  `Margins` and printed in every evidence table. They are revisited when the
  champion exists and its bootstrap spread on a real window is known, and
  week 6's queue evaluation replaces the prices with stated assumptions.
- The bootstrap costs a sort of every row for every resample, twice. A
  seven-day shadow window at the live rate is six hundred million rows, which
  is too many; the gate will run on a uniform sample of the window, stated in
  the table, when that window exists.
- The evidence table names its seed, so a reviewer can regenerate it.

## Options not taken

- **Promote on a better point estimate.** Rewards noise, and the refusal test
  exists to stop it.
- **Superiority rather than non-inferiority.** A challenger that is as good
  and cheaper, or as good and simpler, should be promotable; superiority would
  block it.
- **Judge on offline holdout data instead of the shadow window.** The shadow
  window is the only data scored by the live path, with its features served as
  they were; a holdout would re-open the training-serving gap this platform
  exists to close.
- **An environment variable or a restart for rollback.** Neither is read per
  event, and the plan measures rollback in seconds.

## Addendum, 2026-09-19: rows carry weights

The live shadow window is read from history, which keeps every reviewed or
declined row and a tenth of approved frauds and a hundredth of approved
legitimate rows (ADR 18). Read unweighted, that sample's precision is several
times too high. So `ShadowRow` has a `weight`, PR-AUC counts weighted rows,
decision cost sums weighted costs, and the bootstrap resamples within each
weight, which keeps each resample to the sample's design. With every weight
1, as on the offline track, the computation and its seeded draws are
unchanged, and so is every existing test. The minimum of 50 frauds counts
rows, not weights: a weight adds no evidence. `tests/test_history.py` holds
the weighted PR-AUC and decision cost of a sample to the full data's.

## Sources

- Efron and Tibshirani, *An Introduction to the Bootstrap*, Chapman and Hall,
  1993: the percentile interval and paired resampling.
- Saito and Rehmsmeier, "The Precision-Recall Plot Is More Informative than
  the ROC Plot When Evaluating Binary Classifiers on Imbalanced Datasets",
  PLOS ONE, 2015. https://doi.org/10.1371/journal.pone.0118432
- Walker and Nowacki, "Understanding Equivalence and Noninferiority Testing",
  Journal of General Internal Medicine, 2011: deciding on the interval bound
  against a margin. https://doi.org/10.1007/s11606-010-1513-8
- Sato et al., "Continuous Delivery for Machine Learning", martinfowler.com,
  2019: shadow deployment as a promotion step.
  https://martinfowler.com/articles/cd4ml.html
