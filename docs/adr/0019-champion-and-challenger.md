# 19. The champion and the challenger: what they are trained on, and how they are judged

- Status: accepted, 2026-09-19
- Date: 2026-09-19
- Deciders: the build session, within `PLAN.md` section 5 (week 5) and ADR 1
  (the model is timeboxed)

## Context

Week 5 needs a gradient-boosting champion, an honest neural challenger, both
exported to ONNX and run by the scorer, and their comparison with intervals,
on both tracks (ADR 2). Three rules shape every choice below, and each is
older than the models:

- **Features are computed once** (ADR 6). A training row holds what the
  engine served, never a recomputation.
- **Training at a cutoff uses only labels that had arrived** (ADR 10, rule
  4), enforced by `models/dataset.at_cutoff`.
- **The model is timeboxed** (ADR 1). One well-set model and one honest
  challenger, not a search.

## Decision

**Data.** A track is replayed through the platform's own serving path
(`EngineFeatures`, the scorer's) into a table (`models/dataset.replay_to_parquet`):
every event is served, so every window is exactly what the scorer would hold,
and only the writing is sampled.

- *Real, offline*: the 590,540 competition transactions mapped to events
  (ADR 17), every row kept. Six of the sixteen features exist on this track.
- *Synthetic*: ten days of the generator on the development schedule's
  opening regime, **at a population and a rate both divided by fifty** (4,000
  cards, 3,000 devices, 80 merchants, 20 transactions a second). Every card,
  device and merchant then sees transactions at the live stack's per-entity
  rate, so the features' distributions are the live ones, while the engine's
  memory stays inside this machine's (ADR 15's finding: about 900 bytes per
  held event, which a day at the full live rate would need tens of gigabytes
  for). Every fraud is kept and 5 percent of legitimate rows, each weighted
  by twenty, by the same salted hash draw as ADR 18.

**Split.** In time, never at random (`models/train.split_by_time`), at 70
percent of the period; the test is every transaction from there on,
evaluated once all its labels are in. Early stopping watches the latest 15
percent of the training rows, never the test. On the real track's 182 days
the model is as of the split point, so the week before it, whose labels had
not arrived, is left out and counted. On the synthetic track's ten days a
week's wait would leave three days to train on, so the model is trained a
label delay after the split point, once every training transaction's label
has arrived, on the transactions before the split only: it still never sees
the test period, nor a label that had not arrived when it was trained.

**Champion.** XGBoost, histogram trees, depth 6, learning rate 0.05,
subsample and column sample 0.8, weights used, up to 2,000 trees with early
stopping at 50 on validation PR-AUC. Set before any test number existed and
not searched. Exported to ONNX (opset 15, onnxmltools) and **refused unless
the ONNX scores match the booster's within 1e-4** on the first 5,000 test
rows (`models/train.export_onnx`).

**Challenger.** An FT-Transformer (Gorishniy et al., 2021): a linear token per
input, a learned [CLS] token, two pre-norm Transformer blocks of width 32 with
four heads, a head on [CLS]. Its only preprocessing is inside the graph (a
learned "no history" embedding, `log1p`, standardisation by the training
rows' weighted moments), so the scorer passes it the same vector. Weighted
binary cross-entropy, AdamW at 1e-3, batches of 2,048, early stopping on
weighted validation PR-AUC with patience 2, seeded. Exported to ONNX (opset
17) with the same parity check.

**Judging.** Test PR-AUC as weighted average precision, with a 95 percent
bootstrap interval that resamples within each weight
(`models/evaluate.pr_auc`); the champion against the challenger as a paired
difference on the same rows. Every number names its track.

**Shipping.** Only models trained on the synthetic track ship in the image
(`verdict/models/artifacts/`, `scoring/registry.py`): the competition's terms
keep anything trained on its rows off the live stack (ADR 2, `docs/data.md`).
The scorer's pointer starts at the shipped champion only when there is no
pointer yet, and the challenger scores in shadow (ADR 11).

## Results, real track, offline (2026-09-19)

`docs/champion-real.json`, `docs/challenger-real.json`,
`docs/leak-inflation.json`.

| | Test PR-AUC (95% CI) | Model hop, single row, p50 / p99 |
|---|---|---|
| Base rate (a model that knows nothing) | 0.035 | |
| Champion, XGBoost, 374 trees | 0.0750 (0.0715 to 0.0789) | 0.041 / 0.101 ms |
| Challenger, FT-Transformer, epoch 6 of 8 | 0.0492 (0.0473 to 0.0514) | 0.220 / 0.485 ms |
| Challenger minus champion, paired | -0.0258 (-0.0288 to -0.0230) | |

Trained as of 2018-04-08 on 415,252 transactions (14,641 frauds; 22,873
left out because their labels had not arrived), tested on 152,415 (5,299
frauds).

These numbers are low, and that is the platform's honest position rather
than a modelling failure to be tuned away: on this track only card velocity
and amount features exist, and a card here has a median of one transaction
(ADR 17). The competition's leaderboard models use hundreds of columns the
platform cannot compute in a stream; this project scores only what its
engine computes. Both model hops are far inside the 3 ms budget (ADR 9).

## Results, synthetic track, offline replay (2026-09-19)

`docs/champion-synthetic.json`, `docs/challenger-synthetic.json`. The first
replay of this track produced a champion at **0.9996**, which was a finding
about the generator rather than a result about the model: `session_txn_count`
alone ranked the test set at 0.64, because every attack ran in one long
session and legitimate traffic almost never had one. The scenarios were made
much harder before the schedule is sealed (ADR 21). The numbers below are
from the generator as it now stands, and they replace the earlier ones.

17,809,525 transactions served over ten days; 1,400,900 kept (535,130 frauds,
every one; legitimate rows at 5 percent, weight 20). Trained as of
2027-01-14 on 980,216 rows, tested on 420,684 (161,237 frauds).

| | Test PR-AUC (95% CI) | Model hop, single row, p50 / p99 |
|---|---|---|
| Base rate (a model that knows nothing) | 0.030 | |
| Champion, XGBoost, 1,496 trees | 0.8427 (0.8393 to 0.8464) | 0.134 / 0.215 ms |
| Challenger, FT-Transformer, epoch 9 of 11 | 0.8012 (0.7978 to 0.8050) | 0.215 / 0.366 ms |
| Challenger minus champion, paired | -0.0415 (-0.0437 to -0.0392) | |

The challenger loses here as it does on the real track, by an interval that
excludes zero, so the promotion gate refuses it on both. That is a better
result for the platform than a challenger that wins: the gate has something
real to refuse, and the shadow path is still exercised. It costs about a
tenth of the champion's scoring time and gives up 0.04 PR-AUC to do it.

Both models ship, as `verdict/models/artifacts/champion.onnx` and
`challenger.onnx`, so the live stack's path from pointer to ONNX to decision
is exercised in the dry run with the pair it will actually carry.

### A measurement that was wrong, and what it nearly cost

The champion was first timed at **3.047 ms** p99, over the 3 ms this hop is
budgeted (ADR 9). The response was to cap the model at 700 trees, which
brought the hop inside the budget and dropped the test PR-AUC from 0.8427 to
0.8358. The cap was written into `models/train.py` and this record was
about to carry it as a decision.

It was wrong. The measurement was taken while another project on the same
machine held the CPU at 72 percent. Re-timed on an idle machine, twice and in
both orders, the same two models are:

| | p50 | p99 |
|---|---|---|
| Capped, 699 trees | 0.064 / 0.065 ms | 0.099 / 0.114 ms |
| Uncapped, 1,496 trees | 0.127 / 0.129 ms | 0.218 / 0.197 ms |

Twice the trees costs twice the time, which is what a boosted ensemble should
do, and the uncapped model has about fourteen times the headroom it needs. The
cap was reverted and `MAX_ROUNDS` is 2,000 again. The same fit took 223.7 s
under load and 98.3 s idle, which is the cheap tell that the machine, not the
model, had changed; so is the real track's champion, whose 374 trees score in
0.101 ms at p99, making 3 ms for four times the trees impossible on its face.
Latency numbers in this repository are taken on an idle machine and the load
is checked before and after (`docs/latency-budget.md`).

### Refitting does not reproduce the model id

A model's version is the SHA-256 of its ONNX bytes, which is what makes a
decision traceable to exactly the file that made it. It is not a hash of the
recipe: XGBoost's parallel histogram build accumulates in thread completion
order, so two fits of the same data with the same seed differ in the last
few bits. The two fits of this champion scored 0.84266008 and 0.84266012,
a difference of 4e-8, under different ids. The published figure reproduces;
the file hash identifies an artefact, not a procedure.

## Consequences

- The champion does not have to be good to be the champion; it has to be
  the one the evidence supports. The challenger loses on both tracks by an
  interval that excludes zero, and is refused by the promotion gate on both.
- The synthetic champion is trained on a scaled population. If the live
  population or rate changes (ADR 15's open question), it is retrained on the
  new ratio; the per-entity rate is what must match, and a test of the
  scaled configuration's rates keeps that visible.
- Training depends on `xgboost`, `onnxmltools`, `onnx` and `torch`, in the
  `train` and `challenger` extras; the image carries only `onnxruntime`.

## Sources

- Chen and Guestrin (2016), "XGBoost: A Scalable Tree Boosting System", KDD.
- Gorishniy, Rubachev, Khrulkov and Babenko (2021), "Revisiting Deep
  Learning Models for Tabular Data", NeurIPS.
- Saito and Rehmsmeier (2015), "The Precision-Recall Plot Is More Informative
  than the ROC Plot When Evaluating Binary Classifiers on Imbalanced
  Datasets", PLOS ONE: why PR-AUC.
- ONNX Runtime, and onnxmltools' XGBoost converter.
  https://onnxruntime.ai/ and https://github.com/onnx/onnxmltools
