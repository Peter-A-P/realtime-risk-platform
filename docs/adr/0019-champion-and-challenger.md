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

## Results, synthetic track, offline replay (2026-09-19), and what they show

`docs/champion-synthetic.json`. 17,811,137 transactions served over ten days;
1,400,713 kept (534,635 frauds, every one; legitimate rows at 5 percent,
weight 20). Trained on 975,791 rows, tested on 424,922 (165,473 frauds).

| | Test PR-AUC (95% CI) | Model hop, single row, p50 / p99 |
|---|---|---|
| Champion, XGBoost, 886 trees | 0.9996 (0.9995 to 0.9996) | 0.074 / 0.146 ms |

**The synthetic fraud is far too easy**, and that is a finding about the
generator, not a result about the model. `session_txn_count` alone ranks the
test set at a PR-AUC of 0.64: the attacks run in multi-transaction sessions
that legitimate traffic almost never has. `PLAN.md` section 8 anticipated
this ("Synthetic fraud is too easy or too hard: scenario difficulty is tuned
... to a champion PR-AUC in the range the real data shows") and the tuning
was never done, because until today there was no model to tune against. A
live window on a stream this easy would show a champion that cannot be
improved on and a drift response with nothing to recover, which is not the
evidence the window exists to produce. Tuning the scenarios
(`events/generator/scenarios.py`, not the sealed `regimes.py`) before the
schedule is sealed is Peter's decision, recorded in `docs/STATE.md`; the
challenger is not trained on this track until it is made, because its
result would be discarded with the stream.

The model ships anyway, as `verdict/models/artifacts/champion.onnx`, so the
live stack's path from pointer to ONNX to decision is exercised in the dry
run; it is replaced when the generator is.

## Consequences

- The champion does not have to be good to be the champion; it has to be
  the one the evidence supports. The challenger loses on the real track by
  an interval that excludes zero, and would be refused by the promotion gate.
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
