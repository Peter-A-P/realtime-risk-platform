# Data, and the terms it comes under

Nothing raw is committed to this repository. Loaders fetch, verify a checksum
and write under `data/`, which is gitignored. Every source below records what
it is, what it is used for, and the terms it comes under.

## Status

| Source | Used for | Terms recorded | Downloaded |
|---|---|---|:--:|
| IEEE-CIS Fraud Detection | Real-data offline track | **Not yet: see below** | No |
| `verdict` generator (own) | Synthetic live track | Written here, published with the repository | n/a |
| Sparkov generator | Reference for categories and amounts | Reference only, no data used | No |
| ULB credit-card fraud | Fallback for the real-data track | ODbL | No |

## IEEE-CIS Fraud Detection

About 590,000 card transactions with 400-odd features and roughly 3.5 percent
fraud, published by Vesta Corporation for a 2019 Kaggle competition. It is the
real-data track: the leakage test, online/offline parity, the
champion/challenger comparison and the review-queue evaluation all run on it.

Competition page: https://www.kaggle.com/competitions/ieee-fraud-detection

**The terms are not yet recorded, and no data has been downloaded.** The
competition's rules page is behind a Kaggle login, so it cannot be read or
quoted from a script or an assistant session. It has to be read by the account
holder and recorded here, before the first download, which is a week 2 task on
the build schedule.

Four questions have to be answered here in writing, because the answers change
what this repository may do:

1. **What purposes is the data licensed for?** Competition use only, or
   academic and non-commercial research more broadly? This project is a public
   portfolio piece, which is not obviously either.
2. **May derived works be published?** Trained model artefacts, aggregate
   statistics, feature distributions and evaluation tables are all derived
   works. The README's result tables are the ones that matter.
3. **What may not be redistributed?** Assume the raw data may not be, which is
   why nothing raw is committed and the loader verifies a checksum rather than
   vendoring a copy.
4. **Is external data allowed to be combined with it?** Relevant because the
   synthetic track runs through the same pipeline, even though the two tracks
   are never merged into one table.

If the answers restrict derived works or publication in a way that bites, the
real-data track moves to the ULB credit-card dataset and the README says so
plainly. That fallback is recorded in ADR 2 and in `PLAN.md` section 8, and it
is the reason the pipeline was never built to assume one specific schema.

## `verdict` generator (own)

The synthetic live track. Written in this repository, published with it, and
documented in `docs/generator.md`: entity graph, three fraud patterns, and the
sealed regime schedule. No licence question arises.

## Sparkov transaction generator

https://github.com/namebrandon/Sparkov_Data_Generation

Used as a **public reference only**, for the merchant category vocabulary and
the broad shape of amount distributions. No Sparkov data is downloaded,
generated or redistributed here, and no Sparkov code is copied. The category
list in `verdict/events/generator/entities.py` names it as the source so the
synthetic stream's category mix can be compared against something public
rather than against something invented for this repository.

Licence to record before that changes: if Sparkov code or output is ever used
rather than referenced, its licence goes here first.

## ULB credit-card fraud

https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

285,000 transactions, PCA-transformed features, published under the Open
Database License (ODbL) by the Machine Learning Group at the Université Libre
de Bruxelles. Held as the fallback for the real-data track, and as a second
check on the champion/challenger comparison. The PCA features make it useless
for the feature-store work, which is why it is a fallback rather than the
first choice.

## What this project never touches

No real payment rails, no card networks, no PCI scope, no personal data. Card
numbers in the synthetic stream are opaque identifiers of the form
`card-00000123`, not primary account numbers; the real-data track's
identifiers are already tokenised by its publisher.

No government, benefits or claims scenario appears anywhere in this
repository, and no internal architecture, feature definition or threshold from
any employer system. The employer boundary is stated at the top of `PLAN.md`
and applies to every commit.
