# Data, and the terms it comes under

Nothing raw is committed to this repository. Loaders fetch, verify a checksum
and write under `data/`, which is gitignored. Every source below records what
it is, what it is used for, and the terms it comes under.

## Status

| Source | Used for | Terms recorded | Downloaded |
|---|---|---|:--:|
| IEEE-CIS Fraud Detection | Real-data offline track | **Yes, 2026-09-12** | **Yes, 2026-09-12**; again 2026-09-13 on the build desktop, same checksums |
| `verdict` generator (own) | Synthetic live track | Written here, published with the repository | n/a |
| Sparkov generator | Reference for categories and amounts | Reference only, no data used | No |
| ULB credit-card fraud | Fallback for the real-data track | ODbL | No |

## IEEE-CIS Fraud Detection

Published by Vesta Corporation for the IEEE Computational Intelligence
Society's 2019 technical challenge. It is the real-data track: the leakage
test, online/offline parity, the champion/challenger comparison and the
review-queue evaluation all run on it.

**Measured from the file on 2026-09-12**, rather than quoted from the
competition page, by `verdict data inspect`:

| | |
|---|---|
| Transactions (`train_transaction.csv`) | 590,540 |
| Span of `TransactionDT` | 182.0 days, whole seconds, offset from an unstated reference |
| Mean rate | 0.0376 events per second |
| Fraud share | 3.499% |
| Rows with identity (device) data | 24.4% |
| Merchant identifier | **None** |

Three of those shape what this track can do, and the loader is written against
them rather than around them:

- **There is no merchant.** The platform's feature set includes three
  merchant-keyed features, and this track cannot compute any of them. The
  response is to report per track which features exist, not to promote
  `ProductCD` (a five-valued product category) into a merchant and then
  measure an entity-graph feature against something invented here.
- **There is no device either, which was not known on 2026-09-12.** The
  identity file covers 24.4 percent of transactions, and this document first
  said device-keyed features would exist for that share. Measured as keys on
  2026-09-14, its columns turn out to describe configurations rather than
  devices: fingerprints with over a hundred transactions hold 55 percent of
  the rows that have one, and `DeviceInfo` alone is `Windows` on 40 percent.
  A device feature keyed on that counts how popular a browser is, so the
  mapper names no device. ADR 17 has the measurements.
- **No column is a card.** `card1` to `card6` describe an issuer and a
  product. The mapper's card is those columns with `addr1` and the day the
  account started, which behaves like a card; 11.3 percent of rows cannot
  form it and have no visible history. ADR 17 again.
- **The clock is whole seconds and the stream is sparse.** 0.0376 events per
  second means same-instant collisions are uncommon: 5.75 percent of rows
  share a timestamp with another row, and only 0.053 percent share both a
  timestamp and a card. That number is why `docs/leak-caught.md` carries a
  correction.

Competition page: https://www.kaggle.com/competitions/ieee-fraud-detection

**Rules read and recorded 2026-09-12**, from the competition's own rules page,
by the account holder. The competition itself closed on 3 October 2019; what
still governs use of the data is section 7 of the General Competition Rules.
The four questions this project had to answer, with the clauses that answer
them:

### 1. What purposes is the data licensed for?

> **7.A Data Access and Use.** "You may access and use the Competition Data
> for non-commercial purposes only, including for participating in the
> Competition and on Kaggle.com forums, and for academic research and
> education."

**Non-commercial only.** This repository is a public demonstration: it sells
nothing, charges nothing, offers no service, and is not used to deliver work
to any client or employer. That is non-commercial use in the ordinary sense
of the phrase.

The honest caveat, recorded rather than buried: a portfolio exists to help its
author get hired, and someone could argue that a career purpose is a
commercial one. The rule's own examples ("academic research and education")
sit closer to this project than to a product. The posture taken here is the
conservative one, and it is a constraint on the repository rather than a
belief about it:

- No part of this project may be sold, licensed for a fee, or run as a paid
  service, for as long as it contains anything derived from this data.
- The MIT licence on the repository covers **the code**. It does not and
  cannot relicense the data, and nothing in the repository implies otherwise.

If that ambiguity ever needs to be gone rather than managed, the ULB set is
the fallback, and ADR 2 already records that substitution.

### 2. May derived works be published?

**Yes, and that is the distinction the rules draw.** Section 7.B restricts
"the Competition Data"; it says nothing against publishing results computed
from it. Published evaluation tables, PR-AUC with confidence intervals,
feature distributions and a trained model artefact are not the data.

Two concrete limits follow, and they bind this build:

- **The real-data track's feature store is not publishable.** The offline
  store holds one row per transaction, keyed to that transaction. That is a
  transformed copy of the data rather than a result derived from it, so it
  stays local and gitignored, exactly like the raw files.
- **No row-level output of any kind is published**, including samples,
  fixtures, test data or debugging output that happens to contain real rows.
  Where a test needs data shaped like IEEE-CIS, it uses a synthetic fixture.

### 3. What may not be redistributed?

> **7.B Data Security.** "You agree not to transmit, duplicate, publish,
> redistribute or otherwise provide or make available the Competition Data to
> any party not participating in the Competition." It also requires
> "reasonable and suitable measures to prevent persons who have not formally
> agreed to these Rules from gaining access to the Competition Data."

**The data itself, to anyone.** This is stricter than "do not commit it", and
the second sentence is the one that reaches into the architecture:

- Nothing raw or row-level derived is committed. `.gitignore` covers
  `data/`, and `tests/test_data_terms.py` asserts those rules exist rather
  than trusting that they still do.
- **The real-data track never runs on the live AWS stack.** The live window
  is public: a public dashboard, a public repository, and an instance whose
  volumes and images could be snapshotted. Putting this data there would be
  making it available to people who have not agreed to these rules. The live
  track is synthetic, which was already true for other reasons, and is now
  also a licence requirement.
- The data is not uploaded to CI, not cached in Actions, and not baked into
  any container image.

### 4. Is external data allowed alongside it?

> **7.C External Data.** Permitted, provided it is available to all
> participants at no cost and posted to the competition forum before the
> entry deadline.

**Yes.** Those conditions attach to competition submissions before the
October 2019 entry deadline, and this project makes no submission. The point
is moot in both directions anyway: the only other data here is the
repository's own generator, which is published with the repository and free
to anyone.

The two tracks are never merged into one table regardless. ADR 2 records why.

### One further clause worth flagging

> **8.B Public Code Sharing.** "You are permitted to publicly share
> Competition Code, provided that such public sharing does not violate the
> intellectual property rights of any third party. If you do choose to share
> Competition Code ... you are required to share it on Kaggle.com on the
> discussion forum or kernels associated specifically with the Competition
> ... By so sharing, you are deemed to have licensed the shared code under an
> Open Source Initiative-approved license ... that in no event limits
> commercial use."

This repository goes public at go-live and contains code that reads this
data, which is "Competition Code" as the rules define it.

- The licence half is already satisfied: the repository is MIT, which is
  OSI-approved and does not limit commercial use. The `LICENSE` file exists
  for that reason as much as any other.
- The "share it on Kaggle" half was written for fairness during a live
  competition, and the competition closed in 2019. It is not obviously live
  today. It is also cheap to honour, and honouring it costs nothing: a link
  posted to the competition's discussion forum at go-live. **That is a
  decision for the repository's owner, not a step this build takes on its
  own**, because it publishes something under his name.

### What the loader does about all this

- Downloads nothing on its own. The account holder downloads the archive from
  the competition's Data tab into `data/raw/ieee-fraud-detection/`, which is
  gitignored, or anywhere outside the repository named by the
  `VERDICT_IEEE_CIS_DIR` environment variable. No
  Kaggle API token is stored anywhere in this project.
- Verifies a checksum on what it finds, so a truncated or substituted file is
  an error rather than a strange model.
- Fails loudly if the columns are not the ones recorded here, rather than
  silently mapping a renamed field to the wrong feature.
- Maps rows onto events (`verdict data events`) into `data/`, and prints
  counts only. The event log is a row-by-row copy and is ignored like the
  raw files; so are any error messages that name a card identifier, which
  are printed to the local terminal and nowhere else.

## `verdict` generator (own)

The synthetic live track. Written in this repository, published with it, and
documented in `docs/generator.md`: entity graph, three fraud patterns, and the
sealed regime schedule. No licence question arises, and it is the only track
that runs during the live window.

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
