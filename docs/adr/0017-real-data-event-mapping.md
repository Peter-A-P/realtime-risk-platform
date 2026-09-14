# 17. What a card, a device and a moment are on the real-data track

- Status: accepted, 2026-09-14, **pending review by Peter Parker**. Taken
  during the build so the real-data track could run; each choice is
  measured below and each is reversible by changing one function.
- Date: 2026-09-14
- Deciders: the build session, for Peter Parker's review
- Numbered 17 rather than 8 because `PLAN.md` section 4 already assigns 8 to
  16 to decisions planned for later weeks.

## Context

The IEEE-CIS competition file is a table of transactions. The platform's
engine, store and leakage test all read `TransactionEvent`, which names a
card, a device and a merchant and carries an event time in UTC. Mapping one
onto the other needs three answers the file does not give, and
`docs/STATE.md` listed them as the open questions for this mapper:

1. What is a card?
2. What is a device?
3. What moment does `TransactionDT` count from?

`verdict data inspect` had already established that there is no merchant
identifier at all, and `docs/data.md` records the response: report per track
which features exist, rather than invent a merchant.

Every figure below was measured on 2026-09-14 from `train_transaction.csv`
and `train_identity.csv` (590,540 transactions). Counts only; no row of the
data appears here or anywhere in the repository.

## 1. What is a card

No column identifies a card. The candidates, measured as keys:

| Key | Rows with the key | Distinct keys | Median transactions per key | 99th percentile | Largest key | Multi-transaction keys with mixed fraud labels |
|---|---:|---:|---:|---:|---:|---:|
| `card1` | 100% | 13,553 | 4 | | 14,941 (2.5% of rows) | |
| `card1` to `card6` | 100% | 14,893 | 4 | 674 | 14,112 (2.4%) | 14.9% |
| `card1` to `card6`, `addr1` | 88.9% | 40,417 | 2 | 176 | 5,866 (1.0%) | |
| **`card1` to `card6`, `addr1`, account start day** | **88.7%** | **203,467** | **1** | **19** | **1,414 (0.24%)** | **1.4%** |
| the same, plus `P_emaildomain` | 88.7% | 250,544 | 1 | 14 | 1,414 | |

The account start day is the transaction day (`TransactionDT` in whole days)
less `D1`, which the competition's first-place write-up identified as days
since the card's first use. It is a field of the transaction itself, known
when the transaction is presented, so using it leaks nothing.

`card1` to `card6` describe an issuer and a product; a key holding 14,112
transactions is a card programme, and a velocity feature keyed on it would be
counting the programme's traffic. With `addr1` and the start day added, the
key behaves like a card: most keys have one transaction, the tail is short,
and fraud labels agree within a key on 98.6 percent of keys with more than
one transaction against about 85 percent for the card columns alone, which is
what a card that is either compromised or not would show.

Adding the e-mail domain splits a further 47,000 keys. It was not adopted: an
e-mail domain is a property of a purchaser rather than a card, it is missing
on 16 percent of rows, and a card used by two people is still one card.

**Decision.** A card is `card1` to `card6`, `addr1` and the account start
day, hashed to `ieee-card-<20 hex>`. A row missing `addr1` or `D1` (66,794
rows, 11.3 percent) cannot form the key and gets an identifier of its own,
`ieee-unlinked-<TransactionID>`: it has no visible history, which is true,
rather than a history shared with every other unlinkable row, which is not.

**Known imperfection.** The day boundary of `TransactionDT` is not the
issuer's, so a card transacting near midnight can fall on either side and
split into two keys. That under-links, which makes card velocity features
read lower than the truth for those cards. It never merges two cards.

## 2. What is a device

The identity file covers 24.4 percent of transactions (none of product `W`,
most of the others). Its device columns (`DeviceType`, `DeviceInfo`,
`id_30` operating system, `id_31` browser, `id_33` screen) were measured as a
fingerprint:

| Fingerprint | Rows with it | Distinct | Largest | Share of those rows on fingerprints with over 100 transactions |
|---|---:|---:|---:|---:|
| all five columns, where `DeviceInfo` and `id_31` exist | 20.0% | 9,445 | 6,351 | 54.9% |
| all five columns present | 12.0% | 4,855 | 2,566 | 57.7% |

`DeviceInfo` alone is `Windows` on 40 percent of identity rows. These are
configurations, not devices: the most common fingerprint is one operating
system, browser version and screen size shared by thousands of purchasers.
`device_distinct_cards_1h`, the feature that shows a card-testing burst,
would measure how popular that configuration is.

**Decision.** There is no device on this track. `device_id` is `None`.

## 3. What moment `TransactionDT` counts from

The values run from 86,400 to 15,811,131 whole seconds, 182 days. Public
analyses of the set take the reference as 2017-12-01.

**Decision.** Event time is 2017-12-01T00:00:00Z plus `TransactionDT`
seconds. This is a published convention, not a claim about when the
transactions happened. Nothing downstream reads the calendar: every feature
depends only on differences between event times, which no choice of
reference changes.

## Consequences

- **The wire schema moves to version 2.** `device_id`, `merchant_id` and
  `merchant_category` may be `None`, and have no default, so a producer states
  an absence rather than inheriting one. The fingerprint in
  `docs/generator-hashes.json` changes in the same commit.
- **Six of the sixteen features exist on this track**: the card velocity and
  amount features. The four device features, three merchant features and two
  session features do not, and nor does `card_distinct_merchants_24h`, which is
  keyed on the card but counts merchants and would read 1 on every row.
  `features_on_track()` computes that set from the schema facts rather than
  from a list, and a test pins it.
- **Card-testing and merchant collusion cannot be seen on the real data.**
  Both are entity-graph patterns and the graph is not in the file. The real
  track can show a card behaving unlike itself; the synthetic track carries
  the other two. The README's per-track feature table says so.
- **The point-in-time check runs on the real replay.** `verdict data check`
  replays all 590,540 events and checks every row of a two percent hash
  sample of cards against the definition. First run, 2026-09-14: 5,386 cards,
  67,920 comparisons, 0 violations, 104 seconds.
- **The same-instant leak is remeasured** under this card key in
  `docs/leak-caught.md`, whose earlier figure grouped by `card1`, a key this
  record shows is not a card.
- **Mixed-label share is used here as evidence about a key, never as a
  feature.** It reads the labels of the whole file, which no feature may do.

## Options not taken

- **`card1` alone, or with `addr1`.** Simpler, and wrong by the table above.
- **A device from the identity fingerprint, restricted to rare fingerprints.**
  Choosing which fingerprints count by how often they occur across the file
  uses the whole file's future to define an entity as of a moment. That is a
  leak in the entity definition rather than in a feature.
- **`ProductCD` as a merchant.** Five values. Already rejected in
  `docs/data.md`.
- **Leaving the schema at version 1 and filling absent entities with a
  sentinel identifier.** Every row would share one device and one merchant,
  and the features keyed on them would count the data set.

## Sources

- IEEE-CIS Fraud Detection, Kaggle, 2019. Data description: `TransactionDT`
  is a timedelta from a reference datetime.
  https://www.kaggle.com/competitions/ieee-fraud-detection/data
- Deotte and Yakovlev, "1st Place Solution", IEEE-CIS Fraud Detection, Kaggle,
  2019: identifying clients from card, address and `D1` rather than
  classifying transactions in isolation.
  https://www.kaggle.com/competitions/ieee-fraud-detection/writeups/fraudsquad-1st-place-solution-part-2
- Exploratory analyses of the set using 2017-12-01 as the reference date, for
  example: https://rstudio-pubs-static.s3.amazonaws.com/1312534_360edcecce5d4d9ab679c0bfb4e2dfa4.html
