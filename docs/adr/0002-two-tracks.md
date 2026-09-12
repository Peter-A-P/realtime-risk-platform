# 2. Two tracks through one pipeline: real data offline, synthetic data live

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

The project needs two things from its data that no single public dataset
provides.

It needs **real fraud**: genuine card-transaction outcomes, with the messy
correlations that make a leakage test worth writing and a model comparison
worth reporting. The public competition sets give that, but they arrive as a
static table. IEEE-CIS timestamps are a relative offset, there is no event
ordering at streaming resolution, and 590,000 rows is about ten minutes of
traffic at the target rate. Replaying it is a replay of a table, not a stream.

It also needs **volume and change**: a stream that runs at a thousand events
a second for three months, whose patterns shift on a schedule, so that
throughput, latency, drift detection and retraining have something to be
measured against. No public dataset offers that, and none ever will, because
the organisations that have such data cannot publish it.

The wrong answer to this is to pick one and quietly let it stand for both:
report a latency number measured on synthetic data next to a model number
measured on real data, without saying which is which.

## Options

1. **Real data only.** Replay IEEE-CIS in a loop to make volume. The stream
   then repeats every ten minutes, every drift monitor learns the loop, and
   the throughput number describes a cache.
2. **Synthetic data only.** One generator for everything. Nothing in the
   project ever touches real fraud, and every model number describes patterns
   the project invented for itself, which is circular.
3. **Two tracks through one pipeline.** Both, run through the same feature
   computation and the same code, with every published number labelled by
   the track it came from.

## Decision

Option 3.

- **Real-data offline track.** IEEE-CIS Fraud Detection, replayed in its own
  time order through the same dataflow and feature store. It carries the
  leakage test, the online/offline parity check, the champion/challenger
  comparison and the review-queue evaluation.
- **Synthetic live track.** The `verdict` generator: an entity graph of
  cards, devices and merchants, three publicly documented fraud patterns
  acting on that graph, and a schedule of regime shifts. It carries
  throughput, latency, drift detection, retraining, operations and cost.

Every number in the README names its track. The two are never averaged and
never presented as one result.

**The regime schedule is sealed, not merely committed.** A drift monitor
graded against shifts its author had read is not evidence. The design of the
schedule is public in `regimes.py`, and a published development schedule is
used throughout the build; the realisation that runs in the live window is
derived from a secret the repository does not contain. Before go-live three
hashes are committed: of the secret, of the derived schedule, and of
`regimes.py` itself. On Jul 1 2027 the secret is published and anyone can
re-derive the schedule and check all three. This is a refinement of
`PLAN.md` section 2.1, which said the schedule was hashed and committed
before go-live and revealed on Jul 1; committing the schedule itself in the
clear would have sealed nothing, because the monitors would have been written
by someone who had read it. The plan was amended in the same commit as this
record.

## Consequences

- Nothing in the repository can report one blended number, because the two
  tracks do not share a population. Every table has a track column.
- The generator becomes a deliverable in its own right, with its parameters
  published in `docs/generator.md` and its determinism under test.
- The synthetic fraud share is a parameter, set to about three percent to sit
  near the public competition set's 3.5 percent rather than near a real
  network's base rate. That choice is published, not hidden, and it means the
  synthetic PR-AUC numbers are not comparable with a production system's.
- A cost is accepted: the live-window model numbers describe fraud that this
  repository invented. That is why the real-data track exists, and why the
  live window's headline claims are about latency, throughput and operations.
- If IEEE-CIS's competition rules turn out to restrict derived works in a way
  that bites, the real-data track moves to the ULB credit-card set and the
  README says so. The terms are recorded in `docs/data.md` before the data is
  downloaded.

## Sources

- IEEE-CIS Fraud Detection competition, Vesta Corporation data, Kaggle 2019.
  About 590,000 transactions, 3.5 percent fraud, relative timestamps.
  https://www.kaggle.com/competitions/ieee-fraud-detection
- Sparkov transaction generator, public repository, used as the reference for
  merchant categories and amount distributions.
  https://github.com/namebrandon/Sparkov_Data_Generation
- ULB credit-card fraud dataset (Dal Pozzolo et al.), PCA features, ODbL.
  https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
- Card-testing, account-takeover and merchant-collusion pattern descriptions:
  Visa, "Card Testing: Trends and Mitigation Strategies" (public merchant
  guidance); FTC consumer guidance on account takeover; European Central Bank,
  "Report on card fraud" (public series), for the relative weight of
  card-not-present fraud.
  https://usa.visa.com/support/small-business/security-compliance.html
  https://www.ecb.europa.eu/pub/cardfraud/html/index.en.html
