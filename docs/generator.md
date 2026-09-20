# The `verdict` generator

The synthetic live track. It exists because real card-fraud data has no
streaming timestamps and no volume, and because throughput, drift and
operations cannot be measured without both. ADR 2 records why there are two
tracks rather than one.

Everything here is published on purpose. A generator whose parameters are
secret would make every number measured against it unfalsifiable. The one
thing that is not published before Jul 1 2027 is the realisation of the live
regime schedule, and the section on sealing explains exactly what that means
and why it is the opposite of hiding something.

## What it produces

Three streams, from one pass:

| Stream | Sink | Who may read it |
|---|---|---|
| `transactions.jsonl` | The transaction topic | Everything |
| `labels.jsonl` | The label topic, seven days later | Training and evaluation, never the scorer |
| `ground_truth.jsonl` | Generator-side only | Evaluation and the regime grading. Never a feature, never a model input, never a decision |

Ground truth is what the generator knew when it produced an event: whether it
was fraud, which scenario made it, and which regime was in force. It is in its
own file so that reading it has to be a deliberate act. A test asserts the
transaction log contains none of those words.

## The entity graph

Fraud is a property of a graph, not of a row, so the graph is built first.

| Entity | Reference population | Notes |
|---|---:|---|
| Cards | 200,000 | Each has a published spend profile and a long-tailed activity weight |
| Devices | 150,000 | Fewer than cards, so devices are shared |
| Merchants | 4,000 | Category, long-tailed popularity, an amount scale |

Links and shapes:

- Each card uses one to three devices. Twelve percent of links land in a
  shared-device pool, so shared-device counts vary rather than being noise.
- Card activity is lognormal, so a minority of cards make most transactions.
- Merchant popularity within a category is lognormal: the top decile takes
  more than twice a flat share. A test asserts this, because a flat merchant
  distribution would make merchant-level velocity features useless.
- Six spend profiles (`everyday`, `commuter`, `family`, `online_heavy`,
  `traveller`, `high_spender`) weight the fourteen merchant categories
  differently and set an amount scale. Every category keeps a small floor, so
  no purchase is impossible, only unlikely. That is what makes "unusual for
  this card" a finite quantity rather than an infinite one.
- 0.4 percent of merchants are eligible for the collusion scenario, and at
  least one always is. The count is fixed rather than drawn per merchant: a
  Bernoulli draw at that rate can return none at all on a small population,
  which would silently switch off a whole fraud pattern while the generator
  went on reporting the scenario mix it was asked for.

The category vocabulary is Sparkov's published list, so the synthetic mix can
be compared against a public reference. See `docs/data.md`.

## The fraud patterns

All three are publicly documented card-fraud patterns, cited in ADR 2. Each
plans a whole attack up front; the driver merges it into the legitimate stream
in time order, so attacks overlap ordinary traffic.

**Made harder on 2026-09-19 (ADR 21).** The first version was caught at a
PR-AUC of 0.9996, mostly because every attack ran in one long session. The
table below is the current one; the first version's parameters are in ADR 21.

| Pattern | Signature | Size | Pacing | Amounts |
|---|---|---|---|---|
| Card testing | One to three devices, many cards, one to three card-not-present merchants | 4 to 30 cards | 20 to 900 s apart | 100 to 4,000 cents |
| Account takeover | One card; a device it has never used, or 35% of the time one it has; half its purchases in the card's own categories | 3 to 14 events | 300 to 5,400 s apart | 0.9x to 4x that card's usual |
| Merchant collusion | A colluding merchant, many cards; half the ring's charges go through front merchants in the same category | 15 to 90 cards | 60 to 900 s apart | 1.0x to 1.5x the merchant's usual |

Every attack's online transactions carry the session its card would have
carried anyway, one per card per half hour, so a busy session is no longer a
giveaway. On the legitimate side, 1.5 percent of purchases are big tickets at
3x to 12x the card's usual, 1 percent come from a device the card has not used
before, 3 percent go through shared terminals (a fixed 0.4 percent of devices,
so many cards pass through one device honestly), and 2 percent are drawn to
whichever three merchants are having a busy hour. The busy merchants and the
terminals are derived from the hour and a fixed mapping rather than from
state, so a replay of a seed is the same stream.

Sizes are multiplied by the regime's attack intensity. None of these is
detectable from a single row, which is the point: each one is a bet that the
entity-graph and velocity features will be built correctly.

## The fraud share

The generator is asked for a **share of events**, not for an attack rate. It
derives the attack arrival rate from the share, the scenario mix and the
attack intensity, so changing the mix does not silently change how much fraud
there is. The default is three percent, chosen to sit near the public
competition set's 3.5 percent.

That is far above a real card network's base rate, which is a fraction of one
percent. It is a deliberate, published choice, and it means the synthetic
track's model numbers are not comparable with a production system's. The
real-data track exists for that comparison.

**Warm-up.** A stream that starts with an empty schedule has no attack already
in flight, so its first hours would carry less fraud than its steady state.
The driver plans the attacks that began before the window opened, over three
mean attack durations, and keeps only the events that land inside the window.
Attacks longer than that are still slightly under-represented in a run's first
minutes. Over an 87-day live window this is immaterial; in a short test it is
not, which is why the warm-up is on by default and a test asserts it more than
doubles the fraud share of a cold start.

## The regime schedule, and what "sealed" means

A drift monitor graded against shifts its author had read proves nothing. So
three things are kept apart:

1. **The design is public.** `regimes.py` publishes the kinds of shift and the
   range each parameter may take: fraud-rate multiplier 0.4 to 3.0, amount
   log-shift -0.35 to 0.35, card-not-present share shift -0.10 to 0.25, attack
   intensity 0.6 to 2.5, four to seven regimes, none shorter than a week. A
   test asserts every derived schedule stays inside those ranges.
2. **The development realisation is public.** `DEV_SCHEDULE` is fixed and
   readable, and the whole build uses it. It deliberately contains the case
   that catches a naive monitor: `amount-drift-no-fraud-change` moves the
   amount distribution while the fraud rate and the scenario mix hold still.
   A monitor that fires on that and a retraining that follows are both wrong.
3. **The live realisation is sealed.** The schedule that runs in the live
   window is derived from a secret this repository does not contain. Before
   go-live, `verdict schedule seal` commits three hashes: of the secret, of
   the derived schedule, and of `regimes.py` itself. On Jul 1 2027 the secret
   is published; anyone runs `verdict schedule verify --reveal` and checks all
   three.

This is a refinement of `PLAN.md` section 2.1, which said the schedule was
hashed and committed before go-live. Committing the schedule itself in the
clear would have sealed nothing, because the monitors would have been written
by someone who had read it. The plan was amended in the same commit as the
code, and ADR 2 records the reasoning.

`regimes.py` is frozen from sealing until Jul 1 2027. A test asserts its hash
against `docs/generator-hashes.json`, with line endings normalised so that a
Windows checkout is not mistaken for a tamper.

## Determinism

The same seed produces the same stream, byte for byte. The graph and the event
stream take separate substreams of the seed, so changing the rate does not
change the graph. Two things are hashed and committed in
`docs/generator-hashes.json`, each with a test: the reference entity graph and
the development schedule, plus the wire schema and the `regimes.py` source.

Every `verdict generate` run reports all of those hashes with its output. A
run that cannot name the schedule and graph it used is not reproducible,
however deterministic the generator is.

## Measured, week 1

Track: synthetic live. Build laptop, Windows 11, Python 3.13.15. Five runs of
500,000 events, 95 percent intervals from a t distribution on five runs.

| Measurement | Result |
|---|---|
| Generated and written to the raw log | 12,219 events/s (8,790 to 15,648) |
| Generated only, nothing written | 19,577 events/s (8,146 to 31,008) |
| Fraud share against a 3 percent target | 3.06 percent (2.85 to 3.26) |
| One continuous run of 1,000,000 events | 6,580 events/s, 3.10 percent fraud |

The live rate is 1,000 events per second, so the generator clears it with
between six and twelve times the headroom depending on how much of the run is
spent writing. The intervals are wide because a laptop is a noisy machine and
the raw-log write, not the generator, is what binds: the longest run is the
slowest one. None of this is a platform latency or throughput number. Nothing
is being scored here. The published figures come from the load tests in weeks
4 and 8, against the live stack.

## Running it

```
verdict generate --out data/raw/dev --events 100000 --rate 1000
verdict schedule show
verdict schedule hash
```
