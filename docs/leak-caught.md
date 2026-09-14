# The leak the point-in-time test caught

**Date:** 2026-09-12, week 3, the day the first features were written.
**Caught by:** `verdict/store/leakage.py`, written in week 2, before any
feature existed.
**Fixed in:** the same commit as the features, in
`verdict/features/engine.py`.

The plan predicted this. Section 2.3 says the first velocity window is
expected to get a boundary wrong, and that when the test catches it, that
commit is worth more than the feature. So here is what happened, including
the part where the test caught the author twice.

## What was wrong

The engine serves each event's features before folding that event into its
state, so an event cannot be inside its own window. That was the design, and
it was not enough.

Two transactions can carry the **same timestamp**. When they do, the first
one is folded into the state before the second one is served, so the second
one's window contains it. The window is `[t - w, t)`, half open, and an event
at exactly `t` is outside it by definition. The engine served a count of 1
where the definition said no history at all.

```
event a at 12:00:00.000
event b at 12:00:00.000   <- served card_txn_count_1h = 1, definition says none
event c at 12:05:00.000
```

The fix is a one-instant buffer: an event now waits until an event with a
strictly later timestamp arrives before it joins the history. Same-instant
events therefore never see each other, which is what the definition says and
what a production scorer, deciding on each transaction before the next
arrives, would also do.

How much this was worth is measured below, and the first published version of
that measurement was wrong. The correction is kept in place rather than
quietly edited out, because a document about a leak that nobody noticed is a
poor place to silently fix a number nobody noticed.

## What it would have cost

Three measurements. The first two were taken before the competition data was
downloaded; the third is the one that describes it, and it is the reason this
section was rewritten.

| Stream | Rate | Timestamp resolution | Rows sharing an instant | Values actually corrupted |
|---|---:|---|---:|---|
| Synthetic live | 1,000/s | microsecond | 0.055% | 0 of 60,000 |
| Synthetic, truncated to whole seconds | 1,000/s | second | 99.9% | 2.75% of one feature's values |
| IEEE-CIS, grouped by `card1` | 0.0376/s | second | 5.75% | 0.053%: 312 of 590,540 rows share an instant and a `card1` |
| **IEEE-CIS, by the ADR 17 card, remeasured 2026-09-14** | **0.0376/s** | second | **5.75%** | **0.011%: 65 of 590,540 rows served a wrong value** |

### The correction

An earlier version of this document put the middle row in the table under the
heading "Real-data offline track, as IEEE-CIS is published". That was wrong,
and the error went into the README and the plan's status with it.

The middle row is the **synthetic** stream with its timestamps truncated to
whole seconds. It is a stress test, not a description of anything. The real
competition data carries 590,540 transactions across 182 days, which is
**0.0376 events per second**: about twenty-six thousand times sparser than the
synthetic stream. Collisions there are uncommon, not universal.

The mistake was assuming that second-resolution timestamps were what made
collisions likely. They are half of it. What actually decides collision
frequency is **events per unit of timestamp resolution**, and the two tracks
sit at opposite corners of that: a dense stream with fine timestamps, and a
sparse one with coarse timestamps. They end up in a similar place, with
roughly one row in two thousand affected.

### What the real data says

On IEEE-CIS, 33,932 rows (5.75 percent) share a `TransactionDT` value with
another row. Sharing an instant is only half of what the leak needs, though:
it also has to be the same entity, or no feature value changes. Grouping by
instant and card:

- **312 rows (0.053 percent)** share both an instant and a `card1` value, so
  their card-keyed features would have been wrong.
- 160 rows (0.027 percent) on the tighter `card1`+`addr1` card proxy.
- Those 312 rows are 7.7 percent fraud against a 3.5 percent base rate. That
  is the direction you would expect if bursts are disproportionately
  fraudulent, and with 24 fraudulent rows in the group it is far too small to
  lean on. It is recorded because it points the right way, not because it
  proves anything.

So on the real data the leak would have been small. It would also have been
entirely invisible, permanent, and concentrated slightly in the rows the model
exists to find.

### Remeasured on 2026-09-14, with a card that is a card

The figures above group by `card1`, and ADR 17 has since shown that `card1` is
not a card: its busiest value holds 14,941 transactions. They also count every
row in a group sharing an instant, including the first, which the leak cannot
touch because nothing precedes it. Both overstate. Neither is edited out.

The real-data mapper (ADR 17) defines a card as `card1` to `card6`, `addr1`
and the account start day. The unfixed engine, kept in `tests/test_engine.py`,
was replayed over all 590,540 mapped events and every served value compared
with the definition, for every card:

| | |
|---|---:|
| Values compared | 3,543,240 (6 features on this track) |
| Values wrong | 324 |
| Rows with at least one wrong value | **65 (0.011%)** |
| Card and instant pairs involved | 45 |

Count, sum and seconds-since-last are wrong on all 65 rows; the mean is wrong
on 39 and the maximum on 25, where the event seen too early happened not to
move them. The fixed engine, over the same replay on a 2 percent sample of
cards, has 0 wrong values in 67,920.

**This measurement was itself wrong once, for about an hour, and is recorded
as such.** The first run of the sampled check stored served values by card
and instant, so in a burst every event was compared with the value served to
the last one. It reported 110 rows. Storing values per event gives 65, which
is what the arithmetic says it must be: 110 rows in 45 bursts, less the first
event of each. The check now keys by event, and
`tests/test_ieee_cis_events.py` asserts exact counts on a burst of three so
the overcount cannot come back. It is the same kind of mistake as the second
catch below: correct pieces joined through a key that answers a slightly
different question.

**The offline PR-AUC inflation is not measured here**, because there is no
model yet: model work is week 5. The plan asks for that number, so week 5
trains the champion twice, once on features from the fixed engine and once on
features from the unfixed one, and reports the difference. The unfixed
implementation is kept as a fixture in `tests/test_engine.py` for exactly that
purpose rather than deleted. On 312 rows out of 590,540, the honest
expectation is that the difference will be small and possibly not separable
from noise, and that is worth reporting either way.

## The second catch, which is the more useful story

Before the same-instant leak, the test caught a different mistake, made while
wiring the check itself up.

The first attempt asked the engine for historical values directly: run the
whole replay, then query `engine.lookup` for each past row. That returns
whatever the entity's window holds **now**, not what it held then, so it
reported windows containing events that had not yet happened at the moment
being asked about. The leakage test went red with 1,762 violations across all
16 features, which is what a comprehensive leak looks like.

That was not an engine bug. It was the author using the engine wrongly, and
the fix was to make the misuse impossible: `engine.lookup` now refuses any
query earlier than the last event it has observed, and says where the
historical record actually lives, which is the offline store. The leakage
check reads that instead.

This is the more useful of the two stories, because it is the failure mode
that ships. A leaky feature is a bug someone can find. A **correct** feature
retrieved through a path that quietly answers "as of now" when asked "as of
then" is not visibly anything: it produces a training set that is slightly
too good, from code where every individual piece is right.

## What did not change

The test. Neither fix touched `leakage.py`, and neither adjusted a tolerance,
narrowed a sample or excluded an entity. The repository's rule is that the
leakage test is never weakened to make a feature pass, and the value of that
rule is only visible on a day when the test is inconvenient.
