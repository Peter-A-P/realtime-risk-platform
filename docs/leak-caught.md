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

## What it would have cost

Two measurements, because the answer differs sharply by track, and the
difference is the interesting part.

| Track | Timestamp resolution | Events sharing a timestamp | Feature values corrupted |
|---|---|---:|---:|
| Synthetic live, 1,000 events/s | microsecond | 33 of 60,000 (0.055%) | 0 of 60,000 |
| Real-data offline, as IEEE-CIS is published | second | 19,980 of 20,000 (99.9%) | 55 of 2,000 sampled (2.75%) |

On the synthetic stream the leak is real but rare: collisions happen, and in
this sample none of the colliding pairs shared a card, device or merchant, so
no served value actually changed. Had the test not caught it, the live window
would have run for three months with a defect firing on roughly one event in
two thousand and visible in nothing.

On the real-data track it would have been severe. The public competition
data's `TransactionDT` is a whole-number offset in seconds, so essentially
every event shares its timestamp with others, and 2.75 percent of the sampled
`device_distinct_cards_1h` values were wrong. That feature is the one that
exists to catch card testing, which is precisely a burst of events inside a
single second.

**The offline PR-AUC inflation is not measured here**, because there is no
model yet: model work is week 5. The plan asks for that number, so week 5
trains the champion twice, once on features from the fixed engine and once on
features from the unfixed one, and reports the difference. The unfixed
implementation is kept as a fixture in `tests/test_engine.py` for exactly
that purpose rather than deleted.

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
