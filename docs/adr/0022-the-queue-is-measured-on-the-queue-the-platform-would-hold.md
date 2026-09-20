# 22. The review queue is measured on the queue the platform would hold

- Status: accepted, 2026-09-20
- Date: 2026-09-20
- Deciders: the build session (how to measure ADR 13's ranking, and what to
  refuse to publish)

## Context

ADR 13 chose to rank the review queue by expected loss rather than by the
model's score, on the argument that a score says how likely fraud is and an
analyst's hour is spent on money. `review_queue/ranking.py` has been
testable since it was written: given constructed items it ranks them, and
`simulate_day` runs a shift against a capacity. What it had never been given
is a queue, so the README's headline table carried an empty column where
"dollars caught per analyst-hour" belongs, and the ranking was a decision
with no evidence behind it.

Three things had to be settled before a number meant anything.

## Decision

**The queue is built by the platform, not assembled for the measurement.**
The stream is replayed through the scorer's own engine, one event at a time
so the windows are exactly what the scorer would have held, scored by the
shipped champion, and decided by the shipped `DecisionRules`. An item
reaches the queue only if the rules would have sent it there, by score or by
the large-amount rule. Only the model call is batched, because a per-row
ONNX call over millions of transactions spends most of its time in call
overhead; the scores are identical either way.

**The arrival mix is not sampled.** The training table keeps every fraud and
a twentieth of the legitimate rows (ADR 18), which is right for fitting and
wrong here. Money caught per analyst-hour depends directly on how much of
the queue is fraud, so a queue built from that sample would be twenty times
richer than the real one and would flatter both policies. The replay is
therefore unsampled and writes no table: an unsampled twenty days is
thirty-five million rows, and only about two per hundred reach the queue.

**Nothing is counted from the champion's own training window.** Every event
is served, so the windows stay warm, but items are collected only from the
cutoff onwards. This is not a formality; see the evidence.

**The prices are stated, not implied**: 500 cents for an analyst review, 30
percent of a fraud's amount recovered anyway by chargeback, eight analysts
at twelve reviews an hour, and an item worth nothing after four hours. Every
one is an assumption, and a reader who disagrees can change it and rerun
`verdict queue-eval`.

## Evidence

`docs/queue-eval.json`. Twenty days of synthetic stream, the queue collected
over the thirteen days after the champion's cutoff: 23,531,714 transactions
scored, 458,446 queued (1.95 percent), of which 40.8 percent were fraud.
Capacity is 2,304 reviews a day against about 35,000 arrivals, so 93 percent
of the queue is never opened and the order is nearly the whole game.

| Policy | Caught per analyst-hour |
|---|---|
| By the model's score | $169.50 |
| By expected loss (ADR 13) | $266.95 |
| **Difference, mean over days** | **$97.46 ($48.03 to $150.44)** |

The interval is a bootstrap over the thirteen days and excludes zero.
Expected-loss ranking is worth about 57 percent more an hour than ranking by
score, on this stream, at these prices.

**The first run of this was wrong, and the way it was wrong is the finding.**
It covered three days starting at the beginning of the stream, which is
inside the seven days the champion is fitted on. It reported $221.69 ($190.31
to $237.51), more than double. Two other numbers gave it away: the queue came
out 65.7 percent fraud rather than 40.8, and only 1.02 percent of
transactions reached it rather than 1.95. On data it had been fitted to the
champion's scores were sharper than it can really manage, which made the
queue cleaner than it is and score ranking better than it is. The three-day
window also meant the bootstrap resampled three numbers and returned an
interval it was not entitled to.

## Consequences

- The README's queue column is filled from `docs/queue-eval.json`, with the
  prices and the capacity beside it, because the number means nothing
  without them.
- `verdict queue-eval` defaults to twenty days collected after day seven, so
  the default reproduces the published figure.
- **The queue is richer than a real one**, 41 percent fraud against the low
  single digits a real team would see. That follows from a 3 percent base
  rate meeting a 0.84 PR-AUC champion at a 0.50 review threshold (ADR 19,
  ADR 21), and it is a property of this stream, not a claim about anyone's
  operation. The comparison between the two policies is what transfers; the
  absolute dollars do not.
- The thirteen days span the card-testing-wave regime that begins on day 14,
  so this is not a stationary stretch. That is the honest condition to
  measure a queue under, and the drift run (ADR 12) says what the platform
  made of the same change.
- `models/dataset.serve_and_score` and `scoring/model.BatchModel` came out of
  this work and are shared with the drift run, so both walk a stream the
  same way.

## Sources

- ADR 13, the ranking and the cost model it rests on.
- ADR 18, the sampling this deliberately does not use, and why.
- ADR 19 and ADR 21, the champion and the stream the queue is built from.
