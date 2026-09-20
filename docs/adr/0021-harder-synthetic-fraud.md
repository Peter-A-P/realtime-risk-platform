# 21. The synthetic fraud is made much harder, and the legitimate traffic less tidy

- Status: accepted, 2026-09-19
- Date: 2026-09-19
- Deciders: Peter Parker ("make the fraud much harder"); the build session
  (what to change, and how much)

## Context

`PLAN.md` section 8 listed "synthetic fraud is too easy or too hard" as a risk
and said scenario difficulty would be tuned in week 1 "to a champion PR-AUC in
the range the real data shows". It was not, because in week 1 there was no
model to tune against. On 2026-09-19 there was: the first synthetic champion
scored a test PR-AUC of **0.9996** (ADR 19), and one feature, the number of
transactions in a session, scored 0.64 on its own.

A live window on a stream that easy would show a champion nothing can beat,
a drift response with nothing to recover, and a review queue whose ranking
never matters. Those are the platform properties the window exists to
demonstrate, so the generator had to change before the schedule is sealed.

The sealed file `events/generator/regimes.py` is untouched: what changed is
`scenarios.py` and the legitimate side of `driver.py`.

## What was giving the fraud away

| Giveaway | Why it was decisive |
|---|---|
| Every attack ran in one session, from `ses-atk-<n>`, spanning its whole length | A legitimate session is one card for half an hour, so any session with many transactions was fraud |
| Card testing: 25 to 140 cards, 0.8 to 6 seconds apart, one device, one merchant | No legitimate device or merchant looks remotely like that |
| Takeover: 2.5x to 18x the card's usual, always from an unknown device, always in unusual categories | Three independent giveaways at once |
| Collusion: 1.4x to 6x the merchant's ticket, always at the colluding merchant | The merchant's own hourly figures identified it |
| Legitimate traffic had no large purchases, no new devices, no shared terminals and no busy hours | Every one of the above had nothing honest to be confused with |

## Decision

**Attacks blend in.**

- Every attack's online transactions carry the session its card would have
  carried anyway: one per card per half hour (`scenarios.card_session`).
- **Card testing**: 4 to 30 cards, 20 to 900 seconds apart, from one to three
  devices, at one to three merchants, for 100 to 4,000 cents.
- **Account takeover**: 0.9x to 4x the card's usual amount, 5 to 90 minutes
  apart; 35 percent run from a device the card already uses (malware, a
  stolen phone); half the purchases are in the card's own categories.
- **Merchant collusion**: 1.0x to 1.5x the merchant's ticket, 1 to 15 minutes
  apart, and half of each ring's charges go through other merchants in the
  same category, so no single merchant's figures carry the episode.

**Legitimate traffic stops being tidy** (`driver.py`), because a model needs
honest look-alikes for every signal:

- 1.5 percent of purchases are big tickets, 3x to 12x the card's usual;
- 1 percent come from a device the card has never used;
- 3 percent go through shared terminals, a fixed 0.4 percent of devices, so
  many cards pass through one device honestly;
- 2 percent are drawn to whichever three merchants are having a busy hour,
  chosen from the hour itself, so a merchant's count and distinct cards rise
  for honest reasons too.

Everything stays deterministic: the busy merchants and the shared terminals
come from the hour and a fixed mapping, not from state, so a replay of a seed
is the same stream.

## Evidence

Three days of the scaled training configuration (ADR 19), champion fitted
exactly as before, test PR-AUC with a 95 percent bootstrap interval:

| Generator | Champion test PR-AUC | Strongest single feature |
|---|---|---|
| Before (ADR 19) | 0.9996 (0.9995 to 0.9996) | session transaction count, 0.64 |
| Sessions, card testing, takeover and amounts changed | 0.883 (0.878 to 0.888) | none above 0.05 |
| Collusion spread over front merchants | 0.845 (0.840 to 0.851) | none above 0.05 |
| Legitimate look-alikes added | 0.822 (0.816 to 0.828) | none above 0.05 |

Those are three-day probes on the scaled configuration, run to compare one
change against another. The shipped figure is the full ten-day replay the
champion is actually trained on: **0.8427 (0.8393 to 0.8464)**, with the
challenger at 0.8012 and losing the paired comparison by 0.0415 (ADR 19).
The full run scores higher than the last probe (0.8427 against 0.822) and
the intervals do not overlap, which is expected rather than troubling: ten
days give every card, device and merchant more history than three, so the
velocity and entity features carry more, and there are three times the
training rows. The probe existed to rank one change against the next under
identical conditions, not to predict the shipped number.

The base rate is 3 percent, and the real-data track's champion scores 0.075
against a 3.5 percent base rate (ADR 19), so the synthetic stream is still
much the easier of the two. It is meant to be: the point of the live window
is that the platform's velocity and entity-graph features do their job, not
that fraud is undetectable.

**What still carries the remaining 0.84, and why it stays.** The merchant
features: hourly count, distinct cards and mean amount. A colluding merchant
really does have a high fraud share, and "the merchant's share of later
chargebacks" is the documented signature of the pattern (ADR 2). Hiding it
further would mean rotating which merchants collude, which is bust-out fraud
at new merchants rather than collusion at established ones, and the entity
graph's merchants are fixed and hashed. The scenario mix is set by the
regime schedule, which is sealed and not to be tuned against. So this is
where the tuning stops: no single feature decides any more (the best scores
0.05 alone, against 0.64 before), the model's gain is spread across merchant,
device, card and amount features, and the queue and the promotion gate now
have something to be wrong about.

## Consequences

- The synthetic champion and challenger are retrained on this generator, and
  the numbers in ADR 19 for the synthetic track are replaced.
- Attacks are now easy to mistake for busy honest behaviour, which is the
  point: the review queue's ranking by expected loss (ADR 13) and the
  promotion gate's margins (ADR 11) both become meaningful rather than
  decorative.
- The regime schedule still moves the same knobs, so drift detection is
  unaffected in kind, though a shift now moves a harder distribution.
- `docs/generator.md` carries the current parameters; this record carries the
  ones replaced, so the change is visible rather than quietly rewritten.
- The change lands before sealing, as it must: after sealing, the generator
  is frozen for the window.

## Sources

- The three patterns and their public sources are cited in ADR 2; nothing
  here adds a pattern, it changes how loudly each one announces itself.
- `PLAN.md` section 8, the risk this closes.
