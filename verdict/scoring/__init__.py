"""Scoring: the stream consumer that turns a transaction into a decision.

`consumer.py` is the scorer ADR 8 describes: consume, compute features, score,
apply the rules, write the decision, checkpoint. `model.py` holds the model
interface and the stand-in that occupies it until week 5 trains a champion;
`rules.py` turns a score into an action; `timing.py` holds the per-hop clock
and the statistics the latency budget is reported with; `loadtest.py` drives
the whole path at a fixed rate and measures it.
"""
