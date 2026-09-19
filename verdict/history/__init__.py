"""What the platform keeps of what it decided: a labelled, weighted sample.

At the live rate no disk holds every decision for the whole window (ADR 14),
so the scorer stages every decision with its features, a collector spools
the labels as they arrive, and once a day's labels are all in, the day is
reduced to every reviewed or declined transaction and a published sample of
the rest, each row carrying the weight that makes totals come out right
(ADR 18).
"""
