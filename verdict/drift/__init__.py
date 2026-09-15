"""Drift: noticing that the traffic has moved, and asking a person what to do.

`stats.py` holds the two statistics; `monitors.py` applies them to a day of
served features and scores against a fixed reference; `trigger.py` decides
when enough drift has been seen to open a retraining request. Nothing here
trains, promotes or changes the champion. ADR 12 records the choices.
"""
