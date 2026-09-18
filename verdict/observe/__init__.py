"""Observation: what a running scorer reports about itself.

`metrics.py` turns the scorer's own timings and counts into Prometheus metrics
on a registry of its own, served on a port that listens on localhost unless
told otherwise. Dashboards and alarms read those; nothing in the scoring path
reads them back.
"""
