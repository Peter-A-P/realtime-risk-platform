"""What ranking the review queue by expected loss is worth, measured.

ADR 13 chose expected loss over the model's score, on the argument that a
score says how likely fraud is and an analyst's hour is spent on money. The
ranking and its simulation have been testable since they were written; what
they had never been given is a queue the platform would actually hold. This
builds one: a stream replayed through the scorer's own engine, scored by the
shipped champion, and decided by the shipped rules, so an item reaches the
queue only if `DecisionRules` would have sent it there.

**The arrival mix is not sampled, and that is the point.** The training
table keeps every fraud and a twentieth of the legitimate rows (ADR 18),
which is right for fitting and wrong here: money caught per analyst-hour
depends on how much of the queue is fraud, and a queue built from that
sample is twenty times richer than the real one. It would flatter both
policies and tell nothing about either. So this replays unsampled, and
because an unsampled ten days is eighteen million rows, it writes no table:
the stream is scored in batches and only the items that reach review are
kept, which is a few per thousand.

The labels are used the way a review uses them. A policy never sees
`is_fraud` (ADR 13 states that as a rule and `ranking.Policy` repeats it);
it is read only when an item has been reviewed, to count what the review
found, which is what an analyst opening a case does.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, Final

from verdict.events.schema import Action
from verdict.features.engine import FeatureEngine
from verdict.models.dataset import SCORE_BATCH, Labelled, serve_and_score
from verdict.review_queue.ranking import Costs, QueueItem, compare_policies
from verdict.scoring.model import BatchModel
from verdict.scoring.rules import DecisionRules

ANALYSTS: Final = 8
"""Analysts on shift, stated rather than implied."""

REVIEWS_PER_ANALYST_HOUR: Final = 12
"""Reviews one analyst completes in an hour: five minutes a case."""


def review_items(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    rules: DecisionRules | None = None,
    engine: FeatureEngine | None = None,
    collect_after: dt.datetime | None = None,
    batch: int = SCORE_BATCH,
) -> tuple[list[QueueItem], int]:
    """Serve and decide a stream, keeping what the review queue would hold.

    Every event is served, so the windows are what the scorer would have
    held; `collect_after` only decides which of them are counted. That
    matters because the champion is trained on the front of this same
    stream, and a queue built from the transactions it was fitted on would
    measure the model's memory rather than either ranking policy.

    Args:
        records: Transactions with their labels, in event-time order.
        model: The scoring model, with `score_matrix`.
        rules: The decision rules; the shipped defaults by default.
        engine: The engine to serve from; the platform's own by default.
        collect_after: Keep only items arriving at or after this moment, the
            champion's training cutoff. Everything, by default.
        batch: Rows scored per call.

    Returns:
        The queue's items, and how many transactions were counted, which is
        how many were served when nothing is skipped.
    """
    decide = rules or DecisionRules()
    items: list[QueueItem] = []
    served = 0
    for record, _features, score in serve_and_score(
        records, model=model, engine=engine, batch=batch
    ):
        event = record.event
        if collect_after is not None and event.event_time < collect_after:
            continue
        served += 1
        action, _ = decide.decide(score, event)
        if action is not Action.REVIEW:
            continue
        items.append(
            QueueItem(
                event_id=event.event_id,
                arrived_at=event.event_time,
                score=score,
                amount_cents=event.amount_cents,
                is_fraud=record.label.is_fraud,
            )
        )
    return items, served


def by_day(items: Sequence[QueueItem]) -> list[list[QueueItem]]:
    """Group the queue's items by the day they arrived.

    Args:
        items: The queue's items.

    Returns:
        One list per day with any arrivals, in date order.
    """
    days: dict[dt.date, list[QueueItem]] = {}
    for item in items:
        days.setdefault(item.arrived_at.date(), []).append(item)
    return [days[day] for day in sorted(days)]


def evaluate_queue(
    records: Iterable[Labelled],
    *,
    model: BatchModel,
    costs: Costs | None = None,
    rules: DecisionRules | None = None,
    engine: FeatureEngine | None = None,
    collect_after: dt.datetime | None = None,
    analysts: int = ANALYSTS,
    reviews_per_analyst_hour: int = REVIEWS_PER_ANALYST_HOUR,
) -> dict[str, Any]:
    """Measure expected-loss ranking against score ranking on a real queue.

    Args:
        records: Transactions with their labels, in event-time order.
        model: The scoring model.
        costs: The prices; ADR 13's defaults by default.
        rules: The decision rules; the shipped defaults by default.
        engine: The engine to serve from; the platform's own by default.
        collect_after: Keep only items arriving at or after this moment, the
            champion's training cutoff.
        analysts: Analysts on shift.
        reviews_per_analyst_hour: Reviews one analyst completes in an hour.

    Returns:
        The report: what the stream held, what reached the queue, and what
        the two policies caught, with a bootstrap interval over days.

    Raises:
        ValueError: If the stream produced fewer than two days of queue.
    """
    prices = costs or Costs()
    items, served = review_items(
        records, model=model, rules=rules, engine=engine, collect_after=collect_after
    )
    days = by_day(items)
    if len(days) < 2:
        msg = f"an interval over days needs at least two days of queue, got {len(days)}"
        raise ValueError(msg)
    comparison = compare_policies(
        days,
        costs=prices,
        analysts=analysts,
        reviews_per_analyst_hour=reviews_per_analyst_hour,
    )
    frauds = sum(item.is_fraud for item in items)
    return {
        "track": "synthetic, offline replay",
        "model": model.version,
        "scored_after_cutoff": served,
        "cutoff": collect_after.isoformat() if collect_after else None,
        "queued": len(items),
        "queued_share": round(len(items) / served, 6) if served else 0.0,
        "queue_fraud_share": round(frauds / len(items), 6) if items else 0.0,
        "days": len(days),
        "costs": asdict(prices),
        "capacity": {
            "analysts": analysts,
            "reviews_per_analyst_hour": reviews_per_analyst_hour,
            "reviews_per_day": analysts * reviews_per_analyst_hour * 24,
        },
        "caught_per_analyst_hour_cents": {
            "by_score": round(comparison.score_per_hour, 2),
            "by_expected_loss": round(comparison.loss_per_hour, 2),
            "difference": round(comparison.difference, 2),
            "low": round(comparison.low, 2),
            "high": round(comparison.high, 2),
            "resamples": comparison.resamples,
        },
    }


def write_report(report: dict[str, Any], path: Path) -> Path:
    """Write a queue report as JSON.

    Args:
        report: What `evaluate_queue` returned.
        path: Where it goes.

    Returns:
        The path written.
    """
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path
