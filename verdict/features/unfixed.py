"""The feature engine as it was before the same-instant leak was found. Wrong on purpose.

Never used to serve a decision. It exists for two measurements:

- the leakage check's own test, which must show the check can still fail
  (`tests/test_engine.py`, `tests/test_ieee_cis_events.py`);
- the leak's cost, which `docs/leak-caught.md` left open until a model
  existed: the champion is trained and evaluated on features from this
  engine and from the fixed one, and the difference in offline PR-AUC is
  what the leak would have claimed that production could never deliver
  (`verdict/models/leak.py`).

The bug: an event was observed as soon as it was served, so a second event
at the same instant saw the first inside its `[t - w, t)` window.
"""

from __future__ import annotations

from verdict.events.schema import TransactionEvent
from verdict.features.engine import FeatureEngine, FeatureRow


class ObserveImmediatelyEngine(FeatureEngine):
    """Serve, then observe at once: the same-instant leak, kept for measuring it."""

    def process(self, event: TransactionEvent) -> list[FeatureRow]:
        """Serve, then observe immediately, which is the bug.

        Args:
            event: The event.

        Returns:
            The features, computed before this event but after any event
            sharing its timestamp.
        """
        rows = self.serve(event)
        self.observe(event)
        return rows
