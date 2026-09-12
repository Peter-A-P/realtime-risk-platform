"""Replaying the raw log in time order.

Everything downstream, the leakage test, the parity test, training and the
queue evaluation, starts from a replay. A replay has to satisfy two
properties, and this module's job is to enforce them rather than to hope:

1. **It is in event-time order.** The generator emits in order, but a raw log
   can be concatenated from several runs, and at-least-once delivery means a
   live capture can contain anything. An out-of-order replay silently
   corrupts every window, so `replay` checks as it goes rather than trusting.
2. **It can be cut at a point in time.** "What did the world look like at
   `t`?" is the question the leakage test asks constantly, and the only
   honest answer comes from the events strictly before `t`.

The IEEE-CIS replay that this module will also have to serve is **not here
yet**, and deliberately so: its competition terms have not been read, so
whether this repository may use it at all is unresolved (`docs/data.md`).
Writing a column mapping against a dataset that may have to be swapped for
another is work done in the wrong order.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path

from verdict.events.rawlog import read_transactions
from verdict.events.schema import TransactionEvent


class OutOfOrderError(ValueError):
    """Raised when a replay finds an event older than the one before it."""

    def __init__(self, previous: TransactionEvent, offending: TransactionEvent) -> None:
        """Name both events, so the log can be inspected at that point.

        Args:
            previous: The event that came before.
            offending: The event that went backwards.
        """
        super().__init__(
            f"raw log is out of order: {offending.event_id} at "
            f"{offending.event_time.isoformat()} follows {previous.event_id} at "
            f"{previous.event_time.isoformat()}"
        )
        self.previous = previous
        self.offending = offending


def in_time_order(
    events: Iterable[TransactionEvent], *, strict: bool = True
) -> Iterator[TransactionEvent]:
    """Pass events through, checking they never go backwards.

    Args:
        events: The events to check.
        strict: Raise on the first event that goes backwards. With `strict`
            off, out-of-order events are dropped instead, which is what a
            live capture wants when it would rather lose a late event than
            corrupt every window after it. The count of what was dropped is
            the caller's business to report; the chaos tests in week 8 cover
            the behaviour deliberately.

    Yields:
        The events, in the order they arrived.

    Raises:
        OutOfOrderError: If an event precedes its predecessor and `strict`.
    """
    previous: TransactionEvent | None = None
    for event in events:
        if previous is not None and event.event_time < previous.event_time:
            if strict:
                raise OutOfOrderError(previous, event)
            continue
        previous = event
        yield event


def replay(directory: Path, *, strict: bool = True) -> Iterator[TransactionEvent]:
    """Replay a raw transaction log in event-time order.

    Args:
        directory: The raw log directory.
        strict: Whether to raise on an out-of-order event.

    Yields:
        Each transaction, validated through the same decoder a consumer uses.
    """
    yield from in_time_order(read_transactions(directory), strict=strict)


def replay_until(
    directory: Path, as_of: dt.datetime, *, strict: bool = True
) -> Iterator[TransactionEvent]:
    """Replay only what had happened strictly before a moment.

    This is the cut the leakage test needs: the world as a consumer standing
    at `as_of` would have seen it, with nothing from the present instant and
    nothing from the future.

    Args:
        directory: The raw log directory.
        as_of: The moment to stop at, exclusive.
        strict: Whether to raise on an out-of-order event.

    Yields:
        Transactions strictly before `as_of`.
    """
    for event in replay(directory, strict=strict):
        if event.event_time >= as_of:
            return
        yield event


def before(events: Sequence[TransactionEvent], as_of: dt.datetime) -> list[TransactionEvent]:
    """Return the events strictly before a moment, from an in-memory replay.

    Args:
        events: The events, in any order.
        as_of: The moment, exclusive.

    Returns:
        The eligible events, oldest first.
    """
    selected = [event for event in events if event.event_time < as_of]
    selected.sort(key=lambda event: event.event_time)
    return selected


def span(events: Sequence[TransactionEvent]) -> tuple[dt.datetime, dt.datetime] | None:
    """Report the first and last event times in a replay.

    Args:
        events: The events.

    Returns:
        The earliest and latest event time, or None if there are no events.
    """
    if not events:
        return None
    times = [event.event_time for event in events]
    return min(times), max(times)
