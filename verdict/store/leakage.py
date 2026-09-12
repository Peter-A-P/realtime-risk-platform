"""The point-in-time correctness test.

Written in week 2, before the first feature exists, and never weakened to make
a feature pass. A failing leakage test means the feature is wrong.

## What leakage is, and why nothing else catches it

A feature leaks when its value at time `t` depends on anything that happened
at or after `t`. The classic is a velocity count that includes the
transaction being scored: trivial to write, and enormously flattering,
because the count is one higher for exactly the rows the model is trying to
identify.

Nothing else in a normal pipeline catches this:

- Offline metrics do not, because the leak makes them *better*. A leak is
  indistinguishable from a good feature until the model reaches production.
- Production monitoring does not, because the model still returns scores.
  It merely underperforms, quietly, forever.
- A holdout split does not, because the leak is present in the holdout too.

So the test has to be structural rather than statistical. It does not ask
whether a feature looks suspicious. It recomputes the feature from the raw
event log using only events strictly before the moment it describes, and
compares that with what the store served.

## The two checks

**Point-in-time.** For a sample of rows, recompute every feature from the raw
log as of the event's own time and compare with the served value. Any
difference is a violation, and the report says which feature, which entity,
which moment, and by how much.

**Label-shift invariance.** Every training row has two times: when the
transaction happened, and when its label arrived seven days later. The
features must describe the first. Move the label times earlier and recompute:
a correct feature cannot move, because it never read the label time; a
training join that used the label time as its as-of point moves, because it
has just lost a week of future it should never have had.

The second check survives a mistake the first cannot see. If the serving path
and the reference shared a convention error, say `<=` where both meant `<`,
the first check would compare two equally wrong numbers and pass. The second
compares a value against itself under a different label time, so there is no
shared convention to hide behind.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from verdict.events.schema import TransactionEvent
from verdict.store.features import (
    EntityKind,
    FeatureSpec,
    entity_id_of,
    evaluate_spec,
)

ServedLookup = Callable[[FeatureSpec, str, dt.datetime], float]
"""How the harness asks the thing under test for a value.

Given a feature, an entity id and an as-of time, return what the store would
have served. In week 3 this wraps the online store and the offline recompute;
in the tests it wraps a deliberately correct or deliberately leaky
implementation.
"""

TOLERANCE: float = 1e-9
"""How close a served value must be to the reference.

These are counts, sums and second-differences, so they should match exactly.
The tolerance exists for float addition order, not as slack: it is nine
decimal places, far tighter than any real leak.
"""


@dataclass(frozen=True, slots=True)
class LeakageViolation:
    """One disagreement between a served feature and the reference.

    Attributes:
        feature: The feature's name.
        entity_id: The entity it was computed for.
        as_of: The moment it describes.
        served: What the store served.
        reference: What the reference computed from the raw log.
        check: Which check found it.
    """

    feature: str
    entity_id: str
    as_of: dt.datetime
    served: float
    reference: float
    check: str

    def __str__(self) -> str:
        """Render the violation for a test failure message.

        Returns:
            A single line naming the feature, the moment and the gap.
        """
        return (
            f"{self.feature} for {self.entity_id} as of {self.as_of.isoformat()}: "
            f"served {self.served}, reference {self.reference} "
            f"(difference {self.served - self.reference}, check: {self.check})"
        )


@dataclass(frozen=True, slots=True)
class LeakageReport:
    """The result of a leakage check.

    Attributes:
        violations: Every disagreement found.
        rows_checked: How many entity-and-moment pairs were examined.
        features_checked: How many features were examined.
    """

    violations: tuple[LeakageViolation, ...] = ()
    rows_checked: int = 0
    features_checked: int = 0

    @property
    def clean(self) -> bool:
        """Whether the check found nothing.

        Returns:
            True if there were no violations.
        """
        return not self.violations

    def summary(self) -> str:
        """Describe the result, for a test failure message or a log line.

        Returns:
            A human-readable summary, listing up to five violations.
        """
        if self.clean:
            return f"no leakage: {self.features_checked} features over {self.rows_checked} rows"
        head = "\n".join(f"  {violation}" for violation in self.violations[:5])
        more = f"\n  ... and {len(self.violations) - 5} more" if len(self.violations) > 5 else ""
        return (
            f"LEAKAGE: {len(self.violations)} violations across "
            f"{self.features_checked} features and {self.rows_checked} rows\n{head}{more}"
        )

    def raise_if_leaking(self) -> None:
        """Fail loudly if anything leaked.

        Raises:
            LeakageError: If there is at least one violation.
        """
        if not self.clean:
            raise LeakageError(self)


class LeakageError(AssertionError):
    """Raised when a feature depends on something it could not have known."""

    def __init__(self, report: LeakageReport) -> None:
        """Carry the report into the failure message.

        Args:
            report: The failing report.
        """
        super().__init__(report.summary())
        self.report = report


@dataclass
class _Collector:
    """Accumulates violations while a check runs."""

    violations: list[LeakageViolation] = field(default_factory=list)
    rows: int = 0

    def compare(
        self,
        spec: FeatureSpec,
        entity_id: str,
        as_of: dt.datetime,
        served: float,
        reference: float,
        check: str,
    ) -> None:
        """Record a comparison, keeping it only if it disagrees.

        Args:
            spec: The feature.
            entity_id: The entity.
            as_of: The moment.
            served: What the store served.
            reference: What the reference computed.
            check: Which check this is.
        """
        self.rows += 1
        if abs(served - reference) > TOLERANCE:
            self.violations.append(
                LeakageViolation(
                    feature=spec.name,
                    entity_id=entity_id,
                    as_of=as_of,
                    served=served,
                    reference=reference,
                    check=check,
                )
            )


def check_point_in_time(
    specs: Sequence[FeatureSpec],
    events: Sequence[TransactionEvent],
    served: ServedLookup,
    sample: Sequence[TransactionEvent] | None = None,
) -> LeakageReport:
    """Recompute every feature from the raw log and compare with the store.

    Args:
        specs: The features to check. An empty set passes: in week 2 there are
            no features yet, and the test standing green over nothing is the
            point, because week 3 cannot add one without it.
        events: The raw event log. The reference sees only this.
        served: How to ask the store what it would have served.
        sample: Which events to check, each at its own event time. Defaults to
            every event, which is what a small replay wants; a day of live
            traffic passes a sample instead.

    Returns:
        The report. Empty violations means every served value equalled a
        recomputation that could not see the present or the future.
    """
    rows = list(events if sample is None else sample)
    collector = _Collector()
    for spec in specs:
        for row in rows:
            entity_id = entity_id_of(row, spec.entity)
            if entity_id is None:
                continue
            reference = evaluate_spec(spec, events, entity_id, row.event_time)
            collector.compare(
                spec,
                entity_id,
                row.event_time,
                served(spec, entity_id, row.event_time),
                reference,
                "point-in-time",
            )
    return LeakageReport(
        violations=tuple(collector.violations),
        rows_checked=collector.rows,
        features_checked=len(specs),
    )


@dataclass(frozen=True, slots=True)
class TrainingRow:
    """One row of a training set, before its features are attached.

    A training row has two times, and confusing them is the second classic
    leak. The features must describe `event_time`, the moment the decision
    had to be made. `label_time` is when the outcome became known, seven days
    later, and nothing about the features may depend on it.

    Attributes:
        entity_ids: The entity identifier per kind, as strings.
        event_time: When the transaction happened, and the moment every
            feature on this row describes.
        label_time: When its outcome became known.
    """

    entity_ids: dict[str, str]
    event_time: dt.datetime
    label_time: dt.datetime


RetrievalLookup = Callable[[FeatureSpec, str, dt.datetime, dt.datetime], float]
"""How the harness asks the retrieval path for a training-time value.

Given a feature, an entity id, an event time and a label time, return the
value the training set would carry. A correct implementation ignores the
label time completely; the point of passing it in is to find out whether it
does.
"""


def check_label_shift_invariance(
    specs: Sequence[FeatureSpec],
    rows: Sequence[TrainingRow],
    retrieve: RetrievalLookup,
    shift: dt.timedelta = dt.timedelta(days=3),
) -> LeakageReport:
    """Move the label times earlier and assert nothing about the features moves.

    A label arrives seven days after its transaction. If the training-set
    join ever uses the label time rather than the event time as its as-of
    point, every feature silently gains a week of future, and the offline
    model looks wonderful. Shifting the label times exposes that directly:
    the correct features cannot move, because they never depended on the
    label time, and the leaky ones move because they did.

    This check survives a mistake that the point-in-time check cannot see. If
    the serving path and the reference shared a convention error, both would
    be wrong in the same direction and compare equal. Here each value is
    compared against itself computed under a different label time, so there
    is no shared convention to hide behind.

    Args:
        specs: The features to check.
        rows: Training rows, each carrying both of its times.
        retrieve: The training-time retrieval under test.
        shift: How far earlier to move the label times. Any non-zero shift
            that keeps the label after the event will do.

    Returns:
        The report.
    """
    collector = _Collector()
    for spec in specs:
        for row in rows:
            entity_id = row.entity_ids.get(str(spec.entity))
            if entity_id is None:
                continue
            as_labelled = retrieve(spec, entity_id, row.event_time, row.label_time)
            shifted = retrieve(spec, entity_id, row.event_time, row.label_time - shift)
            collector.compare(
                spec,
                entity_id,
                row.event_time,
                as_labelled,
                shifted,
                "label-shift-invariance",
            )
    return LeakageReport(
        violations=tuple(collector.violations),
        rows_checked=collector.rows,
        features_checked=len(specs),
    )


def training_rows_from(
    events: Sequence[TransactionEvent], label_delay: dt.timedelta = dt.timedelta(days=7)
) -> list[TrainingRow]:
    """Build training rows from a replay, with the platform's label delay.

    Args:
        events: The events to build rows for.
        label_delay: How long a label takes to arrive.

    Returns:
        One row per event, carrying every entity the event names.
    """
    rows: list[TrainingRow] = []
    for event in events:
        entity_ids = {
            str(kind): entity_id
            for kind in EntityKind
            if (entity_id := entity_id_of(event, kind)) is not None
        }
        rows.append(
            TrainingRow(
                entity_ids=entity_ids,
                event_time=event.event_time,
                label_time=event.event_time + label_delay,
            )
        )
    return rows
