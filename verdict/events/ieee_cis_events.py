"""Turning IEEE-CIS rows into the platform's events.

`ieee_cis.py` takes the archive in and reports what is in it. This module
maps each transaction row onto the wire schema, so the real-data track runs
through the same engine, the same store and the same leakage test as the
synthetic one. ADR 17 records the decisions and the measurements behind them;
the short version is three answers to three questions the file does not
answer for itself.

**What is a card?** No column is. `card1` to `card6` describe a card's issuer
and product, and the busiest value of all six together holds 14,112
transactions, which is a card programme rather than a card. Adding the
billing region (`addr1`) and the day the account started, which is the
transaction day less `D1`, gives something that behaves like one: a median of
one transaction per key, a 99th percentile of 19, and fraud labels that agree
within a key on 98.6 percent of keys with more than one transaction, against
about 85 percent for the card columns alone. That construction is the one the
competition's first-place write-up published. A row missing `addr1` or `D1`
cannot form the key, and its card is recorded as unlinked: an identifier used
by that transaction only, so it has no history rather than a borrowed one.

**What is a device?** Nothing here. The identity columns describe a
configuration (operating system, browser version, screen size), and the
fingerprints with more than a hundred transactions hold 55 percent of the rows
that have one. A device feature keyed on that would count how popular a
browser is. `device_id` is `None` on this track.

**What is the event time?** `TransactionDT` is seconds from an unstated
reference. Public analyses of the set take the reference as 2017-12-01, and
this module uses that date as a fixed convention rather than a claim: nothing
downstream reads the calendar, only differences between times, and those do
not depend on the choice. It is published here and does not change.

Everything else follows from the file. There is no merchant identifier, so
`merchant_id` and `merchant_category` are `None`. The set is card-not-present
e-commerce throughout, so every entry mode is `ECOMMERCE`. Amounts carry up
to three decimal places, from currency conversion upstream, and are rounded
half to even to whole cents.

Nothing this module produces may be committed or published: it is a
row-by-row transformation of the competition data (`docs/data.md`).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from verdict.events.ieee_cis import TRANSACTION_FILE
from verdict.events.rawlog import LABELS_FILE, TRANSACTIONS_FILE
from verdict.events.schema import EntryMode, LabelEvent, TransactionEvent
from verdict.store.features import FEATURE_SET, EntityKind, FeatureSpec

IEEE_CIS_EPOCH: Final = dt.datetime(2017, 12, 1, tzinfo=dt.UTC)
"""The reference `TransactionDT` counts from, by published convention.

Fixed. Changing it moves every event time and no feature value, which is
exactly why it is safe to fix and pointless to revisit.
"""

LABEL_DELAY: Final = dt.timedelta(days=7)
"""How long after a transaction its label is taken to arrive.

The same simulated delay as the synthetic track. The competition publishes
the outcome, not when it became known.
"""

SECONDS_PER_DAY: Final = 86_400

CARD_COLUMNS: Final[tuple[str, ...]] = (
    "card1",
    "card2",
    "card3",
    "card4",
    "card5",
    "card6",
    "addr1",
)
"""The columns that, with the account start day, make a card key."""

REQUIRED_FOR_LINK: Final[tuple[str, ...]] = ("addr1", "D1")
"""Without these the key cannot be formed, and the card is unlinked."""

USED_COLUMNS: Final[tuple[str, ...]] = (
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "isFraud",
    "D1",
    *CARD_COLUMNS,
)
"""Every column this mapper reads. Nothing else in the file is touched."""

ENTITIES_ON_TRACK: Final = frozenset({EntityKind.CARD})
"""The entities an IEEE-CIS event can name. Only the card."""

ABSENT_FIELDS: Final = frozenset({"device_id", "merchant_id", "merchant_category", "session_id"})
"""Fields that are `None` on every event this mapper produces."""

_MISSING: Final = "~"
"""How an empty card column is written into a key."""


class MappingError(ValueError):
    """Raised when the file is not shaped the way this mapper was written for."""


@dataclass(frozen=True, slots=True)
class MappedRecord:
    """One competition row, as the platform sees it.

    Attributes:
        event: The transaction, carrying nothing the acquirer would not have
            known when it was presented.
        label: Its outcome, arriving `LABEL_DELAY` later.
        card_linked: Whether the card key could be formed.
        amount_rounded: Whether the amount had to be rounded to a cent.
    """

    event: TransactionEvent
    label: LabelEvent
    card_linked: bool
    amount_rounded: bool


@dataclass(slots=True)
class MappingReport:
    """What a mapping pass produced, in counts only.

    Counts, never rows: this is the part of a real-data run that may be
    published.

    Attributes:
        rows: Rows mapped.
        fraud_rows: Rows labelled fraud.
        linked_rows: Rows whose card key could be formed.
        amounts_rounded: Rows whose amount was rounded to a cent.
        first_event: The earliest event time.
        last_event: The latest event time.
        cards: Distinct linked card keys.
    """

    rows: int = 0
    fraud_rows: int = 0
    linked_rows: int = 0
    amounts_rounded: int = 0
    first_event: dt.datetime | None = None
    last_event: dt.datetime | None = None
    _cards: set[str] = field(default_factory=set)

    def add(self, record: MappedRecord) -> None:
        """Count one record.

        Args:
            record: The record.
        """
        self.rows += 1
        self.fraud_rows += int(record.label.is_fraud)
        self.amounts_rounded += int(record.amount_rounded)
        if record.card_linked:
            self.linked_rows += 1
            self._cards.add(record.event.card_id)
        if self.first_event is None:
            self.first_event = record.event.event_time
        self.last_event = record.event.event_time

    @property
    def cards(self) -> int:
        """How many distinct linked cards were seen.

        Returns:
            The count.
        """
        return len(self._cards)

    def to_json(self) -> str:
        """Render the report.

        Returns:
            Pretty-printed JSON.
        """
        rows = max(1, self.rows)
        return json.dumps(
            {
                "rows": self.rows,
                "fraud_share": round(self.fraud_rows / rows, 5),
                "linked_share": round(self.linked_rows / rows, 4),
                "unlinked_rows": self.rows - self.linked_rows,
                "linked_cards": self.cards,
                "amounts_rounded_share": round(self.amounts_rounded / rows, 4),
                "first_event": None if self.first_event is None else self.first_event.isoformat(),
                "last_event": None if self.last_event is None else self.last_event.isoformat(),
                "epoch": IEEE_CIS_EPOCH.isoformat(),
                "entities_on_track": sorted(str(kind) for kind in ENTITIES_ON_TRACK),
                "features_on_track": [spec.name for spec in features_on_track()],
            },
            indent=2,
        )


def event_time_of(transaction_dt: int) -> dt.datetime:
    """Place a `TransactionDT` offset on the platform's clock.

    Args:
        transaction_dt: Whole seconds from the reference.

    Returns:
        The event time, timezone-aware UTC.

    Raises:
        MappingError: If the offset is negative.
    """
    if transaction_dt < 0:
        msg = f"TransactionDT cannot be negative, got {transaction_dt}"
        raise MappingError(msg)
    return IEEE_CIS_EPOCH + dt.timedelta(seconds=transaction_dt)


def amount_cents_of(text: str) -> tuple[int, bool]:
    """Convert a `TransactionAmt` value to whole cents.

    Parsed as a decimal from the file's own text, never through a float, so
    `0.285` is two hundred and eighty-five thousandths and not whatever a
    binary fraction makes of it.

    Args:
        text: The amount as written in the file.

    Returns:
        The amount in cents, rounded half to even, and whether rounding
        changed it.

    Raises:
        MappingError: If the amount is not a number or is not positive once
            rounded, which the schema would refuse anyway, with a less useful
            message.
    """
    try:
        dollars = Decimal(text.strip())
    except InvalidOperation as error:
        msg = f"TransactionAmt {text!r} is not a number"
        raise MappingError(msg) from error
    exact = dollars * 100
    cents = exact.quantize(Decimal(1), rounding=ROUND_HALF_EVEN)
    if cents <= 0:
        msg = f"TransactionAmt {text!r} is not a positive number of cents"
        raise MappingError(msg)
    return int(cents), cents != exact


def _normalise(value: str | None) -> str:
    """Write a card column's value the same way however pandas read it.

    The file writes `315.0` for a value that is an integer category, and an
    empty cell for a missing one.

    Args:
        value: The raw cell.

    Returns:
        A canonical string, `~` for missing.
    """
    if value is None:
        return _MISSING
    text = value.strip()
    if not text or text.lower() == "nan":
        return _MISSING
    try:
        number = float(text)
    except ValueError:
        return text
    return str(int(number)) if number.is_integer() else text


def card_key(row: Mapping[str, str | None]) -> str | None:
    """Form the card key for one row, or say that it cannot be formed.

    Args:
        row: The row's cells, by column name.

    Returns:
        The key, or None when `addr1` or `D1` is missing.

    Raises:
        MappingError: If `TransactionDT` or `D1` is not a number.
    """
    if any(_normalise(row.get(name)) == _MISSING for name in REQUIRED_FOR_LINK):
        return None
    try:
        day = int(float(str(row["TransactionDT"]))) // SECONDS_PER_DAY
        days_since_start = int(float(str(row["D1"])))
    except (KeyError, ValueError) as error:
        msg = f"cannot read TransactionDT and D1 from row {row.get('TransactionID')!r}"
        raise MappingError(msg) from error
    parts = [_normalise(row.get(name)) for name in CARD_COLUMNS]
    parts.append(str(day - days_since_start))
    return "|".join(parts)


def card_id_for(key: str | None, transaction_id: str) -> str:
    """Turn a card key into the identifier the platform keys on.

    Args:
        key: The card key, or None if it could not be formed.
        transaction_id: The row's `TransactionID`.

    Returns:
        A stable hashed identifier for a linked card, or an identifier that
        belongs to this one transaction for an unlinked one.
    """
    if key is None:
        return f"ieee-unlinked-{transaction_id}"
    return f"ieee-card-{hashlib.sha256(key.encode('utf-8')).hexdigest()[:20]}"


def map_row(row: Mapping[str, str | None]) -> MappedRecord:
    """Map one competition row onto an event and its label.

    Args:
        row: The row's cells, by column name, as text.

    Returns:
        The mapped record.

    Raises:
        MappingError: If a required cell is missing or malformed.
    """
    try:
        transaction_id = _normalise(row["TransactionID"])
        seconds = int(float(str(row["TransactionDT"])))
        amount_text = str(row["TransactionAmt"])
        fraud_text = _normalise(row["isFraud"])
    except (KeyError, ValueError) as error:
        msg = f"row {row.get('TransactionID')!r} lacks a required cell: {error}"
        raise MappingError(msg) from error
    if transaction_id == _MISSING or fraud_text not in {"0", "1"}:
        msg = f"row {row.get('TransactionID')!r} has no id or no 0/1 label"
        raise MappingError(msg)

    key = card_key(row)
    cents, rounded = amount_cents_of(amount_text)
    event_time = event_time_of(seconds)
    event = TransactionEvent(
        event_id=f"ieee-{transaction_id}",
        event_time=event_time,
        card_id=card_id_for(key, transaction_id),
        device_id=None,
        merchant_id=None,
        amount_cents=cents,
        merchant_category=None,
        entry_mode=EntryMode.ECOMMERCE,
    )
    label = LabelEvent(
        event_id=event.event_id,
        label_time=event_time + LABEL_DELAY,
        is_fraud=fraud_text == "1",
    )
    return MappedRecord(
        event=event, label=label, card_linked=key is not None, amount_rounded=rounded
    )


def iter_records(directory: Path, *, chunk_size: int = 100_000) -> Iterator[MappedRecord]:
    """Map the competition's training transactions, in the file's time order.

    The file is already ordered by `TransactionDT`, and this refuses rather
    than sorts if that ever stops being true, for the same reason the raw-log
    replay does: a replay that quietly reorders its input is not a replay of
    it.

    Args:
        directory: Where the competition files are.
        chunk_size: Rows read at a time.

    Yields:
        One mapped record per row.

    Raises:
        FileNotFoundError: If the transaction file is not there.
        MappingError: If a column this mapper needs is absent, or the file is
            not in time order.
    """
    import pandas as pd

    path = directory / TRANSACTION_FILE
    if not path.exists():
        msg = f"no {TRANSACTION_FILE} in {directory}"
        raise FileNotFoundError(msg)
    header = tuple(str(name) for name in pd.read_csv(path, nrows=0).columns)
    missing = [name for name in USED_COLUMNS if name not in header]
    if missing:
        msg = f"{path} lacks columns this mapper reads: {missing}"
        raise MappingError(msg)

    previous: int | None = None
    chunks = pd.read_csv(
        path, usecols=list(USED_COLUMNS), dtype=str, keep_default_na=False, chunksize=chunk_size
    )
    for chunk in chunks:
        for cells in chunk.to_dict(orient="records"):
            row = {
                str(name): (None if value == "" else str(value)) for name, value in cells.items()
            }
            record = map_row(row)
            seconds = int(float(str(row["TransactionDT"])))
            if previous is not None and seconds < previous:
                msg = (
                    f"{path} is not in TransactionDT order at TransactionID "
                    f"{row['TransactionID']}: {seconds} follows {previous}"
                )
                raise MappingError(msg)
            previous = seconds
            yield record


def features_on_track(specs: Sequence[FeatureSpec] = FEATURE_SET) -> tuple[FeatureSpec, ...]:
    """The features this track can compute honestly.

    A feature is on the track when its entity exists here and the field it
    aggregates is not one that is always absent. The second condition is not
    a formality: `card_distinct_merchants_24h` is keyed on the card, which
    exists, but counts merchants, which do not, and would report exactly one
    distinct merchant on every row.

    Args:
        specs: The features to filter. Defaults to the platform's own set.

    Returns:
        The features that remain, in definition order.
    """
    return tuple(
        spec
        for spec in specs
        if spec.entity in ENTITIES_ON_TRACK
        and (spec.field is None or spec.field not in ABSENT_FIELDS)
    )


def write_event_log(records: Iterable[MappedRecord], directory: Path) -> MappingReport:
    """Write mapped records as a raw event log, and count what went in.

    Two files, not the synthetic track's three: there is no ground truth here
    beyond the label, so there is no ground-truth file to write. The output
    belongs under `data/`, which is ignored in full.

    Args:
        records: The records, in time order.
        directory: Where to write. Created if missing; existing logs are
            replaced rather than appended to, because a second mapping pass
            appended to a first would be a log with every event twice.

    Returns:
        The counts.
    """
    directory.mkdir(parents=True, exist_ok=True)
    report = MappingReport()
    with (
        (directory / TRANSACTIONS_FILE).open("w", encoding="utf-8") as transactions,
        (directory / LABELS_FILE).open("w", encoding="utf-8") as labels,
    ):
        for record in records:
            transactions.write(record.event.to_json())
            transactions.write("\n")
            labels.write(record.label.to_json())
            labels.write("\n")
            report.add(record)
    return report
