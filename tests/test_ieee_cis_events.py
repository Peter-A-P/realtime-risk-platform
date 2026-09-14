"""Mapping competition rows onto the platform's events.

The competition data may not be committed, so every row here is synthetic,
written in the shape of the published file. The tests pin the three
decisions ADR 17 records (what a card is, that there is no device or
merchant, where the clock starts) and the properties that make the mapped
replay safe to compute features from: nothing about the outcome on the event,
time order refused rather than repaired, and a feature set that does not
quietly include features this track cannot compute.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping
from pathlib import Path

import pytest
from typer.testing import CliRunner

from verdict.cli import DATA_ENV, app
from verdict.events.ieee_cis import TRANSACTION_FILE
from verdict.events.ieee_cis_events import (
    IEEE_CIS_EPOCH,
    LABEL_DELAY,
    USED_COLUMNS,
    MappingError,
    amount_cents_of,
    card_id_for,
    card_key,
    event_time_of,
    features_on_track,
    iter_records,
    map_row,
    write_event_log,
)
from verdict.events.rawlog import LABELS_FILE, TRANSACTIONS_FILE, read_labels, read_transactions
from verdict.events.schema import EntryMode, TransactionEvent
from verdict.features.engine import FeatureEngine, FeatureRow
from verdict.features.replay_check import check_replay, is_sampled
from verdict.store.features import FEATURE_SET, EntityKind, feature_names
from verdict.store.leakage import check_point_in_time

HEADER = (
    "TransactionID,isFraud,TransactionDT,TransactionAmt,ProductCD,card1,card2,card3,card4,"
    "card5,card6,addr1,addr2,P_emaildomain,D1,V1"
)


def a_row(**cells: str) -> dict[str, str | None]:
    """One row as the mapper receives it, with anything not given filled in."""
    row: dict[str, str | None] = {
        "TransactionID": "3000001",
        "isFraud": "0",
        "TransactionDT": "86400",
        "TransactionAmt": "49.95",
        "card1": "13926",
        "card2": "321.0",
        "card3": "150.0",
        "card4": "visa",
        "card5": "226.0",
        "card6": "debit",
        "addr1": "315.0",
        "D1": "0.0",
    }
    row.update(cells)
    return {name: (value if value != "" else None) for name, value in row.items()}


def csv_line(row: Mapping[str, str | None]) -> str:
    values = {name: ("" if value is None else value) for name, value in row.items()}
    columns = HEADER.split(",")
    return ",".join(values.get(name, "W" if name == "ProductCD" else "") for name in columns)


def write_file(directory: Path, rows: list[dict[str, str | None]]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    lines = [HEADER, *(csv_line(row) for row in rows)]
    (directory / TRANSACTION_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return directory


def a_replay() -> list[dict[str, str | None]]:
    """A small replay with the shapes that matter.

    One card seen over three days with `D1` counting up, so its key holds;
    a burst of three events at one instant on another card; a row with no
    `addr1`, which cannot be linked; and ordinary traffic in between.
    """
    rows: list[dict[str, str | None]] = []
    ident = 3_000_000
    for step in range(120):
        ident += 1
        seconds = 86_400 + step * 900
        day_offset = (seconds // 86_400) - 1
        card = str(10_000 + step % 7)
        rows.append(
            a_row(
                TransactionID=str(ident),
                TransactionDT=str(seconds),
                TransactionAmt=f"{10 + step % 13}.{step % 100:02d}",
                card1=card,
                D1=f"{day_offset}.0",
                isFraud="1" if step % 17 == 0 else "0",
            )
        )
    burst_at = 86_400 + 40 * 900 + 1
    for _ in range(3):
        ident += 1
        rows.append(
            a_row(
                TransactionID=str(ident),
                TransactionDT=str(burst_at),
                card1="99999",
                TransactionAmt="1.00",
                D1="0.0",
            )
        )
    ident += 1
    rows.append(a_row(TransactionID=str(ident), TransactionDT=str(burst_at + 5), addr1=""))
    rows.sort(key=lambda row: int(str(row["TransactionDT"])))
    return rows


class ObserveImmediatelyEngine(FeatureEngine):
    """The engine before the same-instant fix: serve, then observe at once.

    The same bug `tests/test_engine.py` keeps as a fixture, restated here
    because test modules are not importable from one another in this layout.
    """

    def process(self, event: TransactionEvent) -> list[FeatureRow]:
        """Serve, then observe immediately, which is the bug.

        Args:
            event: The event.

        Returns:
            The features, which can include events sharing this instant.
        """
        rows = self.serve(event)
        self.observe(event)
        return rows


# --- the clock and the money ----------------------------------------------


def test_the_clock_starts_at_the_published_convention() -> None:
    assert dt.datetime(2017, 12, 1, tzinfo=dt.UTC) == IEEE_CIS_EPOCH
    assert event_time_of(86_400) == dt.datetime(2017, 12, 2, tzinfo=dt.UTC)
    assert event_time_of(15_811_131).tzinfo is dt.UTC


def test_a_negative_offset_is_refused() -> None:
    with pytest.raises(MappingError):
        event_time_of(-1)


@pytest.mark.parametrize(
    ("text", "cents", "rounded"),
    [
        ("49.95", 4_995, False),
        ("68.5", 6_850, False),
        ("0.285", 28, True),  # 28.5 cents rounds to the even 28
        ("0.295", 30, True),  # 29.5 cents rounds to the even 30
        ("31937.391", 3_193_739, True),
    ],
)
def test_amounts_are_decimal_and_round_half_to_even(text: str, cents: int, rounded: bool) -> None:
    """Parsed from the file's text, never through a binary float."""
    assert amount_cents_of(text) == (cents, rounded)


@pytest.mark.parametrize("text", ["0.004", "0", "-3.00", "twelve"])
def test_an_amount_that_is_not_a_positive_cent_is_refused(text: str) -> None:
    with pytest.raises(MappingError):
        amount_cents_of(text)


# --- what a card is -------------------------------------------------------


def test_one_card_keeps_its_key_as_the_days_go_by() -> None:
    """`D1` counts days since the account started, so day less `D1` holds still."""
    monday = a_row(TransactionDT=str(86_400 * 3), D1="10.0")
    thursday = a_row(TransactionDT=str(86_400 * 6 + 3_600), D1="13.0")
    assert card_key(monday) == card_key(thursday)


def test_a_different_account_start_is_a_different_card() -> None:
    """Same issuer, product and billing region, opened on another day."""
    first = a_row(TransactionDT=str(86_400 * 3), D1="10.0")
    second = a_row(TransactionDT=str(86_400 * 3), D1="40.0")
    assert card_key(first) != card_key(second)


def test_the_file_writing_an_integer_as_a_float_does_not_split_a_card() -> None:
    assert card_key(a_row(addr1="315.0", card2="321")) == card_key(
        a_row(addr1="315", card2="321.0")
    )


@pytest.mark.parametrize("missing", ["addr1", "D1"])
def test_a_row_that_cannot_form_the_key_is_unlinked_not_merged(missing: str) -> None:
    """Two unlinkable rows must not become one card with a shared history."""
    first = map_row(a_row(TransactionID="1", **{missing: ""}))
    second = map_row(a_row(TransactionID="2", **{missing: ""}))
    assert not first.card_linked
    assert first.event.card_id != second.event.card_id
    assert first.event.card_id == card_id_for(None, "1")


def test_a_linked_card_identifier_carries_no_raw_column_values() -> None:
    """The identifier is a hash, so an event log line does not spell out the key."""
    event = map_row(a_row()).event
    assert event.card_id.startswith("ieee-card-")
    assert "13926" not in event.card_id
    assert len(event.card_id) <= 64


# --- what the event carries -----------------------------------------------


def test_there_is_no_device_merchant_or_session_on_this_track() -> None:
    event = map_row(a_row()).event
    assert event.device_id is None
    assert event.merchant_id is None
    assert event.merchant_category is None
    assert event.session_id is None
    assert event.entry_mode is EntryMode.ECOMMERCE


def test_the_outcome_travels_separately_and_a_week_later() -> None:
    record = map_row(a_row(isFraud="1"))
    assert "is_fraud" not in record.event.model_dump()
    assert record.label.event_id == record.event.event_id
    assert record.label.is_fraud is True
    assert record.label.label_time == record.event.event_time + LABEL_DELAY


@pytest.mark.parametrize("cells", [{"isFraud": "2"}, {"isFraud": ""}, {"TransactionID": ""}])
def test_a_row_without_an_id_or_a_binary_label_is_refused(cells: dict[str, str]) -> None:
    with pytest.raises(MappingError):
        map_row(a_row(**cells))


# --- reading the file -----------------------------------------------------


def test_the_file_is_read_in_its_own_order(tmp_path: Path) -> None:
    rows = a_replay()
    records = list(iter_records(write_file(tmp_path, rows), chunk_size=17))
    assert [record.event.event_id for record in records] == [
        f"ieee-{row['TransactionID']}" for row in rows
    ]


def test_a_file_out_of_time_order_is_refused_not_sorted(tmp_path: Path) -> None:
    rows = a_replay()
    rows[10], rows[50] = rows[50], rows[10]
    with pytest.raises(MappingError, match="not in TransactionDT order"):
        list(iter_records(write_file(tmp_path, rows)))


def test_a_file_missing_a_column_the_mapper_reads_is_refused(tmp_path: Path) -> None:
    (tmp_path / TRANSACTION_FILE).write_text(
        "TransactionID,isFraud,TransactionDT,TransactionAmt\n1,0,86400,10.00\n", encoding="utf-8"
    )
    with pytest.raises(MappingError, match="lacks columns"):
        list(iter_records(tmp_path))


def test_the_mapper_reads_nothing_it_does_not_name() -> None:
    """A mapper that grew a column would be a mapper whose ADR is out of date."""
    assert set(USED_COLUMNS) == {
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "isFraud",
        "D1",
        "card1",
        "card2",
        "card3",
        "card4",
        "card5",
        "card6",
        "addr1",
    }


def test_the_event_log_is_two_files_replaced_not_appended(tmp_path: Path) -> None:
    source = write_file(tmp_path / "source", a_replay())
    out = tmp_path / "events"
    first = write_event_log(iter_records(source), out)
    second = write_event_log(iter_records(source), out)
    assert first.rows == second.rows == len(a_replay())
    assert len(list(read_transactions(out))) == first.rows
    assert len(list(read_labels(out))) == first.rows
    assert sorted(path.name for path in out.iterdir()) == sorted([TRANSACTIONS_FILE, LABELS_FILE])
    report = json.loads(second.to_json())
    assert report["unlinked_rows"] == 1
    assert report["entities_on_track"] == ["card"]


# --- which features exist here --------------------------------------------


def test_only_card_features_that_do_not_count_merchants_are_on_the_track() -> None:
    names = feature_names(features_on_track())
    assert names == (
        "card_txn_count_1h",
        "card_txn_count_24h",
        "card_amount_sum_1h",
        "card_amount_mean_24h",
        "card_amount_max_24h",
        "card_seconds_since_last",
    )
    assert "card_distinct_merchants_24h" not in names


def test_every_feature_off_the_track_is_off_for_a_stated_reason() -> None:
    """Ten of sixteen are off, and each is keyed or aggregated on an absent thing."""
    on = set(features_on_track())
    off = [spec for spec in FEATURE_SET if spec not in on]
    assert len(off) == 10
    for spec in off:
        assert spec.entity is not EntityKind.CARD or spec.field == "merchant_id"


# --- the leakage check, on the mapped replay ------------------------------


def test_the_engine_passes_the_full_point_in_time_check_on_a_mapped_replay(
    tmp_path: Path,
) -> None:
    events = [record.event for record in iter_records(write_file(tmp_path, a_replay()))]
    specs = features_on_track()
    engine = FeatureEngine(specs)
    served: dict[tuple[str, str, dt.datetime], float] = {}
    for event in events:
        for row in engine.process(event):
            for name, value in row.values.items():
                served[(name, row.entity_id, row.as_of)] = value

    report = check_point_in_time(
        specs, events, lambda spec, entity, at: served[(spec.name, entity, at)]
    )
    assert report.clean, report.summary()
    assert report.rows_checked == len(events) * len(specs)


def test_the_sampled_check_agrees_with_the_full_one_when_it_samples_everything(
    tmp_path: Path,
) -> None:
    events = [record.event for record in iter_records(write_file(tmp_path, a_replay()))]
    result = check_replay(events, features_on_track(), per_mille=1000)
    assert result.report.clean, result.report.summary()
    assert result.events == result.events_kept == len(events)
    assert result.report.rows_checked == len(events) * len(features_on_track())


def test_the_sampled_check_catches_the_unfixed_engine(tmp_path: Path) -> None:
    """The burst of three at one instant is what the unfixed engine gets wrong.

    And it gets exactly two of the three wrong: the first event in the burst
    sees nothing either way. Counting three would mean the check compared
    every event in the burst with the value served to the last one, which is
    the fault this assertion was added to pin.
    """
    events = [record.event for record in iter_records(write_file(tmp_path, a_replay()))]
    specs = features_on_track()
    result = check_replay(events, specs, per_mille=1000, engine=ObserveImmediatelyEngine(specs))
    counts = [v for v in result.report.violations if v.feature == "card_txn_count_1h"]
    assert sorted(v.served for v in counts) == [1.0, 2.0]
    assert all(v.reference == -1.0 for v in counts)


def test_the_sample_is_a_property_of_the_identifier_alone() -> None:
    ids = [f"ieee-card-{index:06d}" for index in range(20_000)]
    chosen = [entity for entity in ids if is_sampled(entity, 20)]
    assert chosen == [entity for entity in ids if is_sampled(entity, 20)]
    assert 0.015 < len(chosen) / len(ids) < 0.025
    assert all(is_sampled(entity, 1000) for entity in ids[:50])


def test_a_sample_rate_outside_one_to_a_thousand_is_refused() -> None:
    with pytest.raises(ValueError, match="per_mille"):
        check_replay([], features_on_track(), per_mille=0)


# --- the command line -----------------------------------------------------


def test_data_events_reads_the_directory_from_the_environment(tmp_path: Path) -> None:
    source = write_file(tmp_path / "elsewhere", a_replay())
    out = tmp_path / "events"
    result = CliRunner().invoke(
        app, ["data", "events", "--out", str(out)], env={DATA_ENV: str(source)}
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["rows"] == len(a_replay())


def test_data_check_passes_on_the_mapped_replay_and_prints_counts_only(tmp_path: Path) -> None:
    source = write_file(tmp_path / "elsewhere", a_replay())
    result = CliRunner().invoke(
        app, ["data", "check", "--per-mille", "1000"], env={DATA_ENV: str(source)}
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.stdout)
    assert report["violations"] == 0
    assert report["events_replayed"] == len(a_replay())
    assert "ieee-card-" not in result.stdout
