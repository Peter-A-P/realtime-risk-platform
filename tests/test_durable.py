"""State files survive a machine stopped at any moment: the old or the new, never neither.

On 2026-09-29 a frozen instance was terminated and two files that had been
replaced by rename, without an fsync, were left empty: the transactions
feed's saved place and the alert relay's state. These tests hold every
replacement to syncing the data before the rename and leaving the old file
whole if anything fails first.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from verdict import durable
from verdict.live.feed import Feed, SnapshotStore


def test_a_replacement_carries_the_new_contents_and_leaves_nothing_behind(tmp_path: Path) -> None:
    path = tmp_path / "state" / "place.snapshot"
    durable.write_bytes(path, b"first")
    durable.write_bytes(path, b"second")
    assert path.read_bytes() == b"second"
    assert sorted(p.name for p in path.parent.iterdir()) == ["place.snapshot"]


def test_the_data_is_synced_before_the_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fsync(fd: int) -> None:
        order.append("fsync")
        real_fsync(fd)

    def replace(a: str, b: str) -> None:
        order.append("replace")
        real_replace(a, b)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)
    durable.write_text(tmp_path / "told.json", "{}")
    assert order.index("fsync") < order.index("replace")


def test_a_failure_before_the_rename_leaves_the_old_file_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "told.json"
    durable.write_text(path, '{"a": "b"}')

    def fail(a: object, b: object) -> None:
        raise OSError("the machine stopped")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError, match="stopped"):
        durable.write_text(path, '{"c": "d"}')
    assert path.read_text(encoding="utf-8") == '{"a": "b"}'
    assert [p.name for p in tmp_path.iterdir()] == ["told.json"]


def test_a_finished_file_from_another_writer_is_synced_and_moved(tmp_path: Path) -> None:
    temporary, path = tmp_path / "hour.parquet.tmp", tmp_path / "hour.parquet"
    temporary.write_bytes(b"rows")
    durable.settle(temporary, path)
    assert path.read_bytes() == b"rows"
    assert not temporary.exists()


def test_the_feed_saves_its_place_durably(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    synced: list[int] = []
    real_fsync = os.fsync

    def fsync(fd: int) -> None:
        synced.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    store = SnapshotStore(tmp_path, Feed.TRANSACTIONS)
    store.save(b"the generator's state")
    assert store.load() == b"the generator's state"
    assert synced
