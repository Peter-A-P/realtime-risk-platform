"""Replacing a file so that a machine killed at any moment leaves the old or the new one.

Writing a temporary file and renaming it over the old one is atomic for
readers, but not for the disk: without an fsync of the file before the rename
and of its directory after, a machine that stops hard can keep the rename and
lose the data, and leave a file of zero bytes where either version would
have done. That happened on 2026-09-29, when a frozen instance was
terminated: the transactions feed's saved place and the alert relay's state
were both left empty, and the feed would not start (ADR 27's addendum of that
day). Every piece of state the live stack keeps on its data volume is
replaced through here.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def sync_directory(directory: Path) -> None:
    """Make the renames in a directory durable.

    A directory cannot be opened for syncing on Windows, where this only runs
    in tests; the rename there is as durable as the platform makes it.

    Args:
        directory: The directory.
    """
    if os.name != "posix":
        return
    handle = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def settle(temporary: Path, path: Path) -> None:
    """Make a finished temporary file durable, then move it over `path`.

    For files another library wrote (Parquet, NumPy): sync the file's data,
    rename it into place, and sync the directory so the rename is durable
    too.

    Args:
        temporary: The finished file, in the same directory as `path`.
        path: Where it belongs.
    """
    with temporary.open("rb+") as file:
        os.fsync(file.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def write_bytes(path: Path, data: bytes) -> None:
    """Replace `path` with `data`, durably and atomically.

    Args:
        path: The file.
        data: Its new contents.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    temporary = Path(name)
    try:
        with os.fdopen(handle, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    sync_directory(path.parent)


def write_text(path: Path, text: str) -> None:
    """Replace `path` with `text` in UTF-8, durably and atomically.

    Args:
        path: The file.
        text: Its new contents.
    """
    write_bytes(path, text.encode("utf-8"))
