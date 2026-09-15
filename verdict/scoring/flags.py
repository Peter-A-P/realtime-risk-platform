"""The champion pointer: which model scores, read on every event.

`PLAN.md` section 2.5 makes rollback a configuration flag read per event, and
the drill in week 5 times how long it takes from flipping it to the previous
champion serving. So the flag is a small file, and the model source checks it
on every call to `current`.

Three things keep that honest:

- **Reading it is cheap enough to do per event.** `current` compares the
  file's modification time and size with what it last loaded, which is one
  `stat` call, and parses the file only when those change. The cost lands in
  the `model` hop, where the load test reports it.
- **Writing it is atomic.** A new pointer is written to a temporary file in
  the same directory and moved over the old one with `os.replace`, so a
  scorer reading mid-write sees the old pointer or the new one, never half of
  either.
- **A bad flag does not stop scoring.** A pointer naming a model the scorer
  does not have, or a file that does not parse, is refused: scoring continues
  on the last good champion, and the refusal is counted so it shows up. A
  rollback that takes the scorer down would be worse than no rollback.

What this does not do is promote. `set_champion` exists for the drill and for
applying a merged promotion; the decision to promote is a pull request with
the shadow evidence (ADR 11, week 5), never a call to this module.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from verdict.scoring.model import Model


class FlagError(ValueError):
    """Raised when a pointer cannot be written as asked."""


@dataclass(frozen=True, slots=True)
class Pointer:
    """What the flag file says.

    Attributes:
        champion: The model version that scores.
        previous: The version a rollback returns to, if any.
        changed_at: When the pointer was last written.
    """

    champion: str
    previous: str | None
    changed_at: str

    def to_json(self) -> str:
        """Render the file's contents.

        Returns:
            JSON with a trailing newline.
        """
        return (
            json.dumps(
                {
                    "champion": self.champion,
                    "previous": self.previous,
                    "changed_at": self.changed_at,
                },
                indent=2,
            )
            + "\n"
        )


def read_pointer(path: Path) -> Pointer:
    """Read and validate a pointer file.

    Args:
        path: The flag file.

    Returns:
        The pointer.

    Raises:
        FlagError: If the file is missing, does not parse, or lacks a champion.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        msg = f"cannot read the champion pointer at {path}: {error}"
        raise FlagError(msg) from error
    champion = payload.get("champion") if isinstance(payload, dict) else None
    if not isinstance(champion, str) or not champion:
        msg = f"the champion pointer at {path} names no champion"
        raise FlagError(msg)
    previous = payload.get("previous")
    return Pointer(
        champion=champion,
        previous=previous if isinstance(previous, str) and previous else None,
        changed_at=str(payload.get("changed_at", "")),
    )


def _write_atomically(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def set_champion(path: Path, version: str, known: Mapping[str, Model]) -> Pointer:
    """Point the scorer at a model, keeping the current champion as the rollback target.

    Args:
        path: The flag file. Created if missing.
        version: The model version to score with.
        known: The models the scorer has, by version. Pointing at anything
            else is refused here, before a scorer would have to refuse it.

    Returns:
        The pointer written.

    Raises:
        FlagError: If the version is not a model the scorer has.
    """
    if version not in known:
        msg = f"no model {version!r}; known: {sorted(known)}"
        raise FlagError(msg)
    previous: str | None = None
    if path.exists():
        current = read_pointer(path)
        previous = current.champion if current.champion != version else current.previous
    pointer = Pointer(
        champion=version,
        previous=previous,
        changed_at=dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds"),
    )
    _write_atomically(path, pointer.to_json())
    return pointer


def rollback(path: Path) -> Pointer:
    """Swap the champion and the previous champion.

    Args:
        path: The flag file.

    Returns:
        The pointer written.

    Raises:
        FlagError: If there is no previous champion to return to.
    """
    current = read_pointer(path)
    if current.previous is None:
        msg = "there is no previous champion to roll back to"
        raise FlagError(msg)
    pointer = Pointer(
        champion=current.previous,
        previous=current.champion,
        changed_at=dt.datetime.now(dt.UTC).isoformat(timespec="milliseconds"),
    )
    _write_atomically(path, pointer.to_json())
    return pointer


class FlaggedModels:
    """A model source that follows the champion pointer, event by event."""

    def __init__(self, path: Path, known: Mapping[str, Model]) -> None:
        """Start following a pointer.

        Args:
            path: The flag file. Must exist and name a known model, because a
                scorer with no champion at start-up has nothing to fall back
                to.
            known: The models available, by version.

        Raises:
            FlagError: If the pointer is unreadable or names an unknown model.
        """
        self.path = path
        self.known = dict(known)
        self.refused = 0
        pointer = read_pointer(path)
        if pointer.champion not in self.known:
            msg = f"the pointer names {pointer.champion!r}, which this scorer does not have"
            raise FlagError(msg)
        self._model = self.known[pointer.champion]
        self._stamp = self._signature()

    def _signature(self) -> tuple[int, int, int] | None:
        # The file id is in the signature because `os.replace` installs a new
        # file: two pointers written within one timestamp tick, of the same
        # length, still differ by id.
        try:
            status = self.path.stat()
        except OSError:
            return None
        return status.st_ino, status.st_mtime_ns, status.st_size

    def current(self) -> Model:
        """The champion, rereading the pointer only if the file has changed.

        Returns:
            The model to score with.
        """
        stamp = self._signature()
        if stamp == self._stamp:
            return self._model
        self._stamp = stamp
        try:
            pointer = read_pointer(self.path)
        except FlagError:
            self.refused += 1
            return self._model
        model = self.known.get(pointer.champion)
        if model is None:
            self.refused += 1
            return self._model
        self._model = model
        return self._model
