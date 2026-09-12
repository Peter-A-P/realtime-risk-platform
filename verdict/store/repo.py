"""The Feast feature repository, generated from the feature specifications.

The repository is not hand-written. Every entity, source and feature view
here is derived from `FEATURE_SET`, so there is one place a feature is
defined and the store is a projection of it. Hand-writing the Feast
definitions alongside the specifications would be the same duplication this
project exists to argue against, one layer down: two descriptions of the same
feature, drifting apart at their own pace.

## How the pieces line up with the platform

- **One feature view per entity kind.** Feast keys a view on an entity, and
  the platform's features are keyed on card, device, merchant or session, so
  the grouping falls out. A view exists only if at least one feature is keyed
  on that entity, which is why week 2 produces none.
- **Push sources, not materialisation.** The usual Feast arrangement computes
  features in a batch job and materialises them into the online store. This
  platform computes once, in the Bytewax dataflow, and pushes the result to
  both stores (`PushMode.ONLINE_AND_OFFLINE`). Feast is then the registry,
  the point-in-time join engine and the online read path, but never a second
  place where a feature is computed. ADR 6 records this.
- **The TTL is the longest window in the view.** A feature whose window is an
  hour is meaningless eight hours later, and an online store that serves it
  anyway is serving a number nobody computed. Unbounded features pin the view
  to `UNBOUNDED_TTL`.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from verdict.store.features import (
    FEATURE_SET,
    EntityKind,
    FeatureSpec,
    validate_feature_set,
)

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from feast import Entity, FeatureView

PROJECT: Final = "verdict"
"""The Feast project name. It prefixes every table in the online store."""

TIMESTAMP_FIELD: Final = "event_timestamp"
"""The event-time column. Feast's point-in-time joins key on it.

It is event time, never ingestion or processing time. A join on processing
time is a leak wearing a different hat: it would attach whatever the pipeline
happened to know by the time it got round to the row.
"""

UNBOUNDED_TTL: Final = dt.timedelta(days=90)
"""TTL for views holding an unbounded feature.

Feast requires a finite TTL. Ninety days covers the live window with room,
and an entity dormant for longer is one whose lifetime aggregates have
stopped meaning anything anyway.
"""

ENTITY_JOIN_KEYS: Final[dict[EntityKind, str]] = {
    EntityKind.CARD: "card_id",
    EntityKind.DEVICE: "device_id",
    EntityKind.MERCHANT: "merchant_id",
    EntityKind.SESSION: "session_id",
}
"""The column each entity joins on. These are the event's own field names."""


def view_name(kind: EntityKind) -> str:
    """Name the feature view for an entity kind.

    Args:
        kind: The entity kind.

    Returns:
        The view name.
    """
    return f"{kind}_features"


def push_source_name(kind: EntityKind) -> str:
    """Name the push source for an entity kind.

    Args:
        kind: The entity kind.

    Returns:
        The push source name.
    """
    return f"{kind}_features_push"


def specs_by_entity(
    specs: Sequence[FeatureSpec] = FEATURE_SET,
) -> dict[EntityKind, list[FeatureSpec]]:
    """Group features by the entity they are keyed on.

    Args:
        specs: The features. Defaults to the platform's own set.

    Returns:
        One entry per entity kind that has at least one feature.
    """
    validate_feature_set(list(specs))
    grouped: dict[EntityKind, list[FeatureSpec]] = {}
    for spec in specs:
        grouped.setdefault(spec.entity, []).append(spec)
    return grouped


def view_ttl(specs: Sequence[FeatureSpec]) -> dt.timedelta:
    """Choose a view's TTL from the features in it.

    Args:
        specs: The features in one view.

    Returns:
        The longest window among them, or `UNBOUNDED_TTL` if any is unbounded.
    """
    if any(spec.window is None for spec in specs):
        return UNBOUNDED_TTL
    windows = [spec.window for spec in specs if spec.window is not None]
    return max(windows) if windows else UNBOUNDED_TTL


def build_definitions(
    specs: Sequence[FeatureSpec] = FEATURE_SET, *, data_dir: Path | None = None
) -> tuple[list[Entity], list[FeatureView]]:
    """Build the Feast objects for a feature set.

    Args:
        specs: The features. Defaults to the platform's own set.
        data_dir: Where the offline Parquet files live. Defaults to `data`,
            relative to the repository directory Feast is run from.

    Returns:
        The entities and the feature views. Entities are returned for every
        kind that has features; a kind with none produces neither.
    """
    from feast import Entity, FeatureView, Field, FileSource, PushSource
    from feast.types import Float64
    from feast.value_type import ValueType

    root = data_dir or Path("data")
    grouped = specs_by_entity(specs)

    entities: list[Entity] = []
    views: list[FeatureView] = []
    for kind, kind_specs in grouped.items():
        join_key = ENTITY_JOIN_KEYS[kind]
        entity = Entity(name=str(kind), join_keys=[join_key], value_type=ValueType.STRING)
        batch = FileSource(
            name=f"{view_name(kind)}_batch",
            path=str(root / f"{view_name(kind)}.parquet"),
            timestamp_field=TIMESTAMP_FIELD,
        )
        view = FeatureView(
            name=view_name(kind),
            entities=[entity],
            ttl=view_ttl(kind_specs),
            schema=[Field(name=spec.name, dtype=Float64) for spec in kind_specs],
            source=PushSource(name=push_source_name(kind), batch_source=batch),
            online=True,
            description=f"Features keyed on {kind}, computed once by the dataflow.",
        )
        entities.append(entity)
        views.append(view)
    return entities, views


def feature_refs(specs: Sequence[FeatureSpec] = FEATURE_SET) -> list[str]:
    """Render the feature references Feast retrieval expects.

    Args:
        specs: The features. Defaults to the platform's own set.

    Returns:
        Strings of the form `view:feature`, in definition order.
    """
    return [f"{view_name(spec.entity)}:{spec.name}" for spec in specs]


def store_config(
    repo_dir: Path, *, online_store: str = "sqlite", redis_connection: str | None = None
) -> dict[str, Any]:
    """Build the contents of `feature_store.yaml`.

    Args:
        repo_dir: The repository directory. Paths are written relative to it.
        online_store: `sqlite` for the build laptop and the tests, `redis` for
            the local stack and the live region.
        redis_connection: The Redis connection string, when the online store
            is Redis.

    Returns:
        The configuration, ready to be written as YAML.

    Raises:
        ValueError: If Redis is asked for without a connection string, or the
            online store is not one this platform supports.
    """
    del repo_dir
    match online_store:
        case "sqlite":
            online: dict[str, Any] = {"type": "sqlite", "path": "data/online.db"}
        case "redis":
            if not redis_connection:
                msg = "the redis online store needs a connection string"
                raise ValueError(msg)
            online = {"type": "redis", "connection_string": redis_connection}
        case _:
            msg = f"unsupported online store {online_store!r}"
            raise ValueError(msg)
    return {
        "project": PROJECT,
        "provider": "local",
        "registry": "data/registry.db",
        "online_store": online,
        "offline_store": {"type": "file"},
        # Version 3 is the current key layout. Pinning it means an upgrade of
        # Feast cannot silently change how online keys are encoded, which
        # would strand every value already written during the live window.
        "entity_key_serialization_version": 3,
    }


def write_repo(
    repo_dir: Path,
    specs: Sequence[FeatureSpec] = FEATURE_SET,
    *,
    online_store: str = "sqlite",
    redis_connection: str | None = None,
    specs_ref: str | None = None,
) -> Path:
    """Write a Feast repository to disk, generated from the specifications.

    Args:
        repo_dir: Where to write it. Created if missing.
        specs: The features. Defaults to the platform's own set.
        online_store: `sqlite` or `redis`.
        redis_connection: Connection string when the store is Redis.
        specs_ref: Where the generated module should import its features
            from, as `module:attribute`. The default is the platform's own
            `FEATURE_SET`, which is what the real repository uses; the tests
            point it at their own fixtures so they can exercise the store
            before week 3 has defined any features.

    Returns:
        The path to the written `feature_store.yaml`.
    """
    import yaml

    repo_dir.mkdir(parents=True, exist_ok=True)
    (repo_dir / "data").mkdir(exist_ok=True)
    config_path = repo_dir / "feature_store.yaml"
    config_path.write_text(
        yaml.safe_dump(
            store_config(repo_dir, online_store=online_store, redis_connection=redis_connection),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (repo_dir / "definitions.py").write_text(
        _definitions_module(specs, specs_ref), encoding="utf-8"
    )
    return config_path


def apply_repo(repo_dir: Path) -> None:
    """Apply a generated repository, registering its entities and views.

    Feast discovers definitions by importing the Python files in the
    repository directory by bare module name, so the directory has to be on
    the import path and has to be the working directory while it happens.
    Doing that here, once, keeps the arrangement in one supported place
    rather than in every caller. The import cache is cleared for the
    generated module so that applying a second repository in the same process
    does not quietly re-register the first one's definitions.

    Args:
        repo_dir: The repository directory.
    """
    import os
    import sys

    from feast.repo_config import load_repo_config
    from feast.repo_operations import apply_total

    resolved = repo_dir.resolve()
    config = load_repo_config(resolved, resolved / "feature_store.yaml")
    previous_cwd = Path.cwd()
    sys.path.insert(0, str(resolved))
    sys.modules.pop("definitions", None)
    try:
        os.chdir(resolved)
        apply_total(config, resolved, skip_source_validation=True)
    finally:
        os.chdir(previous_cwd)
        sys.modules.pop("definitions", None)
        if str(resolved) in sys.path:
            sys.path.remove(str(resolved))


def _definitions_module(specs: Sequence[FeatureSpec], specs_ref: str | None) -> str:
    """Render the module Feast imports when it applies the repository.

    The generated file holds no feature knowledge of its own: it imports the
    specifications and calls `build_definitions`. That is deliberate. A
    generated file that restated each window and aggregation would be a
    second description of every feature, drifting from the first at its own
    pace, which is the failure this platform exists to argue against.

    Args:
        specs: The features the repository was generated for, named in the
            docstring so the file says what it is.
        specs_ref: Where to import the features from, as `module:attribute`,
            or None to use the platform's own set.

    Returns:
        The module source.
    """
    names = ", ".join(spec.name for spec in specs) or "none yet"
    if specs_ref is None:
        imports = "from verdict.store.repo import build_definitions\n"
        call = "build_definitions()"
    else:
        module, _, attribute = specs_ref.partition(":")
        imports = (
            f"from verdict.store.repo import build_definitions\nfrom {module} import {attribute}\n"
        )
        call = f"build_definitions({attribute})"
    return (
        '"""Generated by `verdict.store.repo`. Do not edit.\n\n'
        f"Features in this repository: {names}.\n"
        'Edit `verdict/store/features.py` and regenerate.\n"""\n\n'
        f"{imports}\n"
        f"entities, views = {call}\n"
        "globals().update({entity.name: entity for entity in entities})\n"
        "globals().update({view.name: view for view in views})\n"
    )
