"""The models a scorer can decide with: the stand-in, and the shipped ONNX files.

A model reaches the instance inside the image (ADR 14), as an ONNX file in
`verdict/models/artifacts/`, named for its role (`champion.onnx`,
`challenger.onnx`). Only models trained on the synthetic track ship: the
competition's terms keep anything trained on its rows off the live stack
(`docs/data.md`, ADR 2). Each is known by its version, which names the hash of
its bytes, so the champion pointer and every decision name exactly the file.

Nothing here chooses which model decides. The pointer does (`flags.py`), and a
pointer moves only by the merged pull request that carries the shadow
evidence (ADR 11), or by a rollback.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from verdict.scoring.model import Model, StandInModel

ARTIFACTS: Final = Path(__file__).resolve().parents[1] / "models" / "artifacts"
"""Where shipped models live, inside the package so the image carries them."""


def known_models(directory: Path = ARTIFACTS) -> dict[str, Model]:
    """Every model this build can score with, by version.

    Args:
        directory: Where the ONNX files are.

    Returns:
        The stand-in, and one model per ONNX file, each under its version.
    """
    stand_in = StandInModel()
    known: dict[str, Model] = {stand_in.version: stand_in}
    if directory.is_dir():
        from verdict.scoring.onnx_model import OnnxModel

        for path in sorted(directory.glob("*.onnx")):
            model = OnnxModel(path, prefix=path.stem)
            known[model.version] = model
    return known


def version_of(role: str, directory: Path = ARTIFACTS) -> str:
    """The version of the shipped model in a role.

    Args:
        role: `champion` or `challenger`, the file's stem.
        directory: Where the ONNX files are.

    Returns:
        Its version.

    Raises:
        FileNotFoundError: If no model ships in that role.
    """
    from verdict.scoring.onnx_model import model_version

    path = directory / f"{role}.onnx"
    if not path.exists():
        msg = f"no {role} ships in {directory}"
        raise FileNotFoundError(msg)
    return model_version(path, role)


def path_of(version: str, directory: Path = ARTIFACTS) -> Path:
    """The shipped file a version names.

    Args:
        version: A model version, `<stem>-<hash>`.
        directory: Where the ONNX files are.

    Returns:
        The file.

    Raises:
        FileNotFoundError: If no shipped file has that version.
    """
    from verdict.scoring.onnx_model import model_version

    for path in sorted(directory.glob("*.onnx")):
        if model_version(path, path.stem) == version:
            return path
    msg = f"no shipped model has version {version}"
    raise FileNotFoundError(msg)
