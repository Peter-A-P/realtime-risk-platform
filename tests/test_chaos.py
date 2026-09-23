"""The broker experiment, as a test: a frozen broker does not cost the scorer its state.

`docs/failure-modes.md`, "The broker stops answering": frozen for 15 seconds
from inside a scorer batch, the scorer used to stop on its flush and restart
cold. This runs the same fault against the local broker and asserts one
scorer throughout and every transaction decided exactly once.

It needs Docker, the local stack (deploy/compose) and about a minute, so it
runs only when asked: `VERDICT_CHAOS=1`.
"""

from __future__ import annotations

import os
import shutil

import pytest

from verdict.chaos.faults import FAULTS, run_fault
from verdict.stream.redpanda import broker_reachable

pytestmark = pytest.mark.skipif(
    os.environ.get("VERDICT_CHAOS") != "1" or shutil.which("docker") is None,
    reason="set VERDICT_CHAOS=1, with Docker and deploy/compose running",
)


def test_a_broker_frozen_inside_a_batch_is_waited_out() -> None:
    if not broker_reachable():
        pytest.skip("no broker at the local stack's address")
    report = run_fault("pause", seconds=15, rate=500, count=20_000)
    assert not report.restarted_cold, report.scorers
    assert len(report.scorers) == 1
    assert report.decided_once == 20_000
    assert report.decided_twice_or_more == 0


def test_an_unknown_fault_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown fault"):
        run_fault("unplug", seconds=1)
    assert "pause" in FAULTS
