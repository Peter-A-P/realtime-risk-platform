"""The chaos faults that come in the data, without a broker (verdict/chaos/faults.py)."""

from __future__ import annotations

import pytest


def test_each_data_fault_changes_only_what_falls_in_its_window() -> None:
    import datetime as dt

    from pydantic import ValidationError

    from verdict.chaos.faults import data_fault, reverse_in_blocks
    from verdict.events.schema import TransactionEvent
    from verdict.scoring.loadtest import generate_events

    events = generate_events(40, rate=10)
    window = range(10, 20)
    inside, outside = events[12], events[30]

    twice = data_fault("duplicates", intensity=1.0, window=window)
    assert len(twice(12, inside)) == 2
    assert len(twice(30, outside)) == 1

    cut = data_fault("poison", intensity=1.0, window=window)
    with pytest.raises(ValidationError):
        TransactionEvent.model_validate_json(cut(12, inside)[0])
    assert TransactionEvent.model_validate_json(cut(30, outside)[0]) == outside

    renamed = data_fault("schema-change", intensity=1.0, window=window)
    with pytest.raises(ValidationError):
        TransactionEvent.model_validate_json(renamed(12, inside)[0])

    skewed = data_fault("clock-skew", intensity=300.0, window=window)
    moved = TransactionEvent.model_validate_json(skewed(12, inside)[0])
    assert moved.event_time - inside.event_time == dt.timedelta(seconds=300)
    assert TransactionEvent.model_validate_json(skewed(30, outside)[0]) == outside

    reordered = reverse_in_blocks(events, window, block=5)
    assert reordered[10:15] == events[10:15][::-1]
    assert reordered[15:20] == events[15:20][::-1]
    assert reordered[:10] == events[:10]
    assert reordered[20:] == events[20:]

    with pytest.raises(ValueError, match="not a data fault"):
        data_fault("pause", intensity=1.0, window=window)


def test_a_capture_keeps_a_traceback_whole_and_reads_client_and_broker_lines() -> None:
    from verdict.chaos.capture import broker_records, harness_records

    t = "2026-10-17T00:00:00.000000+00:00"
    lines = [
        (t, "stderr", "%3|1792250325.866|FAIL|verdict#producer-14| [thrd:x]: Connect failed"),
        (t, "stderr", "Traceback (most recent call last):"),
        (t, "stderr", '  File "service.py", line 3, in run'),
        (t, "stderr", "    scorer.poll()"),
        (t, "stderr", "KafkaException: KafkaError{code=_TRANSPORT}"),
        (t, "stdout", "scoring from localhost:19092"),
    ]
    client, error, plain = harness_records(lines)
    assert client["service"] == "scorer-kafka-producer"
    assert client["severity"] == "ERROR"
    assert client["facility"] == "FAIL"
    assert error["exception_type"] == "KafkaException"
    assert error["traceback"].count("\n") == 3
    assert plain == {"ts": t, "service": "scorer", "severity": "INFO", "message": lines[-1][2]}

    (warn,) = broker_records(
        "2026-10-17T00:00:01.5Z WARN  2026-10-17 00:00:01,500 [shard 0:main] raft - slow"
    )
    assert warn["severity"] == "WARNING"
    assert warn["message"] == "[shard 0:main] raft - slow"
    assert warn["service"] == "broker"
    assert warn["ts"] == "2026-10-17T00:00:01.5Z"


def test_the_capture_plan_covers_every_fault_with_its_repeats_and_controls() -> None:
    from verdict.chaos.capture import TARGETED, plan
    from verdict.chaos.faults import FAULTS

    runs = plan(repeats=5, controls=2)
    assert {run.fault for run in runs} == set(FAULTS)
    assert sum(run.fault == "none" for run in runs) == 2
    for fault in FAULTS:
        if fault != "none":
            assert sum(run.fault == fault for run in runs) == 5
    assert set(TARGETED) == set(FAULTS)
