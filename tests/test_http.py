"""The HTTP endpoint makes the same decision as the consumer, and says when it cannot.

The comparison in Rule C candidate 3 is only fair if the two paths decide
identically, so the first test feeds the same events to both and compares the
decisions. The rest pin the three ways HTTP differs: a duplicate or a late
transaction is refused with 409, a malformed one with 422, and every response
carries its own timing.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
from fastapi.testclient import TestClient
from httpx2 import Response

from verdict.events.schema import DecisionEvent, EntryMode, MerchantCategory, TransactionEvent
from verdict.features.engine import FeatureEngine
from verdict.scoring.consumer import StreamScorer
from verdict.scoring.core import Decider, EngineFeatures
from verdict.scoring.http_api import SCORE_PATH, create_app
from verdict.scoring.model import FixedModel, StandInModel
from verdict.stream.memory import MemoryBroker

START = dt.datetime(2027, 4, 5, 12, 0, tzinfo=dt.UTC)


def an_event(index: int, *, card: str = "card-1", amount: int = 2_500) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"evt-{index}",
        event_time=START + dt.timedelta(seconds=index),
        card_id=card,
        device_id=f"dev-{index % 4}",
        merchant_id="mer-1",
        amount_cents=amount,
        merchant_category=MerchantCategory.GROCERY_POS,
        entry_mode=EntryMode.ECOMMERCE,
    )


def a_decider() -> Decider:
    return Decider(features=EngineFeatures(FeatureEngine()), models=FixedModel(StandInModel()))


@pytest.fixture
def broker() -> MemoryBroker:
    broker = MemoryBroker()
    broker.create_topic("transactions", 1)
    broker.create_topic("decisions", 2)
    return broker


@pytest.fixture
def client(broker: MemoryBroker) -> TestClient:
    return TestClient(create_app(a_decider(), stream=broker.open()))


def post(client: TestClient, event: TransactionEvent) -> Response:
    return client.post(
        SCORE_PATH,
        content=event.to_json(),
        headers={"content-type": "application/json"},
    )


def test_http_and_the_consumer_make_identical_decisions(broker: MemoryBroker) -> None:
    """Same events, same order, same decider configuration: same scores, actions, rules."""
    events = [an_event(i, card=f"card-{i % 3}", amount=1_000 + i * 997) for i in range(40)]

    client = TestClient(create_app(a_decider()))
    over_http = [DecisionEvent.model_validate_json(post(client, e).content) for e in events]

    producer = broker.open()
    for event in events:
        producer.produce("transactions", event.card_id, event.to_json().encode("utf-8"))
    scorer = StreamScorer(broker.open(), decider=a_decider())
    while scorer.poll():
        pass
    reader = broker.open()
    over_stream = {
        d.event_id: d
        for d in (
            DecisionEvent.model_validate_json(r.value)
            for r in reader.consume("decisions", "check", max_records=100)
        )
    }

    for decision in over_http:
        twin = over_stream[decision.event_id]
        assert (decision.score, decision.action, decision.rule) == (
            twin.score,
            twin.action,
            twin.rule,
        )


def test_a_decision_is_returned_and_written_to_the_stream(
    client: TestClient, broker: MemoryBroker
) -> None:
    response = post(client, an_event(1))
    assert response.status_code == 200
    decision = DecisionEvent.model_validate_json(response.content)
    written = broker.open().consume("decisions", "check")
    assert [DecisionEvent.model_validate_json(r.value) for r in written] == [decision]


def test_every_response_carries_its_hops(client: TestClient) -> None:
    header = post(client, an_event(1)).headers["server-timing"]
    names = [part.split(";")[0].strip() for part in header.split(",")]
    assert names == ["queue", "features", "model", "decision", "persist"]
    assert all(float(part.split("dur=")[1]) >= 0 for part in header.split(","))


def test_a_repeated_transaction_is_refused_and_not_counted_twice(
    client: TestClient, broker: MemoryBroker
) -> None:
    assert post(client, an_event(1)).status_code == 200
    again = post(client, an_event(1))
    assert again.status_code == 409
    assert again.json()["detail"] == "already decided"
    assert len(broker.open().consume("decisions", "check")) == 1


def test_a_transaction_older_than_one_already_scored_is_refused(client: TestClient) -> None:
    """The ordering the stream gets from one partition, HTTP has to refuse."""
    assert post(client, an_event(5)).status_code == 200
    late = post(client, an_event(2))
    assert late.status_code == 409
    assert late.json()["detail"] == "arrived after later transactions"
    assert post(client, an_event(2)).status_code == 409
    assert post(client, an_event(6)).status_code == 200


@pytest.mark.parametrize(
    "body",
    [
        "{}",
        "not json",
        json.dumps({**json.loads(an_event(1).to_json()), "schema_version": 1}),
        json.dumps({**json.loads(an_event(1).to_json()), "is_fraud": True}),
    ],
)
def test_anything_that_is_not_a_current_transaction_is_refused(
    client: TestClient, body: str
) -> None:
    """Including one that tries to carry its own label."""
    response = client.post(SCORE_PATH, content=body, headers={"content-type": "application/json"})
    assert response.status_code == 422


def test_without_a_stream_the_demo_still_decides() -> None:
    client = TestClient(create_app(a_decider()))
    assert post(client, an_event(1)).status_code == 200
    assert client.get("/healthz").json() == {"ok": True, "decided": 1}


# --- the load client for the comparison ------------------------------------


def test_the_load_client_reads_the_endpoint_s_own_timing_back() -> None:
    from verdict.scoring.httpload import parse_server_timing

    header = "queue;dur=0.250, features;dur=1.500, model;dur=0.010, decision;dur=0.030, "
    header += "persist;dur=4.000"
    assert parse_server_timing(header) == {
        "queue": 250_000.0,
        "features": 1_500_000.0,
        "model": 10_000.0,
        "decision": 30_000.0,
        "persist": 4_000_000.0,
    }


def test_the_load_client_ignores_timings_it_does_not_recognise() -> None:
    """A proxy may add its own entries, and a hop may arrive without a duration."""
    from verdict.scoring.httpload import parse_server_timing

    assert parse_server_timing(None) == {}
    assert parse_server_timing("cdn-cache;desc=HIT, features;dur=2.000, model") == {
        "features": 2_000_000.0
    }
    assert parse_server_timing("features;dur=not-a-number") == {}


@pytest.mark.slow
def test_a_small_http_load_run_measures_every_exchange_it_offered() -> None:
    """The client, the server it starts, and the join between them, end to end."""
    from verdict.scoring import httpload, loadtest

    events = loadtest.generate_events(200, rate=200.0)
    with httpload.a_server(port=8137) as base:
        result = httpload.run_once(base, events, rate=200.0, warmup=20, connections=2)
    assert result.sent == 200
    assert result.measured == 180
    assert result.connections == 2
    assert sum(result.answered.values()) == 180
    assert result.end_to_end["p50"] <= result.end_to_end["p99"]
    for hop in httpload.SERVER_HOPS:
        assert result.server[hop]["p50"] >= 0.0
    assert set(result.backlog) == {"early_p50", "late_p50"}
