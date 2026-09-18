"""A synchronous HTTP endpoint, for the demo and for the comparison it loses.

`PLAN.md` section 2.4 makes the scorer a stream consumer and keeps this
endpoint for two reasons: a demo someone can call, and Rule C candidate 3,
which loads both at the live rate on the same host and compares their tails.
The decision is made by the same `core.Decider` the consumer uses, so the
comparison is between transports, not between two scorers.

Three things are different about HTTP, and they are the evidence rather than
defects to hide:

- **Requests are serialised.** The feature engine holds per-entity state and
  is not safe to call from two threads at once, and the web server runs
  handlers on a thread pool. A lock serialises them. Time spent waiting for
  it is reported as `queue`, and it is latency the stream path does not have,
  because a consumer reads one event at a time by construction.
- **Order is not guaranteed.** Two clients can deliver transactions out of
  event-time order. The engine refuses a late event rather than corrupt its
  windows, so this endpoint answers 409 for one. The stream path never sees
  that, because one partition is ordered.
- **A decision is durable before the response.** A caller acts on the answer,
  so by default the decision is flushed to the stream before it is returned.
  The consumer acknowledges a batch at a time; this acknowledges a request at
  a time, and the `persist` timing shows what that costs. `durable=False`
  exists so the comparison can separate the two effects.

Every response carries a `Server-Timing` header with the hops, so a load
client can split its own round-trip time without a second channel.
"""

from __future__ import annotations

import threading
import time
from typing import Final

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from verdict.events.schema import (
    TransactionEvent,
    UnknownSchemaVersionError,
    decode_transaction,
)
from verdict.features.engine import LateEventError
from verdict.scoring.core import Decider
from verdict.stream.base import Stream

SCORE_PATH: Final = "/v1/score"


def create_app(
    decider: Decider,
    *,
    stream: Stream | None = None,
    decisions_topic: str = "decisions",
    shadow_topic: str = "shadow",
    durable: bool = True,
) -> FastAPI:
    """Build the application.

    Args:
        decider: What makes each decision; shared with nothing else at run
            time, since the engine's state belongs to one scorer.
        stream: Where decisions are written. None writes nowhere, for the
            demo; the comparison always passes one.
        decisions_topic: The topic decisions go to.
        shadow_topic: The topic a shadow model's would-be decisions go to.
        durable: Whether to flush each decision before responding.

    Returns:
        The FastAPI application.
    """
    app = FastAPI(title="verdict scorer", version="0.1.0", docs_url=None, redoc_url=None)
    lock = threading.Lock()

    @app.get("/healthz")
    def healthz() -> dict[str, object]:
        return {"ok": True, "decided": decider.stats.decided}

    @app.post(SCORE_PATH)
    async def score(request: Request) -> Response:
        received = time.perf_counter_ns()
        raw = await request.body()
        try:
            event = decode_transaction(raw)
        except UnknownSchemaVersionError as error:
            return JSONResponse({"detail": str(error)}, status_code=422)
        except ValidationError as error:
            return JSONResponse(
                {"detail": "not a transaction", "errors": error.errors(include_url=False)},
                status_code=422,
            )
        return await run_in_threadpool(_decide, event, received)

    def _decide(event: TransactionEvent, received: int) -> Response:
        with lock:
            started = time.perf_counter_ns()
            try:
                outcome = decider.decide(event, started)
            except LateEventError as error:
                return JSONResponse(
                    {"detail": "arrived after later transactions", "reason": str(error)},
                    status_code=409,
                )
            if outcome is None:
                return JSONResponse(
                    {"detail": "already decided", "event_id": event.event_id}, status_code=409
                )
            before_persist = time.perf_counter_ns()
            if stream is not None:
                stream.produce(decisions_topic, event.card_id, outcome.payload)
                if outcome.shadow_payload is not None:
                    stream.produce(shadow_topic, event.card_id, outcome.shadow_payload)
                if durable:
                    stream.flush()
            persisted = time.perf_counter_ns()
        timing = {
            "queue": started - received,
            "features": outcome.features_ns,
            "model": outcome.model_ns,
            "decision": outcome.decision_ns,
            "persist": persisted - before_persist,
        }
        header = ", ".join(f"{name};dur={value / 1e6:.3f}" for name, value in timing.items())
        return Response(
            content=outcome.payload,
            media_type="application/json",
            headers={"Server-Timing": header},
        )

    return app
