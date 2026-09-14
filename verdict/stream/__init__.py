"""The stream: one interface, an in-process implementation and Redpanda.

ADR 3 puts Redpanda under the local stack and Kinesis under the live one,
behind one contract. `base.py` is that contract; `memory.py` implements it in
process for tests and replays; `redpanda.py` implements it on the Kafka
protocol. Kinesis arrives in week 7, and has to pass the same contract tests.
"""

from verdict.stream.base import (
    Position,
    Record,
    Stream,
    StreamError,
    UnknownTopicError,
)

__all__ = ["Position", "Record", "Stream", "StreamError", "UnknownTopicError"]
