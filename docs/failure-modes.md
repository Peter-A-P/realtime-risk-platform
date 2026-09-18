# Failure modes

One section per failure: what was done to the platform, what it did, what was
changed, and the test that holds the change in place. `PLAN.md` lists the
failures to be tried (a store going away, the stream throttling, consumer lag,
clock skew, a poison event, duplicates, events out of order, a schema change).
They are added here as they are tried, in the order they were found, and a
section is not removed when its fault is fixed: the observed behaviour before
the fix is the evidence that the fix was needed.

## A record the scorer cannot decide (poison event, schema change, out of order)

**Found:** 2026-09-18, reading the scorer's code for something else, before any
live traffic. Not yet exercised on the live stack.

**What was done.** Three records the scorer cannot decide, each put on the
transaction topic between two good ones: bytes that are not JSON, a
transaction with `schema_version` 3 (a producer deployed ahead of the
scorer), and a transaction earlier in event time than the one before it.

**What it did.** Each raised out of the scorer's `poll` before the batch was
checkpointed. The scorer stopped. On restart it resumed from the last
checkpoint, read the same record, and stopped again: three restarts, three
identical stops at the same record, and nothing behind it decided. One bad
message was a permanent outage of the whole decision path, and the only way
out would have been an operator moving the group's offset by hand past a
record they had not seen.

**What changed.** A record that cannot be decided for one of those three
reasons is set aside: it goes to the `dead-letter` topic with its bytes
untouched and the reason, is flushed with the batch, and is checkpointed past
like any decided record. A run of fifty in a row stops the scorer instead,
because that is a fault upstream rather than a bad record, and setting a whole
stream aside quietly is worse than stopping. Any other error while deciding
still stops the scorer at once. ADR 8's addendum of 2026-09-18 records the
decision and its cost: a transaction set aside gets no decision.

**Held in place by** `tests/test_scoring.py`: one malformed record set aside
and a restarted scorer finding nothing left to do; a newer schema version set
aside with its reason; a late transaction set aside with no decision while the
next one is decided; a run of records past the limit stopping the scorer; and
a good record resetting the count. `tests/test_stream_parity.py` holds a
reordering stream to the reference and reports the late events as missing
decisions.

**Still open.** Replaying the dead-letter topic once a fault is fixed is a
manual step with no tool yet, and nothing alarms on the dead-letter count.
Both belong with the dashboards and alarms, on the live stack.
