# 5. Feast as the registry, the point-in-time join and the online read

- Status: accepted
- Date: 2026-09-12
- Deciders: Peter Parker

## Context

The platform needs four things from a feature store, and they are not
usually provided by the same thing:

1. **A registry.** One place that says what a feature is, so the model, the
   monitors and the review queue all mean the same thing by
   `card_txn_count_1h`.
2. **Point-in-time correct historical retrieval.** Given a set of training
   rows, attach each feature as of that row's own event time. This is the
   join that decides whether the model is trained on a world that existed.
3. **An online read inside the latency budget.** The scoring path gets 5 ms
   for feature fetch out of a 50 ms end-to-end budget.
4. **One write path.** ADR 6 requires that features are computed once, by the
   dataflow, and land in both stores from that one computation.

The fourth requirement is the awkward one, because the usual feature-store
arrangement is the opposite of it: compute features in a batch job over a
warehouse, then materialise them into an online store on a schedule. That
design has two computations in it by construction, or one computation and a
copy that can silently fall behind.

## Options

1. **No feature store: our own registry, Redis for online, Parquet for
   offline, and a hand-written point-in-time join.** Fewest dependencies and
   total control. The point-in-time join is the part of this project most
   worth getting right and least worth writing from scratch, and "we wrote
   our own feature store" is a weaker claim in an interview than "we used one
   and can say exactly what it does and does not do for us".
2. **Feast in its usual arrangement**: offline store as the source of truth,
   `materialize` into the online store on a schedule. Rejected because it
   violates ADR 6 directly. The materialisation window is exactly the gap
   where training-serving skew lives.
3. **Feast with push sources.** The dataflow computes once and pushes the
   result to both stores in one call (`PushMode.ONLINE_AND_OFFLINE`). Feast
   is the registry, the join engine and the online read path, and is never a
   place where a feature is computed.
4. **Tecton, or a managed store.** Out of budget, and it would put the most
   interesting part of the project behind a vendor.

## Decision

Option 3, with the repository generated from the feature specifications
rather than hand-written (`verdict/store/repo.py`).

Measured before accepting, on the build laptop, Feast 0.66.0, Python 3.13.15,
sqlite online store:

| Measurement | Result |
|---|---|
| Online read, steady state | p50 0.76 ms, p95 1.22 ms, p99 1.76 ms |
| Online read, first call after start-up | about 42 ms |
| Direct read of the same sqlite file | p50 0.21 ms |
| Point-in-time join after a push | serves the old value before the push time and the new one after |

The steady-state read fits the 5 ms budget hop with room. The first call does
not, by a factor of eight, which is an operational fact rather than a
benchmark curiosity: **the scorer must warm the store at start-up**, or the
first events after every deploy and every spot replacement blow the latency
budget. That goes in the runbook and into the start-up path in week 4.

The numbers above come from a sqlite online store on a laptop. The live store
is Redis on one instance, which is a different measurement, taken in week 4
against the real budget.

## Consequences

- Feast brings 48 packages with it, including FastAPI, uvicorn, dask and
  gunicorn, none of which this platform uses. That is a real cost: a larger
  image, a wider supply-chain surface, and more that can break on a Python
  upgrade. It is accepted for the join engine and the registry, and the major
  version is pinned with that reason in `pyproject.toml`.
- `entity_key_serialization_version` is pinned at 3 in the generated
  configuration. A Feast upgrade that changed the online key layout would
  strand every value written during the live window; pinning it turns that
  into a visible migration rather than a silent one.
- The feature views are generated from `FEATURE_SET`, one view per entity
  kind, with each view's TTL set to the longest window in it. A store that
  serves an hour-old feature a day later is serving a number nobody computed.
- Because the dataflow pushes rather than Feast materialising, Feast's own
  materialisation path is unused, and so is the scheduling that usually comes
  with it. Anyone reading this repository expecting the standard arrangement
  will find it absent on purpose.
- The online read may still have to leave the hot path. If the week 4
  measurement against Redis puts Feast's Python overhead outside the budget,
  the scorer reads Redis directly using the key layout Feast writes, and
  Feast keeps the registry and the offline join. That fallback is cheap
  precisely because the key serialisation is pinned.

## Sources

- Feast documentation: push sources and stream feature computation, which is
  the arrangement this ADR adopts.
  https://docs.feast.dev/reference/data-sources/push
- Feast point-in-time joins and the entity dataframe contract.
  https://docs.feast.dev/getting-started/concepts/point-in-time-joins
- Feast online store reference, including the Redis and sqlite stores.
  https://docs.feast.dev/reference/online-stores
- Measurements: `tests/test_store_repo.py::test_push_then_read_online_and_point_in_time`
  for the behaviour, and the latency figures above, taken with the spike
  recorded in this ADR. The published budget numbers come from week 4.
