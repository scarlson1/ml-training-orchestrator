# Potential Interview Questions

## Key Architectural Pattern: Point-in-Time Correctness

The trickiest part of any ML pipeline. Every feature is keyed to `scheduled_departure_utc` — never actual departure — so that at inference time, you only use information that was knowable _before_ the flight departed. The `int_flights_enriched` dbt model enforces this with a `QUALIFY row_number() = 1` window to pick only weather observations that occurred before the scheduled departure.

**Why `get_historical_features` is not just a SELECT — the interview explanation:**

When you call `get_historical_features(entity_df, features)`, Feast does:

Takes your `entity_df` — one row per training example, each with an entity key and an `event_timestamp`.
For each row, finds all feature rows matching the entity key.
Filters to rows where `feature.event_ts <= entity.event_timestamp`.
Within that filtered set, picks the latest row (maximum `event_ts`).
Returns that value as the feature for that training example.
This is the "as-of join" or "point-in-time join." On a whiteboard, draw two timelines: one for feature snapshots (computed each hour) and one for flight events (one per scheduled departure). The PIT join connects each flight to the most recent feature snapshot that existed before that flight departed.

A plain SQL `SELECT latest_value FROM features WHERE entity = X` would give every flight the same "latest" feature value regardless of when the flight happened — which leaks future information into the training set.

**"Walk me through your feature store design."**

Feast with a file-based offline store (Parquet on MinIO/R2) and Redis for online serving. Five feature views organized by entity type: origin airport, destination airport, carrier, route, and aircraft tail. Calendar features are excluded from the feature store deliberately — they're deterministic functions of the timestamp and cheaper to compute on-the-fly than to store and retrieve.

**"Why is get_historical_features not just a SELECT?"**

It's an as-of join. For each row in your entity dataframe — which has an entity key and a timestamp — Feast finds the latest feature snapshot where the feature's event_ts is less than or equal to the entity's timestamp. A plain SELECT of the latest value would give every training example the same feature value regardless of when the event happened, leaking future information into the training set. I have a test that plants two different values at T=10:00 and T=14:00, then requests features for events at T=11:30 and T=15:00, and asserts each gets its correct historical value.

**"How do you keep training and serving features consistent?"**

One feature view definition → one Parquet source → materialized to both the offline store (for get_historical_features during training) and the online store (Redis, for inference). The same field names, same TTLs, same types. There's no separate "training features" codebase — the Feast schema is the contract.

**"How do you prevent training/serving skew?"**

There are two separate code paths — bmo.batch_scoring.score (Feast offline, PIT join) and bmo.serving.feature_client (Feast online, latest value) — but both use exactly the same FEATURE_REFS list and the same FEATURE_COLUMNS ordering. The constants are defined once in each module and documented as "must match ALL_FEATURE_REFS in training.py." The integration test test_feast_roundtrip.py from Phase 4 verifies write-then-read equality. The only difference between batch and online is the join strategy, which is intentional.

**"What happens if Redis goes down?"**

FeatureClient.get_features() wraps the Feast call in a try/except and returns None. The FastAPI /predict endpoint checks for None and returns a 503 Service Unavailable with an explanatory message. The \_fail_closed_count Prometheus counter increments, making the degradation visible in Grafana. The /health endpoint returns status: 'degraded' (not unhealthy) so Fly.io doesn't replace the machine — it's still able to serve if Redis recovers.

**"How do you do a zero-downtime model swap?"**

deployed_api Dagster asset writes model_config.json to S3. The FastAPI /admin/reload endpoint calls ModelLoader.reload(), which downloads the new model in an asyncio.Lock block so in-flight requests finish on the old model before the swap. The lock is released as soon as the new model is loaded — subsequent requests use the new model. No container restart required.

**"How does batch scoring prevent data leakage?"**

event_timestamp = min(scheduled_departure_utc, run_time) per entity row. For a flight scheduled at 10am that we're scoring at 6am, event_timestamp = 6am. Feast's offline get_historical_features returns only features where feature_ts <= 6am. For a historical backfill of a flight that departed at 10am last week, event_timestamp = 10am last week — Feast returns features as they existed at 10am, not any data from after the flight.
