# Feature Store

## Overview

The feature store is built on [Feast](https://docs.feast.dev/) with an S3 Parquet offline store and a Redis online store (Upstash in production). It serves the flight delay prediction model at inference time and produces the point-in-time correct training dataset.

The data flow is:

```text
dbt (DuckDB) ──► feast_feature_export ──► S3 Parquet (offline store)
                                                │
                               feast_materialized_features
                                                │
                                                ▼
                                       Redis (online store)
                                                │
                                    FeatureClient.get_features()
                                                │
                                                ▼
                                   XGBoost inference (FastAPI)
```

The `feast_feature_export` Dagster asset runs after each `dbt build` and writes one Parquet file per entity type to S3. `feast_materialized_features` then calls `store.materialize_incremental()` to push the latest values from S3 into Redis. Both run hourly.

---

## Entities

Entities are declared in [feature_repo/entities.py](../feature_repo/entities.py). An entity maps to a join key that is known **at prediction time** — this is the key design constraint.

> **Why no `flight` entity?** A flight entity with `flight_id` as the join key would require knowing the BTS-assigned flight ID before the flight has happened, which is impossible. At serving time you know: which airport the flight departs from, which carrier, which aircraft (from the inbound assignment), and which route. Entities are designed around what is knowable at prediction time.

| Entity | Join key | Value type | Description |
| --- | --- | --- | --- |
| `origin_airport` | `origin` | STRING | IATA departure airport code (e.g. ORD, ATL) |
| `dest_airport` | `dest` | STRING | IATA arrival airport code |
| `carrier` | `carrier` | STRING | BTS two-letter carrier code (e.g. AA, UA, DL) |
| `route` | `route_key` | STRING | Origin-destination pair: `{origin}-{dest}` (e.g. ORD-ATL) |
| `aircraft_tail` | `tail_number` | STRING | FAA tail number identifying a specific aircraft |

---

## Feature Views

Declared in [feature_repo/feature_views.py](../feature_repo/feature_views.py). A `FeatureView` ties together an entity, a data source, a TTL, and a schema.

**TTL semantics**: The TTL is the maximum age of a feature value that Feast will return from the online store. If the latest stored value in Redis is older than the TTL, Feast returns `null` for that feature. The TTL also drives the null-out window in the offline PIT join — feature values older than the TTL relative to the label's `event_timestamp` are set to `null` rather than filtered out (to preserve rows in the training set while marking stale lookups).

See the [Feast FeatureView docs](https://docs.feast.dev/reference/feature-repository/feature-views) for the full configuration reference.

### `origin_airport_features`

Entity: `origin_airport` | TTL: 26h (24h window + 2h lag buffer) | Source: `s3://<feast_s3_base>/origin_airport/`

| Feature | Type | Description |
| --- | --- | --- |
| `origin_flight_count_1h` | Int64 | Flight departures in the last hour |
| `origin_avg_dep_delay_1h` | Float32 | Mean departure delay (minutes) over 1h window |
| `origin_pct_delayed_1h` | Float32 | Fraction of departures delayed ≥15 min over 1h window |
| `origin_avg_dep_delay_24h` | Float32 | Mean departure delay over 24h window |
| `origin_pct_cancelled_24h` | Float32 | Cancellation rate over 24h window |
| `origin_avg_dep_delay_7d` | Float32 | Mean departure delay over 7-day window |
| `origin_pct_delayed_7d` | Float32 | Fraction of departures delayed over 7d window |
| `origin_congestion_score_1h` | Float32 | Derived congestion index (delays × volume) over 1h |

### `dest_airport_features`

Entity: `dest_airport` | TTL: 26h | Source: `s3://<feast_s3_base>/dest_airport/`

| Feature | Type | Description |
| --- | --- | --- |
| `dest_avg_arr_delay_1h` | Float32 | Mean arrival delay (minutes) over 1h window |
| `dest_pct_delayed_1h` | Float32 | Fraction of arrivals delayed ≥15 min over 1h window |
| `dest_avg_arr_delay_24h` | Float32 | Mean arrival delay over 24h window |
| `dest_pct_diverted_24h` | Float32 | Diversion rate over 24h window |

### `carrier_features`

Entity: `carrier` | TTL: 8d (7d window + 1d buffer) | Source: `s3://<feast_s3_base>/carrier/`

| Feature | Type | Description |
| --- | --- | --- |
| `carrier_on_time_pct_7d` | Float32 | On-time arrival percentage over 7d |
| `carrier_cancellation_rate_7d` | Float32 | Cancellation rate over 7d |
| `carrier_avg_delay_7d` | Float32 | Mean departure delay (minutes) over 7d |
| `carrier_flight_count_7d` | Int64 | Total flights operated over 7d |

### `route_features`

Entity: `route` | TTL: 8d | Source: `s3://<feast_s3_base>/route/`

| Feature | Type | Description |
| --- | --- | --- |
| `route_avg_dep_delay_7d` | Float32 | Mean departure delay over 7d for this route |
| `route_avg_arr_delay_7d` | Float32 | Mean arrival delay over 7d |
| `route_pct_delayed_7d` | Float32 | Fraction of flights delayed over 7d |
| `route_cancellation_rate_7d` | Float32 | Cancellation rate over 7d |
| `route_avg_elapsed_7d` | Float32 | Mean actual elapsed time (minutes) over 7d |
| `route_distance_mi` | Float32 | Route distance in miles |

### `aircraft_features`

Entity: `aircraft_tail` | TTL: 12h | Source: `s3://<feast_s3_base>/aircraft/`

The 12h TTL reflects that cascading delay is only meaningful if the inbound flight landed recently. This feature is written by the PySpark `cascading_delay` job and re-exported to S3 by `feast_feature_export` via Iceberg (not DuckDB).

| Feature | Type | Description |
| --- | --- | --- |
| `cascading_delay_min` | Float32 | Inbound arrival delay propagated forward (minutes) |
| `turnaround_min` | Float32 | Elapsed minutes between inbound arrival and scheduled departure |

### Design Patterns

**Entity granularity follows prediction-time knowledge**: all five entities (`origin`, `dest`, `carrier`, `route_key`, `tail_number`) can be resolved before a flight departs. `flight_id` is excluded because it's a BTS surrogate assigned post-flight.

**TTL is wider than the feature window**: carrier and route features use 7-day windows with an 8-day TTL to absorb up to 24h of pipeline lag without returning stale-null at the online store. Airport features use 26h for the same reason.

### Mapping Feature Views to dbt / Parquet Sources

`feast_feature_export` writes **two Parquet files per feature view** to S3, serving different consumers:

| File | Contents | Consumer |
| --- | --- | --- |
| `data.parquet` | One row per entity, `event_ts = now()` | `feast materialize_incremental` → Redis online store. The `now()` stamp prevents TTL expiry at serving time. |
| `training.parquet` | All historical rows, original `event_ts` values, sorted by `event_ts` | `PITJoiner` (training dataset builder). Real timestamps are required so the ASOF JOIN can find the correct feature snapshot for each historical flight. |

The entity-to-path mapping:

| Feature view | dbt model | S3 prefix | Entity col |
| --- | --- | --- | --- |
| `origin_airport_features` | `feat_origin_airport_windowed` | `/origin_airport/` | `origin` |
| `dest_airport_features` | `feat_dest_airport_windowed` | `/dest_airport/` | `dest` |
| `carrier_features` | `feat_carrier_rolling` | `/carrier/` | `carrier` |
| `route_features` | `feat_route_rolling` | `/route/` | `route_key` |
| `aircraft_features` | `staging.feat_cascading_delay` (Iceberg/PySpark) | `/aircraft/` | `tail_number` |

All sources use `event_ts` as the `timestamp_field`.

---

## Feature Services

Declared in [feature_repo/feature_services.py](../feature_repo/feature_services.py). A `FeatureService` is a named bundle of feature views used for a specific serving scenario. See the [Feast FeatureService docs](https://docs.feast.dev/reference/feature-repository/feature-services).

### Pre-Defined Feature Sets for Serving

**`flight_delay_prediction`** — full bundle for the production model. Provide all five entity keys (`origin`, `dest`, `carrier`, `route_key`, `tail_number`) to get 24 features in one round-trip.

**`flight_delay_early_prediction`** — lightweight bundle for early prediction before aircraft assignment is known (i.e., `tail_number` is unavailable). Contains only:
- `origin_airport_features`: `origin_avg_dep_delay_1h`, `origin_congestion_score_1h`
- `carrier_features`: `carrier_on_time_pct_7d`, `carrier_avg_delay_7d`

The production serving code does not use `FeatureService` objects directly — it passes `ONLINE_FEATURE_REFS` explicitly to `get_online_features()` for column-order stability (see [Train–Serve Consistency](#trainstrike-serve-consistency)).

---

## Offline Store

### S3 Parquet

The offline store is `type: file` in `feature_store.yaml`. Feast reads Parquet files directly from S3 via boto3 (picks up `AWS_*` env vars automatically). In development this points to a MinIO instance; in production it points to Cloudflare R2.

`feast_feature_export` writes two zstd-compressed Parquet files per entity type under `<feast_s3_base>/<entity_type>/`:

- **`data.parquet`** — one row per entity key, `event_ts = now()`. Read by `feast materialize_incremental` to push values into Redis.
- **`training.parquet`** — all historical rows with original `event_ts` values, sorted by `event_ts`. Read by `PITJoiner` during `build_dataset` for the training ASOF JOIN.

### Point-in-Time Join at Training Time

The training dataset builder does **not** use Feast's `get_historical_features()`. Instead it uses DuckDB's [ASOF JOIN](https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins) directly against the S3 Parquet files. This has three advantages:

1. The SQL is auditable — the exact query that ran is visible in Dagster logs.
2. It works without a running Feast registry (useful in CI).
3. ASOF JOIN semantics are testable in isolation.

The join logic lives in [src/bmo/training_dataset_builder/pit_join.py](../src/bmo/training_dataset_builder/pit_join.py). For each feature view, it executes:

```sql
WITH features AS (
    SELECT <entity_col> AS __entity_key, event_ts, <feature_cols>
    FROM read_parquet('<s3_path>')
    WHERE event_ts <= <as_of>       -- no future features
    ORDER BY __entity_key, event_ts -- required for ASOF correctness
),
pit AS (
    SELECT labels.flight_id, labels.event_timestamp,
           features.event_ts, age_seconds, <feature_cols>
    FROM labels
    ASOF LEFT JOIN features
        ON labels.<entity_col> = features.__entity_key
        AND labels.event_timestamp >= features.event_ts
)
SELECT flight_id,
    -- TTL mask: null out features older than TTL rather than drop the row
    CASE WHEN age_seconds > <ttl_seconds> THEN NULL ELSE <col> END AS <col>, ...
FROM pit
```

TTL violations null-out the feature column rather than dropping the row, so the model is trained on partial information and the pipeline can track TTL-miss rates.

The `as_of` parameter is set to `datetime.now(UTC)` at pipeline run time, acting as a leakage guard: no feature snapshot timestamped after the run starts can enter the training data.

### `feast historical_features` Usage

Feast's `get_historical_features()` is intentionally **not used** here. If you need to use it for a new use case, be aware that it performs an in-memory join in Python and requires the full Feast registry to be applied first. For large datasets, prefer the DuckDB ASOF JOIN approach.

---

## Online Store (Redis via Upstash)

### Materialization Workflow

After `feast_feature_export` writes S3 Parquet, the `feast_materialized_features` Dagster asset calls:

```python
store.materialize_incremental(end_date=datetime.now(UTC))
```

`materialize_incremental` reads the high-water mark (last materialization timestamp) per feature view from the Feast registry, fetches only rows newer than that mark from S3, and upserts them into Redis. It is idempotent and safe to re-run.

### Feast Hourly Materialization Schedule (Dagster)

Defined in [dagster_project/schedules/feast_hourly.py](../dagster_project/schedules/feast_hourly.py). Runs `feast_materialized_features` at the top of every hour (`0 * * * *`). The 1-hour cadence matches the shortest feature window (1h rolling stats), so the online store is always within one hour of the dbt feature model outputs.

The `feast_feature_export` asset has a `FreshnessPolicy` that expects a new materialization within 55 minutes of each hour. Both assets in the chain (`feast_feature_export` → `feast_materialized_features`) declare this policy; Dagster will surface stale alerts in the UI if either misses its window.

### Sub-Millisecond Lookup at Inference Time

Online feature retrieval is handled by [src/bmo/serving/feature_client.py](../src/bmo/serving/feature_client.py). At inference time, a single `get_online_features()` call resolves all 24 features from Redis in one round-trip:

```python
response = store.get_online_features(
    features=ONLINE_FEATURE_REFS,   # list of "view_name:feature_name" strings
    entity_rows=[{
        'origin': ..., 'dest': ..., 'carrier': ...,
        'route_key': ..., 'tail_number': ...,
    }],
)
```

Redis stores each feature value under a key derived from the entity key and feature view name. Feast serializes entity keys using `entity_key_serialization_version: 3` (pinned in `feature_store.yaml` — do not change without re-materializing all feature views).

---

## Train–Serve Consistency

### How Feast Enforces It

The same `FeatureView` definitions in `feature_repo/feature_views.py` govern both offline (training) and online (serving) retrieval. The feature names, dtypes, entity keys, and TTLs are declared once and reused. This ensures that a feature named `origin_avg_dep_delay_1h` in the training dataset refers to exactly the same computation as at serving time.

The `ONLINE_FEATURE_REFS` list in [feature_client.py](../src/bmo/serving/feature_client.py) and `ALL_FEATURE_REFS` in [training.py](../dagster_project/assets/training.py) must be kept in sync and in the same order. XGBoost uses column positions, not names, when producing predictions — if the order diverges between training and serving, the model produces silently incorrect outputs.

### What Can Still Go Wrong

**Column order drift**: `ONLINE_FEATURE_REFS` and `ALL_FEATURE_REFS` are defined independently in two files. Adding or reordering a feature in one without updating the other will corrupt predictions at inference time without raising an error.

**TTL mismatch**: if you change a TTL in `feature_repo/feature_views.py` without updating the matching `FeatureViewConfig` in `pit_join.py`, the training TTL and serving TTL diverge — features that pass the TTL check at training time will expire faster (or slower) online.

**Stale materialization**: if the hourly Dagster schedule misses multiple runs (e.g., Redis restart, pipeline failure), online features become stale. `FeatureClient` is **fail-closed**: any `null` feature that is not an aircraft feature causes `get_online_features()` to return `None`, which the API converts to a 503. Aircraft features (`cascading_delay_min`, `turnaround_min`) are imputed as 0.0 when `tail_number` is unknown.

---

## Registry

### `feature_store.yaml` Options

Located at [feature_repo/feature_store.yaml](../feature_repo/feature_store.yaml). The full option reference is in the [Feast feature_store.yaml docs](https://docs.feast.dev/reference/feature-repository/feature-store-yaml).

```yaml
project: bmo
registry: ${FEAST_REGISTRY_PATH}
provider: local

offline_store:
  type: file

online_store:
  type: redis
  connection_string: ${REDIS_URL}

entity_key_serialization_version: 3
```

| Variable | Dev | Prod |
| --- | --- | --- |
| `FEAST_REGISTRY_PATH` | `data/registry.db` (local SQLite) | `s3://staging/feast/registry.db` (R2, shared) |
| `REDIS_URL` | `localhost:6379` | `<upstash-host>:6379,ssl=true,password=<pw>` |

`registry.db` is generated by `feast apply` — never commit it. The SQLite dev registry is local to the machine; the R2 prod registry is shared between the Dagster worker and the FastAPI serving container.

`entity_key_serialization_version: 3` is pinned explicitly. Changing this version corrupts the registry and causes all materialized Redis keys to be unreadable until a full re-materialization.

### Applying Changes (`feast apply`)

Run from the `feature_repo/` directory:

```bash
cd feature_repo
feast apply
```

`feast apply` reads all Python files in the directory, registers entity/feature view/feature service definitions in the registry, and validates the schema. It does **not** materialize data. After applying, trigger `feast_materialized_features` in Dagster to push the updated definitions to Redis.

See the [feast apply CLI reference](https://docs.feast.dev/reference/feast-cli-commands#feast-apply).

### Idempotency Guarantees

`feast apply` is idempotent. Re-applying the same definitions updates the registry in place without deleting materialized data.

`materialize_incremental` is idempotent. It tracks the high-water mark per feature view and will not re-send data already in Redis.

Changing a feature view schema (adding/removing a feature column) and running `feast apply` does not automatically re-materialize. You must trigger `feast_materialized_features` or run `feast materialize` manually after schema changes.

---

## Operating the Feature Store

### Inspecting the Registry

```bash
cd feature_repo

# list all registered feature views
feast feature-views list

# list entities
feast entities list

# list feature services
feast feature-services list

# describe a specific feature view
feast feature-views describe origin_airport_features
```

See the [Feast CLI reference](https://docs.feast.dev/reference/feast-cli-commands) for all commands.

### Re-Materializing Online Features

To force a full re-materialization of all feature views (e.g., after a Redis flush):

```bash
cd feature_repo
feast materialize <start_time> <end_time>
# example:
feast materialize 2024-01-01T00:00:00 2025-01-01T00:00:00
```

Or trigger `feast_materialized_features` in the Dagster UI, which calls `materialize_incremental` from the last high-water mark.

To reset the high-water mark and force full re-processing, delete and re-apply the registry:

```bash
rm data/registry.db   # dev only — use the S3 path in prod
feast apply
# then trigger feast_materialized_features in Dagster
```

### Debugging Missing Features at Serve Time

**Step 1: Check Redis connectivity**

The `/health` endpoint calls `FeatureClient.ping_redis()`. A 503 from `/health` means Redis is unreachable.

**Step 2: Check materialization recency**

In the Dagster UI, verify that `feast_materialized_features` materialized within the last hour. The `materialized_through` metadata field shows the timestamp of the last successful run.

**Step 3: Check feature values in Redis directly**

```python
from feast import FeatureStore
store = FeatureStore(repo_path='feature_repo/')
response = store.get_online_features(
    features=['origin_airport_features:origin_avg_dep_delay_1h'],
    entity_rows=[{'origin': 'ORD'}],
)
print(response.to_dict())
```

A `None` value means either: (a) the entity was never materialized, (b) the value expired due to TTL, or (c) materialization wrote a `null` for that entity key.

**Step 4: Check the Parquet export**

```python
import pandas as pd
# data.parquet = one row per entity, event_ts=now() — for Redis
df = pd.read_parquet('s3://<feast_s3_base>/origin_airport/data.parquet')
print(df[df['origin'] == 'ORD'])
```

If the entity is missing from the Parquet, the issue is upstream in the dbt `feat_origin_airport_windowed` model or in `feast_feature_export`.

**Step 5: Check training feature coverage (if training AUC is 0.5)**

If the model trains to AUC ≈ 0.5 with empty feature importance, the `training.parquet` files may not cover the training data's date range. Inspect null rates:

```python
import s3fs, pyarrow.parquet as pq
from bmo.common.config import settings
from bmo.training.train import _get_feature_columns

fs = s3fs.S3FileSystem(key=settings.s3_access_key_id,
                       secret=settings.s3_secret_access_key,
                       endpoint_url=settings.s3_endpoint_url)

# Check training.parquet timestamp coverage
with fs.open('staging/feast/origin_airport/training.parquet', 'rb') as f:
    df = pq.read_table(f).to_pandas()
print(f"Rows: {len(df)}, event_ts range: {df['event_ts'].min()} → {df['event_ts'].max()}")
```

If `training.parquet` has a single timestamp or doesn't cover the label date range, re-run `feast_feature_export` from the Dagster UI. The ASOF JOIN condition `label.event_timestamp >= feature.event_ts` will match nothing if all feature snapshots postdate the training flights.
