# Feature Engineering

## Overview

Features are built in two layers:

1. **dbt on DuckDB** — SQL window functions over the Iceberg staging tables. Runs as a single unpartitioned `dbt build` call orchestrated by Dagster. Produces five feature tables plus two marts.
2. **PySpark** — one feature (`feat_cascading_delay`) that requires a shuffle-heavy self-join on 58 M+ rows, which DuckDB cannot handle without loading the entire dataset into memory. PySpark writes the result back to Iceberg; dbt reads it through `stg_feat_cascading_delay`.

The final assembled row lives in `mart_training_dataset`, which is the direct input to the training job.

---

## dbt Models (DuckDB)

Project name: `bmo`. Profile: `bmo`. All SQL lives under `dbt_project/models/`.

### Staging Views

Staging models are materialized as **ephemeral** (inlined into downstream SQL — no database object is created). They are thin pass-throughs from the Iceberg `iceberg_staging` source with light renaming and null filters.

| Model | Source table | Key outputs |
| --- | --- | --- |
| `stg_flights` | `staging.staged_flights` | `flight_id` (MD5 surrogate), UTC timestamps, delay columns, `dep_del15`, `arr_del15`, `cancelled`, `diverted`, `distance_mi`, BTS `day_of_week` (1=Mon, 7=Sun) |
| `stg_weather` | `staging.staged_weather` | `station_id`, `iata_code`, `obs_time_utc`, temp, wind, precip, visibility, `sky_conditions`, `present_weather`, pressure |
| `stg_dim_airport` | `staging.dim_airport` | `iata_code`, `icao_code`, lat/lon, `elevation_ft`, `tz_database_timezone`, `lcd_station_id` |
| `stg_dim_route` | `staging.dim_route` | `airline_iata`, `origin`, `dest`, `distance_mi` |
| `stg_feat_cascading_delay` | `staging.feat_cascading_delay` | `flight_id`, `tail_number`, `prev_arr_delay_min`, `prev_dest`, `prev_actual_arrival_utc`, `turnaround_min` — written by PySpark, read here |

The `flight_id` surrogate key is `MD5(flight_date || carrier || flight_number || origin || dest)`. Both `stg_flights.sql` and `cascading_delay.py` compute this identically so joins across the dbt/PySpark boundary work without a mapping table.

### Intermediate: `int_flights_enriched`

Materialized **ephemeral**. Joins `stg_flights` to `stg_weather` twice (once for origin, once for destination) and derives boolean weather condition columns.

#### Point-in-Time Weather Join

Weather observations are matched to flights using a point-in-time (PIT) strategy to prevent data leakage:

- **Origin weather**: latest observation within the 3-hour window `[scheduled_departure_utc − 3h, scheduled_departure_utc]`.
- **Destination weather**: latest observation within the 6-hour window `[scheduled_departure_utc − 6h, scheduled_departure_utc]` (destination weather is known less precisely at departure time, so the window is wider).

Both joins use `QUALIFY row_number() OVER (PARTITION BY flight_id ORDER BY obs_time_utc DESC NULLS LAST) = 1` to reduce the fan-out to exactly one row per flight. The custom dbt test `no_future_leakage` on `origin_obs_time_utc` asserts that the chosen observation never exceeds `scheduled_departure_utc`.

Derived boolean columns added in the final `SELECT`:

| Column | Logic |
| --- | --- |
| `origin_is_thunderstorm` | `origin_present_weather ILIKE '%TS%'` |
| `origin_is_low_vis` | `present_weather ILIKE '%FG%'` OR `origin_visibility_mi < 3.0` |
| `origin_is_high_wind` | `origin_wind_kts > 25.0` |
| `dest_is_thunderstorm` | `dest_present_weather ILIKE '%TS%'` |
| `dest_is_low_vis` | `dest_visibility_mi < 3.0` |

### Feature Models

Feature models are materialized as **tables**. Window functions over millions of rows are expensive; materializing avoids recomputing them on every training query.

#### `feat_origin_airport_windowed` (1h / 24h / 7d)

Source: `stg_flights`. Partition key: `origin`. Order key: `scheduled_departure_utc`.

| Column | Window | Description |
| --- | --- | --- |
| `origin_flight_count_1h` | 1 h | Number of departures in the past hour |
| `origin_avg_dep_delay_1h` | 1 h | Average departure delay (minutes) |
| `origin_pct_delayed_1h` | 1 h | Fraction of flights with `dep_del15 = true` |
| `origin_congestion_score_1h` | 1 h | `flight_count_1h / 10.0` — a 0–50 proxy for runway congestion (10 ≈ 100 hourly movements) |
| `origin_flight_count_24h` | 24 h | Departures in the past 24 hours |
| `origin_avg_dep_delay_24h` | 24 h | Average departure delay |
| `origin_pct_cancelled_24h` | 24 h | Cancellation rate |
| `origin_avg_dep_delay_7d` | 7 d | 7-day trailing average departure delay |
| `origin_pct_delayed_7d` | 7 d | 7-day trailing on-time rate (`dep_del15`) |

> **Known leakage**: DuckDB `RANGE BETWEEN ... AND CURRENT ROW` includes the current flight in its own window. This is a deliberate simplicity tradeoff for the project — the effect on trained models is negligible at scale.

#### `feat_dest_airport_windowed` (1h / 24h)

Source: `stg_flights`. Partition key: `dest`.

| Column | Window | Description |
| --- | --- | --- |
| `dest_avg_arr_delay_1h` | 1 h | Average arrival delay at destination |
| `dest_pct_delayed_1h` | 1 h | Fraction of arrivals delayed ≥15 min |
| `dest_avg_arr_delay_24h` | 24 h | 24-hour trailing average arrival delay |
| `dest_pct_cancelled_24h` | 24 h | 24-hour cancellation rate at destination |
| `dest_pct_diverted_24h` | 24 h | 24-hour diversion rate at destination |

#### `feat_carrier_rolling` (7d)

Source: `stg_flights`. Partition key: `carrier`.

| Column | Description |
| --- | --- |
| `carrier_on_time_pct_7d` | Fraction of flights NOT delayed ≥15 min in past 7 days |
| `carrier_cancellation_rate_7d` | 7-day cancellation rate |
| `carrier_avg_delay_7d` | 7-day average departure delay (minutes) |
| `carrier_flight_count_7d` | Total departures in past 7 days |

#### `feat_route_rolling` (7d)

Source: `stg_flights`. Partition key: `(origin, dest)`. Route key: `origin || '-' || dest`.

| Column | Description |
| --- | --- |
| `route_avg_dep_delay_7d` | 7-day average departure delay |
| `route_avg_arr_delay_7d` | 7-day average arrival delay |
| `route_pct_delayed_7d` | 7-day on-time rate |
| `route_cancellation_rate_7d` | 7-day cancellation rate |
| `route_avg_elapsed_7d` | 7-day average actual elapsed time (minutes) |
| `route_distance_mi` | Static route distance — `MAX(distance_mi) OVER (PARTITION BY origin, dest)` collapses a constant to a scalar |

#### `feat_calendar` (Hour, Day-of-Week, Holiday)

Zero-join; computed purely from `scheduled_departure_utc` and `flight_date`.

| Column | Description |
| --- | --- |
| `scheduled_hour_utc` | Hour of day (0–23) in UTC |
| `day_of_week` | BTS convention: 1=Monday, 7=Sunday |
| `month_of_year` | 1–12 |
| `quarter` | 1–4 (`CEIL(month / 3.0)`) |
| `is_weekend` | `day_of_week IN (6, 7)` |
| `is_holiday` | Left-join against an inline `holidays` CTE covering US major travel holidays 2018–2026 (New Year's Day, Independence Day, Thanksgiving, Christmas, Labor Day, Memorial Day) |
| `is_rush_hour` | UTC hour 12–14 OR 21–23 (approximate morning/evening rush; uses UTC as a nationwide proxy — no origin-tz join) |

> The `holidays` CTE is hardcoded SQL. A comment in the model flags it as a candidate for a dbt seed if the list grows. To add a new year, append rows to the CTE in `feat_calendar.sql`.

### Marts

#### `mart_training_dataset`

Materialized as **table**. The single wide row consumed by the training job. Assembles all feature tables via `LEFT JOIN ... USING (flight_id)` on `int_flights_enriched` as the spine.

Label columns (targets):

| Column | Type | Description |
| --- | --- | --- |
| `dep_delay_min` | float | Departure delay in minutes (regression target) |
| `arr_delay_min` | float | Arrival delay in minutes |
| `is_dep_delayed` | bool | Binary classification target (`dep_del15`) |
| `is_arr_delayed` | bool | `arr_del15` |
| `cancelled` | bool | |
| `diverted` | bool | |

Origin hub size (`origin_hub_size`) is joined from the `hub_airports` dbt seed (`dbt_project/seeds/hub_airports.csv`), which maps IATA codes to `major_hub` / `regional` / `small_regional`. Flights whose origin is not in the seed coalesce to `'small_regional'`.

Cascading delay columns (`cascading_delay_min`, `turnaround_min`) are `NULL` until the PySpark job has run at least once.

#### `mart_predictions`

Materialized as **table**, tagged `serving` and `monitoring`. Joins batch prediction Parquet files (written by the `batch_predictions` Dagster asset to `s3://$S3_BUCKET_STAGING/predictions/**/data.parquet`) with actuals from `stg_flights` via a `LEFT JOIN` on `flight_id`.

At compile time, a Jinja `{% if execute %}` block counts matching files with DuckDB's `glob()`. If no prediction files exist yet, the model emits a typed empty `SELECT WHERE 1=0` rather than erroring. Once files exist, `read_parquet()` with a glob pattern is used.

`actual_is_delayed` is `NULL` for recent flights because BTS actuals have an approximately 60-day publication lag. This mart feeds the `drift_report` Evidently asset.

### Dagster–dbt Integration

#### `BmoDbtTranslator` and Asset Key Mapping

`dagster_project/assets/features_dbt.py` defines `BmoDbtTranslator`, a subclass of `DagsterDbtTranslator`. It overrides `get_asset_key` for dbt source nodes so that Dagster draws edges from the Python staging assets to dbt models — without this, the cross-boundary DAG edges are broken.

The mapping (`_SOURCE_TO_ASSET_KEY`):

| dbt source (`iceberg_staging.*`) | Dagster asset key |
| --- | --- |
| `staged_flights` | `staged_flights` |
| `staged_weather` | `staged_weather` |
| `dim_airport` | `dim_airport` |
| `dim_route` | `dim_route` |
| `feat_cascading_delay` | `feat_cascading_delay` |

`get_asset_spec` is also overridden to inject two additional upstream dependencies that dbt cannot see:
- `mart_drift_metrics` depends on `drift_report` (Evidently asset writes drift Parquet that DuckDB reads).
- `mart_predictions` depends on `batch_predictions` (writes prediction Parquet read by `read_parquet()`).

#### Auto-Loading dbt Models as Dagster Assets

The `@dbt_assets` decorator reads `dbt_project/target/manifest.json` at import time and registers every dbt model as a Dagster asset. The decorated function `bmo_dbt_assets` is the executor — it runs `dbt build` — but individual nodes in the Dagster asset graph correspond to each model (`feat_origin_airport_windowed`, `feat_carrier_rolling`, etc.).

`dbt build` runs models + tests + seeds in DAG order and yields `AssetMaterialization` and `AssetCheckResult` events as it progresses.

`bmo_dbt_assets` is unpartitioned even though its upstream `staged_flights` is a `MonthlyPartitionsDefinition` asset. This is intentional: the dbt feature models read the entire Iceberg table in one pass; partitioning dbt would trigger 84+ redundant runs. Dagster emits a warning about the partitioned→unpartitioned edge — this is expected.

`AutomationCondition.eager()` is applied so `dbt build` is triggered automatically whenever any upstream Python asset is materialized.

---

## PySpark: `feat_cascading_delay`

Source: `bmo/pyspark_jobs/cascading_delay.py`. Entry point: `compute_cascading_delay(spark)`.

### What It Computes

For each flight, the feature records how late the **same aircraft** (identified by `tail_number`) arrived on its immediately preceding leg. Late incoming aircraft are a leading cause of departure delays; the feature gives the model a direct signal for this propagation effect.

Output columns per flight:

| Column | Description |
| --- | --- |
| `prev_arr_delay_min` | Arrival delay (minutes) of the aircraft's previous flight |
| `prev_dest` | Airport where the aircraft was before this flight |
| `prev_actual_arrival_utc` | Timestamp of the previous actual arrival |
| `turnaround_min` | `(scheduled_departure_utc − prev_actual_arrival_utc) / 60` — buffer time between turns |

The first flight of each aircraft's day has `NULL` for all four columns.

### LAG Window per `tail_number`

```python
w = Window.partitionBy('tail_number').orderBy('scheduled_departure_utc')

F.lag('arr_delay_min', 1).over(w).alias('prev_arr_delay_min')
F.lag('dest', 1).over(w).alias('prev_dest')
F.lag('actual_arrival_utc', 1).over(w).alias('prev_actual_arrival_utc')
```

The `flight_id` surrogate is recomputed in PySpark using the same `MD5(flight_date || carrier || flight_number || origin || dest)` formula as `stg_flights.sql`, so the join from dbt's `stg_feat_cascading_delay` back to `mart_training_dataset` requires no mapping table.

**Why PySpark and not DuckDB?** The self-join on `tail_number` ordered by `scheduled_departure_utc` is a shuffle-heavy operation. Spark handles it natively with partition-aware window functions; DuckDB would need to sort the entire 58 M-row dataset into memory.

### Iceberg on S3 via JdbcCatalog

The SparkSession uses Iceberg's `JdbcCatalog` (catalog name `bmo`) backed by the same PostgreSQL database that PyIceberg's `SqlCatalog` writes. Both tools share the `iceberg_tables` row and read/write the same physical metadata files in MinIO/S3.

Table references use two-part names (`staging.staged_flights`) with `spark.sql.defaultCatalog=bmo`.

On first run, `feat_cascading_delay` may not exist yet. The job checks `spark.catalog.tableExists('staging.feat_cascading_delay')` and creates the table with an explicit `LOCATION` before writing:

```sql
CREATE TABLE staging.feat_cascading_delay
USING iceberg
LOCATION 's3a://<S3_BUCKET_STAGING>/iceberg/feat_cascading_delay'
AS SELECT * FROM _feat_cascading_delay_tmp WHERE 1=0
```

This pins the table to the canonical flat path (`s3://staging/iceberg/<table>`) rather than the default `warehouse/{namespace}/{table}` path that `createOrReplace()` would use.

Subsequent runs call `result.writeTo(...).overwrite(F.lit(True))` to replace all rows.

### Running Locally vs. Production

| Setting | Local (MinIO) | Production (R2 / AWS S3) |
| --- | --- | --- |
| `s3_endpoint_url` | `http://...` | `https://...` |
| `fs.s3a.connection.ssl.enabled` | `false` | `true` (derived automatically from URL scheme) |
| `fs.s3a.endpoint.region` | set to avoid AWS region auto-discovery | same |

SSL-enabled is derived at runtime from `settings.s3_endpoint_url.startswith('https://')` to avoid the `AWSRedirectException` (`region null`) that S3A raises when it receives a 301 redirect from a non-AWS endpoint.

The Dagster asset `feat_cascading_delay` (`dagster_project/assets/features_python.py`) wraps the job:

```python
spark = make_spark_session('bmo-cascading-delay')
row_count = compute_cascading_delay(spark)
spark.stop()
```

It is in the `features` asset group and declares `staged_flights` as its upstream dependency. dbt's `stg_feat_cascading_delay` declares `feat_cascading_delay` as a source, which `BmoDbtTranslator` maps back to this Dagster asset key, completing the DAG edge.

---

## Adding a New Feature

### dbt Feature Model Checklist

1. Create `dbt_project/models/features/feat_<name>.sql`.
   - Select `flight_id` and `scheduled_departure_utc AS event_ts` as the first two columns — marts join on `flight_id`.
   - Use `{{ ref('stg_flights') }}` (or another staging model) as the source.
   - Declare named `WINDOW` clauses at the bottom; do not repeat `OVER (...)` inline.
2. Add column tests to `dbt_project/models/features/_features_schema.yml` — at minimum `not_null` on `flight_id` and an `accepted_range` on any percentage column.
3. Add the new feature columns to `mart_training_dataset.sql` via a `LEFT JOIN feat_<name> USING (flight_id)`.
4. Features are materialized as `table` automatically (set in `dbt_project.yml` for the `features` layer).
5. Rebuild the manifest: `cd dbt_project && dbt parse` (or `dbt build`). Dagster reads `target/manifest.json` at import time.

### PySpark Feature Checklist

1. Add a job function in `src/bmo/pyspark_jobs/<feature_name>.py` returning `row_count: int`.
2. Write the result to `staging.<feature_name>` in Iceberg. On first run, create the table with an explicit `LOCATION` pointing to `s3a://{settings.s3_bucket_staging}/iceberg/<feature_name>`.
3. Add a Dagster `@asset` in `dagster_project/assets/features_python.py` that calls `make_spark_session(...)`, runs the job, and calls `spark.stop()` in a `finally` block.
4. Add a dbt staging pass-through in `dbt_project/models/staging/stg_<feature_name>.sql` that selects from `{{ source('iceberg_staging', '<feature_name>') }}`.
5. Register the new source in `dbt_project/models/staging/_sources.yml` and add the `(source_name, table_name) → AssetKey` mapping to `_SOURCE_TO_ASSET_KEY` in `features_dbt.py`.
6. Add the columns to `mart_training_dataset.sql` via `LEFT JOIN stg_<feature_name> USING (flight_id)`.

---

## Feature Naming Conventions

| Prefix | Layer | Description |
| --- | --- | --- |
| `stg_` | Staging | Thin pass-through from Iceberg source; ephemeral |
| `int_` | Intermediate | Multi-source joins and derivations; ephemeral |
| `feat_` | Features | ML feature tables; materialized as table |
| `mart_` | Marts | Final assembled outputs; materialized as table |

Column names encode their aggregation window:

- `_1h` — 1-hour trailing window
- `_24h` — 24-hour trailing window
- `_7d` — 7-day trailing window
- `_pct_` — a fraction in [0, 1]; tested with `dbt_utils.accepted_range`
- `_min` — delay in minutes
- `_kts` — wind speed in knots
- `_mi` — distance or visibility in miles

---

## Known Limitations & Edge Cases

**Current-row leakage in window features.** DuckDB's `RANGE BETWEEN INTERVAL '...' PRECEDING AND CURRENT ROW` includes the current flight in its own aggregate. For training this is a minor bias at scale; for online serving the feature is recomputed from historical rows only, so the behavior diverges. Switching to `RANGE BETWEEN INTERVAL '...' PRECEDING AND INTERVAL '1 second' PRECEDING` would eliminate the leakage.

**`stg_feat_cascading_delay` fails until PySpark runs.** The `feat_cascading_delay` Iceberg table does not exist until `compute_cascading_delay` has been called at least once. `dbt run` will error on `stg_feat_cascading_delay` if the table is absent. Run the `feat_cascading_delay` Dagster asset first, then run dbt.

**BTS actuals lag.** `mart_predictions` joins predictions to actuals via `LEFT JOIN`. `actual_is_delayed` is `NULL` for any flight within approximately 60 days of the score date because BTS has not yet published the data.

**`is_rush_hour` uses UTC.** The calendar feature approximates rush hours using UTC (12–14 and 21–23) rather than local departure-airport time. This is accurate for east-coast hubs but drifts for west-coast airports. A corrected version would join `stg_dim_airport.tz_database_timezone` and convert `scheduled_departure_utc` to local time.

**`feat_calendar` holidays end in 2025/2026.** The inline `holidays` CTE covers dates through 2025 (some through 2026). Add rows for future years or migrate to a dbt seed (`dbt_project/seeds/`) if the list grows.

**`hub_airports` seed must be kept in sync with IATA codes.** If a new origin airport is not in `hub_airports.csv`, it silently coalesces to `'small_regional'` in `mart_training_dataset`. Check the seed when adding coverage for new airports.
