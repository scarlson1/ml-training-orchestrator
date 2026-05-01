# Tables Reference: S3, Iceberg, DuckDB, and Postgres

This catalog documents the table-like data stores used by the orchestrator:

- Postgres application tables for monitoring and live accuracy.
- Postgres Iceberg catalog metadata for S3-backed Iceberg tables.
- S3-compatible object storage tables: Iceberg, raw Parquet, Feast offline Parquet, prediction outputs, training datasets, and monitoring outputs.
- DuckDB/dbt tables built from the S3/Iceberg sources.

Local defaults come from `.env.example` and Docker Compose:

| Setting               | Typical value                       | Purpose                                                                         |
| --------------------- | ----------------------------------- | ------------------------------------------------------------------------------- |
| `S3_BUCKET_RAW`       | `raw`                               | Raw ingestion Parquet and manifests                                             |
| `S3_BUCKET_STAGING`   | `staging`                           | Iceberg warehouse, dbt/Feast exports, predictions, datasets, monitoring outputs |
| `S3_BUCKET_REJECTED`  | `rejected`                          | Rows rejected by staging validation                                             |
| `FEAST_S3_BASE`       | `s3://staging/feast`                | Feast offline-store Parquet root                                                |
| `DATASET_S3_BASE`     | `s3://staging/datasets`             | Content-addressed training dataset root                                         |
| `DUCKDB_PATH`         | `/tmp/bmo_features.duckdb`          | dbt/DuckDB database file                                                        |
| `ICEBERG_CATALOG_URI` | `postgresql+psycopg2://.../iceberg` | PyIceberg SQL catalog metadata database                                         |
| `POSTGRES_DB`         | `bmo`                               | Application DB for MLflow, monitoring tables, etc.                              |

## Lineage and API Access

This section is the quick answer for each table-like object: which Dagster asset produces it, what consumes it, and whether FastAPI should query it directly.

General API rule:

- FastAPI dashboard endpoints should query small serving-oriented tables/marts: Postgres `drift_metrics`, Postgres `live_accuracy`, or DuckDB marts such as `mart_predictions`.
- FastAPI inference should query Feast online features through `FeatureClient`, not S3, DuckDB, or Iceberg.
- Raw, rejected, and internal staging tables should usually not be queried by FastAPI. Query a curated mart or Postgres table instead.

| Table / object                                         | Produced by                                                   | Consumed by                                                                                           | FastAPI direct?                                      | Query instead from `api.py`                                                                                                            |
| ------------------------------------------------------ | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `drift_metrics` Postgres                               | `drift_report` Dagster asset via SQLAlchemy upsert            | `drift_retrain_sensor`, `/api/drift/metrics`, `/api/drift/heatmap`, `/api/psi/{feature_name}`         | Yes                                                  | Query directly with `get_db()` / SQLAlchemy                                                                                            |
| `live_accuracy` Postgres                               | `ground_truth_backfill` Dagster asset                         | `/api/model-stats`, `/api/accuracy`, React dashboard                                                  | Yes                                                  | Query directly with `get_db()` / SQLAlchemy                                                                                            |
| Iceberg catalog metadata DB                            | PyIceberg/Spark catalog calls inside staging and Spark assets | PyIceberg, Spark, dbt DuckDB Iceberg plugin                                                           | No                                                   | Query registered Iceberg tables through PyIceberg/Spark/DuckDB, not catalog internals                                                  |
| `staging.staged_flights` Iceberg                       | `staged_flights` Dagster asset                                | `bmo_dbt_assets` models, `feat_cascading_delay`, `batch_predictions` indirectly through `stg_flights` | Usually no                                           | Query `mart_predictions`, `mart_training_dataset`, or `stg_flights` through DuckDB if an operational endpoint truly needs flight rows  |
| `staging.staged_weather` Iceberg                       | `staged_weather` Dagster asset                                | `bmo_dbt_assets` models, especially `stg_weather` and `int_flights_enriched`                          | No                                                   | Query weather-derived fields from `mart_training_dataset` or a curated DuckDB endpoint                                                 |
| `staging.dim_airport` Iceberg                          | `dim_airport` Dagster asset                                   | `staged_flights`, `dim_route`, `bmo_dbt_assets` models                                                | Rarely                                               | Query `stg_dim_airport` through DuckDB for read-only admin/reference endpoints                                                         |
| `staging.dim_route` Iceberg                            | `dim_route` Dagster asset                                     | `bmo_dbt_assets` models                                                                               | Rarely                                               | Query `stg_dim_route` or route fields in `mart_training_dataset` through DuckDB                                                        |
| `staging.feat_cascading_delay` Iceberg                 | `feat_cascading_delay` Dagster asset                          | `stg_feat_cascading_delay`, `mart_training_dataset`, `feast_feature_export` aircraft export           | No                                                   | Query `mart_training_dataset` for offline analysis or Feast/Redis through `FeatureClient` for inference                                |
| `raw.faa_airports` Parquet                             | `raw_faa_airports` Dagster asset                              | `station_map`, `dim_airport`                                                                          | No                                                   | Query `stg_dim_airport` or `staging.dim_airport`                                                                                       |
| `raw.openflights_routes` Parquet                       | `raw_openflights_routes` Dagster asset                        | `dim_route`                                                                                           | No                                                   | Query `stg_dim_route` or route fields in `mart_training_dataset`                                                                       |
| `raw.bts_flights` Parquet                              | `raw_bts_flights` Dagster asset                               | `staged_flights`                                                                                      | No                                                   | Query `stg_flights`, `mart_predictions`, or `mart_training_dataset`                                                                    |
| `raw.noaa_weather` Parquet                             | `raw_noaa_weather` Dagster asset                              | `staged_weather`                                                                                      | No                                                   | Query `stg_weather` or weather-derived fields in `mart_training_dataset`                                                               |
| `rejected.bts` Parquet                                 | `staged_flights` validation path                              | Humans/debugging only                                                                                 | No                                                   | Add a separate admin-only debugging endpoint if needed; do not use for dashboard/inference                                             |
| `rejected.noaa` Parquet                                | `staged_weather` validation path                              | Humans/debugging only                                                                                 | No                                                   | Add a separate admin-only debugging endpoint if needed; do not use for dashboard/inference                                             |
| `feast.origin_airport` Parquet                         | `feast_feature_export`                                        | Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`    | No for API dashboards; no for inference              | For inference use `FeatureClient.get_features()`; for analytics query `mart_training_dataset`                                          |
| `feast.dest_airport` Parquet                           | `feast_feature_export`                                        | Feast historical retrieval and materialization                                                        | No                                                   | For inference use `FeatureClient.get_features()`; for analytics query `mart_training_dataset`                                          |
| `feast.carrier` Parquet                                | `feast_feature_export`                                        | Feast historical retrieval and materialization                                                        | No                                                   | For inference use `FeatureClient.get_features()`; for analytics query `mart_training_dataset`                                          |
| `feast.route` Parquet                                  | `feast_feature_export`                                        | Feast historical retrieval and materialization                                                        | No                                                   | For inference use `FeatureClient.get_features()`; for analytics query `mart_training_dataset`                                          |
| `feast.aircraft` Parquet                               | `feast_feature_export` from `staging.feat_cascading_delay`    | Feast historical retrieval and materialization                                                        | No                                                   | For inference use `FeatureClient.get_features()`; for analytics query `mart_training_dataset`                                          |
| `training_datasets` Parquet                            | `training_dataset` Dagster asset                              | `trained_model`, `drift_report` reference distribution, MLflow tags/artifacts                         | No                                                   | Query model/accuracy summaries from `live_accuracy`; use direct S3 only for offline model/debug workflows                              |
| `predictions` Parquet                                  | `batch_predictions` Dagster asset                             | `mart_predictions`, `drift_report` current-window feature retrieval                                   | Usually no                                           | Query `mart_predictions` through DuckDB; `/api/predictions` already follows this pattern                                               |
| `monitoring.metrics` Parquet                           | `drift_report` Dagster asset                                  | `mart_drift_metrics`, archive/history                                                                 | No for API                                           | Query Postgres `drift_metrics` for API freshness; query `mart_drift_metrics` for DuckDB/offline lineage                                |
| `monitoring.reports` HTML                              | `drift_report` Dagster asset                                  | GitHub Pages sync, Dagster metadata links                                                             | No table query                                       | Link to the object/report URL rather than querying it                                                                                  |
| `serving.model_config` JSON                            | `deployed_api` Dagster asset                                  | FastAPI service reload/startup workflow                                                               | Not as a table                                       | Use `/model-info` for loaded model state; `/admin/reload` to refresh                                                                   |
| `stg_*`, `feat_*`, `int_*`, `mart_*` DuckDB/dbt models | `bmo_dbt_assets` Dagster dbt asset wrapper                    | Feast export, training, serving, monitoring, ad hoc analysis                                          | Yes for curated marts; cautious for staging/features | Prefer `mart_predictions`, `mart_training_dataset`, `mart_drift_metrics`; use lower-level dbt models only for internal/admin endpoints |

## Query Setup

### DuckDB against S3 Parquet

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_region = 'us-east-1';
SET s3_access_key_id = '<access-key>';
SET s3_secret_access_key = '<secret-key>';
SET s3_endpoint = 'localhost:9000';
SET s3_url_style = 'path';
SET s3_use_ssl = false;
```

### DuckDB against Iceberg

dbt configures the `iceberg` extension and `dbt.adapters.duckdb.plugins.iceberg` plugin in `dbt_project/profiles.yml`. Inside project code, dbt sources such as `{{ source('iceberg_staging', 'staged_flights') }}` resolve through the PyIceberg SQL catalog.

For ad hoc DuckDB queries, scan the table location directly when the catalog plugin is unavailable:

```sql
INSTALL iceberg;
LOAD iceberg;

SELECT *
FROM iceberg_scan('s3://staging/iceberg/staged_flights/', allow_moved_paths = true)
WHERE flight_date = DATE '2026-01-15';
```

### PyIceberg

```python
from bmo.common.iceberg import make_catalog

catalog = make_catalog()
table = catalog.load_table('staging.staged_flights')
df = table.scan(
    selected_fields=('flight_date', 'origin', 'dest', 'dep_delay_min')
).to_pandas()
```

### Spark Iceberg

The Spark session in `src/bmo/pyspark_jobs/session.py` configures catalog `bmo` with `org.apache.iceberg.jdbc.JdbcCatalog`.

```sql
SELECT origin, COUNT(*) AS flights
FROM staging.staged_flights
WHERE flight_date >= DATE '2026-01-01'
GROUP BY origin
ORDER BY flights DESC;
```

### SQLAlchemy/Postgres

```python
from sqlalchemy import create_engine, text

engine = create_engine('postgresql+psycopg2://bmo:bmo@localhost:5432/bmo')

with engine.begin() as conn:
    rows = conn.execute(
        text("""
            SELECT feature_name, psi_score, is_breached
            FROM drift_metrics
            WHERE report_date = :report_date
            ORDER BY rank
        """),
        {'report_date': '2026-01-15'},
    ).mappings().all()
```

## Postgres Tables

These are created by `infra/postgres/create_monitoring_tables.sql`.

- `drift_metrics` - calculated from each report → creates one row per feature.
- `live_accuracy` - Compute live accuracy metrics from `mart_predictions`. Once `raw_bts_flights` ingests actual delayed data, computes accuracy of model by comparing raw_bts_flights to the predictions produced by the model (~60 day lag)

### `drift_metrics`

Created by `infra/postgres/create_monitoring_tables.sql`. Populated by the `drift_report` Dagster asset using SQLAlchemy upserts.

One row per `(report_date, feature_name)` from drift monitoring.

Lineage and API access:

- Produced by: `drift_report`.
- Consumed by: `drift_retrain_sensor`, `/api/drift/metrics`, `/api/drift/heatmap`, `/api/psi/{feature_name}`.
- FastAPI: query directly with SQLAlchemy via `get_db()`.

| Column          | Type                                 | Notes                                      |
| --------------- | ------------------------------------ | ------------------------------------------ |
| `id`            | `SERIAL PRIMARY KEY`                 | Surrogate key                              |
| `report_date`   | `DATE NOT NULL`                      | Drift report partition date                |
| `feature_name`  | `TEXT NOT NULL`                      | Feature column name                        |
| `psi_score`     | `DOUBLE PRECISION NOT NULL`          | Population Stability Index                 |
| `kl_divergence` | `DOUBLE PRECISION`                   | Optional KL divergence                     |
| `rank`          | `INTEGER NOT NULL`                   | Feature importance rank, `1` is highest    |
| `is_breached`   | `BOOLEAN NOT NULL`                   | True when PSI exceeds threshold            |
| `model_version` | `TEXT`                               | Champion model version used for the report |
| `computed_at`   | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Write timestamp                            |

Constraints and indexes:

- `UNIQUE (report_date, feature_name)` for idempotent upserts.
- Indexes on `rank`, `computed_at`, and `report_date`.

#### SQLAlchemy example

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://user:pass@host:5432/db")

with engine.connect() as conn:
    rows = conn.execute(
        text("""
            SELECT
                report_date,
                feature_name,
                psi_score,
                rank,
                is_breached
            FROM drift_metrics
            WHERE report_date BETWEEN :start_date AND :end_date
            ORDER BY report_date DESC, rank ASC
        """),
        {"start_date": "2026-01-01", "end_date": "2026-01-31"},
    ).mappings().all()
```

**Typical query:**

```sql
SELECT report_date, feature_name, psi_score, rank
FROM drift_metrics
WHERE rank <= 10
  AND computed_at > NOW() - INTERVAL '7 days'
  AND is_breached
ORDER BY computed_at DESC, rank ASC;
```

SQLAlchemy upsert shape:

```python
from sqlalchemy import create_engine, text

engine = create_engine(postgres_url)

with engine.begin() as conn:
    conn.execute(
        text("""
            INSERT INTO drift_metrics (
                report_date, feature_name, psi_score, kl_divergence,
                rank, is_breached, model_version, computed_at
            )
            VALUES (
                :report_date, :feature_name, :psi_score, :kl_divergence,
                :rank, :is_breached, :model_version, :computed_at
            )
            ON CONFLICT (report_date, feature_name) DO UPDATE SET
                psi_score = EXCLUDED.psi_score,
                kl_divergence = EXCLUDED.kl_divergence,
                rank = EXCLUDED.rank,
                is_breached = EXCLUDED.is_breached,
                model_version = EXCLUDED.model_version,
                computed_at = EXCLUDED.computed_at
        """),
        rows,
    )
```

### `live_accuracy`

Created by `infra/postgres/create_monitoring_tables.sql`. Populated by `ground_truth_backfill` after BTS actuals arrive and `mart_predictions` can join predictions to labels.

One row per `(score_date, model_version)` once ground truth becomes available.

Lineage and API access:

- Produced by: `ground_truth_backfill`.
- Consumed by: `/api/model-stats`, `/api/accuracy`, React dashboard.
- FastAPI: query directly with SQLAlchemy via `get_db()`.

| Column                 | Type                                 | Notes                                   |
| ---------------------- | ------------------------------------ | --------------------------------------- |
| `id`                   | `SERIAL PRIMARY KEY`                 | Surrogate key                           |
| `score_date`           | `DATE NOT NULL`                      | Prediction date                         |
| `model_version`        | `TEXT NOT NULL`                      | Model registry version                  |
| `n_flights`            | `INTEGER NOT NULL`                   | Scored flights in the group             |
| `n_with_actuals`       | `INTEGER NOT NULL`                   | Rows where actuals are available        |
| `accuracy`             | `DOUBLE PRECISION`                   | Classification accuracy                 |
| `precision_score`      | `DOUBLE PRECISION`                   | Precision                               |
| `recall_score`         | `DOUBLE PRECISION`                   | Recall                                  |
| `f1`                   | `DOUBLE PRECISION`                   | F1                                      |
| `roc_auc`              | `DOUBLE PRECISION`                   | ROC AUC, null if only one class exists  |
| `log_loss`             | `DOUBLE PRECISION`                   | Log loss, null if only one class exists |
| `brier_score`          | `DOUBLE PRECISION`                   | Brier score                             |
| `positive_rate`        | `DOUBLE PRECISION`                   | Predicted positive rate                 |
| `actual_positive_rate` | `DOUBLE PRECISION`                   | Observed positive rate                  |
| `computed_at`          | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Write timestamp                         |

#### Constraints / indexes

- Unique constraint: `(score_date, model_version)`
- Indexes: `score_date`, `model_version`

<!-- markdownlint-disable MD024 -->

#### SQLAlchemy example

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://user:pass@host:5432/db")

with engine.connect() as conn:
    rows = conn.execute(
        text("""
            SELECT
                model_version,
                AVG(roc_auc) AS avg_roc_auc,
                MAX(score_date) AS last_scored,
                SUM(n_flights) AS total_flights
            FROM live_accuracy
            GROUP BY model_version
            ORDER BY MAX(score_date) DESC
        """)
    ).mappings().all()
```

**Typical query:**

```sql
SELECT
    model_version,
    MIN(score_date) AS first_score_date,
    MAX(score_date) AS last_score_date,
    AVG(roc_auc) AS avg_roc_auc,
    AVG(f1) AS avg_f1,
    SUM(n_with_actuals) AS total_actuals
FROM live_accuracy
GROUP BY model_version
ORDER BY last_score_date DESC;
```

## Iceberg catalog metadata tables

`infra/postgres/init.sql` creates a separate `iceberg` database. PyIceberg's `SqlCatalog` and Spark's `JdbcCatalog` store Iceberg metadata rows there. These are catalog internals, not analytical data tables.

Expected user-facing tables registered in the catalog:

- `staging.staged_flights`
- `staging.staged_weather`
- `staging.dim_airport`
- `staging.dim_route`
- `staging.feat_cascading_delay`

Use PyIceberg, Spark, or DuckDB Iceberg scans to query the data. Do not query or mutate the catalog metadata tables directly unless debugging catalog state.

## S3 Iceberg Tables

All Iceberg tables store metadata and data files under `s3://<S3_BUCKET_STAGING>/iceberg/<table_name>`. The catalog name is `bmo`, and the namespace is `staging`.

- `staging.staged_flights` (Iceberg) - cleaned up data from `raw_bts_flights`. monthly partition.
  - `s3://<S3_BUCKET_STAGING>/iceberg/staged_flights`
- `staging.staged_weather` (Iceberg) - noaa weather data. month transform on `obs_time_utc`
  - `s3://<S3_BUCKET_STAGING>/iceberg/staged_weather`
- `staging.dim_airport` (Iceberg) - unpartitioned and (almost) unchanging
  - `s3://<S3_BUCKET_STAGING>/iceberg/dim_airport`
- `staging.dim_route` (Iceberg) - unpartitioned route dimension loaded from OpenFlights and consumed by dbt route/reference models
  - `s3://<S3_BUCKET_STAGING>/iceberg/dim_route`
- `staging.feat_cascading_delay` (Iceberg) - produce by pyspark & consumed by dbt model `stg_feat_cascading_delay`
  - `s3a://<S3_BUCKET_STAGING>/iceberg/feat_cascading_delay`

### `staging.staged_flights`

Source: raw BTS Parquet. Writer: `src/bmo/staging/flights.py`. Partitioning: month transform on `flight_date`.

Location: `s3://staging/iceberg/staged_flights`

Lineage and API access:

- Produced by: `staged_flights`.
- Consumed by: `bmo_dbt_assets`, `feat_cascading_delay`, and `batch_predictions` indirectly through DuckDB `stg_flights`.
- FastAPI: usually do not query this Iceberg table directly. Query `mart_predictions` for scored/actual joined rows, `mart_training_dataset` for training/feature analysis, or `stg_flights` through DuckDB only for internal flight-row admin endpoints.

| Column                    | Type                 | Notes                                  |
| ------------------------- | -------------------- | -------------------------------------- |
| `year`                    | `int16`              | BTS year                               |
| `month`                   | `int8`               | BTS month                              |
| `day_of_month`            | `int8`               | Day of month                           |
| `day_of_week`             | `int8`               | BTS convention: 1 = Monday, 7 = Sunday |
| `flight_date`             | `date32`             | Local flight date                      |
| `reporting_airline`       | `string`             | Carrier code                           |
| `tail_number`             | `string`             | Aircraft tail number                   |
| `flight_number`           | `int32`              | Carrier flight number                  |
| `origin`                  | `string`             | Origin IATA                            |
| `dest`                    | `string`             | Destination IATA                       |
| `origin_tz`               | `string`             | Origin timezone                        |
| `dest_tz`                 | `string`             | Destination timezone                   |
| `scheduled_departure_utc` | `timestamp[us, UTC]` | Scheduled departure in UTC             |
| `actual_departure_utc`    | `timestamp[us, UTC]` | Null for cancelled flights             |
| `scheduled_arrival_utc`   | `timestamp[us, UTC]` | Scheduled arrival in UTC               |
| `actual_arrival_utc`      | `timestamp[us, UTC]` | Null for cancelled/diverted flights    |
| `dep_delay_min`           | `float32`            | Signed departure delay                 |
| `arr_delay_min`           | `float32`            | Signed arrival delay                   |
| `dep_del15`               | `bool`               | Departure delayed at least 15 minutes  |
| `arr_del15`               | `bool`               | Arrival delayed at least 15 minutes    |
| `cancelled`               | `bool`               | Cancelled flag                         |
| `cancellation_code`       | `string`             | BTS cancellation code                  |
| `diverted`                | `bool`               | Diverted flag                          |
| `crs_elapsed_min`         | `float32`            | Scheduled elapsed minutes              |
| `actual_elapsed_min`      | `float32`            | Actual elapsed minutes                 |
| `distance_mi`             | `float32`            | BTS distance                           |
| `carrier_delay_min`       | `float32`            | Carrier delay component                |
| `weather_delay_min`       | `float32`            | Weather delay component                |
| `nas_delay_min`           | `float32`            | NAS delay component                    |
| `late_aircraft_delay_min` | `float32`            | Late aircraft delay component          |

**DuckDB/dbt query:**

```sql
SELECT origin, COUNT(*) AS flights, AVG(dep_delay_min) AS avg_dep_delay
FROM {{ source('iceberg_staging', 'staged_flights') }}
WHERE flight_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
GROUP BY origin
ORDER BY flights DESC;
```

**PyIceberg query:**

```python
table = make_catalog().load_table('staging.staged_flights')
df = table.scan(
    selected_fields=('flight_date', 'origin', 'dep_delay_min')
).to_pandas()
```

### `staging.staged_weather`

Source: raw NOAA LCD Parquet. Writer: `src/bmo/staging/weather.py`. Partitioning: month transform on `obs_time_utc`.

Location: `s3://staging/iceberg/staged_weather`

Lineage and API access:

- Produced by: `staged_weather`.
- Consumed by: `bmo_dbt_assets`, mainly `stg_weather` and `int_flights_enriched`.
- FastAPI: **do not query directly**. Query weather-derived fields from `mart_training_dataset`, or add a curated DuckDB model/API endpoint if the dashboard needs weather diagnostics.

| Column                   | Type                 | Notes                   |
| ------------------------ | -------------------- | ----------------------- |
| `station_id`             | `string`             | NOAA LCD station ID     |
| `iata_code`              | `string`             | Airport IATA            |
| `obs_time_utc`           | `timestamp[us, UTC]` | Observation timestamp   |
| `temp_f`                 | `float32`            | Temperature             |
| `dew_point_f`            | `float32`            | Dew point               |
| `relative_humidity_pct`  | `float32`            | Relative humidity       |
| `wind_speed_kts`         | `float32`            | Wind speed              |
| `wind_dir_deg`           | `float32`            | Wind direction          |
| `precip_1h_in`           | `float32`            | One-hour precipitation  |
| `visibility_mi`          | `float32`            | Visibility              |
| `sky_conditions`         | `string`             | NOAA sky condition text |
| `present_weather`        | `string`             | NOAA weather text       |
| `sea_level_pressure_hpa` | `float32`            | Sea-level pressure      |

**DuckDB direct scan:**

```sql
SELECT iata_code, obs_time_utc, temp_f, wind_speed_kts
FROM iceberg_scan('s3://staging/iceberg/staged_weather/', allow_moved_paths = true)
WHERE iata_code = 'ORD'
  AND obs_time_utc >= TIMESTAMPTZ '2026-01-01 00:00:00+00';
```

### `staging.dim_airport`

Source: OurAirports plus NOAA station map. Writer: `src/bmo/staging/dimensions.py`. Partitioning: unpartitioned.

Location: `s3://staging/iceberg/dim_airport`

Lineage and API access:

- Produced by: `dim_airport`.
- Consumed by: `staged_flights`, `dim_route`, and `bmo_dbt_assets`.
- FastAPI: rarely query directly. Query `stg_dim_airport` through DuckDB for read-only reference/admin endpoints. In dbt models, use `{{ source('iceberg_staging', 'dim_airport') }}`.

| Column                 | Type      | Notes                       |
| ---------------------- | --------- | --------------------------- |
| `iata_code`            | `string`  | Airport IATA                |
| `icao_code`            | `string`  | Airport ICAO                |
| `name`                 | `string`  | Airport name                |
| `latitude_deg`         | `float64` | Latitude                    |
| `longitude_deg`        | `float64` | Longitude                   |
| `elevation_ft`         | `float32` | Elevation                   |
| `iso_region`           | `string`  | Region, e.g. `US-CA`        |
| `tz_database_timezone` | `string`  | IANA timezone               |
| `lcd_station_id`       | `string`  | Matched NOAA LCD station ID |

#### dbt model SQL example

```sql
SELECT iata_code, name, tz_database_timezone, lcd_station_id
FROM {{ source('iceberg_staging', 'dim_airport') }}
WHERE iso_region = 'US-IL';
```

For FastAPI/ad hoc DuckDB, query the dbt staging model instead:

```sql
SELECT iata_code, airport_name, tz_database_timezone, lcd_station_id
FROM stg_dim_airport
WHERE iso_region = 'US-IL';
```

### `staging.dim_route`

Source: OpenFlights routes plus great-circle distance. Writer: `src/bmo/staging/dimensions.py`. Partitioning: unpartitioned.

Location: `s3://staging/iceberg/dim_route`

Lineage and API access:

- Produced by: `dim_route`.
- Consumed by: `bmo_dbt_assets` via `stg_dim_route`; route-level features and `mart_training_dataset` use route keys/distance downstream.
- FastAPI: do not query the Iceberg table directly for normal dashboard/API calls. Query `stg_dim_route` through DuckDB for route reference data, or query route fields from `mart_training_dataset` for model/feature analytics.

| Column         | Type      | Notes              |
| -------------- | --------- | ------------------ |
| `airline_iata` | `string`  | Airline IATA       |
| `origin`       | `string`  | Origin IATA        |
| `dest`         | `string`  | Destination IATA   |
| `distance_mi`  | `float64` | Haversine distance |

dbt model SQL example:

```sql
SELECT origin, dest, COUNT(*) AS carrier_routes, AVG(distance_mi) AS distance_mi
FROM {{ source('iceberg_staging', 'dim_route') }}
GROUP BY origin, dest
ORDER BY carrier_routes DESC;
```

For FastAPI/ad hoc DuckDB, query the dbt staging model instead:

```sql
SELECT origin, dest, COUNT(*) AS carrier_routes, AVG(distance_mi) AS distance_mi
FROM stg_dim_route
GROUP BY origin, dest
ORDER BY carrier_routes DESC;
```

### `staging.feat_cascading_delay`

Source: Spark window over `staging.staged_flights`. Writer: `src/bmo/pyspark_jobs/cascading_delay.py`. Partitioning: not explicitly configured in the Spark create table path.

Location: `s3a://staging/iceberg/feat_cascading_delay`

Lineage and API access:

- Produced by: `feat_cascading_delay`.
- Consumed by: `stg_feat_cascading_delay`, `mart_training_dataset`, and `feast_feature_export` for the `feast.aircraft` offline table.
- FastAPI: **do not query directly**. For inference, use `FeatureClient.get_features()` so Feast/Redis returns `aircraft_features:cascading_delay_min` and `aircraft_features:turnaround_min`. For offline/admin analysis, query `mart_training_dataset` or `stg_feat_cascading_delay` through DuckDB.

| Column                    | Type      | Notes                                                        |
| ------------------------- | --------- | ------------------------------------------------------------ |
| `flight_id`               | string    | md5 surrogate key matching `stg_flights`                     |
| `tail_number`             | string    | Aircraft tail number                                         |
| `scheduled_departure_utc` | timestamp | Current flight departure                                     |
| `prev_arr_delay_min`      | numeric   | Previous flight arrival delay                                |
| `prev_dest`               | string    | Previous destination                                         |
| `prev_actual_arrival_utc` | timestamp | Previous actual arrival                                      |
| `turnaround_min`          | numeric   | Minutes from previous arrival to current scheduled departure |

Spark SQL:

```sql
SELECT tail_number, scheduled_departure_utc, prev_arr_delay_min, turnaround_min
FROM staging.feat_cascading_delay
WHERE tail_number IS NOT NULL
ORDER BY scheduled_departure_utc DESC;
```

## Raw S3 Parquet Tables

Raw tables are object-store datasets, not Iceberg. They are written to `s3://raw/...` and consumed by staging jobs.

### `raw.faa_airports`

Path: `s3://raw/faa/airports.parquet`. Writer: `src/bmo/ingestion/faa.py`.

Lineage and API access:

- Produced by: `raw_faa_airports`.
- Consumed by: `station_map` and `dim_airport`.
- FastAPI: **do not query directly**. Query `stg_dim_airport` through DuckDB, or `staging.dim_airport` through Iceberg only for low-level debugging.

| Column                 | Type    | Notes                       |
| ---------------------- | ------- | --------------------------- |
| `iata_code`            | string  | Airport IATA                |
| `icao_code`            | string  | Airport ICAO                |
| `name`                 | string  | Airport name                |
| `type`                 | string  | Medium/large airport filter |
| `latitude_deg`         | float64 | Latitude                    |
| `longitude_deg`        | float64 | Longitude                   |
| `elevation_ft`         | float32 | Elevation                   |
| `municipality`         | string  | City/municipality           |
| `iso_region`           | string  | Region                      |
| `tz_database_timezone` | string  | IANA timezone               |

DuckDB:

```sql
SELECT iata_code, name, tz_database_timezone
FROM read_parquet('s3://raw/faa/airports.parquet')
WHERE iso_region = 'US-CA';
```

### `raw.openflights_routes`

Path: `s3://raw/openflights/routes.parquet`. Writer: `src/bmo/ingestion/faa.py`.

Lineage and API access:

- Produced by: `raw_openflights_routes`.
- Consumed by: `dim_route`.
- FastAPI: **do not query directly**. Query `stg_dim_route` through DuckDB, or route fields from `mart_training_dataset` for analytics.

| Column         | Type   | Notes                    |
| -------------- | ------ | ------------------------ |
| `airline_iata` | string | Airline IATA             |
| `origin`       | string | Origin IATA              |
| `dest`         | string | Destination IATA         |
| `codeshare`    | bool   | Codeshare flag           |
| `stops`        | int8   | Number of stops          |
| `equipment`    | string | Aircraft equipment codes |

DuckDB:

```sql
SELECT origin, dest, COUNT(*) AS carriers
FROM read_parquet('s3://raw/openflights/routes.parquet')
WHERE stops = 0
GROUP BY origin, dest;
```

### `raw.bts_flights`

Path: `s3://raw/bts/year=YYYY/month=MM/data.parquet`. Writer: `src/bmo/ingestion/bts.py`.

Sidecar manifest: `s3://raw/bts/_manifests/YYYY-MM.json`.

Lineage and API access:

- Produced by: `raw_bts_flights`.
- Consumed by: `staged_flights`.
- FastAPI: **do not query directly**. Query `stg_flights` for staged flight rows, `mart_predictions` for prediction/actual joins, or `mart_training_dataset` for model feature/label analysis.

| Column                    | Type    | Notes                                 |
| ------------------------- | ------- | ------------------------------------- |
| `year`                    | int16   | BTS year                              |
| `month`                   | int8    | BTS month                             |
| `day_of_month`            | int8    | Day of month                          |
| `day_of_week`             | int8    | BTS day of week                       |
| `flight_date`             | date32  | Local flight date                     |
| `reporting_airline`       | string  | Carrier                               |
| `tail_number`             | string  | Aircraft                              |
| `flight_number`           | int32   | Flight number                         |
| `origin`                  | string  | Origin IATA                           |
| `origin_city`             | string  | Origin city                           |
| `origin_state`            | string  | Origin state                          |
| `dest`                    | string  | Destination IATA                      |
| `dest_city`               | string  | Destination city                      |
| `dest_state`              | string  | Destination state                     |
| `crs_dep_time_hhmm`       | int16   | Scheduled local departure HHMM        |
| `dep_time_hhmm`           | int16   | Actual local departure HHMM           |
| `dep_delay_min`           | float32 | Signed departure delay                |
| `dep_delay_min_nonneg`    | float32 | Non-negative departure delay          |
| `dep_del15`               | bool    | Departure delayed at least 15 minutes |
| `crs_arr_time_hhmm`       | int16   | Scheduled local arrival HHMM          |
| `arr_time_hhmm`           | int16   | Actual local arrival HHMM             |
| `arr_delay_min`           | float32 | Signed arrival delay                  |
| `arr_delay_min_nonneg`    | float32 | Non-negative arrival delay            |
| `arr_del15`               | bool    | Arrival delayed at least 15 minutes   |
| `cancelled`               | bool    | Cancelled flag                        |
| `cancellation_code`       | string  | Cancellation code                     |
| `diverted`                | bool    | Diverted flag                         |
| `crs_elapsed_min`         | float32 | Scheduled elapsed minutes             |
| `actual_elapsed_min`      | float32 | Actual elapsed minutes                |
| `air_time_min`            | float32 | Air time                              |
| `distance_mi`             | float32 | BTS distance                          |
| `carrier_delay_min`       | float32 | Carrier delay                         |
| `weather_delay_min`       | float32 | Weather delay                         |
| `nas_delay_min`           | float32 | NAS delay                             |
| `security_delay_min`      | float32 | Security delay                        |
| `late_aircraft_delay_min` | float32 | Late aircraft delay                   |

DuckDB partition query:

```sql
SELECT month, COUNT(*) AS rows
FROM read_parquet('s3://raw/bts/year=2026/month=*/data.parquet', hive_partitioning = true)
GROUP BY month
ORDER BY month;
```

### `raw.noaa_weather`

Path: `s3://raw/noaa/year=YYYY/month=MM/data.parquet`. Writer: `src/bmo/ingestion/noaa.py`.

Sidecars:

- `s3://raw/noaa/_manifests/YYYY-MM.json`
- `s3://raw/noaa/_station_map.json`
- `s3://raw/noaa/_annual/YYYY/<station_id>.csv` cache files

Lineage and API access:

- Produced by: `raw_noaa_weather`.
- Consumed by: `staged_weather`.
- FastAPI: **do not query directly**. Query `stg_weather` for staged observations or weather-derived fields from `mart_training_dataset`.

Schema matches the staged weather columns:

| Column                   | Type               |
| ------------------------ | ------------------ |
| `station_id`             | string             |
| `iata_code`              | string             |
| `obs_time_utc`           | timestamp[us, UTC] |
| `temp_f`                 | float32            |
| `dew_point_f`            | float32            |
| `relative_humidity_pct`  | float32            |
| `wind_speed_kts`         | float32            |
| `wind_dir_deg`           | float32            |
| `precip_1h_in`           | float32            |
| `visibility_mi`          | float32            |
| `sky_conditions`         | string             |
| `present_weather`        | string             |
| `sea_level_pressure_hpa` | float32            |

DuckDB:

```sql
SELECT iata_code, DATE_TRUNC('day', obs_time_utc) AS day, AVG(temp_f) AS avg_temp_f
FROM read_parquet('s3://raw/noaa/year=2026/month=*/data.parquet', hive_partitioning = true)
GROUP BY iata_code, day;
```

## Rejected S3 Parquet Tables

Rejected rows are debugging datasets. They are not part of the analytical mart path.

### `rejected.bts`

Path: `s3://rejected/bts/year=YYYY/month=MM/rejected.parquet`. Writer: `stage_flights`.

Lineage and API access:

- Produced by: `staged_flights` validation.
- Consumed by: humans/debug workflows only.
- FastAPI: do not query for normal API/dashboard workflows. If needed, expose through an admin-only debugging endpoint, not public dashboard endpoints.

Schema: `staging.staged_flights` plus:

| Column             | Type   | Notes                             |
| ------------------ | ------ | --------------------------------- |
| `rejection_reason` | string | First matching validation failure |

Known reasons include `invalid_iata_code`, `implausible_distance`, `missing_scheduled_departure_utc`, and `missing_actual_departure_for_operated_flight`.

### `rejected.noaa`

Path intended by path helpers: `s3://rejected/noaa/year=YYYY/month=MM/rejected.parquet`.

Current writer note: `stage_weather` computes `rejected_key = noaa.raw_key(year, month)`, which resolves to `noaa/year=YYYY/month=MM/data.parquet`; use care when browsing rejected NOAA outputs.

Lineage and API access:

- Produced by: `staged_weather` validation.
- Consumed by: humans/debug workflows only.
- FastAPI: do not query for normal API/dashboard workflows. If needed, expose through an admin-only debugging endpoint, not public dashboard endpoints.

Schema: raw/staged NOAA weather columns for rows with implausible temperature or negative wind speed.

## Feast Offline Store Tables

The `feast_feature_export` Dagster asset exports dbt feature tables from DuckDB to S3 Parquet so Feast can perform historical joins and online materialization.

Base path: `FEAST_S3_BASE`, typically `s3://staging/feast`.

### `feast.origin_airport`

Path: `s3://staging/feast/origin_airport/data.parquet`.

Lineage and API access:

- Produced by: `feast_feature_export` from `feat_origin_airport_windowed`.
- Consumed by: Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`, and online serving after materialization.
- FastAPI: do not query this Parquet directly. For inference, call `FeatureClient.get_features()`. For analytics, query `mart_training_dataset` or the dbt feature model through DuckDB.

| Column                       | Type      | Notes                           |
| ---------------------------- | --------- | ------------------------------- |
| `origin`                     | string    | Entity key                      |
| `event_ts`                   | timestamp | Feature timestamp               |
| `origin_flight_count_1h`     | integer   | 1-hour flight count             |
| `origin_avg_dep_delay_1h`    | float     | 1-hour average departure delay  |
| `origin_pct_delayed_1h`      | float     | 1-hour delayed fraction         |
| `origin_avg_dep_delay_24h`   | float     | 24-hour average departure delay |
| `origin_pct_cancelled_24h`   | float     | 24-hour cancellation rate       |
| `origin_avg_dep_delay_7d`    | float     | 7-day average departure delay   |
| `origin_pct_delayed_7d`      | float     | 7-day delayed fraction          |
| `origin_congestion_score_1h` | float     | Congestion proxy                |

### `feast.dest_airport`

Path: `s3://staging/feast/dest_airport/data.parquet`.

Lineage and API access:

- Produced by: `feast_feature_export` from `feat_dest_airport_windowed`.
- Consumed by: Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`, and online serving after materialization.
- FastAPI: do not query this Parquet directly. For inference, call `FeatureClient.get_features()`. For analytics, query `mart_training_dataset` or the dbt feature model through DuckDB.

| Column                   | Type      |
| ------------------------ | --------- |
| `dest`                   | string    |
| `event_ts`               | timestamp |
| `dest_avg_arr_delay_1h`  | float     |
| `dest_pct_delayed_1h`    | float     |
| `dest_avg_arr_delay_24h` | float     |
| `dest_pct_diverted_24h`  | float     |

### `feast.carrier`

Path: `s3://staging/feast/carrier/data.parquet`.

Lineage and API access:

- Produced by: `feast_feature_export` from `feat_carrier_rolling`.
- Consumed by: Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`, and online serving after materialization.
- FastAPI: do not query this Parquet directly. For inference, call `FeatureClient.get_features()`. For analytics, query `mart_training_dataset` or the dbt feature model through DuckDB.

| Column                         | Type      |
| ------------------------------ | --------- |
| `carrier`                      | string    |
| `event_ts`                     | timestamp |
| `carrier_on_time_pct_7d`       | float     |
| `carrier_cancellation_rate_7d` | float     |
| `carrier_avg_delay_7d`         | float     |
| `carrier_flight_count_7d`      | integer   |

### `feast.route`

Path: `s3://staging/feast/route/data.parquet`.

Lineage and API access:

- Produced by: `feast_feature_export` from `feat_route_rolling`.
- Consumed by: Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`, and online serving after materialization.
- FastAPI: do not query this Parquet directly. For inference, call `FeatureClient.get_features()`. For analytics, query `mart_training_dataset` or the dbt feature model through DuckDB.

| Column                       | Type      |
| ---------------------------- | --------- |
| `route_key`                  | string    |
| `event_ts`                   | timestamp |
| `route_avg_dep_delay_7d`     | float     |
| `route_avg_arr_delay_7d`     | float     |
| `route_pct_delayed_7d`       | float     |
| `route_cancellation_rate_7d` | float     |
| `route_avg_elapsed_7d`       | float     |
| `route_distance_mi`          | float     |

### `feast.aircraft`

Path: `s3://staging/feast/aircraft/data.parquet`.

Lineage and API access:

- Produced by: `feast_feature_export` from `staging.feat_cascading_delay`.
- Consumed by: Feast historical retrieval, `feast_materialized_features`, `training_dataset`, `batch_predictions`, and online serving after materialization.
- FastAPI: do not query this Parquet directly. For inference, call `FeatureClient.get_features()`. For analytics, query `mart_training_dataset` or `stg_feat_cascading_delay` through DuckDB.

| Column                | Type      |
| --------------------- | --------- |
| `tail_number`         | string    |
| `event_ts`            | timestamp |
| `cascading_delay_min` | float     |
| `turnaround_min`      | float     |

DuckDB ASOF join example:

```sql
WITH labels AS (
    SELECT
        flight_id,
        origin,
        scheduled_departure_utc AS event_timestamp
    FROM mart_training_dataset
    WHERE scheduled_departure_utc >= TIMESTAMPTZ '2026-01-01 00:00:00+00'
),
features AS (
    SELECT origin, event_ts, origin_avg_dep_delay_1h
    FROM read_parquet('s3://staging/feast/origin_airport/data.parquet')
    ORDER BY origin, event_ts
)
SELECT labels.flight_id, features.origin_avg_dep_delay_1h
FROM labels
ASOF LEFT JOIN features
    ON labels.origin = features.origin
   AND labels.event_timestamp >= features.event_ts;
```

Feast retrieval example:

```python
from feast import FeatureStore

store = FeatureStore(repo_path='feature_repo')

features = store.get_historical_features(
    entity_df=entity_df[
        ['flight_id', 'origin', 'dest', 'carrier', 'tail_number', 'route_key', 'event_timestamp']
    ],
    features=[
        'origin_airport_features:origin_avg_dep_delay_1h',
        'carrier_features:carrier_on_time_pct_7d',
    ],
).to_df()
```

## S3 Derived Parquet Tables

### `training_datasets`

Path: `s3://staging/datasets/<version_hash>/data.parquet`.

Sidecar: `s3://staging/datasets/<version_hash>/card.json`.

Writer: `build_dataset()` in `src/bmo/training_dataset_builder/builder.py`.

Lineage and API access:

- Produced by: `training_dataset`.
- Consumed by: `trained_model`, MLflow model metadata/tags, and `drift_report` as the reference feature distribution.
- FastAPI: **do not query directly**. For dashboard/API model performance, query `live_accuracy`. Use this S3 dataset only for offline model debugging, reproducibility, and drift-reference workflows.

Data structure:

- Entity columns: `flight_id`, `event_timestamp`, `origin`, `dest`, `carrier`, `tail_number`, `route_key`.
- Label columns from `mart_training_dataset`: `dep_delay_min`, `arr_delay_min`, `is_dep_delayed`, `is_arr_delayed`, `cancelled`, `diverted`.
- Requested feature columns from the Feast feature views listed above.

`card.json` structure:

| Field                 | Notes                                 |
| --------------------- | ------------------------------------- |
| `version_hash`        | SHA-256 identifier for dataset inputs |
| `feature_refs`        | Sorted `view:feature` list            |
| `feature_set_version` | Feature repo/tree version             |
| `feature_ttls`        | Feature view TTLs in seconds          |
| `as_of`               | Data cutoff timestamp                 |
| `row_count`           | Number of dataset rows                |
| `label_distribution`  | Per-label stats                       |
| `schema_fingerprint`  | Column/dtype hash                     |
| `created_at`          | Dataset creation time                 |
| `storage_path`        | Parquet path                          |

DuckDB:

```sql
SELECT
    is_dep_delayed,
    AVG(origin_avg_dep_delay_1h) AS avg_origin_delay_feature,
    COUNT(*) AS rows
FROM read_parquet('s3://staging/datasets/<version_hash>/data.parquet')
GROUP BY is_dep_delayed;
```

PyArrow:

```python
import pyarrow.parquet as pq
import s3fs

fs = s3fs.S3FileSystem(key=access_key, secret=secret_key, endpoint_url=endpoint_url)
table = pq.read_table(
    'staging/datasets/<version_hash>/data.parquet',
    filesystem=fs,
    columns=['flight_id', 'is_dep_delayed', 'origin_avg_dep_delay_1h'],
)
```

### `predictions`

Path: `s3://staging/predictions/date=YYYY-MM-DD/data.parquet`.

Writer: `score_partition()` in `src/bmo/batch_scoring/score.py`. Hive partitioning is by path component `date=YYYY-MM-DD`.

Lineage and API access:

- Produced by: `batch_predictions`.
- Consumed by: `mart_predictions` and `drift_report` current-window entity retrieval.
- FastAPI: usually do not query the raw Parquet directly. Query `mart_predictions` through DuckDB; `/api/predictions` is the preferred API pattern.

| Column                    | Type      | Notes                        |
| ------------------------- | --------- | ---------------------------- |
| `flight_id`               | string    | Flight key                   |
| `origin`                  | string    | Origin IATA                  |
| `dest`                    | string    | Destination IATA             |
| `carrier`                 | string    | Carrier                      |
| `tail_number`             | string    | Aircraft                     |
| `route_key`               | string    | `origin-dest`                |
| `scheduled_departure_utc` | timestamp | Scheduled departure          |
| `predicted_delay_proba`   | float32   | Delay probability            |
| `predicted_is_delayed`    | int8      | Probability threshold result |
| `model_name`              | string    | Registered model name        |
| `model_version`           | string    | Model version                |
| `score_date`              | string    | Partition date               |
| `scored_at`               | string    | UTC scoring timestamp        |

DuckDB:

```sql
SELECT
    date,
    model_version,
    COUNT(*) AS scored_flights,
    AVG(predicted_delay_proba) AS avg_delay_probability
FROM read_parquet('s3://staging/predictions/**/data.parquet', hive_partitioning = true)
WHERE date >= DATE '2026-01-01'
GROUP BY date, model_version
ORDER BY date DESC;
```

### `monitoring.metrics`

Path: `s3://staging/monitoring/metrics/date=YYYY-MM-DD/drift_metrics.parquet`.

Writer: `drift_report` in `dagster_project/assets/monitoring.py`. Also upserted into Postgres `drift_metrics`.

Lineage and API access:

- Produced by: `drift_report`.
- Consumed by: `mart_drift_metrics` and offline drift-history analysis.
- FastAPI: do not query the S3 Parquet for normal dashboard freshness. Query Postgres `drift_metrics` directly with SQLAlchemy. Use `mart_drift_metrics` through DuckDB for offline lineage or bulk historical analysis.

| Column          | Type             | Notes                   |
| --------------- | ---------------- | ----------------------- |
| `report_date`   | string/date      | Report date             |
| `feature_name`  | string           | Feature                 |
| `psi_score`     | float            | PSI                     |
| `kl_divergence` | float            | KL divergence           |
| `rank`          | integer          | Feature importance rank |
| `is_breached`   | bool             | Drift threshold result  |
| `model_version` | string           | Champion model version  |
| `computed_at`   | string/timestamp | Computation timestamp   |

DuckDB:

```sql
SELECT feature_name, AVG(psi_score) AS avg_psi, COUNT(*) AS breach_days
FROM read_parquet(
    's3://staging/monitoring/metrics/**/drift_metrics.parquet',
    hive_partitioning = true
)
WHERE report_date >= CURRENT_DATE - INTERVAL '30 days'
  AND is_breached
GROUP BY feature_name
ORDER BY avg_psi DESC;
```

### `monitoring.reports`

Path: `s3://staging/monitoring/reports/date=YYYY-MM-DD/drift_report.html`.

This is an HTML report object, not a table. Query by listing S3 keys or linking directly from Dagster metadata.

Lineage and API access:

- Produced by: `drift_report`.
- Consumed by: GitHub Pages sync and Dagster metadata links.
- FastAPI: do not query as a table. Link to the report object/URL if the UI needs report navigation.

### `serving.model_config`

Path: `s3://staging/serving/model_config.json`.

Writer: `deployed_api` in `dagster_project/assets/serving.py`. Read by the FastAPI service for model hot-swaps.

Lineage and API access:

- Produced by: `deployed_api`.
- Consumed by: FastAPI service startup/reload workflow.
- FastAPI: do not expose as a table. Use `/model-info` for current loaded state and `/admin/reload` to refresh the loaded champion model.

| Field           | Notes                        |
| --------------- | ---------------------------- |
| `model_name`    | Registered model name        |
| `model_version` | Champion model version       |
| `model_uri`     | MLflow model URI             |
| `registered_at` | MLflow registry timestamp    |
| `tags`          | Model registry tags          |
| `published_at`  | Config publication timestamp |

## DuckDB/dbt Tables

These tables live in `DUCKDB_PATH` and are built by dbt. They are analytical tables/views over the S3/Iceberg sources and are what most local SQL should target.

### Staging models

Produced by: `bmo_dbt_assets`.

| Model                      | Columns / structure                                                                                   | Source                         | Consumed by                                                        | FastAPI/API access                                                                         |
| -------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------ | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `stg_flights`              | `flight_id`, flight identifiers, UTC timestamps, delay/cancellation fields, distance, BTS date fields | `staging.staged_flights`       | feature models, `mart_predictions`, `batch_predictions` query path | Usually query `mart_predictions`; use `stg_flights` only for internal flight-row endpoints |
| `stg_weather`              | Weather observations by station/IATA/time                                                             | `staging.staged_weather`       | `int_flights_enriched`                                             | Do not expose directly; query weather-derived mart fields instead                          |
| `stg_dim_airport`          | Airport dimension fields with station/timezone metadata                                               | `staging.dim_airport`          | dbt staging/features/reference joins                               | OK for read-only reference/admin endpoints                                                 |
| `stg_dim_route`            | Airline/origin/dest/distance                                                                          | `staging.dim_route`            | route analysis/reference joins                                     | OK for read-only route reference endpoints; otherwise query route fields in marts          |
| `stg_feat_cascading_delay` | `flight_id`, aircraft previous-leg delay, previous destination, turnaround                            | `staging.feat_cascading_delay` | `mart_training_dataset`, Feast aircraft export                     | Do not use for inference; use `FeatureClient.get_features()`                               |

### Intermediate model

`int_flights_enriched` joins `stg_flights` with origin and destination weather using point-in-time windows:

- Origin weather within three hours before scheduled departure.
- Destination weather within six hours before scheduled departure.
- Derived weather booleans: thunderstorm, low visibility, high wind.

Lineage and API access:

- Produced by: `bmo_dbt_assets`.
- Consumed by: `mart_training_dataset`.
- FastAPI: **do not query directly** for normal dashboard endpoints. Query `mart_training_dataset` if weather-enriched feature/label context is needed.

### Feature models

Produced by: `bmo_dbt_assets`.

| Model                          | Key columns                                            | Feature columns                                                             | Consumed by                                     | FastAPI/API access                                                                                           |
| ------------------------------ | ------------------------------------------------------ | --------------------------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `feat_origin_airport_windowed` | `flight_id`, `origin`, `event_ts`                      | origin rolling counts, delays, cancellation rate, congestion                | `mart_training_dataset`, `feast_feature_export` | Do not query for inference; use `FeatureClient.get_features()`. For analytics prefer `mart_training_dataset` |
| `feat_dest_airport_windowed`   | `flight_id`, `dest`, `event_ts`                        | destination rolling arrival delay, delayed fraction, cancellation/diversion | `mart_training_dataset`, `feast_feature_export` | Do not query for inference; use `FeatureClient.get_features()`. For analytics prefer `mart_training_dataset` |
| `feat_carrier_rolling`         | `flight_id`, `carrier`, `event_ts`                     | carrier on-time rate, cancellation rate, average delay, count               | `mart_training_dataset`, `feast_feature_export` | Do not query for inference; use `FeatureClient.get_features()`. For analytics prefer `mart_training_dataset` |
| `feat_route_rolling`           | `flight_id`, `origin`, `dest`, `route_key`, `event_ts` | route delay/cancellation/elapsed/distance features                          | `mart_training_dataset`, `feast_feature_export` | Do not query for inference; use `FeatureClient.get_features()`. For analytics prefer `mart_training_dataset` |
| `feat_calendar`                | `flight_id`, `event_ts`                                | hour, day, month, quarter, weekend, holiday, rush-hour flags                | `mart_training_dataset`                         | Query only for internal feature debugging; otherwise prefer `mart_training_dataset`                          |

### Mart models

#### `mart_training_dataset`

One row per flight, combining labels and feature columns for training.

Lineage and API access:

- Produced by: `bmo_dbt_assets`.
- Consumed by: `training_dataset`, feature/debug analysis, and possible internal dashboard endpoints.
- FastAPI: can query through DuckDB for internal/admin analytics, but not for online inference. For public model performance dashboards, prefer `live_accuracy`; for prediction summaries, prefer `mart_predictions`.

Key groups:

- Identity: `flight_id`, `flight_date`, `carrier`, `origin`, `dest`, `tail_number`, `scheduled_departure_utc`.
- Labels: `dep_delay_min`, `arr_delay_min`, `is_dep_delayed`, `is_arr_delayed`, `cancelled`, `diverted`.
- Feature columns from origin, destination, carrier, route, weather, hub size, calendar, and cascading delay.

Example:

```sql
SELECT
    carrier,
    COUNT(*) AS rows,
    AVG(is_dep_delayed::DOUBLE) AS delay_rate,
    AVG(origin_pct_delayed_1h) AS avg_origin_pct_delayed_1h
FROM mart_training_dataset
GROUP BY carrier
ORDER BY rows DESC;
```

#### `mart_predictions`

Reads `s3://staging/predictions/**/data.parquet` and left joins BTS actuals from `stg_flights`.

There's ~60 day delay for BTS actual data.

Lineage and API access:

- Produced by: `bmo_dbt_assets` from `predictions` Parquet and `stg_flights`.
- Consumed by: `ground_truth_backfill`, `/api/predictions`, and prediction/actual dashboards.
- FastAPI: yes, query through DuckDB. This is preferred over directly scanning `s3://staging/predictions/**/data.parquet` because it includes the actuals join.

Columns:

- Prediction fields: `flight_id`, `origin`, `dest`, `carrier`, `tail_number`, `route_key`, `scheduled_departure_utc`, `predicted_delay_proba`, `predicted_is_delayed`, `model_name`, `model_version`, `score_date`, `scored_at`.
- Actual fields: `actual_dep_delay_min`, `actual_is_delayed`, `actual_departure_utc`, `cancelled`.

Example:

```sql
SELECT
    score_date,
    model_version,
    COUNT(*) AS rows,
    AVG(predicted_is_delayed::DOUBLE) AS predicted_positive_rate,
    AVG(actual_is_delayed::DOUBLE) AS actual_positive_rate
FROM mart_predictions
WHERE actual_is_delayed IS NOT NULL
GROUP BY score_date, model_version
ORDER BY score_date DESC;
```

#### `mart_drift_metrics`

Reads `s3://staging/monitoring/metrics/**/drift_metrics.parquet`.

Lineage and API access:

- Produced by: `bmo_dbt_assets` from `monitoring.metrics` Parquet, with an explicit Dagster dependency on `drift_report`.
- Consumed by: offline drift-history analysis and dbt lineage.
- FastAPI: usually no. Query Postgres `drift_metrics` for API freshness and sensor-aligned data. Use `mart_drift_metrics` only for DuckDB/offline historical analysis.

Columns:

- `report_date`
- `feature_name`
- `psi_score`
- `kl_divergence`
- `importance_rank`
- `is_breached`
- `model_version`
- `computed_at`

Example:

```sql
SELECT feature_name, MAX(psi_score) AS max_psi
FROM mart_drift_metrics
WHERE report_date >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY feature_name
ORDER BY max_psi DESC;
```

## FastAPI Query Patterns

`src/bmo/serving/api.py` exposes two different query paths:

- Postgres via SQLAlchemy for small dashboard tables: `drift_metrics` and `live_accuracy`.
- DuckDB per request for local/dbt analytical tables: currently `mart_predictions`.

The inference endpoint `/predict` does not query these analytical tables. It reads online features from Feast/Redis through `FeatureClient`, then scores with the loaded MLflow model.

### Postgres from FastAPI

Use the shared SQLAlchemy engine dependency for Postgres-backed API routes:

```python
from datetime import date

from fastapi import Depends
from sqlalchemy import Engine, create_engine, text

from bmo.common.config import settings
from bmo.serving.schemas import DriftMetricRow, DriftResponse

_pg_engine: Engine | None = None


def get_db() -> Engine:
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(settings.postgres_url, pool_pre_ping=True)
    return _pg_engine


@app.get('/api/drift/metrics', response_model=DriftResponse, tags=['api'])
async def drift(
    start: date | None = None,
    end: date | None = None,
    db: Engine = Depends(get_db),
) -> DriftResponse:
    with db.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    report_date::text,
                    feature_name,
                    psi_score,
                    kl_divergence,
                    rank,
                    is_breached,
                    model_version
                FROM drift_metrics
                WHERE report_date BETWEEN
                    COALESCE(:start, (SELECT MAX(report_date) FROM drift_metrics) - INTERVAL '30 days')
                    AND
                    COALESCE(:end, (SELECT MAX(report_date) FROM drift_metrics))
                ORDER BY report_date DESC, rank ASC
            """),
            {'start': start, 'end': end},
        ).mappings().all()

    data = [DriftMetricRow(**row) for row in rows]
    latest = data[0].report_date if data else str(date.today())
    return DriftResponse(
        rows=data,
        report_date=latest,
        n_breached=sum(row.is_breached and row.report_date == latest for row in data),
    )
```

The existing Postgres-backed API routes are:

| Endpoint                                                           | Backing table   | Query purpose                    |
| ------------------------------------------------------------------ | --------------- | -------------------------------- |
| `GET /api/drift/metrics?start=YYYY-MM-DD&end=YYYY-MM-DD`           | `drift_metrics` | Drift rows for a date range      |
| `GET /api/model-stats`                                             | `live_accuracy` | Aggregate model-version accuracy |
| `GET /api/drift/heatmap?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` | `drift_metrics` | Feature/date heatmap             |
| `GET /api/accuracy`                                                | `live_accuracy` | 90-day live accuracy time series |

Example HTTP calls:

```bash
curl 'http://localhost:8000/api/drift/metrics?start=2026-01-01&end=2026-01-31'
curl 'http://localhost:8000/api/model-stats'
curl 'http://localhost:8000/api/accuracy'
```

Per-feature PSI should use a path parameter route in FastAPI:

```python
@app.get('/api/psi/{feature_name}', response_model=PsiResponse, tags=['api'])
async def psi(feature_name: str, db: Engine = Depends(get_db)) -> PsiResponse:
    with db.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    report_date,
                    psi_score,
                    kl_divergence,
                    is_breached
                FROM drift_metrics
                WHERE feature_name = :feature_name
                ORDER BY report_date
            """),
            {'feature_name': feature_name},
        ).mappings().all()

    return PsiResponse(rows=[PsiRow(**row) for row in rows])
```

Example call:

```bash
curl 'http://localhost:8000/api/psi/origin_avg_dep_delay_1h'
```

### DuckDB from FastAPI

Use DuckDB for dbt marts and any table that resolves to S3/Iceberg through the DuckDB database file. Do not share a DuckDB connection globally; open a read-only connection per request and close it in the worker function.

```python
import asyncio
from typing import Any, cast

import duckdb
from fastapi import Depends

from bmo.common.config import settings
from bmo.serving.schemas import PredictionRow, PredictionsResponse


def get_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(settings.duckdb_path, read_only=True)


@app.get('/api/predictions', response_model=PredictionsResponse, tags=['api'])
async def predictions(
    days: int = 30,
    con: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> PredictionsResponse:
    def _query() -> list[dict[str, Any]]:
        try:
            return cast(
                list[dict[str, Any]],
                con.execute(
                    """
                    SELECT
                        score_date::text AS score_date,
                        model_version,
                        COUNT(*) AS n_flights,
                        AVG(predicted_delay_proba) AS avg_proba,
                        AVG(predicted_is_delayed::int) AS positive_rate,
                        COUNT(*) FILTER (WHERE actual_is_delayed IS NOT NULL) AS n_with_actuals
                    FROM mart_predictions
                    WHERE score_date >= CURRENT_DATE - INTERVAL (? || ' days')
                    GROUP BY score_date, model_version
                    ORDER BY score_date DESC
                    """,
                    [days],
                ).df().to_dict('records'),
            )
        finally:
            con.close()

    rows = await asyncio.to_thread(_query)
    return PredictionsResponse(rows=[PredictionRow(**row) for row in rows])
```

Example HTTP call:

```bash
curl 'http://localhost:8000/api/predictions?days=30'
```

For a new FastAPI endpoint over a DuckDB/dbt table, keep the same pattern:

```python
@app.get('/api/routes/delay')
async def route_delay(days: int = 30, con: duckdb.DuckDBPyConnection = Depends(get_duckdb)):
    def _query() -> list[dict[str, Any]]:
        try:
            return cast(
                list[dict[str, Any]],
                con.execute(
                    """
                    SELECT
                        route_key,
                        COUNT(*) AS n_flights,
                        AVG(dep_delay_min) AS avg_dep_delay_min,
                        AVG(is_dep_delayed::int) AS delay_rate
                    FROM mart_training_dataset
                    WHERE scheduled_departure_utc >= CURRENT_DATE - INTERVAL (? || ' days')
                    GROUP BY route_key
                    HAVING COUNT(*) >= 50
                    ORDER BY delay_rate DESC
                    LIMIT 25
                    """,
                    [days],
                ).df().to_dict('records'),
            )
        finally:
            con.close()

    return {'rows': await asyncio.to_thread(_query)}
```

### Querying S3 Parquet directly from FastAPI

Prefer dbt marts or Postgres tables for dashboard endpoints. If an endpoint must read S3 Parquet directly, configure DuckDB `httpfs` inside the per-request connection before `read_parquet()`.

```python
@app.get('/api/drift/s3-summary')
async def drift_s3_summary(
    days: int = 30,
    con: duckdb.DuckDBPyConnection = Depends(get_duckdb),
):
    def _query() -> list[dict[str, Any]]:
        try:
            con.execute('INSTALL httpfs; LOAD httpfs;')
            con.execute('SET s3_region = ?', [settings.s3_region])
            con.execute('SET s3_access_key_id = ?', [settings.s3_access_key_id])
            con.execute('SET s3_secret_access_key = ?', [settings.s3_secret_access_key])
            con.execute('SET s3_endpoint = ?', [settings.s3_endpoint])
            con.execute("SET s3_url_style = 'path'")
            con.execute('SET s3_use_ssl = false')

            return cast(
                list[dict[str, Any]],
                con.execute(
                    """
                    SELECT
                        feature_name,
                        AVG(psi_score) AS avg_psi,
                        MAX(psi_score) AS max_psi,
                        COUNT(*) FILTER (WHERE is_breached) AS breach_days
                    FROM read_parquet(
                        's3://staging/monitoring/metrics/**/drift_metrics.parquet',
                        hive_partitioning = true
                    )
                    WHERE report_date::DATE >= CURRENT_DATE - INTERVAL (? || ' days')
                    GROUP BY feature_name
                    ORDER BY max_psi DESC
                    LIMIT 25
                    """,
                    [days],
                ).df().to_dict('records'),
            )
        finally:
            con.close()

    return {'rows': await asyncio.to_thread(_query)}
```

### Querying Iceberg from FastAPI

For app endpoints, prefer DuckDB tables already built by dbt. If a route needs to inspect an Iceberg table directly, either use DuckDB `iceberg_scan()` or PyIceberg in a worker thread.

DuckDB direct Iceberg scan:

```python
@app.get('/api/flights/by-origin')
async def flights_by_origin(
    start: date,
    end: date,
    con: duckdb.DuckDBPyConnection = Depends(get_duckdb),
):
    def _query() -> list[dict[str, Any]]:
        try:
            con.execute('INSTALL iceberg; LOAD iceberg;')
            return cast(
                list[dict[str, Any]],
                con.execute(
                    """
                    SELECT origin, COUNT(*) AS n_flights
                    FROM iceberg_scan(
                        's3://staging/iceberg/staged_flights/',
                        allow_moved_paths = true
                    )
                    WHERE flight_date BETWEEN ? AND ?
                    GROUP BY origin
                    ORDER BY n_flights DESC
                    LIMIT 25
                    """,
                    [start, end],
                ).df().to_dict('records'),
            )
        finally:
            con.close()

    return {'rows': await asyncio.to_thread(_query)}
```

PyIceberg direct scan:

```python
@app.get('/api/airports')
async def airports() -> dict[str, list[dict[str, object]]]:
    def _query() -> list[dict[str, object]]:
        catalog = make_catalog()
        table = catalog.load_table('staging.dim_airport')
        df = table.scan(
            selected_fields=('iata_code', 'name', 'tz_database_timezone', 'lcd_station_id')
        ).to_pandas()
        return df.sort_values('iata_code').to_dict('records')

    return {'rows': await asyncio.to_thread(_query)}
```

### Online inference path

`POST /predict` should use Feast online features, not S3 or DuckDB. The request contains only entity keys:

```bash
curl -X POST 'http://localhost:8000/predict' \
  -H 'Content-Type: application/json' \
  -d '{
    "flight_id": "AA123_20260430_0900",
    "origin": "ORD",
    "dest": "LAX",
    "carrier": "AA",
    "tail_number": "N12345",
    "route_key": "ORD-LAX"
  }'
```

The API then calls:

```python
feature_df = client.get_features(request)
```

`FeatureClient` retrieves these Feast feature refs from Redis:

- `origin_airport_features:*`
- `dest_airport_features:*`
- `carrier_features:*`
- `route_features:*`
- `aircraft_features:*`

If any feature is null or stale, `/predict` returns HTTP 503 instead of scoring with incomplete inputs.

## MLflow and Dagster Metadata

Docker Compose points MLflow at Postgres for backend metadata and `s3://mlflow-artifacts/` for artifacts. Dagster also has a `dagster` database created by `infra/postgres/init.sql`. Those systems create their own internal tables; this project code does not define their schemas directly. Treat them as service-owned metadata stores and query them through MLflow/Dagster APIs unless debugging infrastructure.
