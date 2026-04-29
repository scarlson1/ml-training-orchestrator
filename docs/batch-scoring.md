# Batch Scoring

## Overview

Batch scoring runs daily at 6am UTC and writes flight delay predictions for all flights
scheduled that day to S3 as Hive-partitioned Parquet. Predictions are consumed by the
drift report (8am UTC), the ground truth backfill job, and ad-hoc DuckDB queries.

The core function is `score_partition()` in [src/bmo/batch_scoring/score.py](../src/bmo/batch_scoring/score.py).
The Dagster asset that orchestrates it is `batch_predictions` in
[dagster_project/assets/serving.py](../dagster_project/assets/serving.py).

---

## Daily Scoring Pipeline

### Asset: `batch_predictions`

```text
group:    serving
deps:     registered_model, feast_materialized_features
schedule: daily_batch_score_schedule (0 6 * * *)
freshness: must land by 07:00 UTC, no older than 1h
```

Each run:

1. Reads the partition date from `context.partition_key` (`YYYY-MM-DD`).
2. Fetches the `@champion` model from MLflow registry.
3. Queries DuckDB `stg_flights` for all flights on that date to build the entity
   DataFrame.
4. Sets `event_timestamp = LEAST(scheduled_departure_utc, run_time)` so forward-looking
   partitions use only currently-available features (no leakage).
5. Calls `score_partition()`, which retrieves Feast features, loads the model, and writes
   predictions.
6. Returns a `MaterializeResult` with metadata logged to the Dagster UI.

### Partition Strategy

`batch_predictions` uses a `DailyPartitionsDefinition(start_date='2024-01-01')`.
Each partition key is a calendar date string (`'2024-06-15'`). Dagster queues partitions
as independent runs, so backfills and the daily schedule never interfere.

### Feature Retrieval from Feast Offline Store

`score_partition()` calls `feast.FeatureStore.get_historical_features()` with the entity
DataFrame and a fixed list of 44 feature references (`BATCH_FEATURE_REFS`). Feast performs
a PIT join against the offline store, returning the feature values that were available at
each flight's `event_timestamp`.

Feature groups (order is fixed — XGBoost column indexing depends on it):

| Group | Features |
| --- | --- |
| Origin airport | `origin_flight_count_1h`, `origin_avg_dep_delay_1h`, `origin_pct_delayed_1h`, `origin_avg_dep_delay_24h`, `origin_pct_cancelled_24h`, `origin_avg_dep_delay_7d`, `origin_pct_delayed_7d`, `origin_congestion_score_1h` |
| Dest airport | `dest_avg_arr_delay_1h`, `dest_pct_delayed_1h`, `dest_avg_arr_delay_24h`, `dest_pct_diverted_24h` |
| Carrier | `carrier_on_time_pct_7d`, `carrier_cancellation_rate_7d`, `carrier_avg_delay_7d`, `carrier_flight_count_7d` |
| Route | `route_avg_dep_delay_7d`, `route_avg_arr_delay_7d`, `route_pct_delayed_7d`, `route_cancellation_rate_7d`, `route_avg_elapsed_7d`, `route_distance_mi` |
| Aircraft | `cascading_delay_min`, `turnaround_min` |

Rows where one or more features are null are filled with `0.0` before inference. The count
is logged as `null_feature_rows` in the asset metadata and in `BatchScoreResult`.

### Model Version Pinning

The asset loads `models:/bmo_flight_delay/@champion` at run time. The exact model version
is recorded in `BatchScoreResult.model_version` and written to every output row, so you
can always trace which model produced a given prediction.

To score with a specific version instead of the champion alias, pass a versioned URI
directly to `score_partition()`:

```python
model_uri = 'models:/bmo_flight_delay/42'
```

### Output Schema & Metadata

Each Parquet file has the following columns:

| Column | Type | Description |
| --- | --- | --- |
| `flight_id` | string | Unique flight identifier |
| `origin` | string | Origin airport IATA code |
| `dest` | string | Destination airport IATA code |
| `carrier` | string | Carrier IATA code |
| `tail_number` | string | Aircraft tail number |
| `route_key` | string | `origin-dest` composite key |
| `scheduled_departure_utc` | timestamp | Scheduled departure (UTC) |
| `predicted_delay_proba` | float32 | Raw delay probability \[0, 1\] |
| `predicted_is_delayed` | int8 | Binary prediction (1 if proba ≥ threshold) |
| `model_name` | string | MLflow model name |
| `model_version` | string | MLflow model version |
| `score_date` | string | Partition date (`YYYY-MM-DD`) |
| `scored_at` | string | ISO-8601 UTC timestamp of scoring run |

The default delay threshold is `0.5`. To change it, pass `delay_threshold` to
`score_partition()`.

### S3 Output Location

```text
s3://staging/predictions/date=YYYY-MM-DD/data.parquet
```

Files use zstd compression and Hive-style directory partitioning, which enables predicate
pushdown when querying with DuckDB without a catalog.

---

## Schedule

### Dagster Daily Schedule

| Schedule | Cron | Default status |
| --- | --- | --- |
| `daily_batch_score_schedule` | `0 6 * * *` | STOPPED |

The schedule is disabled by default. Enable it in the Dagster UI or via the CLI:

```bash
dagster schedule start daily_batch_score_schedule
```

The freshness policy deadline is `0 7 * * *` — Dagster will alert if the asset has not
materialized within the 1-hour window.

### Relationship to Nightly Retrain

```text
05:00 UTC  feat_hourly_schedule  — Feast materialization
06:00 UTC  daily_batch_score     — batch_predictions asset
08:00 UTC  daily_drift           — drift_report asset (reads yesterday's predictions)
```

The drift report reads the previous day's completed partition (all departed flights)
against the training distribution. If PSI exceeds 0.2 it triggers a retraining run.

---

## Idempotency

### Overwriting an Existing Partition

`_write_predictions()` opens the S3 path with `s3fs.open('wb')`, which overwrites any
existing file. Re-running the same partition produces the same bytes (same model version,
same feature snapshot), so retries are safe.

### Handling Missing Feature Partitions

If Feast has no materialized features for the requested date the PIT join returns all-null
feature rows. These are filled with `0.0` and scored, but `null_feature_rows` in the
result will equal `row_count`. Check the `feat_hourly_schedule` run for that day to
diagnose the upstream gap before backfilling.

---

## Inspecting Predictions

### Querying via DuckDB

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")

df = conn.execute("""
    SELECT *
    FROM read_parquet('s3://staging/predictions/date=2024-06-15/data.parquet')
    WHERE predicted_is_delayed = 1
    ORDER BY predicted_delay_proba DESC
    LIMIT 20
""").df()
```

To scan across multiple dates using Hive partition pruning:

```python
df = conn.execute("""
    SELECT score_date, COUNT(*) AS total, AVG(predicted_delay_proba) AS avg_proba
    FROM read_parquet(
        's3://staging/predictions/**/data.parquet',
        hive_partitioning = true
    )
    WHERE score_date BETWEEN '2024-06-01' AND '2024-06-30'
    GROUP BY score_date
    ORDER BY score_date
""").df()
```

### Monitoring Prediction Distribution

Key signals available in the Dagster asset metadata after each run:

| Metadata key | What to watch |
| --- | --- |
| `positive_rate` | Sustained shift > ±10 pp vs. historical baseline warrants investigation |
| `null_feature_rows` | Non-zero values mean Feast materialization is lagging |
| `model_version` | Should be stable between retrains; unexpected change means a new champion was promoted |
| `row_count` | Sharp drop means flights are missing from `stg_flights` |

The `drift_report` asset (downstream) computes PSI across all feature dimensions and
emits a structured report to S3 at `s3://staging/drift/date=YYYY-MM-DD/report.json`.

---

## Backfilling Predictions

### When to Backfill

- A new champion model was promoted and you want predictions re-scored under the new model.
- Feast materialization failed for one or more days and has since been repaired.
- A bug in `score_partition()` was fixed and affected partitions need to be re-run.

### Running a Backfill Partition Range

**Via Dagster UI:** navigate to `batch_predictions` → Partitions → select the date range
→ Materialize selected.

**Via CLI:**

```bash
dagster asset materialize \
  --select batch_predictions \
  --partition-range 2024-06-01...2024-06-30
```

Each partition runs as an independent job. Because writes are idempotent, you can safely
run multiple partitions in parallel. Monitor `null_feature_rows` in each run's metadata
to confirm Feast had valid features for the range.
