# Monitoring

## Overview

The monitoring layer closes the feedback loop between model deployment and model quality. It runs as Phase 10 in the Dagster pipeline — downstream of batch scoring — and has three responsibilities:

1. **Drift detection** — compare production feature distributions to training-time distributions daily and flag features that have shifted significantly.
2. **Ground truth backfill** — compute realized accuracy metrics once BTS actuals arrive (~60 days after scoring).
3. **Automated retraining** — trigger a full retrain when PSI breaches on high-importance features.

```text
batch_predictions (daily)
        │
        ▼
  drift_report ──► HTML report → S3 → GitHub Pages
  (Evidently PSI)  ──► DriftMetricsRow → drift_metrics (Postgres)
                   ──► metrics Parquet → mart_drift_metrics (dbt)
        │
        ▼
  drift_retrain_sensor (hourly) ──► PSI > 0.2 on top-10 feature?
        │                                       │
        │                                       ▼
        │                             RunRequest → training_dataset
        │                                         → full retrain
        ▼
  bmo_dbt_assets (mart_predictions)
        │
        ▼
  ground_truth_backfill ──► live_accuracy (Postgres)
```

---

## Drift Detection

### Evidently HTML Reports

[Evidently AI](https://docs.evidentlyai.com) generates a self-contained HTML report for each daily partition. The report uses `DataDriftPreset` with `method='psi'` — every feature column gets a reference vs. current distribution histogram and a PSI score. The report marks a column as "drifted" when PSI ≥ `psi_threshold` (0.2).

Reports are written to S3 and synced to GitHub Pages by the `evidently-reports.yml` GitHub Actions workflow:

```text
s3://staging/monitoring/reports/date=YYYY-MM-DD/drift_report.html
```

See [Evidently DataDriftPreset docs](https://docs.evidentlyai.com/presets/data-drift) for report customization options.

### PSI (Population Stability Index)

PSI is the primary drift metric. It measures how much a feature's distribution has shifted between a reference (training) sample and the current (production) window.

```text
PSI = Σ (A_i − E_i) × ln(A_i / E_i)
```

Where `A_i` is the fraction of production observations in bin `i` and `E_i` is the fraction of training observations in the same bin.

PSI is computed independently of Evidently (in `src/bmo/monitoring/drift.py:_compute_psi`) so the Postgres rows and HTML report are always consistent. Bin edges are derived from **reference-distribution percentiles** (not fixed-width), which makes the metric scale-invariant and gives uniform sensitivity across the distribution.

Edge cases handled:

- Constant/near-constant features (duplicate percentile edges): merged via `np.unique`; returns `0.0` if fewer than 2 unique edges remain.
- Empty bins: replaced with `epsilon=1e-4` to avoid `ln(0)`.
- Production values outside the training range: clipped to the training min/max and counted in the nearest boundary bin.

#### PSI Threshold (0.2) — Rationale

| PSI range  | Interpretation                      | Action                  |
|------------|-------------------------------------|-------------------------|
| < 0.1      | Stable — no action needed           | —                       |
| 0.1 – 0.2  | Moderate shift — monitor            | Investigate, no trigger |
| > 0.2      | Significant drift — retrain trigger | `drift_retrain_sensor`  |
| > 0.4      | Severe drift                        | Investigate urgently    |

These thresholds are the industry standard from credit-risk modeling where PSI originated. See [Population Stability Index reference](https://www.listendata.com/2015/05/population-stability-index.html).

The sensor threshold (`_PSI_THRESHOLD = 0.2`) in [dagster_project/sensors/drift_retrain_sensor.py](../dagster_project/sensors/drift_retrain_sensor.py) and the asset threshold (`_PSI_THRESHOLD = PSI_MODERATE`) in [dagster_project/assets/monitoring.py](../dagster_project/assets/monitoring.py) must remain in sync. Both import from `bmo.monitoring.drift.PSI_MODERATE`.

### Feature-Level vs. Prediction Drift

The `drift_report` asset checks feature distributions, not prediction distributions. This is intentional:

- Feature drift is **leading** — it's detectable before accuracy degrades.
- Prediction drift requires actuals to interpret and has ~60-day lag.
- Per-feature granularity tells you *which* data distribution changed (weather, carrier, airport congestion), which guides root-cause investigation.

KL divergence `D_KL(current ∥ reference)` is also computed and stored per feature. It is asymmetric and unbounded, so it is **not used for alerting** — it is stored for relative comparison between features when investigating drift root cause.

### `drift_report` Asset

**Source:** [dagster_project/assets/monitoring.py](../dagster_project/assets/monitoring.py)

**Partition:** `DailyPartitionsDefinition` — one run per day, matching `batch_predictions`.

**Freshness policy:** deadline `0 10 * * *` (10am UTC), lower bound 1 hour — the Dagster UI marks this asset stale if it hasn't been materialized by 10am.

**Trigger:** Automatically run by `daily_drift_report_schedule` at 8am UTC (2 hours after batch scoring at 6am). Can also be backfilled from the Dagster UI.

**Steps:**

1. Load the champion model version and its `dataset_version_hash` tag from the MLflow registry.
2. Load the **reference** feature distribution: up to 20,000 randomly sampled rows (seed=42) from `s3://staging/datasets/{hash}/data.parquet`. Fixed seed ensures reproducible reference samples for a given dataset version.
3. Load the **current** feature distribution: read entity IDs from the last 7 days of `batch_predictions` Parquet, then join against the latest Feast offline Parquet snapshots per entity key (one dedup per entity, keyed on `event_ts`). This avoids the O(n×m) memory blowup of `get_historical_features()`.
4. Load `feature_importance.json` from the champion model's MLflow run artifacts to rank features. Falls back to uniform importance if the artifact is missing.
5. Call `compute_drift()` → `DriftReportResult` with Evidently HTML + per-feature `DriftMetricsRow` list.
6. Write HTML report to `s3://staging/monitoring/reports/date=YYYY-MM-DD/drift_report.html`.
7. Write metrics Parquet to `s3://staging/monitoring/metrics/date=YYYY-MM-DD/drift_metrics.parquet` (consumed by `mart_drift_metrics` dbt model).
8. Upsert `DriftMetricsRow` rows to the `drift_metrics` Postgres table (polled by `drift_retrain_sensor`).

**Materialize result metadata** (visible in Dagster UI):

- `n_features_checked`, `n_features_breached`, `max_psi`
- `drift_detected` (bool), `breached_features` (comma-separated list of top-5)
- `html_report_path`, `metrics_parquet_path`
- `reference_rows`, `current_rows`

**Idempotency:** Re-running the same partition produces identical results for the same champion model and feature store state. HTML and Parquet are overwritten; Postgres uses `ON CONFLICT DO UPDATE`.

### Postgres Storage Schema

```sql
CREATE TABLE drift_metrics (
    report_date     DATE        NOT NULL,
    feature_name    TEXT        NOT NULL,
    psi_score       FLOAT       NOT NULL,
    kl_divergence   FLOAT,
    rank            INTEGER     NOT NULL,  -- 1 = highest importance by training feature_importance.json
    is_breached     BOOLEAN     NOT NULL,  -- psi_score > 0.2
    model_version   TEXT,
    computed_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (report_date, feature_name)
);
```

The `rank` column is derived from the champion model's `feature_importance.json` artifact. Features not present in that artifact get rank based on their position in `FEATURE_COLUMNS` (the order used in `score.py`).

### GitHub Pages Publishing

The `evidently-reports.yml` GitHub Actions workflow syncs Evidently HTML reports from S3 to GitHub Pages on a daily schedule. Reports are publicly browsable at:

```text
https://<user>.github.io/<repo>/evidently-reports/date=YYYY-MM-DD/drift_report.html
```

The workflow uses `aws s3 sync` with `S3_ENDPOINT_URL` pointing at Cloudflare R2.

---

## Ground Truth Backfill

### `ground_truth_backfill` Asset

**Source:** [dagster_project/assets/monitoring.py](../dagster_project/assets/monitoring.py)

**Partition:** Non-partitioned — runs over all available data at once.

**Trigger:** Downstream of `bmo_dbt_assets` in the Dagster asset graph. Run automatically after each BTS ingestion that adds new actuals.

**Purpose:** Compute realized classification metrics per `(score_date, model_version)` pair, once BTS ground truth is available.

### Matching Predictions to Actuals

The `mart_predictions` dbt model performs the join:

```text
batch_predictions (predicted_proba, model_version)
        │
        │ LEFT JOIN on (flight_id, flight_date)
        ▼
staged_flights (actual arr_del15 — available ~60 days later)
        │
        ▼
mart_predictions.actual_is_delayed  ← NULL until BTS publishes
```

`ground_truth_backfill` queries `mart_predictions WHERE actual_is_delayed IS NOT NULL`, groups by `(score_date, model_version)`, and upserts to `live_accuracy`. Because the join is a LEFT JOIN, rows with `actual_is_delayed IS NULL` are skipped — the asset never uses incomplete data.

### BTS Publication Lag

BTS publishes on-time data with approximately a **60-day lag**. A flight that departed on March 1 will have confirmed actuals available around May 1. The `ground_truth_backfill` asset is designed to be re-run whenever new BTS data is ingested — it is safe to run at any time.

### Live Accuracy Tracking

Metrics computed per `(score_date, model_version)`:

| Metric                  | Description                                       |
|-------------------------|---------------------------------------------------|
| `roc_auc`               | ROC AUC — only computed when both classes present |
| `f1`                    | F1 score                                          |
| `precision_score`       | Precision                                         |
| `recall_score`          | Recall                                            |
| `brier_score`           | Calibration quality (lower is better)             |
| `positive_rate`         | Model's predicted positive rate                   |
| `actual_positive_rate`  | Ground truth positive rate                        |
| `n_with_actuals`        | Flight count with confirmed outcomes              |

```sql
CREATE TABLE live_accuracy (
    score_date           DATE    NOT NULL,
    model_version        TEXT    NOT NULL,
    roc_auc              FLOAT,
    f1                   FLOAT,
    precision_score      FLOAT,
    recall_score         FLOAT,
    brier_score          FLOAT,
    positive_rate        FLOAT,
    actual_positive_rate FLOAT,
    n_with_actuals       INTEGER,
    PRIMARY KEY (score_date, model_version)
);
```

`ON CONFLICT DO UPDATE` makes re-runs idempotent — rows for a `score_date` that gains new actuals are updated in place.

---

## Drift Retrain Sensor

**Source:** [dagster_project/sensors/drift_retrain_sensor.py](../dagster_project/sensors/drift_retrain_sensor.py)

### How It Works

`drift_retrain_sensor` is a [Dagster `@sensor`](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) that polls `drift_metrics` in Postgres. When PSI > 0.2 on any feature with `rank <= 10` (top 10 by importance), it yields a `RunRequest` targeting the `nightly_retrain` job, which materializes `training_dataset` and triggers the full retrain pipeline.

Retrain logic is extracted into `src/bmo/monitoring/retrain_trigger.py:should_retrain()` — a pure function that takes a list of `DriftMetricsRow` and returns `(bool, list[str])`. This keeps the business rule unit-testable in isolation from Dagster and from the sensor.

### Polling Interval

`minimum_interval_seconds=3600` (hourly). Drift metrics are computed once per day, so hourly polling is frequent enough to respond the same day drift is detected without redundant evaluations.

### Trigger Conditions

Both conditions must be true:

1. New `drift_metrics` rows exist since the last cursor timestamp.
2. At least one row has `psi_score > 0.2` AND `rank <= 10`.

Only the top-10 features by training importance trigger retrains. Lower-ranked features are informational — they are stored and visible in dashboards but do not cause automated retraining. This prevents noisy, low-signal features from triggering unnecessary retrains.

### Auto-Retrain Flow

```text
drift_retrain_sensor fires
        │
        ▼
RunRequest(run_key='drift-retrain-YYYY-MM-DD',
           tags={'trigger': 'drift_sensor',
                 'n_breached_features': ...,
                 'breached_features': ...})
        │
        ▼
nightly_retrain job
        │
        ▼
training_dataset → trained_model → [evaluation gate] → registered_model
```

The `tags` dict is propagated to the Dagster run, making drift-triggered retrains distinguishable from scheduled retrains in the UI run history.

### Preventing Retrain Loops

Two mechanisms prevent back-to-back retrains:

1. **`run_key` deduplication** — `run_key='drift-retrain-YYYY-MM-DD'` is date-based. Dagster deduplicates by `run_key`, so a second sensor tick on the same day will not start a second retrain even if PSI is still breached.
2. **Cursor advancement** — the sensor updates its cursor to `MAX(computed_at)` of the queried rows after every tick, regardless of whether a retrain was triggered. On the next tick, only rows newer than the cursor are evaluated — so a day's worth of PSI rows is considered exactly once.

The combination means: one retrain per calendar day maximum, even if the sensor fires multiple times.

### Graceful Pre-Monitoring Behavior

If `drift_metrics` doesn't exist yet (early in a new deployment, before Phase 10 has run), the `_query_drift_metrics()` helper catches the `sqlalchemy` exception and returns an empty list. The sensor yields a `SkipReason` and does not crash the Dagster daemon.

---

## Alerts

### `run_failure_sensor`

**Source:** [dagster_project/sensors/run_failure_sensor.py](../dagster_project/sensors/run_failure_sensor.py)

`run_failure_sensor` uses Dagster's [`@run_failure_sensor`](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#run-failure-sensor) decorator — unlike a polled `@sensor`, this is event-driven. The Dagster daemon calls it automatically whenever any run in the code location transitions to `FAILURE` state.

When triggered, it posts a Discord embed to `DISCORD_WEBHOOK_URL` with:

- Job name
- Run ID (truncated to 12 characters)
- Error message (truncated to 1000 characters to stay within Discord's embed field limit)

If `DISCORD_WEBHOOK_URL` is not set, the sensor logs a warning and returns silently. It does not raise, to avoid creating a failure alert loop.

### Alert Destination Configuration

1. Create a Discord webhook: **Server Settings → Integrations → Webhooks → New Webhook → Copy URL**. See [Discord webhook docs](https://discord.com/developers/docs/resources/webhook).

2. Add to `.env`:

   ```env
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/<id>/<token>
   ```

3. The sensor activates automatically on next Dagster daemon reload.

No configuration is needed in the Dagster UI — the sensor is registered in `definitions.py` and picks up the env var at runtime.

---

## Dashboards

The React frontend [react/src/routes/](../react/src/routes/) surfaces monitoring data across three routes:

| Route        | What it shows                                              | API endpoints                                           |
|--------------|------------------------------------------------------------|---------------------------------------------------------|
| `/drift`     | PSI heatmap per feature per day; per-feature PSI trend     | `GET /api/drift/metrics`, `GET /api/psi/{feature_name}` |
| `/accuracy`  | ROC AUC, F1, Brier score per model version over time       | `GET /api/accuracy`                                     |
| `/models`    | Model registry stats — avg AUC, last scored date, count    | `GET /api/models`                                       |

`GET /api/drift/summary` returns a quick-glance aggregate (PSI breach count, max PSI, severity per feature) used for the top-of-page status indicator.

### What to Watch (Key Metrics)

| Signal                                     | Location               | Elevated means                                                       |
|--------------------------------------------|------------------------|----------------------------------------------------------------------|
| PSI breach count (top-10)                  | `/drift` dashboard     | Feature distribution shifted — retrain may be needed                |
| Max PSI (top-10)                           | `/drift` dashboard     | Severity indicator; > 0.4 warrants immediate investigation           |
| ROC AUC trend (live)                       | `/accuracy` dashboard  | Model accuracy degrading despite no PSI trigger                      |
| `positive_rate` vs `actual_positive_rate`  | `/accuracy` dashboard  | Model is miscalibrated — check Brier score                           |
| `bmo_feature_null_total` (Prometheus)      | `/metrics`             | Features missing from Redis; Feast TTL expiry or materialization lag |

### Interpreting PSI Values

The PSI heatmap on the `/drift` route colors cells by severity:

- **Green** (`severity='green'`): PSI < 0.1 — stable.
- **Amber** (`severity='amber'`): PSI 0.1–0.2 — moderate shift, monitoring.
- **Red** (`severity='red'`): PSI > 0.2 and `is_breached=true` — retrain triggered.

When a feature turns amber, check whether the shift is due to seasonality (expected), a data pipeline issue (unexpected), or a genuine distribution change in the underlying population.

### Prediction Volume Anomalies

Prediction volume drops (visible in `/predictions`) can indicate:

- Feast TTL expiry causing 503s from the serving API (`bmo_feature_null_total` Prometheus counter will spike).
- Fly.io container restart (check `bmo_model_info` Prometheus metric for `loaded_at` timestamp).
- Upstream batch scoring failure (check `daily_score_schedule` run history in Dagster UI).

---

## Operational Procedures

### Investigating a Drift Alert

1. Open the Dagster UI → **Assets** → `drift_report` → find the partition that triggered → check `breached_features` in the materialize result metadata.
2. Open the `/drift` dashboard and look at the PSI trend for each breached feature. Determine when the drift started.
3. Open the Evidently HTML report for that date (linked in the materialize metadata as `html_report_path`) to see distribution histograms side-by-side.
4. Check whether the breach aligns with an external event:
   - New BTS month ingested → feature window changed?
   - Seasonal pattern (holiday travel, weather season)?
   - Feast materialization failure reducing current-window sample size?
5. If the drift is genuine (not a data artifact), the `drift_retrain_sensor` will have already queued a retrain. Verify in Dagster UI → **Runs** → filter by tag `trigger=drift_sensor`.
6. If you believe the drift is spurious, you can re-run the `drift_report` partition manually after investigating. The re-run will overwrite the Postgres row.

### Manually Triggering a Retrain

From the Dagster UI:

1. Go to **Jobs** → `nightly_retrain` → **Materialize**.
2. Optionally add tags: `{"trigger": "manual", "reason": "drift investigation"}`.

From CLI:

```bash
dagster job execute -j nightly_retrain -m dagster_project
```

The retrain job materializes `training_dataset` with the latest feature data and runs the full training pipeline through `registered_model`.

### Resetting the Ground Truth Baseline

If `live_accuracy` has stale or incorrect rows (e.g., after a re-ingestion that changes `staged_flights` actuals):

```sql
-- Delete rows for a specific model version and date range, then re-run ground_truth_backfill
DELETE FROM live_accuracy
WHERE model_version = '<version>'
  AND score_date BETWEEN '2024-01-01' AND '2024-03-31';
```

Then re-materialize `ground_truth_backfill` from the Dagster UI. The asset will recompute and re-upsert the deleted rows.

### Resetting the Drift Sensor Cursor

If the sensor cursor is stuck at a stale timestamp (e.g., after a database restore):

1. Dagster UI → **Sensors** → `drift_retrain_sensor` → **Reset cursor**.
2. The sensor will re-evaluate all rows in `drift_metrics` from epoch. This is safe — `run_key` deduplication prevents duplicate retrains for dates that already have a completed run.

### Checking Drift for a Specific Feature

```sql
SELECT report_date, psi_score, kl_divergence, is_breached
FROM drift_metrics
WHERE feature_name = 'origin_avg_dep_delay_1h'
ORDER BY report_date DESC
LIMIT 30;
```

Or use the `/api/psi/{feature_name}` endpoint, which is what the `/drift` dashboard's feature trend chart queries.
