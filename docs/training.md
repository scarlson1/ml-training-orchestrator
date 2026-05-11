# Model Training

## Overview

The training pipeline converts the assembled feature mart into a deployable XGBoost binary classifier. It spans three Dagster assets in the `training` group:

```text
feast_materialized_features  ─┐
mart_training_dataset        ─┤─→  training_dataset  →  trained_model  →  registered_model
```

| Asset | Source | Description |
| --- | --- | --- |
| `training_dataset` | `dagster_project/assets/training.py` | PIT-correct Parquet dataset written to S3 with a `DatasetHandle` sidecar |
| `trained_model` | `dagster_project/assets/training.py` | Optuna HPO sweep (50 trials); best XGBoost run logged to MLflow |
| `registered_model` | `dagster_project/assets/training.py` | Passes evaluation gate checks, registers to MLflow Model Registry, promotes champion |

Business logic lives in `src/bmo/` and is Dagster-agnostic:

| Module | Responsibility |
| --- | --- |
| `bmo.training_dataset_builder` | `build_dataset()`, `DatasetHandle`, leakage guards, PIT join |
| `bmo.training.train` | `train_single_run()`: time split, XGBoost fit, MLflow logging |
| `bmo.training.hpo` | `run_hpo()`: Optuna study orchestration |
| `bmo.training.reproduce` | `reproduce_run()`: byte-equality verification |
| `bmo.training.models.xgboost_model` | `fit_xgboost()`: XGBClassifier wrapper |

---

## Training Dataset Builder

`build_dataset()` in `src/bmo/training_dataset_builder/builder.py` is the public entry point. It is idempotent: if the content-addressed output path already exists in S3, the function returns the cached `DatasetHandle` without re-running the join.

Execution steps:

1. Validate `label_df` has required columns (`flight_id`, `event_timestamp`, `origin`, `dest`, `carrier`).
2. Compute the content-addressed `version_hash` (SHA-256). Check S3 cache.
3. Execute PIT join via `PITJoiner` — the expensive step.
4. Run all four leakage guards; raise `LeakageError` on any `error`-severity violation.
5. Write Parquet (`data.parquet`) and metadata sidecar (`card.json`) to S3.
6. Return `DatasetHandle`.

### Content Addressing (SHA-256)

Every dataset has a `version_hash` — a SHA-256 over its inputs, computed in `compute_dataset_hash()`:

```python
payload = {
    'feature_refs': sorted_refs,            # sorted list of "view:feature" strings
    'as_of': as_of.isoformat(),             # temporal upper bound
    'feature_set_version': ...,             # git tree hash of feature_repo/
    'label_hash': hashlib.sha256(label_bytes).hexdigest(),  # Parquet bytes of sorted label_df
    'code_version': git_sha,                # HEAD SHA of the training repo
}
version_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
```

The dataset is written to `s3://staging/datasets/{version_hash}/data.parquet` with a JSON sidecar at `s3://staging/datasets/{version_hash}/card.json`. Two runs with identical inputs produce the same hash and reuse the cached dataset.

### What Goes Into the Hash

| Input | Why it's included |
| --- | --- |
| `feature_refs` (sorted) | Adding or removing a feature produces a new hash → new dataset |
| `as_of` | Training on data through different dates must yield distinct datasets |
| Label content (Parquet bytes, sorted) | A new backfilled partition or corrected label produces a new hash |
| `feature_set_version` (git tree hash of `feature_repo/`) | Changing a TTL or feature definition invalidates the cache |
| `code_version` (HEAD SHA) | Pinned for audit; the hash changes if the pipeline code changes |

### Leakage Guards

Four guards run in `run_all_guards()` (`src/bmo/training_dataset_builder/leakage_guards.py`). All guards run regardless of whether earlier guards found violations, so you get a full picture in one pass.

#### Guard 1: Event Timestamp Bounds

Checks that no label row has `event_timestamp > as_of`. If `as_of` is `None` (development only), emits a **warning** — not an error — because unbounded datasets are intentional in local development.

```text
error: 3 label rows have event_timestamp > as_of (2024-06-01T00:00:00+00:00).
       These flights have not yet departed and must be excluded from training.
```

Common cause: a timezone bug where local time was passed as UTC.

#### Guard 2: No Future Features

Checks that for each feature view, the matched feature snapshot's timestamp (`{view}__feature_ts`) does not exceed the row's `event_timestamp`. The ASOF JOIN should guarantee this structurally; this guard surfaces bugs in the join implementation.

```text
error: Feature view "origin_airport_features": 12 rows have feature_ts > event_timestamp.
```

#### Guard 3: TTL Compliance

Checks that the age of the matched feature snapshot `(event_timestamp − feature_ts)` does not exceed the feature view's configured TTL. This guard is **warning** severity because sparse data at the start of a backfill naturally causes stale features.

When a feature age exceeds TTL, `PITJoiner` nulls it out (rather than dropping the row) so the model can learn from partial information.

| Feature view | TTL |
| --- | --- |
| `origin_airport_features` | 26 h |
| `dest_airport_features` | 26 h |
| `carrier_features` | 8 d |
| `route_features` | 8 d |
| `aircraft_features` | 12 h |

#### Guard 4: No Target Leakage

Two checks:

1. **Direct overlap**: feature column names that are also label columns (`dep_delay_min`, `is_dep_delayed`, `cancelled`, etc.) — **error**.
2. **Pattern match**: feature column names matching known outcome patterns (`actual_dep_*`, `wheels_off*`, `taxi_out*`, etc.) that could encode post-departure outcomes — **warning**.

### `DatasetHandle` — Dataset Metadata Card

`DatasetHandle` (`src/bmo/training_dataset_builder/dataset_handle.py`) is an immutable Pydantic model written as `card.json` next to every dataset Parquet:

| Field | Description |
| --- | --- |
| `version_hash` | SHA-256 of all inputs; used as S3 path and MLflow parameter |
| `feature_refs` | Sorted `view:feature` strings that went into this dataset |
| `feature_set_version` | Git tree hash of `feature_repo/` |
| `feature_ttls` | `{view_name: ttl_seconds}` — records the TTL at build time |
| `as_of` | Temporal upper bound; `null` means unbounded |
| `row_count` | Number of rows in the Parquet |
| `label_distribution` | Per-target mean, std, min, max, and `positive_rate` (binary targets) |
| `schema_fingerprint` | SHA-256 of column names + dtypes; detects schema drift |
| `created_at` | UTC timestamp when the dataset was built |
| `storage_path` | Full S3 URI of `data.parquet` |

`label_distribution` is logged to MLflow on every training run so sudden drops in `positive_rate` (e.g. 20% → 2%) are immediately visible without querying S3.

`trained_model` reads the `DatasetHandle` from the card sidecar on S3 rather than passing the Python object directly between Dagster assets, which would couple the two assets and prevent independent materialization.

### PIT Join via DuckDB ASOF JOIN

`PITJoiner` (`src/bmo/training_dataset_builder/pit_join.py`) executes one [DuckDB ASOF JOIN](https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins) per feature view and assembles the results.

For each feature view, the SQL has three layers:

```sql
WITH features AS (
    -- Load Parquet, apply as_of cutoff, sort for ASOF JOIN
    SELECT entity_col AS __entity_key, event_ts, feat_col_1, ...
    FROM read_parquet('s3://staging/feast/origin_airport/data.parquet')
    WHERE event_ts <= TIMESTAMPTZ '2024-06-01T00:00:00+00:00'
    ORDER BY __entity_key, event_ts
),
pit AS (
    -- For each label row, find the LATEST feature snapshot ≤ event_timestamp
    SELECT labels.flight_id, labels.event_timestamp,
           features.event_ts AS __feature_ts,
           epoch(labels.event_timestamp) - epoch(features.event_ts) AS age_seconds,
           feat_col_1, ...
    FROM labels
    ASOF LEFT JOIN features
        ON labels.origin = features.__entity_key
        AND labels.event_timestamp >= features.event_ts
)
SELECT flight_id, __feature_ts AS origin_airport_features__feature_ts,
       CASE WHEN age_seconds > 93600 THEN NULL ELSE feat_col_1 END AS feat_col_1, ...
FROM pit
```

The TTL mask in the outer `SELECT` nulls out features (not drops rows) when the feature snapshot is older than the view's TTL.

After joining all five views, the `{view}__feature_ts` columns are used by leakage guards and then dropped from the final Parquet.

DuckDB reads Parquet files directly from S3/MinIO via its [httpfs extension](https://duckdb.org/docs/guides/network_and_cloud/s3_import.html). The same `s3_access_key_id`, `s3_secret_access_key`, and `s3_endpoint` env vars used elsewhere in the project are passed to DuckDB's `SET s3_*` configuration.

---

## Hyperparameter Optimization (Optuna)

`run_hpo()` in `src/bmo/training/hpo.py` orchestrates a full [Optuna](https://optuna.readthedocs.io/en/stable/) HPO sweep over XGBoost hyperparameters. It creates one parent MLflow run for the sweep and up to 50 nested child runs — one per trial — then re-runs the best params as a "champion" child run with full artifact logging.

### Search Space

| Parameter | Type | Range |
| --- | --- | --- |
| `max_depth` | int | 3 – 10 |
| `learning_rate` | float (log scale) | 0.01 – 0.30 |
| `n_estimators` | int | 100 – 1000 |
| `min_child_weight` | int | 1 – 10 |
| `subsample` | float | 0.50 – 1.00 |
| `colsample_bytree` | float | 0.50 – 1.00 |
| `reg_alpha` | float (log scale) | 1e-8 – 10.0 |
| `reg_lambda` | float (log scale) | 1e-8 – 10.0 |
| `gamma` | float | 0.0 – 5.0 |

`learning_rate` and the regularization terms use log-scale sampling because their effect is multiplicative — equal steps on a log scale cover meaningful variation across orders of magnitude.

### Objective Function

Each Optuna trial calls `train_single_run()` with the sampled parameters. The objective returns `test_roc_auc` from the held-out test set, which Optuna uses to guide the next suggestion.

Trial runs log only params and metrics to MLflow (lean logging). The champion re-run at the end logs the full artifact set: feature importance plot, confusion matrix, calibration curve, and the model binary.

The [XGBoostPruningCallback](https://optuna.readthedocs.io/en/stable/reference/generated/optuna.integration.XGBoostPruningCallback.html) hooks into XGBoost's per-round eval reporting (`validation_0-logloss`). After each boosting round, it calls `trial.report(val_logloss, step=round)` and raises `TrialPruned` if `trial.should_prune()` is True. Without this callback, `MedianPruner` can only prune between complete trials and cannot cut a clearly bad trial short mid-training.

### Trial Count & Pruning

| Setting | Value | Rationale |
| --- | --- | --- |
| `n_trials` | 50 (default) | Configurable via `DAGSTER_HPO_N_TRIALS` env var (e.g. `DAGSTER_HPO_N_TRIALS=5` for fast local runs) |
| `sampler` | [TPESampler](https://optuna.readthedocs.io/en/stable/reference/generated/optuna.samplers.TPESampler.html) | First 10 trials are random exploration; subsequent trials use Bayesian optimization |
| `pruner` | [MedianPruner](https://optuna.readthedocs.io/en/stable/reference/generated/optuna.pruners.MedianPruner.html) | Prunes if intermediate value is worse than median of completed trials at the same boosting step |
| `n_startup_trials` (pruner) | 5 | Pruner waits for 5 complete trials before evaluating candidates |
| `n_warmup_steps` (pruner) | 50 | Pruner waits 50 boosting rounds before checking each trial |

The Optuna study is persisted to SQLite at `/tmp/bmo_optuna/study_{hash[:16]}.db`, keyed by `version_hash`. If the Dagster run crashes mid-sweep, restarting picks up where it left off (`load_if_exists=True`). The SQLite file is also logged as an MLflow artifact for auditability. For distributed HPO across multiple Dagster workers, swap `sqlite:///` for a PostgreSQL URL.

### Determinism (Optuna Seed)

`TPESampler(seed=42)` fixes the random seed for the exploration phase. The first 10 trials (random) are reproducible across runs on the same dataset hash. Bayesian trials depend on the outcomes of prior trials; reproducibility there requires the same trial order, which requires the same dataset hash and the same completed-trial history (i.e. the same SQLite study file).

---

## XGBoost Training

`fit_xgboost()` in `src/bmo/training/models/xgboost_model.py` wraps `xgb.XGBClassifier`. It returns an `XGBFitResult` dataclass holding the booster, all metrics, normalized feature importances, and pre-computed test-set predictions.

See the [XGBoost parameter docs](https://xgboost.readthedocs.io/en/stable/parameter.html) for full parameter reference.

### Model Configuration

```python
DEFAULT_PARAMS = {
    'objective': 'binary:logistic',
    'eval_metric': ['logloss', 'auc'],
    'max_depth': 6,
    'learning_rate': 0.05,
    'n_estimators': 500,
    'min_child_weight': 5,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'gamma': 0.0,
    'scale_pos_weight': 1.0,
    'seed': 42,
    'nthread': -1,
}
```

`scale_pos_weight` is computed at runtime from the training split: `count(on_time) / count(delayed)` (approximately 2.33 for a ~30% delay rate). This up-weights the minority class (delayed flights) in the loss function so the model doesn't converge to predicting "on time" for everything. Optuna trials inherit this auto-computed value unless they override it explicitly.

Feature importance is stored as **normalized gain** (`importance_type='gain'`, divided by total gain). Gain is preferred over weight (biased toward high-cardinality features) and cover. The leakage sentinel check in the evaluation gate uses this normalized gain to flag any feature dominating >70% of the signal.

### Early Stopping

`early_stopping_rounds=50`. XGBoost monitors `validation_0-logloss` on the validation split after each boosting round. If logloss does not improve for 50 consecutive rounds, training stops. The best iteration is stored in `XGBFitResult.best_iteration` and logged as an MLflow metric.

The validation split is used **only for early stopping** — it is never used to compute reported metrics. Test metrics (`test_roc_auc`, `test_f1`, etc.) are computed exclusively on the held-out test set.

### Label Definition (Departure Delay Threshold)

The training target is `is_dep_delayed` — a boolean derived from the BTS `dep_del15` field, which is `1` when the departure delay is ≥ 15 minutes. The 15-minute threshold is the official FAA/BTS definition of a delay.

The model is a binary classifier: `P(departure delay ≥ 15 min)`. The companion regression target `dep_delay_min` (minutes of delay) is included in the dataset Parquet but is not used by the default training configuration. To train a regression model, pass `target_column='dep_delay_min'` to `run_hpo()`.

### Time-Based Train/Val/Test Split

`_time_split()` in `src/bmo/training/train.py` sorts rows by `event_timestamp` and splits chronologically:

| Split | Fraction | Purpose |
| --- | --- | --- |
| Train | 68% (earliest) | Model weight fitting |
| Val | 12% | XGBoost early stopping only |
| Test | 20% (latest) | Reported metrics; model never sees during training |

Random splits are not used because windowed features (e.g. `origin_avg_dep_delay_7d`) embed information from the same window period. A random split would place, say, October flights in the test set while September flights in training contain October data in their 7-day aggregates — leaking the test period into training features. A chronological split prevents this entirely.

Null values in feature columns are filled with `0` before passing to XGBoost. XGBoost also handles missing values natively; filling with 0 is conservative and matches the online serving imputation.

---

## MLflow Integration

See [MLflow Tracking docs](https://mlflow.org/docs/latest/tracking.html) and [mlflow.xgboost docs](https://mlflow.org/docs/latest/python_api/mlflow.xgboost.html).

Experiment name: `bmo/flight_delay`. All training runs live under this experiment. The HPO sweep creates one parent run and 50+ nested child runs.

### What Gets Logged (Params, Metrics, Artifacts)

**Params** (logged once at run start, immutable):

| Parameter | Value |
| --- | --- |
| `dataset_version_hash` | SHA-256 of all dataset inputs |
| `dataset_row_count` | Number of rows |
| `dataset_feature_count` | Number of feature columns |
| `feature_set_version` | Git tree hash of `feature_repo/` |
| `git_sha` | HEAD commit of the training repo |
| `target_column` | e.g. `is_dep_delayed` |
| `xgb_max_depth`, `xgb_learning_rate`, … | All XGBoost params prefixed with `xgb_` |
| `hpo_n_trials`, `hpo_sampler`, `hpo_pruner` | HPO sweep config (parent run only) |

**Metrics** (test-set):

| Metric | Description |
| --- | --- |
| `test_roc_auc` | ROC-AUC — threshold-independent ranking quality |
| `test_pr_auc` | PR-AUC — precision-recall tradeoff on the minority class |
| `test_log_loss` | Average log-probability assigned to the true label |
| `test_f1` | Harmonic mean of precision and recall at threshold=0.5 |
| `test_brier_score` | Calibration — mean squared error of P(delay) vs. 0/1 |
| `best_iteration` | Boosting round at which early stopping fired |
| `train_rows`, `test_rows` | Split sizes |

**Artifacts** (champion run only):

- `model/` — XGBoost booster in [UBJ format](https://xgboost.readthedocs.io/en/stable/tutorials/saving_model.html), logged via `mlflow.xgboost.log_model()`
- `dataset_card.json` — full `DatasetHandle` serialized to JSON
- `feature_importance.json` — `{feature_name: normalized_gain}` dict
- `plots/` — feature importance bar chart (top 20), confusion matrix, calibration curve
- `reports/` — Evidently HTML classification report (logged during `registered_model`)
- `optuna_study.db` — SQLite study file (parent run only)

**MLflow Dataset Input** — `mlflow.log_input()` links the training run to the S3 Parquet source, recording the dataset URI and a digest (first 36 chars of `version_hash`) in the MLflow UI.

### `version_hash` as a Tracked Parameter

The `dataset_version_hash` parameter is the chain linking a deployed model back to its exact training data. Given a model version in the MLflow Registry, you can trace:

```text
model_version.tags['dataset_version_hash']
  → s3://staging/datasets/{hash}/card.json   (DatasetHandle)
  → card.json['feature_set_version']         (feature_repo/ git tree hash)
  → card.json['feature_refs']                (exact features used)
  → card.json['as_of']                       (temporal upper bound)
```

This chain is the foundation of the reproducibility guarantee.

### Champion Model Selection Logic

`registered_model` promotes a new model to `champion` alias if its `test_roc_auc` is ≥ the current champion's AUC:

```text
new_auc >= prod_auc  →  promote to champion, archive old champion
new_auc < prod_auc   →  register as challenger only; current champion stays in prod
no existing champion →  promote immediately
```

The promotion comparison is read-before-write: the asset reads the current champion's AUC from `gate_input.prod_metrics` (populated by `load_gate_input()` in the evaluation gate) before deciding whether to promote.

Aliases used:

| Alias | Meaning |
| --- | --- |
| `challenger` | Always set on the new model version, regardless of AUC comparison |
| `champion` | The model currently served by the FastAPI endpoint |
| `archived` (tag) | Former champion displaced by promotion |

The `deployed_api` asset reads `alias=champion` from the registry and writes `model_config.json` to S3. The FastAPI service's `/admin/reload` endpoint hot-swaps the in-memory model without restarting the container.

---

## Reproducibility

### Reproducing a Model from a Run ID

`reproduce_run()` in `src/bmo/training/reproduce.py` re-runs training from an MLflow run ID and asserts byte-equality between the original and reproduced model binary:

1. Load the original run's params (`xgb_*` prefix) and `dataset_card.json` artifact.
2. Re-run `train_single_run()` with `nthread=1` and the same params.
3. Compare SHA-256 of `booster.save_raw(raw_format='ubj')` bytes.
4. Exit 0 on match, 1 on mismatch.

### Byte-Equality Guarantee

Byte-for-byte reproducibility requires three conditions:

1. **Same dataset** — `DatasetHandle.version_hash` identifies the exact Parquet used. The Parquet file at that S3 path is immutable (content-addressed).
2. **Same parameters** — All XGBoost params are stored as MLflow params and restored by `reproduce.py`.
3. **`nthread=1`** — XGBoost's parallel floating-point reductions are non-deterministic under IEEE 754 (addition is not associative for parallel sums). Single-threaded execution forces serial, reproducible arithmetic.

**Hardware caveat**: byte equality also requires the same OS and CPU architecture. x86 Linux and Apple Silicon can produce bit-different BLAS results due to different FP instruction sets. Run `reproduce.py` in the same Docker image used for production training.

### `reproduce.py` Usage

```bash
# Reproduce a specific MLflow run (exits 0 if bytes match, 1 if not)
uv run python -m bmo.training.reproduce <mlflow_run_id>

# Example
uv run python -m bmo.training.reproduce 3f8a2b1c9d4e5f6a
```

The reproduced run is logged to MLflow under the name `reproduce_{run_id[:8]}`. This creates an audit trail: the reproduction run proves the dataset and code that claimed to produce the original model actually do so.

---

## Nightly Retrain Schedule

### Trigger Conditions

Two triggers can initiate a retrain — both target the `nightly_retrain` job, which materializes `training_dataset → trained_model → registered_model`:

**1. Scheduled (`nightly_retrain_schedule`)** — cron `0 1 * * *` (1 AM UTC daily). The 1 AM start is intentional: the Feast hourly materialization runs at midnight, and training reads from `feast_materialized_features`. A 1-hour gap ensures the midnight Feast run completes before training begins reading feature Parquet files.

The schedule ships with `DefaultScheduleStatus.STOPPED`. Enable it in the Dagster UI or set `default_status=DefaultScheduleStatus.RUNNING` for production.

**2. Drift-triggered (`drift_retrain_sensor`)** — polls the `drift_metrics` PostgreSQL table hourly. Triggers a retrain when Population Stability Index (PSI) > 0.2 on any of the top-10 features by importance rank. The `run_key` is date-based (`drift-retrain-YYYY-MM-DD`), so Dagster deduplicates multiple triggers on the same day into a single run.

See [monitoring.md](monitoring.md) for how PSI is computed and which features are ranked.

### Partition Selection

The training pipeline is **unpartitioned**. Each retrain reads the full `mart_training_dataset` (all months of BTS data ingested to date) and the full Feast offline store. The 80/12/20 chronological split ensures the held-out test set always contains the most recent flights.

### Fallback Behavior on Failure

If a retrain fails for any reason (evaluation gate blocks, infra error, OOM), the previous champion model remains in production. No rollback is needed because champion promotion only occurs on `registered_model` materialization success. The FastAPI endpoint continues serving predictions from the last `champion` alias in the registry.

Discord alerts fire on any run failure via the `run_failure_sensor`. The alert embed includes the asset name, error message, and a link to the Dagster run. See [evaluation-gate.md](evaluation-gate.md) for gate-specific failure scenarios.

---

## Extending the Model

### Swapping XGBoost for Another Algorithm

1. Create `src/bmo/training/models/<algo>_model.py`. The module must expose a `fit_<algo>()` function that accepts `(X_train, y_train, X_val, y_val, X_test, y_test, feature_names, params, callbacks)` and returns a result with at minimum `metrics` and `best_iteration`.
2. Add `mlflow.<algo>.log_model()` logging in `train_single_run()` in place of `log_xgboost_model`.
3. Update the Optuna search space in `hpo.py` to match the new algorithm's hyperparameters.
4. The leakage guards, PIT join, `DatasetHandle`, and evaluation gate operate on the dataset and metrics independently — they do not need changes for a new algorithm, as long as `test_roc_auc` and `test_brier_score` are still logged.

The MLlib GBT baseline (`src/bmo/training/models/mllib_baseline.py`) is an example of a non-XGBoost model being logged as a nested child run within the same HPO sweep. It is a sanity-check baseline, not a serving candidate.

### Adding New Input Features

Features flow into training through `ALL_FEATURE_REFS` in `dagster_project/assets/training.py`. Each entry is a `"view_name:feature_column"` string matching a `FeatureViewConfig` in `default_feature_view_configs()`.

To add a new feature:

1. Build the feature in dbt or PySpark (see [feature-engineering.md](feature-engineering.md)) and materialize it into the Feast offline store (see [feature-store.md](feature-store.md)).
2. Add a `FeatureViewConfig` entry in `src/bmo/training_dataset_builder/pit_join.py` — or add the column to an existing view's `feature_cols` list.
3. Append the `"view_name:column_name"` ref to `ALL_FEATURE_REFS` in `training.py`.
4. Materialize `training_dataset` to build a new content-addressed dataset with the updated feature set. The new `version_hash` will differ from all previous datasets.
5. Materialize `trained_model` — Optuna will explore the new feature's contribution in the same 50-trial sweep.

To **ablate** a feature (measure its contribution): remove it from `ALL_FEATURE_REFS`, retrain, and compare AUC between the two MLflow runs. The `dataset_version_hash` param on each run makes it explicit which feature set was used.

> **Note on schema fingerprint**: adding or removing a feature changes `DatasetHandle.schema_fingerprint`. The fingerprint is logged as a Dagster asset metadata value so you can confirm the dataset schema changed as expected without inspecting the Parquet directly.
