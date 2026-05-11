# Testing

## Overview

The test suite is organized into three layers:

| Layer | Marker | When it runs | What it needs |
| --- | --- | --- | --- |
| **Unit** | _(none)_ | Every push / PR | Nothing — fully in-memory |
| **Integration** | `integration` | Manual / nightly | Docker Compose stack |
| **Determinism** | `determinism` | Nightly CI | MLflow local tracking dir |

Unit tests cover the vast majority of logic. Integration tests verify end-to-end data contracts that unit tests cannot (Feast registry, real PIT-join through the Feast SDK). Determinism tests verify that `reproduce_run()` produces byte-identical XGBoost models given the same inputs.

## Test Structure

```
tests/
├── conftest.py                         # shared fixtures + env var bootstrap
├── fixtures/
├── unit/
│   ├── test_ingestion_bts.py
│   ├── test_ingestion_noaa.py
│   ├── test_staging_timezone.py
│   ├── test_pit_join.py
│   ├── test_training.py
│   ├── test_evaluation_gate.py
│   ├── test_dataset_handle.py
│   └── test_serving_feature_client.py
├── integration/
│   ├── test_feast_roundtrip.py
│   └── test_leakage_planted_value.py
└── determinism/
    └── test_reproduce_run.py
```

---

## Unit Tests

### What Gets Mocked (S3, Redis, HTTP)

Unit tests run without any external process. Three categories of things get mocked:

**S3 / `ObjectStore`**  
Ingestion functions (`ingest_month`, `ingest_noaa_month`) take a `store` argument. Tests pass a `MagicMock()` and assert on `store.put_bytes.call_args_list` to verify that the right bytes were written and that the manifest JSON is correct. No real S3 or [MinIO](https://min.io/docs/minio/linux/index.html) is required.

**HTTP**  
BTS ingestion uses `_download_zip`, patched via `monkeypatch.setattr(bts, '_download_zip', ...)`. NOAA ingestion uses `_fetch_lcd_year`, patched the same way. The [`responses`](https://github.com/getsentry/responses) library (declared in the `dev` dependency group) is available for tests that mock `httpx` or `requests` at the transport layer; NOAA's `build_station_map` uses `unittest.mock.patch('bmo.ingestion.noaa.httpx.get')` directly.

**Feast / Redis**  
`FeatureClient` in `test_serving_feature_client.py` patches `bmo.serving.feature_client.FeatureStore` entirely so no Redis connection is attempted.

**MLflow**  
Training tests in `test_training.py` set `mlflow.set_tracking_uri(f'file://{tmp_path / "mlflow"}')` in an `autouse` fixture, routing all MLflow calls to a local temp directory.

### Fixtures (`conftest.py`)

`conftest.py` does two things before any import of `bmo` can trigger `Settings()` instantiation:

1. Sets required environment variables (`S3_ENDPOINT_URL`, `MLFLOW_TRACKING_URI`, etc.) via `os.environ.setdefault`.
2. Provides two shared fixtures used across several unit test files:

**`sample_label_df`** — five flights covering different routes, carriers, and delay statuses. The `event_timestamp` column is UTC-aware. The fixture comments document which flights test which edge cases (carrier join dedup, sparse feature rows, etc.).

**`sample_origin_feature_df`** — simulated origin airport feature Parquet content. Three snapshots per airport, all timed so that the correct ASOF JOIN behavior can be asserted: one snapshot is placed *after* the event timestamp to verify it is excluded.

### Key Test Files & What They Cover

#### `test_ingestion_bts.py`

Tests `bmo.ingestion.bts` against an in-memory ZIP file containing three CSV rows (one normal flight, one cancelled, one delayed with breakdown).

- **`_extract_csv`** — verifies ZIP extraction, rejects ZIPs with multiple CSVs or no CSV.
- **`_csv_to_parquet`** — shape, column names, schema equality, `cancelled` boolean coercion, airport code preservation, nullable delay-breakdown columns.
- **`ingest_month` idempotency** — skips re-ingestion when the source SHA-256 matches the manifest; writes parquet + manifest on first ingest; validates `year`/`month` range (1987 minimum, month 1–12).

#### `test_ingestion_noaa.py`

Tests `bmo.ingestion.noaa` against a minimal LCD CSV (five rows covering FM-15, FM-16, quality flags, trace precipitation, and February filtering).

- **`_strip_quality_flag`** — removes letter suffixes (`s`), star suffixes (`*`), handles negatives.
- **`_parse_lcd_csv`** — keeps only FM-15 rows, filters to the target month, returns the correct column set, strips quality flags from numeric fields, coerces trace precipitation (`T`) to `0.001`, uses `float32` dtypes, attaches UTC timezone.
- **`build_station_map`** — filters to US stations only, excludes inactive stations (past `END` date), handles non-continental airports (Hawaii/Alaska).
- **`ingest_noaa_month`** — writes parquet + manifest, skips 404 stations gracefully, raises `RuntimeError` when no data survives month filtering, combines rows from multiple stations.

#### `test_staging_timezone.py`

Tests `bmo.staging.timezone` utility functions:

- **`hhmm_to_td`** — converts BTS HHMM integers to `timedelta`; accepts 2400 (midnight); rejects invalid minute values.
- **`local_hhmm_to_utc`** — converts local HHMM + date + IANA timezone to UTC `datetime`; tested for CST (UTC-6) and CDT (UTC-5) to verify DST handling. 2400 rolls over to the next calendar day correctly.
- **`arrival_day_offset`** — returns 0 for same-day arrivals, 1 for overnight.

#### `test_pit_join.py`

**The most critical unit tests in the project.** Tests `PITJoiner` and its DuckDB ASOF JOIN implementation against local Parquet files (no S3).

Five scenarios are verified:

| Test | What it checks |
|---|---|
| `test_selects_nearest_before_snapshot` | Picks 13:00 snapshot over 12:00 for a 14:00 event |
| `test_future_snapshot_excluded` | The 14:30 snapshot (after event) must never appear |
| `test_left_join_unmatched_entity_returns_null` | Flights with no matching feature rows keep their rows (NULL features, not dropped) |
| `test_ttl_exceeded_features_are_null` | Stale features (older than TTL) are set to NULL, not returned |
| `test_as_of_filters_feature_snapshots` | Feature snapshots written after `as_of` are excluded (reproducibility) |

The sentinel value pattern — placing `999.0` in a slot that would only appear under a leakage bug — makes failures obvious in assertion messages.

See also: [DuckDB ASOF JOIN docs](https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins).

#### `test_training.py`

Tests `bmo.training.train` with a 120-row in-memory DataFrame and a local MLflow tracking directory. No S3.

- **`test_train_single_run_returns_result`** — `TrainingResult` is returned; `mlflow_run_id` is non-empty; `test_roc_auc` is in [0, 1]; `best_iteration > 0`.
- **`test_train_logs_to_mlflow`** — verifies params (`dataset_version_hash`, `target_column`, `xgb_max_depth`), metrics (`test_roc_auc`, `test_pr_auc`, `test_log_loss`, `test_brier_score`), artifacts (`dataset_card.json`), and that the XGBoost model is loadable via its URI.
- **`test_feature_importance_normalized`** — importance values sum to 1.0 within floating-point tolerance; all values are non-negative.
- **`test_time_split_is_chronological`** — verifies the 80/20 chronological split produces the expected test-set size.
- **`test_train_with_missing_target_raises`** — raises `KeyError` for an unknown target column.

#### `test_evaluation_gate.py`

Tests `bmo.evaluation_gate` checks and the gate runner. All tests use `GateInput` constructed in-memory — no MLflow server, no S3, no XGBoost training.

- **`AUCGateCheck`** — passes above floor, fails below, handles regression margin vs. prod model, includes `auc` in metadata.
- **`LeakageSentinelCheck`** — fails when a single feature dominates above `max_single_importance`, passes exactly at threshold (inclusive), handles empty importance dict.
- **`CalibrationCheck`** — `WARN` severity; never blocking even when failed.
- **`SliceParityCheck`** — mocks `mlflow.xgboost.load_model`; skips when test set is too small; runs without error on a uniform model.
- **`GateResult`** — `overall_passed` is `False` on any `ERROR` failure, `True` when only `WARN` checks fail. `blocking_failures` only includes `ERROR` severity.
- **`run_gate`** — all checks run even after a blocking failure (no short-circuit).

#### `test_dataset_handle.py`

Tests `compute_dataset_hash`, `compute_schema_fingerprint`, and `compute_label_distributions`.

- **Hash stability** — same inputs produce the same hash; row-order shuffling does not change the hash (content-addressed).
- **Hash sensitivity** — changing `feature_refs`, `as_of`, or any label value produces a different hash. `None` as_of is stable (does not use `datetime.now()`).
- **Schema fingerprint** — detects added columns and dtype changes.
- **Label distributions** — binary columns get `positive_rate`; continuous columns do not. Missing columns are skipped gracefully.

#### `test_serving_feature_client.py`

Tests `FeatureClient`'s fail-closed logic around Feast's `get_online_features`. The `FeatureStore` class itself is fully mocked.

- **Success path** — full Feast response returns `(DataFrame, features_complete=True, features_used_pct=1.0)`; entity row passed to Feast contains correct `origin`, `carrier`, `route_key`.
- **Fail-closed** — returns `None` for a single null feature (expired TTL), all-null response, `ConnectionError` from Redis, or a response that omits a feature key entirely.
- **`ping_redis`** — returns `True` on success, `False` on any exception.

---

## Integration Tests

Integration tests carry `pytestmark = pytest.mark.integration` and are excluded from the default `pytest tests/unit` run in CI.

### Prerequisites (Docker Compose Stack)

Before running integration tests locally you need:

```bash
# minimal: Feast SQLite (self-contained) + feast apply
cd feature_repo && uv run feast apply

# full stack (MinIO, Redis, MLflow, Postgres)
docker compose up -d
```

The integration tests in this repo are designed to be as self-contained as possible. `test_feast_roundtrip.py` uses SQLite as the online store and local Parquet as the offline store — no Redis or MinIO required. `test_leakage_planted_value.py` has no external dependencies at all.

### `test_feast_roundtrip.py`

Three test classes, each testing a different concern:

**`TestFeastApply`** — reads the production registry (`feature_repo/data/registry.db`) and asserts that all five expected feature views (`origin_airport_features`, `dest_airport_features`, `carrier_features`, `route_features`, `aircraft_features`), entities (`origin_airport`, `carrier`, `aircraft_tail`), and the `flight_delay_prediction` feature service are registered. Requires `feast apply` to have been run.

**`TestHistoricalPITJoin`** — builds a self-contained `FeatureStore` backed by local Parquet and SQLite (`_build_local_store` helper), then calls `get_historical_features`. Two flights at ORD are tested: one at 11:30 should see the 10:00 snapshot (`avg_delay=5.0`), one at 15:00 should see the 14:00 snapshot (`avg_delay=18.0`). A second test asserts the two values are different — proving Feast is doing a real PIT join, not a latest-value SELECT.

**`TestOnlineRoundtrip`** — calls `store.materialize()` then `store.get_online_features()` against a SQLite online store. Verifies the written value is returned correctly and that an unknown entity returns `null` (not an exception, not `0`).

See also: [Feast documentation](https://docs.feast.dev/getting-started/concepts/point-in-time-joins).

### `test_leakage_planted_value.py`

**The proof that the pipeline handles data leakage correctly.** Two independent defense layers are tested:

**Mode A — Structural (`TestPITJoinExcludesFutureSnapshot`)**: A feature Parquet contains two ORD snapshots — `13:00` (valid, delay=9.8) and `14:30` (future relative to the 14:00 event, delay=999.0). The 999.0 sentinel value is the leakage signal. Tests assert:
1. The sentinel does not appear in the join result.
2. The correct past value (9.8) does appear — preventing a null-returning join from passing test 1 by accident.

**Mode B — Guard (`TestLeakageGuardCatchesFutureFeatureTs`)**: Manually constructs a dataset with a `feature_ts` column set to a future timestamp, then calls `run_all_guards()`. Asserts the guard returns `passed=False` with a `no_future_features` error. A second test confirms the guard passes when all feature timestamps are in the past.

**`TestTargetLeakageGuard`**: Asserts that including a label column (`dep_delay_min`) in `feature_refs` triggers a `no_target_leakage` error.

#### How the Planted-Value Test Works

The sentinel pattern works because:
- A correct ASOF JOIN returns 9.8 (nearest snapshot *before* 14:00).
- A broken join that returns the nearest snapshot overall would return 999.0 (at 14:30, only 30 min away vs 1 hour for 13:00).
- A broken join that returns the latest snapshot would also return 999.0.

The second test (`test_correct_past_snapshot_selected`) is needed because a join that sets everything to NULL would incorrectly pass the `!= 999.0` assertion.

#### What It Catches That Unit Tests Miss

The unit PIT join tests (`test_pit_join.py`) use our own DuckDB-based `PITJoiner`. This integration test uses Feast's `get_historical_features` — a different code path (Arrow Flight + offline store). A bug in either layer is caught by the respective test, and Mode B (the guard) catches any bug that slips through both.

---

## Determinism Tests

### `test_reproduce_run.py`

Tests `bmo.training.reproduce.reproduce_run()`, which re-trains a model from a stored MLflow run ID and asserts that the resulting model file is byte-identical to the original.

**`test_reproduce_run_byte_equality`**: Trains a model with fixed params (`seed=42`, `nthread=1`, `max_depth=3`, `n_estimators=50`) on a 100-row deterministic fixture, then calls `reproduce_run(run_id)` and asserts it returns `True`.

**`test_reproduce_fails_with_different_seed`** (negative test): Trains two models with `seed=42` and `seed=99` and asserts their SHA-256 hashes differ — verifying the byte-equality check is not trivially always `True`.

### Byte-Equality Assertions

Reproducibility requires:
- **Same `seed`** — passed as an XGBoost param and stored in MLflow.
- **`nthread=1`** — multi-threaded XGBoost is not deterministic across runs due to floating-point addition order. The production HPO run uses `nthread=1` for this reason.
- **Same data** — `reproduce_run` loads the dataset via `DatasetHandle.storage_path` from the MLflow run params.
- **Same XGBoost version** — model bytes are XGBoost-version-specific.

### When to Run

Determinism tests are excluded from every-push CI because they are slow (full train cycle) and their hardware caveat makes them fragile in heterogeneous CI environments. Run them manually or in the nightly job:

```bash
uv run pytest tests/determinism -m determinism -v
```

> **Hardware caveat**: byte equality requires the same OS and CPU architecture. These tests pass reliably within the same Docker image. x86 Linux vs Apple Silicon may produce bit-different BLAS results.

---

## Pytest Markers

Markers are declared in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
  "integration: slower tests requiring docker-compose stack",
  "determinism: model byte-equality tests",
]
```

### `@pytest.mark.integration`

Applied at module level in `test_feast_roundtrip.py` and `test_leakage_planted_value.py` via:

```python
pytestmark = pytest.mark.integration
```

### `@pytest.mark.determinism`

Applied at module level in `test_reproduce_run.py` via:

```python
pytestmark = pytest.mark.determinism
```

### Running a Subset of Tests

```bash
# unit tests only (default CI run)
uv run pytest tests/unit -q

# single test file
uv run pytest tests/unit/test_pit_join.py -v

# single test by name
uv run pytest tests/unit/test_pit_join.py::TestPITJoinerCorrectness::test_ttl_exceeded_features_are_null -v

# integration tests only
uv run pytest -m integration -v

# determinism tests only
uv run pytest -m determinism -v

# everything except integration and determinism
uv run pytest -m "not integration and not determinism"

# with coverage
uv run pytest tests/unit --cov=bmo --cov-report=term-missing
```

---

## CI Test Matrix

From `.github/workflows/ci.yml`:

| Job | Trigger | Command | External services |
|---|---|---|---|
| `lint` | push / PR | `ruff check`, `ruff format --check`, `mypy src dagster_project` | None |
| `test` | push / PR | `pytest tests/unit -q` | None (all mocked) |
| `feast-apply` | push / PR (after lint) | `feast apply` + registry object count check | Redis (GitHub service container) |

The `feast-apply` job uses a GitHub Actions [service container](https://docs.github.com/en/actions/use-cases-and-examples/using-containerized-services/about-service-containers) to spin up Redis 7. It verifies `feast apply` is idempotent on a clean checkout and that all five feature views are registered. Integration and determinism tests are not in CI by default — run them in a nightly workflow or manually before releasing a new model.

---

## Writing New Tests

### Unit Test Checklist

- [ ] Place the file in `tests/unit/test_<module_name>.py`.
- [ ] Keep it fully in-memory — no real S3, Redis, or MLflow server.
- [ ] Mock at the boundary your code controls: monkeypatch internal functions (`_download_zip`, `_fetch_lcd_year`) rather than patching deep into third-party libraries.
- [ ] Use `tmp_path` (pytest built-in) for any local file I/O.
- [ ] If your test needs env vars, add them to `conftest.py` via `os.environ.setdefault` (not inside the test, where import order matters).
- [ ] Use sentinel values to make leakage/correctness bugs obvious in assertion messages.
- [ ] For floating-point comparisons, use `pytest.approx` with a sensible tolerance.

### Integration Test Checklist

- [ ] Place the file in `tests/integration/test_<name>.py`.
- [ ] Add `pytestmark = pytest.mark.integration` at the top of the module.
- [ ] Prefer self-contained setups (SQLite online store, local Parquet) over external service dependencies where possible.
- [ ] Document in the module docstring which external services are required and how to start them.
- [ ] Use `scope='module'` for expensive fixtures like `FeatureStore` construction.

### When to Add a Determinism Test

Add a determinism test when:
- A new training entrypoint or model type is added.
- The reproduce path (`reproduce_run`) is modified.
- A new XGBoost version is adopted (run the test to confirm byte equality still holds on the same architecture).

Do **not** add determinism tests for:
- Feature engineering logic (covered by unit tests).
- Evaluation gate checks (in-memory, no model bytes involved).
- Anything that runs on GPU (byte equality is not guaranteed across GPU firmware versions).
