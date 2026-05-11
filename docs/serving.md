# Serving

## Overview

The serving layer is a [FastAPI](https://fastapi.tiangolo.com/) inference service deployed on [Fly.io](https://fly.io/docs/) with [Upstash Redis](https://upstash.com/docs/redis/overall/getstarted) as the online feature store. It exposes a single prediction endpoint (`POST /predict`) plus a set of dashboard API endpoints consumed by the React frontend.

**Key design decisions:**

- **Fail-closed:** If Feast returns any null feature for a required feature group, the API returns `503` rather than serving a prediction with missing data. The caller retries or falls back to a heuristic. See [Feature Retrieval](#feature-retrieval-at-inference-time).
- **Single worker per machine:** The container runs `uvicorn` with `--workers 1`. Fly.io scales horizontally across machines rather than vertically within a machine. This eliminates the need for shared-memory model state.
- **Hot-swap without downtime:** `POST /admin/reload` re-downloads the `@champion` model from MLflow in a background thread while requests continue to be served by the existing in-memory model. See [Hot-Swap Without Downtime](#hot-swap-without-downtime-adminreload).

---

## FastAPI Endpoints

Interactive OpenAPI docs are available at `/docs` (Swagger UI) and `/redoc` when the server is running.

### `GET /health`

Liveness + readiness probe used by Fly.io health checks. Returns `200` in all cases; callers interpret the response body to determine readiness.

**Response schema:**

| Field | Type | Description |
| --- | --- | --- |
| `status` | `string` | `"health"` \| `"degraded"` — degraded if model or Redis is unavailable |
| `model_loaded` | `bool` | Whether the champion model is loaded in memory |
| `redis_reachable` | `bool` | Whether Feast can reach Upstash Redis |
| `model_version` | `string` | Currently loaded model version, or `"unknown"` |

Redis reachability is checked via a minimal `get_online_features` probe, not a direct Redis `PING`. A `degraded` status does not cause Fly.io to replace the machine — it is informational.

---

### `GET /model-info`

Returns metadata about the currently loaded champion model.

**Response schema:**

| Field | Type | Description |
| --- | --- | --- |
| `model_name` | `string` | MLflow registered model name (e.g. `bmo_flight_delay`) |
| `model_version` | `string` | Numeric version string |
| `champion_alias` | `string` | Always `"champion"` |
| `loaded_at` | `string` | ISO 8601 UTC timestamp of when the model was loaded into memory |
| `registered_at` | `string` | ISO 8601 UTC timestamp from the MLflow registry |
| `training_roc_auc` | `float \| null` | `test_roc_auc` metric from the training MLflow run |
| `feature_service` | `string` | Feast feature service name |
| `shadow_version` | `string \| null` | Active shadow model version if `SHADOW_MODEL_VERSION` is set |

---

### `POST /predict`

Predict departure delay probability for a single flight.

**Query parameters:**

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `explain` | `bool` | `false` | Include SHAP feature attributions in the response |

#### Request Schema

```json
{
  "flight_id": "AA123_20240406_0900",
  "origin": "ORD",
  "dest": "LAX",
  "carrier": "AA",
  "tail_number": "N12345",
  "route_key": "ORD-LAX"
}
```

| Field | Required | Description |
| --- | --- | --- |
| `flight_id` | Yes | Opaque string; echoed back in the response for correlation |
| `origin` | Yes | IATA origin airport code |
| `dest` | Yes | IATA destination airport code |
| `carrier` | Yes | BTS carrier code (e.g. `AA`, `DL`, `UA`) |
| `tail_number` | No | Aircraft tail number. Empty string triggers 0-imputation for aircraft features |
| `route_key` | Yes | Composite key formatted as `"{origin}-{dest}"` (e.g. `"ORD-LAX"`) |

#### Response Schema

```json
{
  "flight_id": "AA123_20240406_0900",
  "predicted_is_delayed": false,
  "delay_probability": 0.3142,
  "model_name": "bmo_flight_delay",
  "model_version": "7",
  "features_complete": true,
  "features_used_pct": 1.0,
  "attributions": null
}
```

| Field | Type | Description |
| --- | --- | --- |
| `flight_id` | `string` | Echoed from request |
| `predicted_is_delayed` | `bool` | `true` when `delay_probability >= 0.5` |
| `delay_probability` | `float [0, 1]` | Raw XGBoost probability output |
| `model_name` | `string` | MLflow registered model name |
| `model_version` | `string` | Model version that produced this prediction |
| `features_complete` | `bool` | `false` if aircraft features were imputed (tail_number unknown) |
| `features_used_pct` | `float [0, 1]` | Fraction of features that were present in Redis |
| `attributions` | `list \| null` | SHAP attributions — only present when `?explain=true` |

When `?explain=true`, each attribution object has:

```json
{
  "feature": "origin_pct_delayed_1h",
  "shap_value": 0.2341,
  "feature_value": 0.45
}
```

`shap_value` is in log-odds space. Attributions are sorted by `|shap_value|` descending. SHAP docs: [shap.readthedocs.io](https://shap.readthedocs.io/en/latest/).

#### Feature Lookup Flow

```
POST /predict
    │
    ├─► FeatureClient.get_features(request)
    │       │
    │       └─► Feast.get_online_features(24 feature refs, entity_row)
    │               │
    │               └─► Upstash Redis (online store)
    │
    ├─► null check → 503 if any non-aircraft feature is None
    │
    ├─► ModelLoader.predict(feature_df)     ← XGBoost Booster.inplace_predict
    │
    ├─► [optional] SHAP TreeExplainer(feature_df)
    │
    └─► BackgroundTask: shadow model inference (if SHADOW_MODEL_VERSION set)
```

#### Latency Budget

| Stage | Target p50 | Target p99 |
| --- | --- | --- |
| Feast → Redis round-trip | < 10 ms | < 50 ms |
| XGBoost inference | < 2 ms | < 10 ms |
| End-to-end (`/predict`) | < 50 ms | < 250 ms |

Prometheus histograms `bmo_predict_latency_seconds` and `bmo_feature_retrieval_latency_seconds` track these in production.

#### Error Responses

| Status | Condition |
|---|---|
| `422` | Request body fails Pydantic validation |
| `503` | Model not loaded, feature client not initialized, or any non-aircraft Feast feature is null |
| `500` | Unhandled exception (logged; generic message returned) |

---

### `GET /model-info`

See [above](#get-model-info).

---

### `POST /admin/reload`

Hot-swap the in-memory champion model without restarting the container.

**Authentication:** `Authorization: Bearer <ADMIN_TOKEN>` header required. If `ADMIN_TOKEN` env var is empty, the check is skipped (dev mode only).

**Trigger:** Called automatically by the `deployed_api` Dagster asset when `SERVING_API_URL` and `ADMIN_TOKEN` are set. Can also be called manually.

**Response:**

```json
{
  "status": "reloaded",
  "model_version": "8"
}
```

The reload acquires an `asyncio.Lock` to prevent concurrent predict and reload. In-flight predict requests that were already running are not interrupted — they complete against the old model. New requests after the lock is released use the new model.

---

### `GET /metrics`

Prometheus scrape endpoint. Returns all metrics in the [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/). Scraped by Grafana Cloud.

See [Prometheus Metrics Exposed](#prometheus-metrics-exposed) for the full metric list.

---

## Dashboard API Endpoints

These endpoints are consumed by the React frontend. They read from two data sources:

- **Postgres** — `drift_metrics` and `live_accuracy` monitoring tables (written by Dagster)
- **DuckDB / S3** — `mart_predictions` view over `s3://staging/predictions/**/data.parquet` (written by the `batch_predictions` Dagster asset)

DuckDB connections are opened read-only per request (DuckDB holds a file lock; read-only connections can coexist with Dagster's write connection). All DuckDB queries run in `asyncio.to_thread` to avoid blocking the FastAPI event loop.

If the local DuckDB file does not exist (e.g. dbt hasn't run yet), DuckDB falls back to querying S3 directly via the `httpfs` extension. The S3 fallback stubs `actual_is_delayed` as `NULL`.

If a Postgres monitoring table doesn't exist yet, queries return empty lists rather than crashing — safe during initial setup before any drift reports have run.

---

### `GET /api/drift/summary`

Latest PSI snapshot across all tracked features.

**Response:**

| Field | Type | Description |
| --- | --- | --- |
| `report_date` | `string` | Most recent `report_date` in `drift_metrics` |
| `psi_breaches` | `int` | Features with `is_breached = true` on that date |
| `n_features` | `int` | Total features tracked |
| `max_psi` | `float` | Highest PSI score across all features |
| `model_version` | `string \| null` | Model version the report was computed against |
| `features` | `list` | Per-feature summary: `name`, `psi`, `severity` (`"green"` / `"amber"` / `"red"`) |

Severity thresholds: `psi >= 0.2` → `red` (breached); `psi >= 0.1` → `amber`; below → `green`.

---

### `GET /api/drift/metrics`

Time series of PSI for all features. Defaults to the 30 most recent report dates when no range is provided.

**Query parameters:** `start` (date), `end` (date)

**Response:** `rows` — list of `DriftMetricRow`:

| Field | Type |
|---|---|
| `report_date` | `string` |
| `feature_name` | `string` |
| `psi_score` | `float` |
| `kl_divergence` | `float \| null` |
| `rank` | `int` (1 = most important) |
| `is_breached` | `bool` |
| `model_version` | `string \| null` |

---

### `GET /api/model-stats`

Aggregated performance statistics across all model versions (or just the champion).

**Query parameters:** `champion` (bool, default `false`) — when `true`, filters to the champion alias only.

**Response:** `rows` — list of `ModelRow` aggregated per `model_version`:

`avg_roc_auc`, `avg_accuracy`, `avg_precision_score`, `avg_recall_score`, `avg_f1`, `avg_log_loss`, `avg_brier_score`, `avg_positive_rate`, `avg_actual_positive_rate`, `avg_n_flights_scored`, `total_n_flights`, `last_scored`.

Source table: `live_accuracy` (Postgres).

---

### `GET /api/psi/{feature_name}`

Per-feature PSI time series across all report dates.

**Path parameter:** `feature_name` — must match a value in `drift_metrics.feature_name`.

**Response:** `rows` — list of `PsiRow`: `report_date`, `psi_score`, `kl_divergence`, `is_breached`.

---

### `GET /api/accuracy`

Live accuracy time series for the last 90 days.

**Response:** `rows` — list of `AccuracyRow`:

| Field | Description |
|---|---|
| `score_date` | Date predictions were scored |
| `model_version` | Model version |
| `roc_auc` | Area under ROC curve |
| `f1` | F1 score |
| `precision_score` | Precision |
| `recall_score` | Recall |
| `brier_score` | Brier score (lower = better calibration) |
| `positive_rate` | Predicted delay rate |
| `actual_positive_rate` | Actual delay rate (from BTS actuals) |
| `n_with_actuals` | Flights with ground-truth labels |

BTS actuals are published with a ~60-day lag, so `n_with_actuals` will be 0 for recent dates.

---

### `GET /api/predictions`

Daily prediction volume and delay rate from the `mart_predictions` DuckDB table.

**Query parameters:** `days` (int, default `30`)

**Response:** `rows` — list of `PredictionRow`:

| Field | Description |
|---|---|
| `score_date` | Date |
| `model_version` | Model version |
| `n_flights` | Flights scored |
| `avg_proba` | Average `predicted_delay_proba` |
| `positive_rate` | Fraction predicted delayed |
| `n_with_actuals` | Flights with ground-truth labels |

---

### `GET /api/predictions/today`

Summary card for the most recent scoring run. "Today" means the most recent `score_date` in `mart_predictions`.

**Response:**

| Field | Description |
|---|---|
| `model_version` | Currently loaded model version |
| `model_loaded_at` | ISO timestamp |
| `registered_at` | ISO timestamp of champion registration |
| `n_flights_today` | Flights scored on the most recent date |
| `positive_rate_today` | Fraction predicted delayed |
| `days_since_retrain` | Days since current champion was registered |
| `data_as_of` | Most recent `score_date` string |

---

### `GET /api/routes/history`

On-time performance (OTP) % for a route over the past N days.

**Query parameters:** `origin` (str, 3-letter IATA), `dest` (str, 3-letter IATA), `days` (int, default `14`)

**Response:**

| Field | Description |
|---|---|
| `route_key` | `"{ORIGIN}-{DEST}"` |
| `history` | List of daily OTP integers (%) oldest → newest |
| `days` | Window requested |
| `data_as_of` | Most recent `score_date` in the result |

---

### `GET /api/carriers/comparison`

Average OTP and estimated delay by carrier for a given route.

**Query parameters:** `origin`, `dest`, `days` (default `30`)

**Response:** `carriers` — list of `CarrierPerformance`: `carrier`, `otp` (0–1), `avg_delay` (minutes estimated from delay probability × 60).

---

### `GET /api/network`

Airport-level OTP and average delay for the busiest origins over the past N days.

**Query parameters:** `days` (int, default `7`), `limit` (int, default `12`)

**Response:** `rows` — list of `OriginPerformance`: `origin`, `otp`, `avg_delay_min`, `status_indicator` (`"green"` / `"amber"` / `"red"`).

---

### `GET /api/flights/sample`

Sample flight predictions for the React landing page. Returns one representative flight per major carrier from the most-frequent route in the last 30 days.

**Query parameters:** `limit` (int, default `4`)

**Response:** list of `FlightSample`: `flight_id`, `carrier`, `origin`, `dest`, `scheduled_departure_utc`, `onTimeProb`, `tail_number`.

---

### `GET /api/routes/carrier-history`

Daily delay probability and actuals for a specific carrier on a specific route (last 30 days).

**Query parameters:** `origin`, `dest`, `carrier`

**Response:** `CarrierHistory`: `route_key`, `carrier`, `rows` (list of `CarrierRouteDay`), `data_as_of`.

Each `CarrierRouteDay`: `score_date`, `avg_delay_proba`, `avg_actual_delay_min` (null if actuals not yet available), `n_flights`.

---

## Feature Retrieval at Inference Time

### Redis (Upstash) Online Store

Feast materializes feature values into Upstash Redis on an hourly schedule (`feast_hourly` Dagster schedule). Redis holds the most recent computed values for each entity (origin, dest, carrier, route_key, tail_number).

If Redis contains no record younger than the FeatureView's TTL (e.g. 26 hours for airport features), Feast returns `None` for that feature. The `FeatureClient` treats both "missing" and "expired" as equivalent null conditions.

Upstash docs: [upstash.com/docs/redis](https://upstash.com/docs/redis/overall/getstarted)
Feast online retrieval docs: [docs.feast.dev — get-online-features](https://docs.feast.dev/reference/feature-retrieval#get-online-features)

### Feature Client ([`feature_client.py`](../src/bmo/serving/feature_client.py))

`FeatureClient` wraps `FeatureStore.get_online_features()` and enforces fail-closed semantics. The 24 feature references retrieved per request span 5 feature views:

| Feature view | Entity key | Features |
| --- | --- | --- |
| `origin_airport_features` | `origin` | `origin_flight_count_1h`, `origin_avg_dep_delay_1h`, `origin_pct_delayed_1h`, `origin_avg_dep_delay_24h`, `origin_pct_cancelled_24h`, `origin_avg_dep_delay_7d`, `origin_pct_delayed_7d`, `origin_congestion_score_1h` |
| `dest_airport_features` | `dest` | `dest_avg_arr_delay_1h`, `dest_pct_delayed_1h`, `dest_avg_arr_delay_24h`, `dest_pct_diverted_24h` |
| `carrier_features` | `carrier` | `carrier_on_time_pct_7d`, `carrier_cancellation_rate_7d`, `carrier_avg_delay_7d`, `carrier_flight_count_7d` |
| `route_features` | `route_key` | `route_avg_dep_delay_7d`, `route_avg_arr_delay_7d`, `route_pct_delayed_7d`, `route_cancellation_rate_7d`, `route_avg_elapsed_7d`, `route_distance_mi` |
| `aircraft_features` | `tail_number` | `cascading_delay_min`, `turnaround_min` |

Feature column order matches the training feature matrix exactly. Changing order without retraining corrupts predictions.

### Handling Missing / Stale Features

| Feature type | Null behavior |
|---|---|
| Any non-aircraft feature | Fail-closed: return `None` → API returns `503` |
| `cascading_delay_min`, `turnaround_min` | Soft-null: imputed as `0.0`; `features_complete = false` in response |

When `tail_number` is omitted or empty, aircraft features are always imputed as `0.0`, matching the `fillna(0)` used in batch scoring.

---

## Model Loading

### [`model_loader.py`](../src/bmo/serving/model_loader.py)

`ModelLoader` wraps the MLflow `pyfunc` API and adds:

1. **Async I/O isolation** — `mlflow.pyfunc.load_model()` is blocking I/O (downloads model artifact from MinIO/R2). It runs in `asyncio.to_thread` so the event loop is never blocked during model download.
2. **Thread-safe hot-swap** — `asyncio.Lock` prevents concurrent `reload()` and `predict()`. The lock is held only during the MLflow download; concurrent predict calls proceed without queuing.
3. **SHAP explainer** — `shap.TreeExplainer` is initialized alongside the model using `mlflow.xgboost.load_model()` to get the raw `Booster`. This is used by `?explain=true` on `/predict`.

### Loading Champion Model from MLflow Registry

On startup, `ModelLoader.load()` calls:

```python
client.get_model_version_by_alias(model_name, 'champion')
mlflow.pyfunc.load_model(f'models:/{model_name}@champion')
mlflow.xgboost.load_model(model_uri)   # raw Booster for SHAP
```

The `registered_at` timestamp and `test_roc_auc` metric are fetched from the MLflow run record and exposed via `GET /model-info`.

If no champion alias exists at startup, the server starts in degraded mode and logs a warning. `/predict` returns `503` until a model is loaded (via `/admin/reload`).

MLflow model registry docs: [mlflow.org/docs/latest/model-registry.html](https://mlflow.org/docs/latest/model-registry.html)

### Hot-Swap Without Downtime (`/admin/reload`)

Hot-swap workflow when a new champion is registered:

1. `registered_model` Dagster asset materializes
2. `deployed_api` Dagster asset writes `model_config.json` to `s3://staging/serving/model_config.json`
3. If `SERVING_API_URL` and `ADMIN_TOKEN` are set, `deployed_api` calls `POST /admin/reload` automatically
4. `ModelLoader.reload()` acquires the lock, downloads the new model from MLflow, and atomically replaces `self._model`
5. All new requests use the new model; no container restart required

---

## Shadow Deploy

Set `SHADOW_MODEL_VERSION=<registry_version>` to run a second model on every request without affecting latency.

The primary response is returned immediately. Shadow inference runs in a FastAPI `BackgroundTask` (after the response is sent). Shadow predictions are never returned to callers — they are logged to stdout as structured JSON:

```json
{
  "flight_id": "AA123_20240406_0900",
  "primary_version": "7",
  "shadow_version": "5",
  "primary_proba": 0.3142,
  "shadow_proba": 0.2891,
  "primary_is_delayed": false,
  "shadow_is_delayed": false,
  "agreed": true
}
```

To find disagreements in a log aggregator:

```sql
SELECT * FROM shadow_predictions WHERE agreed = false ORDER BY scored_at DESC
```

Shadow failures are silently swallowed — they can never fail the primary response.

---

## Deployment on Fly.io

The serving container runs on [Fly.io](https://fly.io/docs/). Fly.io provides:
- Regional anycast routing
- HTTP health check integration
- Horizontal scaling (multiple machines per region)

### Auto-Scaling Settings

The container runs `uvicorn` with `--workers 1`. Fly.io scales out by adding machines (horizontal), not by adding workers within a machine (vertical). Single-worker mode simplifies in-memory model state — no shared memory or inter-process synchronization required.

### Health Check Integration

Fly.io polls `GET /health` periodically. The endpoint returns `200` in all cases. If the JSON body shows `status: "degraded"`, Fly.io does not automatically replace the machine — the machine is still reachable but serving degraded. Operators should monitor the `model_loaded` and `redis_reachable` fields via Grafana.

### Building and Running the Container

```bash
# Build
make serving-build   # docker build -f infra/docker/serving.Dockerfile -t bmo-serving:local .

# Run locally (requires .env)
make serving-run
```

The image uses a two-stage build (builder + runtime). Only the `serving` and `iceberg` dependency groups are installed — no Dagster, no Spark, no training deps. This keeps the image small enough for Fly.io's 512 MB RAM VMs.

### Environment Variables at Runtime

| Variable | Required | Description |
| --- | --- | --- |
| `MLFLOW_TRACKING_URI` | Yes | MLflow server URL |
| `MODEL_NAME` | No (default: `bmo_flight_delay`) | Registered model name |
| `ADMIN_TOKEN` | Yes (prod) | Bearer token for `POST /admin/reload`; empty disables the check |
| `SHADOW_MODEL_VERSION` | No | Registry version number to run as shadow model |
| `CORS_ORIGIN_DEV` | No | Additional CORS origin for local frontend development |
| `SERVING_API_URL` | No | Used by `deployed_api` asset to auto-call `/admin/reload` |
| `REDIS_URL` | Yes | Upstash Redis connection string (used by Feast) |
| `S3_ENDPOINT_URL` | Yes | MinIO / R2 endpoint |
| `S3_ACCESS_KEY_ID` | Yes | Object store key |
| `S3_SECRET_ACCESS_KEY` | Yes | Object store secret |
| `POSTGRES_HOST` / `_PORT` / `_DB` / `_USER` / `_PASSWORD` | Yes | Monitoring Postgres |
| `DUCKDB_PATH` | No (default: `/tmp/bmo_features.duckdb`) | Path to local DuckDB file |
| `DUCKDB_S3_ENDPOINT` | No | Overrides S3 endpoint for DuckDB (host:port, no scheme) |

CORS is handled by Caddy (reverse proxy) in addition to FastAPI's `CORSMiddleware`. The allowed origins are `https://ml-training-orchestrator.vercel.app` plus any `CORS_ORIGIN_DEV` override and any `https://ml-training-orchestrator*.vercel.app` preview deployments.

---

## Observability

### Prometheus Metrics Exposed

All metrics are available at `GET /metrics` in Prometheus exposition format.

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `bmo_predict_requests_total` | Counter | `model_version`, `features_complete` | Total prediction requests |
| `bmo_predict_latency_seconds` | Histogram | `model_version` | End-to-end prediction latency. Buckets: 5 ms → 2.5 s |
| `bmo_feature_retrieval_latency_seconds` | Histogram | — | Feast online store round-trip latency. Buckets: 1 ms → 250 ms |
| `bmo_fail_closed_total` | Counter | — | Requests that returned `503` due to missing/stale features |
| `bmo_model` | Info | `model_name`, `version` | Static labels for the currently loaded model |

Prometheus client docs: [github.com/prometheus/client_python](https://github.com/prometheus/client_python)

### Logging Format

Structured JSON logging via [structlog](https://www.structlog.org/en/stable/). Each log line is a JSON object. Key log events:

| Event | Level | Key fields |
| --- | --- | --- |
| Server startup | `info` | `model_name`, `mlflow_uri` |
| Model loaded | `info` | `version`, `training_roc_auc` |
| Null features — fail closed | `warning` | `flight_id`, `origin`, `null_features` |
| Aircraft features imputed | `info` | `flight_id`, `null_features` |
| Shadow prediction | `info` | All `ShadowPrediction` fields |
| Hot-swap complete | `info` | `new_version` |
| Postgres table missing | `warning` | `error` |
| Unhandled exception | `error` | `path`, `method` |

In production, Fly.io ships stdout logs to a log aggregator (e.g. Papertrail, Loki). Shadow disagreements can be queried as shown in [Shadow Deploy](#shadow-deploy).

---

## Local Development

### Running `make serving-dev`

```bash
make serving-dev
# → uv run uvicorn bmo.serving.api:app --reload --port 8080
```

Requires a `.env` file with all environment variables listed above. The `--reload` flag restarts the server on code changes.

OpenAPI docs: [http://localhost:8080/docs](http://localhost:8080/docs)

### Testing the `/predict` Endpoint Locally

```bash
curl -X POST http://localhost:8080/predict \
  -H 'Content-Type: application/json' \
  -d '{
    "flight_id": "AA123_20240406_0900",
    "origin": "ORD",
    "dest": "LAX",
    "carrier": "AA",
    "tail_number": "",
    "route_key": "ORD-LAX"
  }'
```

With SHAP attributions:

```bash
curl -X POST "http://localhost:8080/predict?explain=true" \
  -H 'Content-Type: application/json' \
  -d '{"flight_id": "AA123_20240406_0900", "origin": "ORD", "dest": "LAX", "carrier": "AA", "tail_number": "", "route_key": "ORD-LAX"}'
```

To test the hot-swap endpoint:

```bash
curl -X POST http://localhost:8080/admin/reload \
  -H 'Authorization: Bearer <ADMIN_TOKEN>'
```

Unit tests for the feature client: `make test-serving`

```bash
uv run pytest tests/unit/test_serving_feature_client.py -q
```
