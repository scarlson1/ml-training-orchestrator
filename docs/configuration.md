# Configuration Reference

## Overview

All runtime configuration is managed through environment variables, read at import time by `src/bmo/common/config.py` using `pydantic-settings`. A single `Settings` instance (`settings`) is constructed once and shared across the entire process. Any missing required variable raises a `ValidationError` before the application starts (fail-fast at import time).

Local development uses a `.env` file at the project root. Production uses a `.env` file on the Oracle VM populated by Terraform outputs (Cloudflare R2 creds, Upstash Redis URL).

---

## Environment Variables

### Storage (S3 / MinIO / Cloudflare R2)

All three object stores use the same boto3 API. `S3_ENDPOINT_URL` is what redirects requests away from AWS.

| Variable | Required | Default | Description |
|---|---|---|---|
| `S3_ENDPOINT_URL` | Yes | — | Full URL including scheme. `http://localhost:9000` (MinIO) or `https://<account>.r2.cloudflarestorage.com` (R2). |
| `AWS_ACCESS_KEY_ID` | Yes | — | Access key. Also accepted as `S3_ACCESS_KEY_ID` (pydantic `AliasChoices`). |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | Secret key. Also accepted as `S3_SECRET_ACCESS_KEY`. |
| `AWS_DEFAULT_REGION` | No | `us-east-1` | Region. Also accepted as `AWS_REGION` or `S3_REGION`. |
| `AWS_ENDPOINT_URL` | Yes | — | Picked up automatically by boto3 and Feast. Mirror of `S3_ENDPOINT_URL`. |
| `S3_BUCKET_RAW` | No | `raw` | Bucket for raw ingestion data. |
| `S3_BUCKET_STAGING` | No | `staging` | Bucket for dbt outputs, features, predictions, monitoring. |
| `S3_BUCKET_REJECTED` | No | `rejected` | Bucket for records that fail validation. |
| `FEAST_S3_BASE` | No | `s3://staging/feast` | Base S3 path for Feast feature parquet and registry. |
| `DATASET_S3_BASE` | No | `s3://staging/datasets` | Base S3 path for training dataset parquet. |

**S3 path layout (inside `staging`):**

```text
s3://staging/
  feast/          # Feast feature parquet + registry (FEAST_S3_BASE)
  datasets/       # PIT-correct training datasets (DATASET_S3_BASE)
  predictions/    # Batch scoring outputs (daily partitions)
  monitoring/
    reports/      # Evidently HTML reports (synced to GitHub Pages by CI)
    metrics/      # PSI/KL divergence parquet (consumed by mart_drift_metrics dbt model)
```

---

### PostgreSQL

Dagster, MLflow, and the Iceberg catalog all share one Postgres instance. Dagster uses a `dagster` database; the application uses `bmo`; Iceberg metadata uses an `iceberg` database. All three are created by `infra/postgres/init.sql`.

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_HOST` | Yes | — | Hostname. `localhost` in dev; `postgres` (Docker service name) in prod. |
| `POSTGRES_PORT` | Yes | — | Port, typically `5432`. |
| `POSTGRES_DB` | Yes | — | Application database name, e.g. `bmo`. |
| `POSTGRES_USER` | Yes | — | Database user. |
| `POSTGRES_PASSWORD` | Yes | — | Database password. |
| `ICEBERG_CATALOG_URI` | No | Derived | SQLAlchemy URI for the Iceberg catalog. If unset, `config.py` builds it from `POSTGRES_*` as `postgresql+psycopg2://<user>:<password>@<host>:<port>/iceberg`. |

`settings.postgres_url` and `settings.iceberg_catalog_uri` URL-encode user/password/host/port to handle special characters.

In production Postgres is bound to `127.0.0.1:5432` (loopback only). Dagster and MLflow connect via the Docker service name `postgres` on the shared network.

---

### MLflow

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `MLFLOW_TRACKING_URI` | Yes | — | Tracking server URI. `http://localhost:5000` (dev); `http://mlflow:5000` (prod, overridden in compose). |
| `MLFLOW_AUTH_ADMIN_USERNAME` | Prod only | — | Admin username for MLflow basic auth. |
| `MLFLOW_AUTH_ADMIN_PASSWORD` | Prod only | — | Admin password for MLflow basic auth. |
| `MLFLOW_TRACKING_USERNAME` | Prod only | — | Picked up automatically by the MLflow client once basic auth is enabled. Set to the same value as `MLFLOW_AUTH_ADMIN_USERNAME`. |
| `MLFLOW_TRACKING_PASSWORD` | Prod only | — | Picked up automatically by the MLflow client. |
| `MLFLOW_FLASK_SERVER_SECRET_KEY` | Prod only | — | Random string for Flask session signing (required for CSRF protection). |
| `MLFLOW_DOMAIN` | Prod only | — | Public subdomain for the MLflow UI, e.g. `mlflow.<IP>.sslip.io`. Caddy proxies this to `mlflow:5000`. |
| `MODEL_NAME` | No | `bmo_flight_delay` | MLflow registered model name, used by serving and training assets. |

Admin credentials are set by environment variables (in GitHub Secrets). To enable basic auth:

1. Add `--app-name basic-auth` to the MLflow server command in compose (already present in `compose.prod.yml`).
2. Add env vars: `MLFLOW_TRACKING_USERNAME` and `MLFLOW_TRACKING_PASSWORD` are required by any container connecting to MLflow (the MLflow client picks up these exact names automatically).
3. Set `MLFLOW_FLASK_SERVER_SECRET_KEY` for CSRF protection.
4. Create `infra/docker/basic_auth.ini` to configure the auth SQLite DB path and default permissions (already present in repo).
5. Set `MLFLOW_AUTH_CONFIG_PATH: /mlflow_auth/basic_auth.ini` in the compose `environment:` block.
6. Mount a named Docker volume at the auth DB path so credentials persist across container restarts.

`infra/docker/basic_auth.ini` sets `default_permission = READ`, so any created user is read-only by default — no extra permission grant needed.

Read-only credentials can be created after deployment:

```bash
curl -X POST https://<mlflow-domain>/api/2.0/mlflow/users/create \
  -u "admin:<admin-password>" \
  -H "Content-Type: application/json" \
  -d '{"username": "demo", "password": "<demo-password>"}'
```

[MLflow basic auth docs](https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/)

### Feast / Redis

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `REDIS_URL` | Yes | — | Redis connection string. `localhost:6379` (dev); `<id>.upstash.io:6379,ssl=true,password=<pw>` (prod via Upstash). |
| `FEAST_REGISTRY_PATH` | Yes | — | Path to the Feast registry. `data/registry.db` (dev, local SQLite); `s3://staging/feast/registry.db` (prod, shared between Dagster writer and serving reader). |
| `FEATURE_REPO_DIR` | No | `feature_repo/` | Path to the directory containing `feature_store.yaml`. Used by the `FeastResource` Dagster resource and the serving `FeatureClient`. |

`feature_repo/feature_store.yaml` reads `FEAST_REGISTRY_PATH` and `REDIS_URL` at `feast apply` / `feast materialize` time.

In production Redis is Upstash (managed, TLS). The local dev Redis container is not included in `compose.prod.yml`.

---

### Dagster

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DAGSTER_HOME` | Yes | — | Absolute path to the `dagster_home/` directory. Contains `dagster.yaml`, the DuckDB file, and the DuckDB spill directory. In prod this is a named Docker volume mounted at `/dagster_home`. |
| `DAGSTER_HPO_N_TRIALS` | No | `50` | Number of Optuna trials for the `trained_model` asset HPO sweep. Set to `5` for fast local runs: `DAGSTER_HPO_N_TRIALS=5 make dagster-dev`. |

`dagster_home/dagster.yaml` configures:

- **Storage**: PostgreSQL (reads `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST` from environment; always uses database name `dagster`).
- **Run coordinator**: `QueuedRunCoordinator` with `max_concurrent_runs: 2`.
- **Concurrency**: `default_op_concurrency_limit: 1`.

---

### DuckDB / dbt

DuckDB is the query engine for dbt feature models and the training dataset builder. Its S3 config is session-level (`SET s3_endpoint = '...'`), not global.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DUCKDB_S3_ENDPOINT` | Yes | — | S3 endpoint as `HOST:PORT` — **no scheme**. DuckDB's `httpfs` requires this format. `localhost:9000` (MinIO dev); `<account>.r2.cloudflarestorage.com` (R2 prod). |
| `DUCKDB_PATH` | No | `/tmp/bmo_features.duckdb` | Path to the DuckDB file. In prod: `/dagster_home/bmo_features.duckdb` (set in compose, shared named volume between Dagster and serving). |
| `S3_USE_SSL` | No | `false` | Set to `true` for endpoints requiring TLS (R2 in prod). |

`settings.s3_endpoint` is a computed property that strips `http://` or `https://` from `S3_ENDPOINT_URL` for use by `DuckDBResource`.

The `training_dataset` asset sets session limits at runtime:

```python
SET memory_limit = '4GB'
SET temp_directory = '/dagster_home/duckdb_spill'
```

---

### FastAPI Serving

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `ADMIN_TOKEN` | No | `''` | Bearer token for `POST /admin/reload`. Empty string disables the check (dev mode). |
| `SERVING_API_URL` | No | — | Public URL of the serving API. `http://localhost:8000` (dev); `https://<SERVING_DOMAIN>` (prod). Used by Dagster's `deployed_api` asset to verify the hot-swap after champion promotion. |
| `SERVING_DOMAIN` | Prod only | — | Public domain for the serving API. Caddy obtains and renews a Let's Encrypt TLS cert for this domain automatically. |
| `CORS_ORIGIN_DEV` | Dev only | — | Additional CORS origin appended to the hardcoded allowlist. E.g. `http://localhost:5433`. Omit in production. |
| `SHADOW_MODEL_VERSION` | No | — | MLflow registry version number to run as a shadow model, e.g. `5`. Every `/predict` request also runs the shadow model as a `BackgroundTask` and emits a structured `shadow_prediction` JSON log. Zero latency impact on the primary response. |

**CORS policy** (enforced in both FastAPI middleware and Caddy):

- Allowed origins: `https://ml-training-orchestrator.vercel.app` and `https://ml-training-orchestrator-*.vercel.app` (regex)
- Allowed methods: `GET`, `POST`
- Allowed headers: `Content-Type`, `Authorization`

---

### PySpark

PySpark jobs live in `src/bmo/pyspark_jobs/`. They are not scheduled by Dagster in the default configuration but can be submitted manually or via the `dagster-pyspark` integration.

There are no dedicated `SPARK_*` environment variables. The Spark session is configured in `src/bmo/pyspark_jobs/session.py`. Credentials for S3 access are read from the standard `AWS_*` env vars through Hadoop's S3A connector.

---

### General / Notifications

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DISCORD_WEBHOOK_URL` | No | — | Discord webhook URL. When set, the `run_failure_sensor` posts an embed on any Dagster run failure. If unset, the sensor silently no-ops. |
| `CADDY_EMAIL` | Prod only | — | Email sent to Let's Encrypt for TLS certificate expiry warnings. |
| `DAGSTER_USER` | Prod only | — | Username for Caddy HTTP basic auth on the Dagster UI. |
| `DAGSTER_HASHED_PASSWORD` | Prod only | — | bcrypt hash of the Dagster UI password. Generate: `docker run --rm caddy:alpine caddy hash-password --plaintext 'yourpassword'` |
| `GHCR_OWNER` | Prod only | — | GitHub username. The serving container is pulled from `ghcr.io/<GHCR_OWNER>/bmo-serving:latest`. Built and pushed by CI on merge to `main`. |

To create a Discord webhook: **Server Settings → Integrations → Webhooks → New Webhook → Copy URL**.

---

## Config Validation

### How `src/bmo/common/config.py` Works

`Settings` extends `pydantic_settings.BaseSettings`:

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')
    ...

settings = Settings()
```

Field aliases allow both naming conventions to work. For example, `s3_access_key_id` accepts either `S3_ACCESS_KEY_ID` or `AWS_ACCESS_KEY_ID`:

```python
s3_access_key_id: str = Field(
    validation_alias=AliasChoices('s3_access_key_id', 'aws_access_key_id')
)
```

### Fail-Fast at Import Time

`settings = Settings()` is a module-level call. Any process that imports from `bmo.common.config` (Dagster, serving, sensors) raises `ValidationError` immediately if a required field is missing — before any requests are served or any runs start.

### Computed Properties

| Property | Formula |
| --- | --- |
| `settings.s3_endpoint` | `S3_ENDPOINT_URL` with `http://` or `https://` stripped. Used by `DuckDBResource` (requires `HOST:PORT` only). |
| `settings.postgres_url` | `postgresql+psycopg2://<user>:<password>@<host>:<port>/<db>` (URL-encoded). |
| `settings.iceberg_catalog_uri` | `ICEBERG_CATALOG_URI` if set; otherwise `postgresql+psycopg2://<user>:<password>@<host>:<port>/iceberg`. |

### Environment-Specific Overrides

In `compose.prod.yml`, service-level `environment:` blocks override the `localhost` defaults from `.env`:

```yaml
dagster:
  env_file: ../../.env        # loads POSTGRES_HOST=localhost, etc.
  environment:
    POSTGRES_HOST: postgres   # overrides to Docker service name
    MLFLOW_TRACKING_URI: http://mlflow:5000
    DUCKDB_S3_ENDPOINT: <account>.r2.cloudflarestorage.com
    DAGSTER_HOME: /dagster_home
    DUCKDB_PATH: /dagster_home/bmo_features.duckdb
```

This pattern means the same `.env` file works for both local `dagster dev` and the production Docker stack.

---

## Local vs. Production Differences

| Concern | Local (dev) | Production (Oracle VM) |
| --- | --- | --- |
| Object store | MinIO (`http://localhost:9000`) | Cloudflare R2 (`https://<account>.r2.cloudflarestorage.com`) |
| Redis | Local Redis container (`localhost:6379`) | Upstash managed Redis (`<id>.upstash.io:6379,ssl=true,...`) |
| Feast registry | SQLite file (`data/registry.db`) | S3 path (`s3://staging/feast/registry.db`) |
| MLflow auth | None (open) | Basic auth (`--app-name basic-auth`) |
| Dagster UI auth | None | Caddy HTTP basic auth |
| DuckDB S3 endpoint | `localhost:9000` | `<account>.r2.cloudflarestorage.com` |
| DuckDB path | `/tmp/bmo_features.duckdb` | `/dagster_home/bmo_features.duckdb` (named volume, shared with serving) |
| Postgres binding | `0.0.0.0:5432` | `127.0.0.1:5432` (loopback only) |
| TLS | None | Caddy + Let's Encrypt (auto-renewed) |

---

## Secrets Management

### Local (`.env`)

Copy `.env.example` to `.env` and fill in values. The `.env` file is `.gitignore`d. `pydantic-settings` loads it automatically from the project root.

### Production (GitHub Secrets → Oracle VM)

Secrets are stored in GitHub Actions secrets and written to the VM's `.env` file during the deploy workflow. Cloudflare R2 credentials and the Upstash Redis connection string are output by Terraform and injected at deploy time.

The production `.env` on the VM is loaded by Docker Compose via `env_file: - ../../.env`. Service-level `environment:` blocks in `compose.prod.yml` override specific values so Docker service names are used instead of the `localhost` defaults.

---

## Resource Allocation

### Memory per Container (production)

| Container | `mem_limit` | Notes |
| --- | --- | --- |
| `postgres` | 256 MB | Metadata only |
| `mlflow` | 1 GB | 2 workers; no model artifacts in RAM |
| `dagster` | 4.5 GB | DuckDB ASOF JOIN + XGBoost HPO peak |
| `serving` | 1 GB | Champion model + Feast client |
| `caddy` | 256 MB | TLS + reverse proxy |

### Dagster Concurrency (`dagster_home/dagster.yaml`)

```yaml
run_coordinator:
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 2

concurrency:
  default_op_concurrency_limit: 1
```

### S3 Storage per monthly/daily partition

Partitions are **daily** for batch predictions and drift reports, **monthly** for BTS raw flight data.

| Path | Partition | Notes |
| --- | --- | --- |
| `s3://staging/predictions/**/data.parquet` | Daily (`score_date=YYYY-MM-DD`) | Read by DuckDB mart |
| `s3://staging/monitoring/reports/date=YYYY-MM-DD/` | Daily | Synced to GitHub Pages by CI |
| `s3://staging/monitoring/metrics/date=YYYY-MM-DD/` | Daily | Consumed by `mart_drift_metrics` dbt model |
| `s3://raw/bts/year=YYYY/month=MM/` | Monthly | Raw BTS on-time performance CSVs |
| `s3://staging/datasets/` | Content-addressed (SHA hash) | `skip_if_exists=True`; stable across reruns |
| `s3://mlflow-artifacts/` | Per MLflow run | Model artifacts |

### Timeouts & Concurrency Settings

| Schedule | Cron (UTC) | Assets | Default status |
| --- | --- | --- | --- |
| `feast_hourly_materialization` | `0 * * * *` | `feast_materialized_features` | Running |
| `nightly_retrain_schedule` | `0 1 * * *` | `training_dataset → trained_model → registered_model` | Stopped |
| `daily_batch_score_schedule` | `0 6 * * *` | `batch_predictions` | Stopped |
| `daily_drift_report_schedule` | `0 8 * * *` | `drift_report` | Stopped |

Schedules marked **Stopped** must be enabled in the Dagster UI for production use.

Schedule ordering rationale:

- `00:00` — Feast hourly run pushes fresh features to Redis
- `01:00` — Nightly retrain reads from the Feast offline store (1h gap ensures the midnight Feast run completes first)
- `06:00` — Batch scoring reads features from Redis
- `08:00` — Drift report runs against yesterday's complete prediction set (2h after scoring)
