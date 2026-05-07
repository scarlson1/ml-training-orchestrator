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

### Dagster

### FastAPI Serving

### PySpark

### General

## Config Validation

### How `bmo/common/config.py` Works

### Fail-Fast at Import Time

### Environment-Specific Overrides

## Local vs. Production Differences

## Secrets Management

### Local (`.env`)

### Production (GitHub Secrets → Oracle VM)

## Resource Allocation

### Memory & CPU per Asset

### S3 Storage per monthly/daily partition

### Timeouts & Concurrency Settings
