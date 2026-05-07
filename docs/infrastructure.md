# Infrastructure

## Overview

## Local Development Stack (Docker Compose)

### Services (`compose.dev.yml`)

#### Dagster (User-Code Server + Webserver)

#### PostgreSQL (Metadata + Iceberg Catalog)

#### MinIO (Object Store)

Rough estimates per monthly partition:

##### Flights (raw + staged)

- BTS reports ~600–700K domestic flights/month
- Raw CSV is ~100–200 MB uncompressed; as Parquet + zstd it compresses to ~15–30 MB
- Staged adds UTC timestamps but drops no rows (validated rows only) — similar size, ~15–25 MB
- Rejected rows: a small fraction, likely <1 MB

##### Weather (raw + staged)

- ~350–450 NOAA stations × 720 FM-15 obs/station (hourly × 30 days) = ~300K rows
- 13 narrow columns (mostly float32) — ~3–8 MB as Parquet + zstd

##### Dimension tables (written once, not partitioned)

- dim_airport: ~500 rows — negligible
- dim_route: ~10K–50K rows — <5 MB

##### Full backfill (2018–2024, 84 months)

- Flights: ~84 × 20 MB = ~1.7 GB raw + ~1.5 GB staged
- Weather: ~84 × 5 MB = ~420 MB raw + ~350 MB staged
- Total: ~4 GB, comfortable for a local MinIO instance

One caveat: the raw NOAA layer stores all data that came out of LCD parsing (already filtered to FM-15 + target month), not the full annual CSVs, so it won't balloon. The heavy I/O cost is network (downloading those annual files), not storage.

#### MLflow (Tracking Server)

#### Redis (Feast Online Store)

### Starting & Stopping

### Persisted Volumes

## Production Stack

### `compose.prod.yml` Differences

### Environment Variable Injection

## Docker Images

### `dagster.Dockerfile`

#### Multi-Stage Build

#### uv Dependency Installation

### `serving.Dockerfile`

### Building Locally

### Multi-Arch Builds (amd64 + arm64)

### Image Registry (GHCR)

## Terraform

Terraform is used to manage the Oracle VM & related resources and Cloudflare R2 object storage.

### Oracle Cloud (Always Free VM)

#### VM Provisioning (`infra/terraform/oracle/`)

- VM - configured with 8GB RAM and 1 OCPU
- Reserved IP (consistent across VM instance termination)
- VCN -
- Gateway - connect VCN to public internet
- Route table
- Security list / Firewall configuration - setup for Caddy & SSH

### Oracle VM Firewall Rules

- SSH: allow traffic on port 22
-

#### `cloud-init.sh` — Docker & systemd Setup

- install essentials - `curl`, `git`, `docker`, `uv` etc.
- configure swapfile for DuckDB
- `keepalive` cron to keep VM running
- configure `bmo-compose` systemd service to start docker compose on boot

### Cloudflare R2

Cloudflare R2 is used for object storage in deployment (minIO in dev environment).

Cloudflare R2 does not charge for egress, making it significantly cheaper than alternatives.

#### Bucket Creation (`infra/terraform/r2/`)

Terraform creates the following buckets: `"raw", "staging", "rejected", "mlflow-artifacts"`

### Variables & Secrets (`terraform.tfvars`)

```env
project_name = "bmo-pipeline"

oci_tenancy_ocid     = "ocid1.tenancy.oc1..lskdjf..."
oci_user_ocid        = "ocid1.user.oc1..dfsafg..."
oci_fingerprint      = "66:60:..."
oci_private_key_path = "~/.oci/key_filename.pem"
region               = "us-chicago-1"
ssh_public_key       = "ssh-ed25519 AAA... example@gmail.com"

cloudflare_api_token ="TOKEN"
cloudflare_account_id="ACCOUNT_ID"
```

### Applying Changes

```bash
terraform init # run once to install dependencies

terraform plan # -out tfplan

terraform apply # tfplan
```

## Networking

TODO

### Port Mapping (Local vs. Prod)

TODO

### Oracle Always Free Tier Limits

Allows for up to 24GB RAM & 4 OCPUs. Currently configured with 8GB RAM and 1 OCPU (rest of resources used for other projects).

### Cloudflare R2 Pricing

> **Note:** Free tier limits below are sourced from Cloudflare's documentation (as of mid-2025). Verify at [developers.cloudflare.com/r2/pricing](https://developers.cloudflare.com/r2/pricing/).

#### Free Tier Limits (per month)

| Resource | Free Allowance |
|---|---|
| Storage | 10 GB |
| Class A operations (writes, deletes, lists) | 1,000,000 |
| Class B operations (reads) | 10,000,000 |
| Egress | Free (no egress fees) |

#### Buckets

Terraform creates four buckets: `raw`, `staging`, `rejected`, `mlflow-artifacts`.

`raw` and `rejected` are ingestion-time buckets. `raw` holds the full BTS CSV and NOAA annual-summary downloads before parsing; these are large but temporary — files can be deleted after staging. `rejected` is near-empty in practice (a small fraction of malformed rows). Neither accumulates significantly during normal operations once the initial backfill is complete.

`staging` and `mlflow-artifacts` are the buckets that matter for ongoing storage.

#### `staging` Bucket: Storage Breakdown

| Prefix | Write frequency | File size | Growth pattern |
|---|---|---|---|
| `staging/feast/` | Hourly (overwrite) | ~20–50 MB total (5 files) | **Fixed** — same 5 files overwritten every hour |
| `staging/datasets/` | Per training run (content-addressed) | ~100–500 MB per dataset | **Fixed** — written once per unique training dataset; retrained datasets reuse the same path |
| `staging/serving/model_config.json` | On champion promotion | ~1 KB | **Fixed** — single file, overwritten |
| `staging/predictions/date=*/` | Daily | ~5–50 MB | **Accumulates** — one partition per calendar day, never overwritten |
| `staging/monitoring/reports/date=*/` | Daily | ~2–5 MB (Evidently HTML) | **Accumulates** — one report per calendar day |
| `staging/monitoring/metrics/date=*/` | Daily | ~100 KB (Parquet) | **Accumulates** — one metrics file per calendar day |
| `staging/iceberg/` | Monthly (flights/weather), once (dimensions) | ~20–50 MB/month (incremental partitions) | **Slow accumulation** — Iceberg snapshots + new monthly partitions |

**Fixed-size total (feast + datasets + serving):** ~100–600 MB depending on how many training datasets exist.

**Accumulating prefixes:**

- `staging/predictions/` + `staging/monitoring/`: ~7–55 MB/day combined (low end on quiet days, high end with a full prediction run). A reasonable steady-state estimate is **~10 MB/day**.
- `staging/iceberg/`: New monthly partitions add ~20–50 MB/month; Iceberg metadata snapshots are small.

At ~10 MB/day for predictions + monitoring, plus ~35 MB/month for Iceberg:

| Time horizon | Accumulation estimate |
|---|---|
| 3 months | ~1 GB |
| 1 year | ~4 GB |
| 2.5 years | ~10 GB (free tier ceiling) |

There are no automated cleanup or retention policies in the codebase for `staging/predictions/` or `staging/monitoring/`. Old partitions accumulate indefinitely. At the current rate, the free tier 10 GB limit is reached in roughly **2–3 years** of continuous daily operation.

#### `mlflow-artifacts` Bucket: Storage Breakdown

MLflow artifacts are written once per training run and never overwritten. Each run produces:

- Evidently classification report (HTML): ~2–5 MB
- XGBoost feature importance + calibration plots: ~1–3 MB
- Optuna HPO study database: ~1–5 MB
- Logged model weights (XGBoost booster): ~5–20 MB

**Per run total: ~10–35 MB.** Training runs are infrequent (on-demand or periodic retraining), so this bucket grows slowly.

#### Class A / Class B Operation Estimates

The free tier's 1M Class A (write) and 10M Class B (read) monthly limits are unlikely to be hit.

| Workload | Class A ops/day | Class B ops/day |
|---|---|---|
| Feast hourly export (5 files × 24h) | ~120 | ~120 |
| Daily batch scoring (1 write) | 1 | — |
| Daily monitoring (2 writes) | 2 | — |
| Training dataset builder | 1–2 | 1 (cache check) |
| Serving reads (API reads `model_config.json`) | — | ~1,440 (once/minute) |

Monthly Class A total: ~4,000. Monthly Class B total: ~45,000. Both are far below the 1M / 10M free limits.

#### Summary

This application is designed to stay within Cloudflare R2's free tier for the foreseeable future. The only long-term risk is `staging/predictions/` and `staging/monitoring/` accumulating over years without a cleanup policy. Adding a lifecycle rule or periodic job to delete partitions older than 90–180 days would keep storage well under 10 GB indefinitely.

### FastAPI serving container + Caddy ~~Fly.io Free Tier~~

TODO

### Upstash Redis Free Tier

TODO
