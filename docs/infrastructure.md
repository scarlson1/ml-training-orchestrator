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

TODO

### FastAPI serving container + Caddy ~~Fly.io Free Tier~~

TODO

### Upstash Redis Free Tier

TODO
