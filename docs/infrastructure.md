# Infrastructure

## Overview

The project runs on two environments that share the same Docker Compose service topology:

| Environment | Object store | Redis | Reverse proxy | Deployed via |
| --- | --- | --- | --- | --- |
| **Local dev** | MinIO | Redis 7 (local container) | — | `docker compose -f infra/compose/compose.dev.yml up` |
| **Production** | Cloudflare R2 | Upstash Redis (external) | Caddy | GitHub Actions → Oracle VM systemd service |

Both environments use the same PostgreSQL 17 image for Dagster run history, MLflow experiment tracking metadata, and the Iceberg SQL catalog.

---

## Local Development Stack (Docker Compose)

**File:** [infra/compose/compose.dev.yml](../infra/compose/compose.dev.yml)

Start all services:

```bash
docker compose -f infra/compose/compose.dev.yml up -d
```

Stop and remove containers (volumes persist):

```bash
docker compose -f infra/compose/compose.dev.yml down
```

Wipe all data volumes:

```bash
docker compose -f infra/compose/compose.dev.yml down -v
```

### Services

#### PostgreSQL — Metadata + Iceberg Catalog

```
image: postgres:17
ports: 5432
```

Three logical databases are created at startup by the init scripts:

| Database | Used by |
| --- | --- |
| `bmo` | MLflow experiment runs, metric/param logs, model registry entries |
| `dagster` | Dagster run history, asset materialization records, schedules, sensors, event logs |
| `iceberg` | PyIceberg `SqlCatalog` — stores Iceberg table metadata pointing to S3 locations |

`infra/postgres/init.sql` creates the `dagster` and `iceberg` databases. `infra/postgres/create_monitoring_tables.sql` creates the `drift_metrics` and `live_accuracy` tables used by the drift sensor and ground-truth backfill asset.

PostgreSQL is the canonical source of truth for the Iceberg table catalog. Both PyIceberg (`SqlCatalog`) and PySpark (`JdbcCatalog`) connect to the same `iceberg` database so they share metadata and physical files without a mapping table.

Docs: [postgresql.org/docs](https://www.postgresql.org/docs/)

#### MinIO — Object Store (dev only)

```
image: minio/minio
ports: 9000 (API), 9001 (console)
console: http://localhost:9001  (admin / password123)
```

S3-compatible object store used in local development in place of Cloudflare R2. The `minio-init` sidecar container creates the four buckets on first startup:

| Bucket | Contents |
| --- | --- |
| `raw` | BTS CSV downloads + NOAA annual-summary files before parsing |
| `staging` | Iceberg tables, Feast feature views, training datasets, predictions, monitoring reports |
| `rejected` | Malformed rows rejected during ingestion |
| `mlflow-artifacts` | MLflow model weights, plots, Optuna study files |

Docs: [min.io/docs](https://min.io/docs/minio/linux/index.html)

#### Redis — Feast Online Store (dev only)

```
image: redis:7-alpine
ports: 6379
persistence: AOF (appendonly yes)
```

Stores materialised Feast feature vectors for low-latency online serving. In production this is replaced by [Upstash Redis](#upstash-redis-free-tier).

Docs: [redis.io/docs](https://redis.io/docs/)

#### MLflow — Experiment Tracking Server

```
build: infra/docker/mlflow.Dockerfile
ports: 5000
backend: postgresql://user:password@postgres:5432/bmo
artifacts: s3://mlflow-artifacts/
```

The MLflow image (`ghcr.io/mlflow/mlflow:v3.11.1`) is extended with the MLflow auth extra, `psycopg2-binary`, and `boto3` to support basic auth, PostgreSQL as the backend store, and S3-compatible artifact storage (MinIO in dev, R2 in production). In production, the image entrypoint generates MLflow's auth config from environment variables so the admin password is not committed to the repo.

In production, MLflow is secured with basic-auth (`--app-name basic-auth`) and is only reachable through Caddy at `$MLFLOW_DOMAIN`.

Docs: [mlflow.org/docs](https://mlflow.org/docs/latest/index.html)

#### Dagster — Orchestrator

Dagster only runs in the **production** compose file. In local development, Dagster is started directly on the host:

```bash
dagster dev -m dagster_project.definitions
```

See the [production section](#production-stack) for the containerised setup.

### Persisted Volumes

| Volume | Contents | Notes |
| --- | --- | --- |
| `postgres_data` | All PostgreSQL data | Survives container restarts |
| `minio_data` | All object storage data | Survives container restarts |
| `redis_data` | Feast online store | Survives container restarts; AOF persistence |

---

## Production Stack

**File:** [infra/compose/compose.prod.yml](../infra/compose/compose.prod.yml)

Run with:

```bash
docker compose -f infra/compose/compose.prod.yml --env-file .env up -d
```

In practice the `bmo-compose` systemd service manages the lifecycle — see [cloud-init.sh](#cloud-initsh--docker--systemd-setup).

### Services

Same PostgreSQL and MLflow services as dev, plus:

#### Dagster

```
build: infra/docker/dagster.Dockerfile
image: ghcr.io/<GHCR_OWNER>/bmo-dagster:latest
mem_limit: 4.5g
ports: 127.0.0.1:3000  (loopback only — Caddy proxies public traffic)
```

Runs the full Dagster webserver and user-code server in a single container. The [entrypoint script](../infra/docker/dagster-entrypoint.sh) runs `dbt deps && dbt parse` at startup to generate `manifest.json` before Dagster imports asset definitions that read it.

#### serving

```
build: infra/docker/serving.Dockerfile
image: ghcr.io/<GHCR_OWNER>/bmo-serving:latest
mem_limit: 1g
no ports exposed — Caddy proxies to serving:8080
```

FastAPI inference API. No public port — Caddy proxies HTTPS traffic internally using the Docker network service name `serving`.

#### Caddy — Reverse Proxy + TLS

```
image: caddy:alpine
ports: 80 (ACME HTTP-01 challenge), 443 (HTTPS)
config: infra/docker/Caddyfile
```

Terminates TLS for three domains, all resolved via [sslip.io](https://sslip.io) wildcard DNS (any subdomain of `<IP>.sslip.io` resolves to that IP — no DNS registration needed):

| Variable | Default pattern | Target |
| --- | --- | --- |
| `SERVING_DOMAIN` | `<VM_IP>.sslip.io` | `serving:8080` — public inference API |
| `MLFLOW_DOMAIN` | `mlflow.<VM_IP>.sslip.io` | `mlflow:5000` — experiment tracking UI |
| `DAGSTER_DOMAIN` | `dagster.<VM_IP>.sslip.io` | `dagster:3000` — orchestration UI (basic-auth protected) |

Caddy automatically obtains and renews Let's Encrypt certificates. Certificates are persisted in the `caddy_data` volume — without this, every container restart would re-issue a cert and hit Let's Encrypt's rate limit of 5 duplicate certificates per domain per week.

CORS for the serving endpoint is handled in the Caddyfile: preflight `OPTIONS` requests are answered directly by Caddy; `Access-Control-*` headers are added for the allowed Vercel origins (`ml-training-orchestrator.vercel.app`).

Docs: [caddyserver.com/docs](https://caddyserver.com/docs/)

### `compose.prod.yml` Differences from Dev

| Aspect | Dev | Prod |
| --- | --- | --- |
| Object store | MinIO (local container) | Cloudflare R2 (external) |
| Redis | Local Redis container | Upstash (external, commented out) |
| PostgreSQL port binding | `0.0.0.0:5432` | `127.0.0.1:5432` (loopback only) |
| MLflow auth | Disabled (`--allowed-hosts "*"`) | Basic-auth enabled (`--app-name basic-auth`) |
| Dagster | Host process (`dagster dev`) | Docker container behind Caddy |
| Serving | Host process (`uvicorn`) | Docker container behind Caddy |
| Caddy | Not present | TLS termination for all three domains |
| Memory limits | Not set | Enforced per-container |

### Environment Variable Injection

The `.env` file is written to the VM by the [deploy workflow](#github-actions--cicd) on every deployment. It is sourced by `docker compose` via `env_file: - ../../.env` in the compose file. See [.env.example](../.env.example) for all variables.

---

## Docker Images

### `dagster.Dockerfile`

**File:** [infra/docker/dagster.Dockerfile](../infra/docker/dagster.Dockerfile)

#### Multi-Stage Build

Stage 1 (`builder`) installs all Python dependencies using `uv` into a `.venv` inside the build container. Stage 2 (`runtime`) copies only the `.venv` and application source, keeping the final image free of build tools.

Java 17 (`default-jdk-headless`) is installed in the runtime stage because PySpark requires a JVM. `JAVA_HOME` is set to `/usr/lib/jvm/default-java`.

#### uv Dependency Installation

Dependencies are installed from `uv.lock` with `--frozen` to guarantee reproducible builds. The `--no-install-project` flag on the first `uv sync` call caches the dependency layer independently of source code changes — dependencies only reinstall when `pyproject.toml` or `uv.lock` changes.

Groups installed: `dagster`, `dbt`, `training`, `feast`, `iceberg`, `monitoring`. Dev dependencies (`pytest`, `ruff`, `mypy`) are excluded with `--no-dev`.

Docs: [docs.astral.sh/uv](https://docs.astral.sh/uv/)

#### Entrypoint

[infra/docker/dagster-entrypoint.sh](../infra/docker/dagster-entrypoint.sh) runs at container startup:

1. `dbt deps` — installs dbt packages from `packages.yml` into `dbt_packages/`
2. `dbt parse` — generates `dbt_project/target/manifest.json` using a throwaway DuckDB file at `/tmp/dbt_parse.duckdb` (avoids touching the real feature store)
3. `exec dagster dev` — replaces the shell process so `SIGTERM` from `docker stop` reaches Dagster directly

`manifest.json` must be generated at runtime (not baked into the image) because it embeds source/ref paths resolved from env vars like `DUCKDB_PATH` and `ICEBERG_CATALOG_URI`.

### `serving.Dockerfile`

**File:** [infra/docker/serving.Dockerfile](../infra/docker/serving.Dockerfile)

Minimal image: only the `serving` and `iceberg` dependency groups are installed. Excludes Dagster, Spark, dbt, and training dependencies. Runs as a non-root user (`bmo`, UID 1000).

Entry command: `uvicorn bmo.serving.api:app --host 0.0.0.0 --port 8080 --workers 1`

Single worker per container; horizontal scaling is achieved by running multiple instances behind Caddy.

### `mlflow.Dockerfile`

**File:** [infra/docker/mlflow.Dockerfile](../infra/docker/mlflow.Dockerfile)

Extends `ghcr.io/mlflow/mlflow:v3.11.1` with `mlflow[auth]` (basic auth dependencies), `psycopg2-binary` (PostgreSQL backend store), and `boto3` (S3-compatible artifact storage).

### Building Locally

```bash
# From repo root
docker build -f infra/docker/dagster.Dockerfile -t bmo-dagster:local .
docker build -f infra/docker/serving.Dockerfile -t bmo-serving:local .
```

### Multi-Arch Builds (amd64 + arm64)

CI builds both `linux/amd64` and `linux/arm64` platforms using `docker buildx` + QEMU emulation. The Oracle VM runs `arm64` (Ampere A1); developer machines are typically `amd64`. Both platforms pull the same image tag.

### Image Registry (GHCR)

Images are pushed to [GitHub Container Registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry) at:

- `ghcr.io/<owner>/bmo-dagster:latest` (and `:<git-sha>` for rollbacks)
- `ghcr.io/<owner>/bmo-serving:latest` (and `:<git-sha>`)

Registry-backed layer caching (`type=registry`) keeps CI rebuilds fast when only source code changes.

---

## GitHub Actions / CI/CD

**Workflows:** [.github/workflows/](.github/workflows/)

### `ci.yml` — Tests

Runs on every push and pull request. Executes the Python test suite.

### `build-images.yml` — Build & Push Docker Images

Triggers on push to `main` when any image-relevant path changes (`src/**`, `dagster_project/**`, `dbt_project/**`, `infra/docker/**`, `pyproject.toml`, `uv.lock`).

Builds `bmo-dagster` and `bmo-serving` for `linux/amd64` and `linux/arm64` in parallel using a matrix strategy and pushes to GHCR with both `:latest` and `:<git-sha>` tags.

### `deploy.yml` — Deploy to Oracle VM

Triggers automatically after `build-images.yml` completes successfully on `main`, or manually via `workflow_dispatch`.

Steps:
1. SSH into the Oracle VM using `appleboy/ssh-action`
2. Clone the repo on first deploy; `git reset --hard origin/main` on subsequent ones
3. Write `.env` from GitHub secrets/variables
4. Authenticate with GHCR (`docker login ghcr.io`)
5. `docker system prune -f` to reclaim disk space
6. Pre-pull `bmo-dagster:latest`
7. `systemctl start` (first deploy) or `systemctl restart bmo-compose` (rolling restart)
8. Poll `http://localhost:3000/server_info` for up to 5 minutes — Dagster startup includes `dbt deps + parse + JVM init` (~3.5 minutes)

### `evidently-reports.yml`

Generates Evidently drift reports. See monitoring documentation for details.

---

## Terraform

Terraform manages the Oracle Cloud VM and Cloudflare R2 buckets. State is stored locally in `infra/terraform/terraform.tfstate`.

**Directory:** [infra/terraform/](../infra/terraform/)

Providers used:

| Provider | Version | Purpose |
| --- | --- | --- |
| `oracle/oci` | `~> 6.0` | Oracle Cloud VM and networking |
| `cloudflare/cloudflare` | `~> 4.0` | R2 buckets |
| `hashicorp/tls` | `~> 4.0` | TLS key generation |
| `hashicorp/local` | `~> 2.0` | Writing local output files |

### Oracle Cloud (Always Free VM)

**Module:** [infra/terraform/oracle/](../infra/terraform/oracle/)

#### VM Provisioning

Resources created:

| Resource | Details |
| --- | --- |
| `oci_core_instance` | Ubuntu 22.04 ARM64, `VM.Standard.A1.Flex`, 1 OCPU, 8 GB RAM, 100 GB boot volume |
| `oci_core_public_ip` | Reserved public IP — survives VM termination; `prevent_destroy = true` |
| `oci_core_vcn` | Virtual Cloud Network `10.0.0.0/16` |
| `oci_core_internet_gateway` | Connects VCN to public internet |
| `oci_core_route_table` | Routes `0.0.0.0/0` through the IGW |
| `oci_core_subnet` | Public subnet `10.0.1.0/24` |
| `oci_core_security_list` | Stateful firewall (see rules below) |

The Ubuntu image OCID is looked up dynamically per region — `ignore_changes = [source_details[0].source_id]` prevents spurious replacement diffs when Oracle rotates image OCIDs on minor Ubuntu updates.

Docs: [docs.oracle.com/en-us/iaas/Content/FreeTier](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)

#### Oracle VM Firewall Rules

| Port | Protocol | Direction | Purpose |
| --- | --- | --- | --- |
| 22 | TCP | Ingress | SSH |
| 80 | TCP | Ingress | Let's Encrypt ACME HTTP-01 challenge (Caddy) |
| 443 | TCP | Ingress | HTTPS — public inference API via Caddy |
| 3000 | TCP | Ingress | Dagster webui (also reachable via Caddy at port 443) |
| 5000 | TCP | Ingress | MLflow tracking server |
| ICMP type 3 code 4 | ICMP | Ingress | Path MTU discovery — required for TCP on OCI |
| All | All | Egress | Docker pulls, R2 writes, outbound requests |

> **Note:** Ports 3000 and 5000 are open at the OCI security list level but are only reachable via Caddy subdomains in normal operation. The port-3000 binding in the compose file is `127.0.0.1:3000` (loopback only).

#### `cloud-init.sh` — Docker & systemd Setup

**File:** [infra/terraform/oracle/cloud-init.sh](../infra/terraform/oracle/cloud-init.sh)

Runs once on first VM boot as root. After it completes the VM is ready for `git clone + docker compose up`.

Steps:
1. Wait for the Ubuntu `unattended-upgrades` apt lock to release
2. Install Docker CE (official apt repo, not the distro package) with `docker-compose-plugin` and `buildx`
3. Install `uv` for running Python tools directly on the VM (`feast apply`, `dbt parse`, etc.)
4. Allocate a 4 GB swapfile — Oracle ARM VMs have no swap by default; DuckDB training jobs exhaust RAM without it and trigger the OOM killer
5. Create `/etc/cron.d/vm-keepalive` — pings `http://localhost:3000/server_info` every 30 minutes to prevent Oracle from reclaiming idle Always Free VMs
6. Write and enable the `bmo-compose.service` systemd unit — does NOT start it (the repo isn't cloned yet at cloud-init time; the deploy workflow starts it on first deploy)

The `bmo-compose.service` unit runs `docker compose pull` before `docker compose up` on every start, so `systemctl restart bmo-compose` is equivalent to a rolling deploy.

### Cloudflare R2

**Module:** [infra/terraform/cloudflare_r2/](../infra/terraform/cloudflare_r2/)

Cloudflare R2 is used for object storage in production. It is S3-compatible, so all code uses the standard `boto3` / `s3fs` / DuckDB `httpfs` clients with the R2 endpoint URL.

R2 charges no egress fees, making it significantly cheaper than AWS S3 or GCS for workloads that read data frequently.

Terraform creates four buckets (mirrors the local MinIO layout):

```hcl
locals {
  buckets = toset(["raw", "staging", "rejected", "mlflow-artifacts"])
}
```

Region is set to `ENAM` (Eastern North America). Options: `WNAM`, `ENAM`, `WEUR`, `EEUR`, `APAC`.

Docs: [developers.cloudflare.com/r2](https://developers.cloudflare.com/r2/)

### Variables & Secrets (`terraform.tfvars`)

```
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

See [terraform.tfvars.example](../infra/terraform/terraform.tfvars.example) for the full template.

### Applying Changes

```bash
cd infra/terraform

terraform init   # run once to install providers

terraform plan   # preview changes

terraform apply  # apply
```

> **State file:** `terraform.tfstate` is stored locally and is git-ignored. A commented-out `backend "s3"` block in `main.tf` shows how to migrate state to R2 after the buckets exist.

---

## Networking

### Port Mapping (Local vs. Prod)

| Service | Local port | Prod (external) | Prod (internal Docker network) |
| --- | --- | --- | --- |
| PostgreSQL | `localhost:5432` | Not exposed | `postgres:5432` |
| MinIO API | `localhost:9000` | Not present (R2 used) | — |
| MinIO console | `localhost:9001` | Not present | — |
| Redis | `localhost:6379` | Not present (Upstash used) | — |
| MLflow | `localhost:5000` | `https://$MLFLOW_DOMAIN` | `mlflow:5000` |
| Dagster | `localhost:3000` | `https://$DAGSTER_DOMAIN` | `dagster:3000` |
| Serving API | `localhost:8080` | `https://$SERVING_DOMAIN` | `serving:8080` |
| Caddy HTTP | — | `:80` (ACME challenge) | — |
| Caddy HTTPS | — | `:443` | — |

### DNS

Production domains are resolved via [sslip.io](https://sslip.io) — a free wildcard DNS service where any subdomain of `<IP>.sslip.io` resolves to `<IP>`. No DNS registration or propagation delay is needed; the domain is functional as soon as the VM has its reserved IP.

Example with IP `150.136.1.2`:

| Variable | Value |
| --- | --- |
| `SERVING_DOMAIN` | `150.136.1.2.sslip.io` |
| `MLFLOW_DOMAIN` | `mlflow.150.136.1.2.sslip.io` |
| `DAGSTER_DOMAIN` | `dagster.150.136.1.2.sslip.io` |

### Oracle Always Free Tier Limits

The Always Free tier allows up to 4 OCPUs and 24 GB RAM across all Ampere A1 instances in a tenancy. This project uses 1 OCPU and 8 GB, leaving the rest available for other projects.

Docs: [docs.oracle.com — Always Free Resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)

### Cloudflare R2 Pricing

> **Note:** Free tier limits below are sourced from Cloudflare's documentation (as of mid-2025). Verify at [developers.cloudflare.com/r2/pricing](https://developers.cloudflare.com/r2/pricing/).

#### Free Tier Limits (per month)

| Resource | Free Allowance |
| --- | --- |
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
| --- | --- | --- | --- |
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
| --- | --- |
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
| --- | --- | --- |
| Feast hourly export (5 files × 24h) | ~120 | ~120 |
| Daily batch scoring (1 write) | 1 | — |
| Daily monitoring (2 writes) | 2 | — |
| Training dataset builder | 1–2 | 1 (cache check) |
| Serving reads (API reads `model_config.json`) | — | ~1,440 (once/minute) |

Monthly Class A total: ~4,000. Monthly Class B total: ~45,000. Both are far below the 1M / 10M free limits.

#### Summary

This application is designed to stay within Cloudflare R2's free tier for the foreseeable future. The only long-term risk is `staging/predictions/` and `staging/monitoring/` accumulating over years without a cleanup policy. Adding a lifecycle rule or periodic job to delete partitions older than 90–180 days would keep storage well under 10 GB indefinitely.

### Upstash Redis Free Tier

[Upstash](https://upstash.com) provides a managed Redis instance used as the Feast online store in production. Unlike the local Redis container, Upstash is reachable from any network (the serving container on the Oracle VM, and any future edge/serverless deployment).

The `REDIS_URL` environment variable is set in `.env` on the VM using the Upstash connection string format:

```
REDIS_URL=some_id.upstash.io:6379,ssl=true,password=my_password
```

Feast reads this format directly via its Redis online store configuration. See [Feast Redis online store docs](https://docs.feast.dev/reference/online-stores/redis) for the full connection string options.

**Free tier:** 10,000 commands/day, 256 MB storage. Feast feature lookups at serving time generate ~5–10 Redis commands per prediction request, so the free tier supports ~1,000–2,000 prediction requests/day.

Docs: [upstash.com/docs/redis](https://upstash.com/docs/redis/overall/getstarted)
