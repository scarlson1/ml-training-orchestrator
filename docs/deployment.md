# Deployment

## Dagster

### Startup sequence

On every container start, `infra/docker/dagster-entrypoint.sh` runs before Dagster itself:

1. **`dbt deps`** — installs dbt packages declared in `packages.yml` (e.g. `dbt_utils`) into `dbt_packages/`. Must run before `dbt parse` so macros can be resolved.
2. **`dbt parse`** — reads the project and writes `dbt_project/target/manifest.json`. No SQL is executed; a throwaway DuckDB file (`/tmp/dbt_parse.duckdb`) satisfies the adapter connection check without touching the real feature store.
3. **`dagster dev`** — starts the Dagster webserver + daemon. At import time, `@dbt_assets` in `dagster_project/assets/features_dbt.py` reads `manifest.json` and registers one Dagster asset per dbt model. If `manifest.json` is missing, Dagster crashes before serving any requests.

The manifest is generated at runtime (not baked into the image) because it embeds paths resolved from environment variables (`DUCKDB_PATH`, `ICEBERG_CATALOG_URI`, etc.) that differ between dev and prod.

### DuckDB persistence

dbt writes feature tables into a local DuckDB file. In prod this file lives at `/dagster_home/bmo_features.duckdb`, which is on the `dagster_home` named Docker volume and persists across container restarts. The path is set via `DUCKDB_PATH` in `compose.prod.yml`.

If the container restarts and `feast_feature_export` runs before `bmo_dbt_assets` has written the DuckDB file, it will fail with `database does not exist`. Fix: manually materialize the dbt feature models first (see below).

### Running the feast pipeline manually

The hourly schedule (`feast_hourly_schedule`) only runs `feast_materialized_features` — it assumes `feast_feature_export` has already written fresh Parquet to S3 from a prior dbt run. After a fresh deploy or container restart, trigger the full chain manually from the Dagster UI:

1. Open the Dagster UI at port 3000.
2. Navigate to **Assets → Asset graph**.
3. Click `feast_feature_export` → in the toolbar select **Ancestors** to include all upstream dbt models.
4. Click **Materialize selected**. Dagster runs the dbt models (via `dbt build`) then `feast_feature_export` in dependency order.
5. Optionally add `feast_materialized_features` to the selection to push features to Redis in the same run.

### Asset graph structure

`bmo_dbt_assets` (the Python function) does not appear as a single node in the lineage graph. The `@dbt_assets` decorator expands the dbt manifest into one node per model. The function itself is the executor — it runs `dbt build` — but the visible nodes are the individual models (`feat_origin_airport_windowed`, `feat_carrier_rolling`, etc.). Materializing any dbt model node triggers `dbt build` for the full project.

## Terraform

Terraform is used to manage two deployments: **Oracle VM** and **Cloudflare R2** (S3 storage).

Terraform state is currently managed locally. Uncomment the R2 section in `main.tf` to migrate state to Cloudflare R2.

### Environment Variables

#### `infra/terraform/terraform.tfvars`

- `project_name`
- `oci_tenancy_ocid`
- `oci_user_ocid`
- `oci_fingerprint`
- `oci_private_key_path`
- `region`
- `ssh_public_key`
- `cloudflare_api_token`
- `cloudflare_account_id`

### `cloud-init.sh`

- install required packages: `ca-certificates`, `curl`, `gnupg`, `lsb-release`, `git`, `docker-ce`, `containerd.io`, `docker-buildx-plugin`, `docker-compose-plugin`, etc.
- install & setup `uv`
- run `keepalive` so VM isn't shutdown
- create & enable systemd service for docker compose (`systemctl start` not run until github CI - repo isn't cloned to VM yet)

### Deploy

```bash
# install provider dependencies
terraform init

terraform plan -var-file="terraform.tfvars" -out=tfplan

terraform apply tfplan
```

VM IP and R2 URL are included in outputs to populate github environment variables if not already set. VM uses a consistent IP - don't change when redeployed.

## Github Actions

### `build-images.yml`

Builds `dagster.Dockerfile` and `serving.Dockerfile` → pushes image to registry (`linux/amd64` & `linux/arm64` platforms)

### `deploy.yml`

- clone/pull repo into `~/ml-training-orchestrator`
- creates `.env` from github environment secrets/variables.
- wait for [`cloud-init.sh`](/infra/terraform/oracle/cloud-init.sh) to finish installing docker
- authenticate to GHCR to pull images created in `build-images.yml`
- start/restart bmo-compose service on VM

### TODO: document memory constraint approach

`mart_training_dataset` was causing OOM issues (VM starting shutting down processes to make room for DuckDB; couldn't ssh into VM)

> Obviously would be better to bump up OCPUs and RAM, but I'm out of free tier resources

Fixes:

- Add swap
- DuckDB memory cap (in `training.py`)
- Add container constraints in `compose.prod.yml`

```bash
# cloud-init.sh
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

```python
# training.py
con = duckdb.connect(settings.duckdb_path, read_only=True)
con.execute("SET memory_limit = '2GB'")
con.execute("SET temp_directory = '/dagster_home/duckdb_spill'")

```

```yml
# `compose.prod.yml`
services:
  dagster:
    mem_limit: 3g # DuckDB lives here — give it the most
  postgres:
    mem_limit: 512m
  mlflow:
    mem_limit: 512m
  redis:
    mem_limit: 256m
  caddy:
    mem_limit: 128m
  serving:
    mem_limit: 512m
```
