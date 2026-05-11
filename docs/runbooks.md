# Runbooks

## Backfilling a Historical Partition

### When to Use

- Adding coverage for a new date range (e.g., extending history back to 2018).
- Re-ingesting a corrupt or partially-failed partition after fixing the root cause.
- Recovering after a storage outage that left gaps in the Iceberg tables.

### Step-by-Step

Materialize assets in dependency order. Each monthly partition uses the `YYYY-MM-DD` format (first day of the month).

**Via the Dagster UI:**

1. Open the Dagster UI → **Assets** → **View global asset graph**.
2. Select the asset to backfill (e.g., `raw_bts_flights`).
3. Click **Materialize** → in the partition picker, select the target month(s).
4. Repeat for each asset in order: `raw_bts_flights` → `staged_flights`, `raw_noaa_weather` → `staged_weather`.
5. After all partitioned assets are materialized, trigger the unpartitioned assets that read the full Iceberg table: `feat_cascading_delay` → `bmo_dbt_assets` (dbt build).

**Via the CLI (scripted multi-month backfill):**

```bash
for month in 2024-01-01 2024-02-01 2024-03-01; do
  uv run dg launch --assets raw_bts_flights --partition "$month"
  uv run dg launch --assets staged_flights --partition "$month"
  uv run dg launch --assets raw_noaa_weather --partition "$month"
  uv run dg launch --assets staged_weather --partition "$month"
done

# After all partitions land, rebuild features (unpartitioned — reads the full table)
uv run dg launch --assets feat_cascading_delay
uv run dg launch --assets 'bmo_dbt_assets*'
```

> **Memory note**: On the Oracle VM (1 OCPU / 6–12 GB RAM), run no more than one partition at a time. Concurrent DuckDB + PySpark jobs will exhaust RAM. See [VM Becomes Unresponsive During a Training Run](#vm-becomes-unresponsive-during-a-training-run) if the VM locks up during a backfill.

### Verifying the Backfill

```bash
# Confirm rows landed in Iceberg via DuckDB
cd dbt_project
uv run dbt run-operation print --args '{"msg": "checking row counts"}'

# Or query directly
uv run python - <<'EOF'
import duckdb, os
con = duckdb.connect(os.environ['DUCKDB_PATH'])
print(con.execute("""
  SELECT DATE_TRUNC('month', scheduled_departure_utc) AS month,
         COUNT(*) AS rows
  FROM staging.staged_flights
  GROUP BY 1 ORDER BY 1
""").df())
EOF
```

Open **MinIO** (local) or R2 (prod) and confirm the Iceberg data files exist under `s3://staging/iceberg/staged_flights/`.

---

## Hot-Swapping the Production Model

### When to Hot-Swap

- A new model version was registered as `champion` in the MLflow Registry (the `registered_model` Dagster asset materialized and promoted a new version).
- You want to roll back to a previous `champion` after a quality regression.
- The model was re-trained manually and needs to be loaded into the running serving container without a full restart.

### Step-by-Step (MLflow Registry → `/admin/reload`)

#### 1. Confirm the new champion in MLflow

Open the MLflow UI (local: `http://localhost:5000`, prod: `https://mlflow.<VM_IP>.sslip.io`) → **Models** → `bmo-flight-delay` → verify the desired version has the `champion` alias.

Or via CLI:

```bash
uv run python - <<'EOF'
import mlflow
from mlflow.tracking import MlflowClient
client = MlflowClient()
mv = client.get_model_version_by_alias('bmo-flight-delay', 'champion')
print(f'champion → version {mv.version} (run {mv.run_id[:8]}...)')
EOF
```

#### 2. Hot-swap via `/admin/reload`

```bash
# Production
curl -s -X POST https://<SERVING_DOMAIN>/admin/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Local dev (ADMIN_TOKEN is empty = no auth required)
curl -s -X POST http://localhost:8080/admin/reload
```

Successful response:

```json
{"status": "reloaded", "model_version": "7"}
```

#### 3. Verify the new version is serving

```bash
curl -s https://<SERVING_DOMAIN>/model-info | jq '.model_version'
```

The `bmo_model` Prometheus metric (`/metrics`) also reflects the new version immediately.

### Rollback Procedure

To roll back to a specific version, reassign the `champion` alias in MLflow, then call `/admin/reload`:

```bash
uv run python - <<'EOF'
from mlflow.tracking import MlflowClient
client = MlflowClient()
# Move champion alias to the previous version (e.g., version 5)
client.delete_registered_model_alias('bmo-flight-delay', 'champion')
client.set_registered_model_alias('bmo-flight-delay', 'champion', '5')
print('champion reassigned to version 5')
EOF

curl -s -X POST https://<SERVING_DOMAIN>/admin/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

See the [MLflow Model Registry docs](https://mlflow.org/docs/latest/model-registry.html) for alias management reference.

---

## Re-Materializing Online Features

### When to Re-Materialize

- Redis was flushed, restarted, or migrated (Upstash plan change, Redis version upgrade).
- `feast_materialized_features` missed multiple scheduled runs and online features are stale.
- A new feature view was added or a TTL was changed and the online store needs to be rebuilt.
- The `/health` endpoint returns `redis_reachable: false`.

### Materialization Options

#### Option 1 — Incremental (normal recovery, missed hourly runs)

Trigger `feast_materialized_features` in the Dagster UI. It calls `materialize_incremental()` from the last high-water mark and is safe to re-run:

```bash
uv run dg launch --assets feast_materialized_features
```

#### Option 2 — Full re-materialization (after Redis flush or TTL change)

```bash
# 1. Reset the Feast high-water mark by deleting and re-applying the registry
#    (dev: local SQLite)
rm feature_repo/data/registry.db
cd feature_repo && uv run feast apply

# 2. Full materialize over the desired date range
cd feature_repo
feast materialize 2024-01-01T00:00:00 $(date -u +%Y-%m-%dT%H:%M:%S)
```

In production the registry is in R2 (`s3://staging/feast/registry.db`). Delete it with:

```bash
aws s3 rm s3://staging/feast/registry.db \
  --endpoint-url https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com
cd feature_repo && uv run feast apply
```

#### Option 3 — Trigger via Dagster (full chain)

1. Dagster UI → Assets → select `feast_feature_export` + ancestors (dbt models).
2. Click **Materialize selected** to rebuild Parquet from DuckDB first.
3. Then materialize `feast_materialized_features` to push to Redis.

### Verifying Redis Is Up-to-Date

```python
from feast import FeatureStore
store = FeatureStore(repo_path='feature_repo/')

# Spot-check one entity
response = store.get_online_features(
    features=['origin_airport_features:origin_avg_dep_delay_1h'],
    entity_rows=[{'origin': 'ORD'}],
)
print(response.to_dict())
```

In the Dagster UI, verify that `feast_materialized_features` shows a `materialized_through` metadata value within the last hour.

See [Feast materialize CLI reference](https://docs.feast.dev/reference/feast-cli-commands#feast-materialize) for full options.

---

## Debugging a Failed Dagster Run

### Reading the Run Log

1. Dagster UI → **Runs** → click the failed run ID.
2. Click the **Logs** tab. Filter by `ERROR` or `CRITICAL` to jump to the failure.
3. Click the failed step name in the **Asset graph** view to highlight its log lines.
4. For compute logs (stdout/stderr of the Python process), click the **Compute logs** icon next to the failed op.

Production compute logs are stored in the `dagster_home` Docker volume. To read them directly on the VM:

```bash
ssh ubuntu@<ORACLE_VM_IP>
sudo docker compose -f ~/ml-training-orchestrator/infra/compose/compose.prod.yml \
  logs dagster --tail=200
```

### Common Failure Points by Phase

| Phase | Asset | Common cause | Fix |
| --- | --- | --- | --- |
| Ingestion | `raw_bts_flights` | BTS API timeout or schema change | Re-run the partition; check [BTS transtats site](https://www.transtats.bts.gov/DL_SelectFields.aspx) for format changes |
| Ingestion | `raw_noaa_weather` | NOAA server 503 or rate limit | Re-run after a few minutes |
| Staging | `staged_flights` | Iceberg `CatalogException` | Confirm Postgres is up; run `make dbt-bootstrap` if manifest is stale |
| Staging | `staged_weather` | `NoSuchBucket` | MinIO/R2 credentials wrong or bucket missing; check `S3_ENDPOINT_URL` |
| Features | `feat_cascading_delay` | PySpark OOM (JVM heap) | Reduce `spark.driver.memory` in `make_spark_session` or add more swap |
| Features | `bmo_dbt_assets` | `stg_feat_cascading_delay` source missing | Run `feat_cascading_delay` first — the Iceberg table doesn't exist until PySpark has run at least once |
| Features | `bmo_dbt_assets` | dbt test failure | Check `dbt_project/target/run_results.json` for the failing test; query the failing model in DuckDB |
| Feast | `feast_feature_export` | `DuckDB: database does not exist` | Run `bmo_dbt_assets` first to materialize the DuckDB feature tables |
| Feast | `feast_materialized_features` | Redis connection refused | Check Redis/Upstash is reachable; verify `REDIS_URL` in `.env` |
| Training | `training_dataset` | `LeakageError` | A leakage guard tripped — inspect the error message; do not bypass it |
| Training | `trained_model` | MLflow `RestException` | Confirm MLflow is running and `MLFLOW_TRACKING_URI` is correct |
| Training | `registered_model` | Gate check failure | The new model's AUC was below the evaluation threshold; check MLflow run metrics |

### Re-Running a Specific Asset

After fixing the root cause, re-run only the failed asset from the Dagster UI (click **Re-execute → From failure**) or via the CLI:

```bash
# Re-run a single asset (no partition)
uv run dg launch --assets feat_cascading_delay

# Re-run a partitioned asset
uv run dg launch --assets staged_flights --partition 2024-03-01
```

`Re-execute → From failure` in the UI re-runs only the failed step and its downstream dependents, skipping steps that already succeeded.

See [Dagster re-execution docs](https://docs.dagster.io/concepts/ops-jobs/re-execution) for advanced options.

---

## Investigating Data Leakage

### Symptoms

- Training AUC is implausibly high (> 0.95 on the test split).
- The `no_future_leakage` dbt singular test fails on `int_flights_enriched`.
- The `LeakageError` exception is raised by `build_dataset()` during training.
- A feature's SHAP value is anomalously high compared to its logical relevance.

### Running the Planted-Value Leakage Test

The leakage guard test plants a future value in the feature data and asserts it is rejected by the PIT join:

```bash
make leakage
# Runs: uv run pytest tests/integration/test_leakage_planted_value.py -q
```

The test must pass (0 failures). A failure means a future feature value leaked into a row with an earlier `event_timestamp` — the ASOF JOIN TTL masking is broken.

For the dbt-level PIT correctness test on weather joins:

```bash
cd dbt_project
uv run dbt test --select int_flights_enriched --profiles-dir .
```

This runs the `no_future_leakage` singular test which asserts `origin_obs_time_utc <= scheduled_departure_utc` for every row.

### Tracing Leakage to Its Source

#### Step 1: Identify the leaking column

Run the full determinism test suite, which checks that re-building the same partition produces the same hash:

```bash
make test-det
```

If the hash changes across runs, a non-deterministic data source is writing different values at different times.

#### Step 2: Check weather join windows

In `dbt_project/models/intermediate/int_flights_enriched.sql`, verify:

- Origin weather window: `obs_time_utc BETWEEN scheduled_departure_utc - INTERVAL 3 HOUR AND scheduled_departure_utc`
- Destination weather window: `obs_time_utc BETWEEN scheduled_departure_utc - INTERVAL 6 HOUR AND scheduled_departure_utc`

Both must use `scheduled_departure_utc` (known before departure), not `actual_departure_utc`.

#### Step 3: Check Feast ASOF join TTL masking

The PIT join in [src/bmo/training_dataset_builder/pit_join.py](../src/bmo/training_dataset_builder/pit_join.py) has a TTL masking step. Verify `WHERE event_ts <= :as_of` appears in the `features` CTE — this is the guard that excludes future feature snapshots.

#### Step 4: Check the DuckDB window function boundary

A [known limitation](feature-engineering.md#known-limitations--edge-cases) is that DuckDB's `RANGE BETWEEN ... AND CURRENT ROW` includes the current flight in its own window aggregate. This is a minor bias, not a strict leakage path, but if a feature shows unusually high self-correlation, verify it is not being computed in a way that incorporates the row's own label.

---

## Recovering from a Schema Migration

### Iceberg Schema Evolution Commands

PyIceberg supports additive schema changes (adding nullable columns) without rewriting data files. Use the [PyIceberg schema evolution API](https://py.iceberg.apache.org/api/#schema-evolution):

```python
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import FloatType

catalog = SqlCatalog('bmo', uri='postgresql+psycopg2://user:password@localhost:5432/bmo')
table = catalog.load_table('staging.staged_flights')

with table.update_schema() as update:
    update.add_column('new_feature_col', FloatType())
```

> Non-additive changes (renaming, type widening, dropping columns) require a full table rewrite — write to a new Iceberg table, validate, then swap. Iceberg does not support in-place destructive schema changes on existing data files.

### dbt Model Recompile

After any change to dbt model SQL, sources YAML, or `dbt_project.yml`:

```bash
# Rebuild manifest (required before Dagster restart)
make dbt-bootstrap   # runs dbt deps + dbt parse

# Full build to materialize updated tables
make dbt-build       # runs dbt build
```

If you added or renamed a source, update `_SOURCE_TO_ASSET_KEY` in [dagster_project/assets/features_dbt.py](../dagster_project/assets/features_dbt.py) so Dagster draws the correct DAG edges. Then restart the Dagster server to pick up the new manifest.

### Feast `feast apply` After Schema Change

After adding, removing, or renaming a feature in any `FeatureView` in `feature_repo/feature_views.py`:

```bash
cd feature_repo
uv run feast apply
```

This updates the Feast registry with the new schema. After applying:

1. Trigger `feast_feature_export` in Dagster to re-write the Parquet files with the new schema.
2. Trigger `feast_materialized_features` (or run `feast materialize`) to push the updated values to Redis.

**If you changed `entity_key_serialization_version` in `feature_store.yaml`:** all materialized Redis keys are now unreadable. You must flush Redis and perform a full re-materialization. See [Re-Materializing Online Features](#re-materializing-online-features).

---

## Rotating Secrets / Credentials

### Updating GitHub Secrets

1. Go to the repo → **Settings → Secrets and variables → Actions**.
2. Click the secret name → **Update**.
3. Paste the new value → **Update secret**.

After updating any secret that is embedded in `.env` (all secrets except `ORACLE_SSH_PRIVATE_KEY`), trigger a re-deploy to push the new value to the VM:

- Go to **Actions → Deploy → Run workflow** → click **Run workflow**.

The deploy workflow writes a fresh `.env` from the current GitHub secrets on every run, so the new value will be active after the next compose restart.

For `ORACLE_SSH_PRIVATE_KEY`: no re-deploy is needed — this is only used by the deploy workflow's SSH step, not by the running containers.

See the full required secrets list in [CI/CD → Prerequisites](ci-cd.md#prerequisites-secrets-oracle-vm).

### Propagating to Oracle VM

If you need to update a value immediately without waiting for a full deploy (e.g., rotating `ADMIN_TOKEN` or `REDIS_URL` after a breach):

```bash
ssh ubuntu@<ORACLE_VM_IP>

# Edit .env directly
nano ~/ml-training-orchestrator/.env

# Restart the compose stack to pick up the new value
sudo systemctl restart bmo-compose
```

Wait for Dagster to come back up (~3.5 min) before confirming the stack is healthy:

```bash
curl -s http://localhost:3000/server_info | jq .
```

### Updating `.env.prod`

`.env.prod` is not committed to the repo. It lives only on the Oracle VM at `~/ml-training-orchestrator/.env` (written by the GitHub Actions deploy script). To update it:

1. Update the corresponding GitHub secret or variable.
2. Trigger the deploy workflow (`workflow_dispatch`) — it will overwrite `.env` with the latest values.

---

## Rebuilding the Dagster Code Server

### When to Rebuild the Docker Image

The `dagster` Docker image must be rebuilt when any of the following change:

- Python source files under `src/`, `dagster_project/`, `dbt_project/`, or `feature_repo/`
- `pyproject.toml` or `uv.lock` (dependency changes)
- `infra/docker/dagster.Dockerfile`

The `build-images.yml` GitHub Actions workflow builds and pushes the image automatically on push to `main` when those paths change. To force a rebuild without a code change:

1. Go to **Actions → Build & Push Docker Images → Run workflow**.

To build locally for testing:

```bash
docker build -f infra/docker/dagster.Dockerfile -t bmo-dagster:local .
```

See [CI/CD → build-images.yml](ci-cd.md#build-imagesyml--docker-image-builds) for the full build matrix and caching strategy.

### Restarting the Code Server Without Full Stack Restart

On the Oracle VM, restarting only the `dagster` container avoids taking down Postgres, Redis, and the serving container:

```bash
ssh ubuntu@<ORACLE_VM_IP>
cd ~/ml-training-orchestrator

# Restart only the dagster service (pulls latest image first)
sudo docker compose -f infra/compose/compose.prod.yml pull dagster
sudo docker compose -f infra/compose/compose.prod.yml up -d --no-deps dagster
```

`--no-deps` prevents Docker Compose from also restarting Postgres, MLflow, and other services. The dagster container will run `dbt deps + dbt parse` before starting (via `dagster-entrypoint.sh`), which takes ~3.5 minutes on a cold start.

Poll for readiness:

```bash
until curl -sf http://localhost:3000/server_info > /dev/null; do
  echo "waiting for dagster..."; sleep 10
done
echo "dagster is ready"
```

See [Dagster deployment docs](https://docs.dagster.io/guides/deploy) for server architecture reference.

---

## Inspecting Iceberg Table State

### Querying via DuckDB

DuckDB can read Iceberg tables directly via the [Iceberg extension](https://duckdb.org/docs/extensions/iceberg):

```python
import duckdb, os

con = duckdb.connect()
con.execute('INSTALL iceberg; LOAD iceberg;')
con.execute('INSTALL httpfs; LOAD httpfs;')
con.execute(f"""
  SET s3_endpoint = '{os.environ["S3_ENDPOINT"]}';
  SET s3_access_key_id = '{os.environ["AWS_ACCESS_KEY_ID"]}';
  SET s3_secret_access_key = '{os.environ["AWS_SECRET_ACCESS_KEY"]}';
  SET s3_url_style = 'path';
""")

# Read the latest snapshot of a table
df = con.execute("""
  SELECT COUNT(*), MIN(flight_date), MAX(flight_date)
  FROM iceberg_scan('s3://staging/iceberg/staged_flights/')
""").df()
print(df)
```

For local dev (MinIO), set `s3_endpoint` to `localhost:9000` and `s3_url_style` to `'path'`.

Alternatively, query through dbt's DuckDB connection, which has Iceberg already configured via `profiles.yml`:

```bash
cd dbt_project
uv run dbt show --inline "SELECT COUNT(*) FROM {{ source('iceberg_staging', 'staged_flights') }}" \
  --profiles-dir .
```

### Listing Snapshots

PyIceberg lists the full snapshot history for a table:

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    'bmo',
    uri='postgresql+psycopg2://user:password@localhost:5432/bmo',
)
table = catalog.load_table('staging.staged_flights')

for snap in table.history():
    print(f'snapshot_id={snap.snapshot_id}  timestamp={snap.timestamp_ms}  '
          f'operation={snap.summary.get("operation")}')
```

Each `APPEND` or `OVERWRITE` operation creates a new snapshot. Iceberg retains all snapshots until you explicitly expire them.

See the [PyIceberg table history docs](https://py.iceberg.apache.org/api/#table-history) for full API reference.

### Rolling Back to a Previous Snapshot

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog('bmo', uri='postgresql+psycopg2://user:password@localhost:5432/bmo')
table = catalog.load_table('staging.staged_flights')

# List snapshots to find the target snapshot_id
for snap in table.history():
    print(snap.snapshot_id, snap.timestamp_ms)

# Roll back — updates the table's current snapshot pointer
table.manage_snapshots().rollback_to_snapshot(<snapshot_id>).commit()
```

After rolling back, downstream dbt models and Feast exports will read the rolled-back state on their next run. Trigger `bmo_dbt_assets` to recompute feature tables from the restored data.

> **Note:** Rolling back does not delete the newer data files — it only changes the current snapshot pointer. The newer files remain in S3 and can be restored by rolling forward again.

---

## VM Becomes Unresponsive During a Training Run

### Signs

- Cursor SSH drops mid-session with `Socket closed without exit code`
- Subsequent SSH attempts time out for 10–30 minutes
- VM eventually recovers on its own; Dagster run shows as failed or still running

### Cause

The Oracle ARM VM (1 OCPU / 6 GB RAM) has no swap by default. When DuckDB runs the
`mart_training_dataset` weather PIT joins it can exhaust available RAM, causing the
kernel to thrash trying to free pages. The VM becomes too busy to service SSH connections
until the OOM killer terminates the offending process.

### Confirm It Was OOM

After regaining SSH access:

```bash
dmesg | grep -i "oom\|killed process" | tail -20
free -h
```

### Add Swap (one-time fix, run on the VM)

```bash
# Create a 4 GB swap file on the boot volume
sudo fallocate -l 4G /swapfile

# Lock down permissions — kernel requires this before activating swap
sudo chmod 600 /swapfile

# Format the file as a swap area
sudo mkswap /swapfile

# Activate immediately for this session
sudo swapon /swapfile

# Persist across reboots
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

Verify with `free -h` — you should see 4 GB under the Swap row.

### Terminate the Stuck Run

Go to the Dagster UI → Runs → find the stuck run → click **Terminate**.
If the UI terminate button doesn't work, update the run status directly in Postgres:

```sql
UPDATE runs SET status = 'FAILURE'
WHERE status = 'STARTED'
  AND run_id = '<run-id>';
```

### Long-Term Fix

The Oracle A1.Flex free tier allows up to 4 OCPUs / 24 GB. Bumping to 2 OCPUs / 12 GB
eliminates the memory pressure entirely. Update `infra/terraform/oracle/variables.tf`:

```hcl
variable "vm_ocpus"     { default = 2 }
variable "vm_memory_gb" { default = 12 }
```

Then run `terraform apply` from `infra/terraform/oracle/`.

---

## Disaster Recovery

### Full Stack Rebuild from Scratch

Use this when the Oracle VM is lost or corrupted and needs to be reprovisioned.

#### 1. Provision a new VM with Terraform

```bash
cd infra/terraform
terraform init
terraform plan -var-file="terraform.tfvars" -out=tfplan
terraform apply tfplan
```

Terraform outputs the new VM IP. The Oracle Always Free tier uses a [reserved IP](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/reserved-ip.htm) so the IP is stable across VM replacements.

#### 2. Trigger the first deploy

cloud-init takes 2–5 minutes to install Docker, configure swap, and register the `bmo-compose` systemd unit. Then trigger the deploy workflow:

- GitHub → **Actions → Deploy → Run workflow**

The deploy script will detect that cloud-init hasn't finished and call `sudo cloud-init status --wait` before proceeding. It clones the repo, writes `.env` from GitHub secrets, and starts the compose stack.

#### 3. Initialize infrastructure (one-time after fresh provision)

After the stack is up, run these one-time setup steps:

```bash
ssh ubuntu@<NEW_VM_IP>
cd ~/ml-training-orchestrator

# Register Feast feature views against the R2 registry
$(set -a && . .env && set +a && uv run feast -c feature_repo apply)

# Initialize monitoring tables in Postgres
PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U $POSTGRES_USER -d $POSTGRES_DB \
  -f infra/postgres/create_monitoring_tables.sql
```

#### 4. Restore data from R2 backups

See [Restoring from R2 Backups](#restoring-from-r2-backups) below.

#### 5. Warm the online feature store

Trigger the full Feast pipeline from the Dagster UI:

1. Assets → select `feast_feature_export` + ancestors → **Materialize selected**.
2. Then materialize `feast_materialized_features` to push features to Upstash Redis.

### Restoring from R2 Backups

Cloudflare R2 is the authoritative store for all persistent data. The Oracle VM itself is stateless — all durable state lives in R2 (`staging` and `mlflow-artifacts` buckets) and in the Postgres volume.

#### What is in R2 and survives VM loss

| Path | Content |
| --- | --- |
| `s3://staging/iceberg/` | All Iceberg table data and metadata (flights, weather, features) |
| `s3://staging/datasets/` | Content-addressed training datasets (`data.parquet` + `card.json`) |
| `s3://staging/feast/` | Feast Parquet exports (overwritten hourly — not a true backup) |
| `s3://staging/predictions/` | Batch scoring outputs by date partition |
| `s3://staging/monitoring/` | Drift reports and metrics Parquet |
| `s3://mlflow-artifacts/` | MLflow run artifacts: model weights, Evidently reports, Optuna studies |

#### What is NOT in R2 and must be rebuilt

| Data | How to rebuild |
| --- | --- |
| Postgres: Dagster run history | Cannot be restored — start fresh; no operational impact |
| Postgres: MLflow experiment/run metadata | Partially restorable by pointing MLflow at the same `mlflow-artifacts` R2 bucket — MLflow will re-discover artifacts but run metadata (params, metrics) is lost |
| Postgres: Iceberg catalog (`iceberg_tables` schema) | Recreated automatically by PyIceberg on first staging asset run |
| DuckDB feature store (`bmo_features.duckdb`) | Rebuilt by running `bmo_dbt_assets` after Iceberg data is available |
| Redis online features | Rebuilt by running `feast_materialized_features` |

**Restoring the MLflow model registry:**

The model weights are in R2 under `s3://mlflow-artifacts/`. After a fresh Postgres, MLflow has no registry entries. Re-register the champion model:

```python
import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri('http://localhost:5000')
client = MlflowClient()

# Re-register using the artifact URI directly
model_version = mlflow.register_model(
    model_uri='s3://mlflow-artifacts/<experiment_id>/<run_id>/artifacts/model',
    name='bmo-flight-delay',
)
client.set_registered_model_alias('bmo-flight-delay', 'champion', model_version.version)
print(f'Registered version {model_version.version} as champion')
```

Replace `<experiment_id>/<run_id>` with the path you can browse in the R2 console or via:

```bash
aws s3 ls s3://mlflow-artifacts/ \
  --endpoint-url https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com \
  --recursive | grep 'model.ubj' | sort -k1 -r | head -5
```

See the [MLflow model registry docs](https://mlflow.org/docs/latest/model-registry.html) and [Cloudflare R2 docs](https://developers.cloudflare.com/r2/) for reference.
