# Getting Started

## Prerequisites

| Tool                                                                  | Version      | Notes                                |
| --------------------------------------------------------------------- | ------------ | ------------------------------------ |
| [Python](https://www.python.org/downloads/)                           | 3.11 – 3.12  | 3.13 not yet supported               |
| [uv](https://docs.astral.sh/uv/getting-started/installation/)         | latest       | replaces pip + venv                  |
| [Docker Desktop](https://docs.docker.com/get-started/get-docker/)     | latest       | runs Postgres, MinIO, Redis, MLflow  |
| [Make](https://www.gnu.org/software/make/)                            | any          | macOS: included with Xcode CLI tools |
| [Node.js](https://nodejs.org/) + [pnpm](https://pnpm.io/installation) | 18+ / latest | React frontend only                  |

## Local Development Setup

### 1. Clone & Install Dependencies

```bash
git clone https://github.com/scarlson1/ml-training-orchestrator.git
cd ml-training-orchestrator

# Install Python dependencies (all groups: dev, test, etc.)
uv sync --all-groups

# Install pre-commit hooks
uv run pre-commit install

# Install React dependencies (frontend only)
cd react && pnpm install && cd ..
```

### 2. Environment Configuration

```bash
cp .env.example .env
```

Open `.env` and set **at minimum**:

```bash
# Absolute path to the dagster_home/ directory in this repo
DAGSTER_HOME=/absolute/path/to/ml-training-orchestrator/dagster_home
```

All other defaults in `.env.example` work as-is for local development — MinIO, Postgres, and Redis all use the credentials configured in the Docker Compose stack.

Create `react/.env.local`

```env
AIRLABS_KEY=<API_KEY>
```

### 3. Start the Docker Stack

```bash
make compose-dev
```

This starts four services defined in [infra/compose/compose.dev.yml](../infra/compose/compose.dev.yml):

| Service  | Port        | Purpose                                                     |
| -------- | ----------- | ----------------------------------------------------------- |
| Postgres | 5432        | Dagster metadata, MLflow backend, Iceberg catalog           |
| MinIO    | 9000 / 9001 | S3-compatible object store (raw, staging, rejected buckets) |
| Redis    | 6379        | Feast online feature store                                  |
| MLflow   | 5000        | Experiment tracking UI                                      |

MinIO buckets (`raw`, `staging`, `rejected`) are created automatically by the `minio_init` container on first startup.

### 4. Initialize Infrastructure

```bash
# One-time: registers Feast feature views and entities
make feast-apply

# Required before every `dagster dev` — generates dbt's manifest.json
# (Dagster reads this file at import time; missing manifest = startup failure)
make dbt-bootstrap
```

`make dbt-bootstrap` runs two commands in sequence:

- `dbt deps` — installs dbt packages (equivalent to `pnpm install`)
- `dbt parse` — builds the dbt DAG and validates all model/source references

### 5. Run the Dagster UI

```bash
make dagster-dev
```

Open [http://localhost:3000](http://localhost:3000). The Assets tab shows the full asset graph — every node is a Software-Defined Asset with its dependencies drawn automatically.

**Optionally**, start the React frontend and FastAPI serving in separate terminals:

```bash
# Serving API (port 8080)
make serving-dev

# React dev server (port 5173)
cd react && pnpm dev
```

### 6. Run Your First Pipeline

Materialize assets in dependency order from the Dagster UI or CLI. Start with a single month to verify the full stack works before backfilling history.

#### Via the Dagster UI

1. Go to **Assets** → **View global asset graph**
2. Materialize dimension assets (no partition needed):
   - `raw_faa_airports` → `station_map` → `raw_openflights_routes`
   - `dim_airport` → `dim_route`
3. Materialize monthly partitioned assets (pick one month, e.g. `2024-01-01`):
   - `raw_bts_flights` → `staged_flights`
   - `raw_noaa_weather` → `staged_weather`
4. Materialize feature assets:
   - `feat_cascading_delay` (PySpark)
   - `bmo_dbt_assets` (all 15 dbt models — triggers automatically via `AutomationCondition.eager()`)
5. Materialize feature store assets:
   - `feast_feature_export` → `feast_materialized_features`
6. Build the training dataset and train a model:
   - `training_dataset` → `trained_model` → `registered_model`

#### Via the CLI

```bash
# Dimensions
uv run dg launch --assets raw_faa_airports
uv run dg launch --assets station_map
uv run dg launch --assets raw_openflights_routes
uv run dg launch --assets dim_airport
uv run dg launch --assets dim_route

# Monthly partitions (format: YYYY-MM-DD)
uv run dg launch --assets raw_bts_flights --partition 2024-01-01
uv run dg launch --assets staged_flights --partition 2024-01-01
uv run dg launch --assets raw_noaa_weather --partition 2024-01-01
uv run dg launch --assets staged_weather --partition 2024-01-01

# Features
uv run dg launch --assets feat_cascading_delay

# All dbt models
uv run dg launch --assets 'bmo_dbt_assets*'

# Feature store
uv run dg launch --assets feast_feature_export
uv run dg launch --assets feast_materialized_features

# Training
uv run dg launch --assets training_dataset
uv run dg launch --assets trained_model
uv run dg launch --assets registered_model
```

---

## Verifying the Setup

### Check Services Are Running

```bash
docker compose -f infra/compose/compose.dev.yml ps
```

All four services should show `running`. Verify individual UIs:

- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (user: `minioadmin` / pass: `minioadmin`)
- **MLflow UI**: [http://localhost:5000](http://localhost:5000)
- **Dagster UI**: [http://localhost:3000](http://localhost:3000)

### Run Unit Tests

```bash
make test
```

Integration tests (require the Docker stack to be running):

```bash
make test-int
```

Leakage guard test (plants a future value and asserts it is rejected):

```bash
make leakage
```

### Materialize a Sample Partition

After the full setup, run a quick end-to-end smoke test with a single partition:

```bash
# Ingest and stage one month
uv run dg launch --assets raw_bts_flights --partition 2024-01-01
uv run dg launch --assets staged_flights --partition 2024-01-01

# Verify the data landed in MinIO
# Open http://localhost:9001 → staging bucket → iceberg/staged_flights/
```

Verify PIT correctness (dbt singular test — should report 0 failures):

```bash
cd dbt_project && uv run dbt test --select int_flights_enriched --profiles-dir .
```

---

## Common Setup Issues

### MinIO / S3 Connection Failures

**Symptom:** `ConnectionRefusedError` or `NoSuchBucket` when materializing raw assets.

**Checks:**

1. Confirm MinIO is running: `docker compose -f infra/compose/compose.dev.yml ps minio`
2. Confirm `S3_ENDPOINT_URL=http://localhost:9000` in `.env`
3. Check that the `minio_init` container ran successfully and created the buckets:

   ```bash
   docker compose -f infra/compose/compose.dev.yml logs minio_init
   ```

   You should see `Bucket created successfully` for `raw`, `staging`, and `rejected`.

See the [MinIO docs](https://min.io/docs/minio/container/index.html) for container configuration reference.

### Postgres Catalog Errors

**Symptom:** `sqlalchemy.exc.OperationalError` or Iceberg `CatalogException` on startup or during staging.

**Checks:**

1. Confirm Postgres is running: `docker compose -f infra/compose/compose.dev.yml ps postgres`
2. Confirm env vars match the compose file defaults:

   ```bash
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=bmo
   POSTGRES_USER=bmo
   POSTGRES_PASSWORD=bmo
   ```

3. The Iceberg `iceberg_tables` schema is created automatically by PyIceberg on first use. If it's missing, the staging asset will create it.

See [Dagster Postgres docs](https://docs.dagster.io/guides/deploy/infrastructure/postgres) for metadata store configuration.

### dbt Manifest Not Found

**Symptom:** `FileNotFoundError: .../dbt_project/target/manifest.json` when running `make dagster-dev`.

**Fix:** Run `make dbt-bootstrap` before starting Dagster. This is required after every fresh checkout or branch switch:

```bash
make dbt-bootstrap  # runs dbt deps + dbt parse
make dagster-dev
```

See the [dbt parse docs](https://docs.getdbt.com/reference/commands/parse) for details on what `dbt parse` generates.

### Feast Registry Errors

**Symptom:** `FeatureStoreNotFound` or `Registry not found` when materializing `feast_feature_export`.

**Fix:** Run `make feast-apply` to register feature views, entities, and data sources:

```bash
make feast-apply
```

This must be re-run whenever [feature_repo/](../feature_repo/) definitions change. If using the SQLite registry (default for local dev, set by `FEAST_REGISTRY_PATH=data/registry.db`), confirm the `data/` directory is writable.

See the [Feast apply docs](https://docs.feast.dev/reference/feast-cli-commands#feast-apply) for registry management.

---

## Next Steps

- [architecture.md](architecture.md) — full system design and data flow
- [feature-engineering.md](feature-engineering.md) — dbt models, PySpark features, PIT correctness
- [feature-store.md](feature-store.md) — Feast offline/online store design
- [training.md](training.md) — XGBoost + Optuna HPO, evaluation gate checks
- [serving.md](serving.md) — FastAPI inference API, zero-downtime model swap
- [deployment.md](deployment.md) — Oracle Cloud + Cloudflare R2 production setup
- [runbooks.md](runbooks.md) — backfills, VM rebuilds, sensor management
