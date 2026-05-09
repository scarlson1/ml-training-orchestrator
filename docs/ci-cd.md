# CI/CD

## Overview

Four GitHub Actions workflows handle the full delivery pipeline:

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| `ci.yml` | Every push / PR | Lint, type-check, unit tests, Feast idempotency |
| `build-images.yml` | Push to `main` (path-filtered) | Build & push multi-arch Docker images to GHCR |
| `deploy.yml` | After `build-images.yml` succeeds | SSH deploy to Oracle VM |
| `evidently-reports.yml` | Daily at 10:00 UTC | Sync drift reports from R2 → GitHub Pages |

The normal flow on a `main` push is: `ci` runs in parallel with `build-images` (CI does not gate the build). Once `build-images` completes successfully, `deploy` triggers automatically.

---

## GitHub Actions Workflows

### `ci.yml` — Lint & Test

#### Lint Stage (ruff + mypy)

Runs on `ubuntu-latest`. Uses [astral-sh/setup-uv](https://github.com/astral-sh/setup-uv) to install `uv`, then:

```sh
uv sync --group dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src dagster_project
```

`mypy` runs in `--strict` mode (enforced via `.pre-commit-config.yaml`). The same ruff + mypy checks run as pre-commit hooks locally.

#### Unit Test Stage

Runs on `ubuntu-latest` with a full set of service env vars wired in (MinIO at `localhost:9000`, MLflow at `localhost:5000`, Postgres at `localhost:5432`, Redis at `localhost:6379`). No service containers are spun up by the workflow itself — tests that need live services are expected to mock or skip them.

```sh
uv sync --group dev --group training --group serving
uv run pytest tests/unit -q
```

#### Feast Apply Idempotency Check

Runs after lint passes (`needs: [lint]`). Spins up a Redis 7 service container (port 6379) and uses a file-based Feast registry (`file:///tmp/feast_ci`) so no MinIO is needed.

```sh
uv sync --frozen --no-dev --group feast
cd feature_repo && uv run feast apply
```

Then verifies that exactly the five expected feature views are registered: `origin_airport_features`, `dest_airport_features`, `carrier_features`, `route_features`, `aircraft_features`. Fails if any are missing.

#### CI Triggers

- `push` to `main`
- All `pull_request` events (no branch filter — runs on every PR)

---

### `build-images.yml` — Docker Image Builds

#### Multi-Arch Strategy (amd64 + arm64)

Uses a matrix over two images:

| Matrix name | Dockerfile |
| --- | --- |
| `dagster` | `infra/docker/dagster.Dockerfile` |
| `serving` | `infra/docker/serving.Dockerfile` |

> **Note:** `infra/docker/mlflow.Dockerfile` is **not** built by this workflow. It is a two-line file (`FROM ghcr.io/mlflow/mlflow:v3.11.1` + pip install of `psycopg2-binary` and `boto3`) that is built on the VM the first time `docker compose pull` runs via the `bmo-compose` systemd unit's `ExecStartPre`. See [Production Compose Stack](#production-compose-stack).

QEMU is set up for `arm64` emulation. Both platforms are built in a single `docker/build-push-action` step with `platforms: linux/amd64,linux/arm64`. This ensures the images run on the Oracle Cloud ARM VM without needing a separate ARM runner.

#### Caching

Registry-backed layer cache using `type=registry`:

```yaml
cache-from: type=registry,ref=ghcr.io/<owner>/bmo-<name>:cache
cache-to:   type=registry,ref=ghcr.io/<owner>/bmo-<name>:cache,mode=max
```

`mode=max` stores all intermediate layers, not just the final image. Rebuilds that only change source code (not dependencies) typically reuse the base + dependency layers and are significantly faster.

#### Pushing to GHCR

Uses `GITHUB_TOKEN` for authentication (automatically provided, no setup required). The job has `permissions: packages: write`.

Each image is tagged twice:

- `:latest` — always pulled by the production compose stack
- `:<git-sha>` — immutable tag used for rollbacks (`docker pull ghcr.io/<owner>/bmo-<name>:<sha>`)

OCI labels (`org.opencontainers.image.revision` and `.source`) are embedded so `docker inspect` shows the originating commit and repo.

#### Build Triggers

- `push` to `main` **and** one of the path filters matches:
  - `src/**`, `dagster_project/**`, `dbt_project/**`, `feature_repo/**`
  - `pyproject.toml`, `uv.lock`
  - `infra/docker/**`
  - `.github/workflows/build-images.yml`
- `workflow_dispatch` (manual trigger — required when you need a rebuild without a source change)

---

### `deploy.yml` — Production Deployment

Deploys the control-plane stack (Dagster, MLflow, serving API, Postgres, Caddy) to a single Oracle Cloud ARM VM running Docker Compose.

#### Prerequisites (Secrets, Oracle VM)

Required GitHub secrets and variables:

| Name | Type | Purpose |
| --- | --- | --- |
| `ORACLE_VM_IP` | var | Public IP of the Oracle VM |
| `ORACLE_SSH_PRIVATE_KEY` | secret | SSH private key for the `ubuntu` user |
| `GITHUB_TOKEN` | auto | GHCR pull auth (auto-provided) |
| `S3_ACCESS_KEY_ID` | secret | Cloudflare R2 access key |
| `S3_SECRET_ACCESS_KEY` | secret | Cloudflare R2 secret key |
| `CLOUDFLARE_ACCOUNT_ID` | secret | R2 endpoint prefix |
| `UPSTASH_REDIS_URL` | secret | Feast online store (Upstash Redis) |
| `POSTGRES_PASSWORD` | secret | Postgres superuser password |
| `POSTGRES_DB` | var | Database name |
| `POSTGRES_USER` | var | Database user |
| `MLFLOW_AUTH_ADMIN_USERNAME` | var | MLflow basic-auth admin user |
| `MLFLOW_AUTH_ADMIN_PASSWORD` | secret | MLflow basic-auth admin password |
| `MLFLOW_FLASK_SERVER_SECRET_KEY` | secret | MLflow Flask session secret |
| `ADMIN_TOKEN` | secret | Serving API admin token |
| `SERVING_API_URL` | var | Public URL of the serving API |
| `SERVING_DOMAIN` | var | Domain for Caddy TLS cert |
| `CADDY_EMAIL` | var | Let's Encrypt registration email |
| `DAGSTER_HASHED_PASSWORD` | secret | Dagster UI hashed password |

#### Deployment Steps

The deploy job SSHes into the VM using [appleboy/ssh-action](https://github.com/appleboy/ssh-action) and runs a `set -euo pipefail` script:

1. **Clone or pull** — clones the repo on first deploy; on subsequent deploys runs `git fetch origin main && git reset --hard origin/main`.
2. **Write `.env`** — renders all secrets and variables into a `.env` file at repo root using a heredoc. GitHub Actions resolves `${{ ... }}` expressions before the script is sent over SSH, so the values arrive as literal strings. The file is `chmod 600` immediately after.
3. **Authenticate with GHCR** — `sudo docker login ghcr.io` using `GITHUB_TOKEN`. `sudo` is required because the `ubuntu` user's Docker group membership is only active in login shells; `ssh-action` opens a non-login session.
4. **Prune dangling layers** — `sudo docker system prune -f` frees disk from previous deploys before pulling.
5. **Pre-pull the image** — pulls `bmo-dagster:latest` before the restart so the service downtime is minimal.
6. **Start or restart** — uses the `bmo-compose` systemd service (see below).

#### Waiting for cloud-init

On the very first provision, the VM may still be installing Docker when the deploy runs. The script checks for `docker` in `$PATH` and if absent runs `sudo cloud-init status --wait` (up to the cloud-init timeout, typically a few minutes) before proceeding.

#### `bmo-compose` systemd Service Restart

The compose stack is managed by a `bmo-compose` systemd unit on the VM. The deploy script detects whether the service is already active:

```bash
if sudo systemctl is-active --quiet bmo-compose; then
  sudo systemctl restart bmo-compose --no-block
else
  sudo systemctl start bmo-compose --no-block
fi
```

`--no-block` returns immediately; the health poll below confirms the stack came up.

After issuing the restart, the script polls `http://localhost:3000/server_info` every 5 seconds for up to 5 minutes (60 × 5 s). Dagster's parse + dbt deps + JVM init takes roughly 3.5 minutes cold. The job exits 0 on first successful response, 1 if the timeout is exceeded.

#### Rollback on Failure

Images are tagged with both `:latest` and `:<git-sha>`. To roll back to a specific commit:

```bash
# On the VM:
sudo docker pull ghcr.io/<owner>/bmo-dagster:<previous-sha>
sudo docker tag ghcr.io/<owner>/bmo-dagster:<previous-sha> ghcr.io/<owner>/bmo-dagster:latest
sudo systemctl restart bmo-compose
```

Then re-run the deploy workflow against the previous commit (or `workflow_dispatch` after reverting `main`).

#### Deploy Triggers

- `workflow_run` on `Build & Push Docker Images` completing with `conclusion == 'success'` on `main`
- `workflow_dispatch` — required for the first deploy, and for re-deploys without a code change (e.g., secret rotation, config-only `.env` updates)

---

### `evidently-reports.yml` — Drift Report Publishing

#### Generating Reports

Drift reports are HTML files generated by the Dagster monitoring assets and written to `s3://staging/monitoring/reports/<date=YYYY-MM-DD>/drift_report.html` (Cloudflare R2). This workflow does **not** generate them — it only publishes pre-existing reports.

#### Publishing to GitHub Pages

Runs daily at **10:00 UTC** and on `workflow_dispatch`.

The manual trigger accepts an optional `date_prefix` input (e.g., `date=2024-06`) to sync only reports from a specific month — useful for backfilling historical reports without re-syncing everything.

Steps:

1. **Configure AWS CLI** — sets R2 credentials (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`) via `aws configure`. The endpoint URL is passed as `S3_ENDPOINT_URL` at runtime.
2. **Sync from R2** — `aws s3 sync` pulls all `*/drift_report.html` files from `s3://staging/monitoring/reports/` into `docs/evidently-reports/`. The `--delete` flag removes local reports that no longer exist in R2.
3. **Generate index** — an inline Python script builds a minimal HTML index page (`docs/evidently-reports/index.html`) listing all reports newest-first with clickable links.
4. **Deploy to GitHub Pages** — [peaceiris/actions-gh-pages@v3](https://github.com/peaceiris/actions-gh-pages) pushes the `docs/evidently-reports/` directory to the `gh-pages` branch at `destination_dir: evidently-reports`. `keep_files: true` prevents clobbering other content on the `gh-pages` branch.

The published index is accessible at `https://<owner>.github.io/<repo>/evidently-reports/`.

---

## Production Compose Stack

The deploy workflow does not run `docker compose up` directly — it writes `.env` and then delegates lifecycle management to the `bmo-compose` systemd service, which runs `infra/compose/compose.prod.yml`.

### Services

| Service | Image | Memory limit | Public access |
| --- | --- | --- | --- |
| `postgres` | `postgres:17` | 256 MB | none (loopback `127.0.0.1:5432`) |
| `mlflow` | built from `infra/docker/mlflow.Dockerfile` on the VM | 1 GB | Caddy → `mlflow.$VM_IP.sslip.io` |
| `dagster` | `ghcr.io/<owner>/bmo-dagster:latest` | 4.5 GB | Caddy → `dagster.$VM_IP.sslip.io` (basic auth) |
| `serving` | `ghcr.io/<owner>/bmo-serving:latest` | 1 GB | Caddy → `$SERVING_DOMAIN` |
| `caddy` | `caddy:alpine` | 256 MB | `:80` (ACME), `:443` (HTTPS) |

Only `caddy` exposes public ports. `dagster` additionally binds to `127.0.0.1:3000` so the deploy health check and the VM keepalive cron can reach it locally without going through Caddy.

The `mlflow` image is **not** built by CI. `infra/docker/mlflow.Dockerfile` is a two-line file that extends the upstream `ghcr.io/mlflow/mlflow:v3.11.1` image with `psycopg2-binary` and `boto3`. Docker Compose builds it on the VM the first time `ExecStartPre` runs `docker compose pull`.

### Networking

All five services share the default Compose network. Internal traffic uses Docker service names (`postgres`, `mlflow`, `dagster`, `serving`). `.env` sets `POSTGRES_HOST=postgres` and `MLFLOW_TRACKING_URI=http://mlflow:5000`; the compose file's per-service `environment:` blocks override any localhost-style defaults that `.env` might carry.

### Named Volumes

| Volume | Purpose | Why it must persist |
| --- | --- | --- |
| `postgres_data` | Postgres WAL and data files | — |
| `dagster_home` | Dagster run history, compute logs, DuckDB feature store | Mounted read-only into `serving` so both containers share one DuckDB file |
| `mlflow_auth` | MLflow basic-auth SQLite DB (user accounts) | Recreated DB means all users are lost on restart |
| `caddy_data` | Let's Encrypt TLS certificates | [Rate limit](https://letsencrypt.org/docs/rate-limits/): 5 duplicate certs per domain per week — losing this volume exhausts the limit quickly |
| `caddy_config` | Caddy internal config cache | — |

### Dagster Container Startup Time

The `dagster` container takes roughly 3.5 minutes to become healthy on a cold start. The `dagster-entrypoint.sh` script runs before `dagster dev`:

1. `dbt deps` — downloads dbt packages (e.g. `dbt_utils`) declared in `packages.yml`. Runs on every startup because package state is not baked into the image.
2. `dbt parse` — resolves `ref()`/`source()` calls and writes `dbt_project/target/manifest.json`. Dagster's `@dbt_assets` decorator reads this file at Python import time to register one asset per dbt model; if it's missing or stale, Dagster crashes before serving any requests.
3. JVM startup — `dagster dev` imports PySpark assets, which starts a JVM gateway process.

The deploy health poll (60 × 5 s) accounts for this. See [`infra/docker/dagster-entrypoint.sh`](../infra/docker/dagster-entrypoint.sh) for details.

---

## Caddy Reverse Proxy & TLS

Caddy (`infra/docker/Caddyfile`) is the sole public-facing container and terminates TLS for all three services using [automatic HTTPS](https://caddyserver.com/docs/automatic-https). Virtual host domains are injected from `.env`:

| `.env` variable | Example value | Proxied to |
| --- | --- | --- |
| `SERVING_DOMAIN` | `1.2.3.4.sslip.io` | `serving:8080` |
| `DAGSTER_DOMAIN` | `dagster.1.2.3.4.sslip.io` | `dagster:3000` |
| `MLFLOW_DOMAIN` | `mlflow.1.2.3.4.sslip.io` | `mlflow:5000` |

**DNS via sslip.io:** [sslip.io](https://sslip.io) is a public wildcard DNS service — any hostname that encodes an IP address (e.g. `dagster.1.2.3.4.sslip.io`) resolves to that IP. No domain registrar or DNS record management is needed.

**TLS:** Caddy obtains and renews Let's Encrypt certificates automatically using [ACME HTTP-01 challenge](https://letsencrypt.org/docs/challenge-types/#http-01-challenge) over port 80. Certificates are stored in the `caddy_data` named volume; see [Named Volumes](#named-volumes) for why this volume must persist.

**Dagster basic auth:** The Dagster virtual host adds HTTP basic auth in the Caddyfile using `DAGSTER_USER` and `DAGSTER_HASHED_PASSWORD` (bcrypt). This is independent of any Dagster-native auth.

**CORS for the serving API:** Caddy handles CORS preflight (`OPTIONS`) requests before they reach the FastAPI upstream and responds with the appropriate headers. Allowed origins are the Vercel deployment domains (`ml-training-orchestrator.vercel.app` and `ml-training-orchestrator-*.vercel.app`). CORS headers are also attached to Caddy's own error responses (e.g., 503 when `serving` is down), so the browser sees the real status code rather than a misleading CORS error.

---

## VM Provisioning (Oracle Cloud, cloud-init)

The Oracle Cloud ARM VM is defined in `infra/terraform/oracle/`. Terraform provisions the compute instance and passes `cloud-init.sh` as user data; it runs once on first boot as root.

### What `cloud-init.sh` Does

1. **Waits for the apt lock** — Ubuntu starts `unattended-upgrades` immediately on boot. The script stops it and polls `/var/lib/dpkg/lock-frontend` before touching apt.
2. **Installs Docker from the [official apt repository](https://docs.docker.com/engine/install/ubuntu/)** — the Ubuntu distro package ships an outdated version. The official repo provides `docker-compose-plugin` v2 and `docker-buildx-plugin`.
3. **Installs [uv](https://docs.astral.sh/uv/)** — available on the VM for running Python code directly (e.g., debugging `feast apply` or `dbt parse`) without activating a virtualenv.
4. **Allocates 4 GB of swap** — Oracle Always Free ARM VMs have no swap by default. Without it, DuckDB training jobs exhaust RAM and trigger the OOM killer, making the VM unresponsive for minutes.
5. **Creates a VM keepalive cron** — Oracle [reclaims idle Always Free VMs](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm) after approximately 7 days of inactivity. A cron job (`*/30 * * * *`) pings `http://localhost:3000/server_info` every 30 minutes to prevent reclamation.
6. **Creates and enables the `bmo-compose` systemd unit** — but does **not** start it. The repo is not cloned at cloud-init time. The GitHub Actions deploy workflow starts it on first deploy.

### `bmo-compose` Systemd Unit

The unit file written to `/etc/systemd/system/bmo-compose.service` manages the Compose stack lifecycle:

| Directive | Value |
| --- | --- |
| `WorkingDirectory` | `/home/ubuntu/ml-training-orchestrator` |
| `EnvironmentFile` | `/home/ubuntu/ml-training-orchestrator/.env` |
| `ExecStartPre` | `docker compose -f infra/compose/compose.prod.yml pull --quiet` |
| `ExecStart` | `docker compose -f infra/compose/compose.prod.yml up --remove-orphans` |
| `ExecStop` | `docker compose -f infra/compose/compose.prod.yml down` |
| `Restart` | `on-failure`, 30 s backoff |

The `ExecStartPre` pull means that `sudo systemctl restart bmo-compose` is sufficient to deploy a new image after it has been pushed to GHCR — the unit pulls all updated images before starting.

### First-Deploy Sequence

On a brand-new VM the order of operations is:

1. Terraform applies and provisions the VM.
2. cloud-init runs (Docker install, swap, systemd unit). Takes 2–5 minutes.
3. Trigger the deploy workflow manually via `workflow_dispatch`.
4. The deploy script finds Docker missing → calls `sudo cloud-init status --wait`.
5. Script clones the repo, writes `.env`, authenticates with GHCR, prunes old layers, pre-pulls `bmo-dagster:latest`, starts `bmo-compose`.
6. Health poll confirms Dagster is up (`http://localhost:3000/server_info`).

---

## Required GitHub Secrets

See the table in [Prerequisites (Secrets, Oracle VM)](#prerequisites-secrets-oracle-vm) for the full list. A few notes:

- `GITHUB_TOKEN` is automatically provided by Actions — do not add it manually.
- `DAGSTER_HASHED_PASSWORD` must be a bcrypt hash. Generate one with: `python -c "import bcrypt; print(bcrypt.hashpw(b'yourpassword', bcrypt.gensalt()).decode())"`.
- `S3_ENDPOINT_URL` is used as a **variable** (not secret) in `evidently-reports.yml` — add it under *Settings → Variables → Actions* as well as *Secrets* if needed.

---

## Branch & Merge Strategy

- `main` is the production branch. Merging to `main` is the deployment trigger.
- All development happens on feature branches; open a PR against `main`.
- CI must pass before merging (lint + tests + feast-apply).
- `build-images` does **not** run on PRs — only on pushes to `main`. This keeps PR checks fast.
- There is no staging environment. `main` = production.

---

## Making a Change That Requires a New Docker Build

The `build-images` workflow only runs when files under its `paths` filter change. If your change touches only `docs/`, `tests/`, or other non-image paths, no new image will be built and `deploy` will not trigger.

If you need to force a rebuild (e.g., rotating a base image, bumping a dependency not tracked by a path filter):

1. Touch `infra/docker/dagster.Dockerfile` or `serving.Dockerfile` with a no-op change, **or**
2. Go to *Actions → Build & Push Docker Images → Run workflow* and trigger manually.

After the build completes, `deploy` will trigger automatically. Alternatively trigger `deploy` manually with *Actions → Deploy → Run workflow*.

---

## Adding a New Workflow

1. Create `.github/workflows/<name>.yml`.
2. If it needs to write to GHCR or `gh-pages`, add the appropriate `permissions` block — the repo default is read-only.
3. Never hardcode secrets in workflow files. Use `${{ secrets.NAME }}` or `${{ vars.NAME }}`.
4. If the workflow is a deployment step that should run after `build-images`, use `workflow_run` as the trigger (see `deploy.yml` for the pattern).

---

## Debugging a Failed Workflow Run

### Reading GitHub Actions Logs

1. Go to *Actions* tab → select the failed run.
2. Click the failed job name to expand it.
3. Click the failed step to see its output.
4. For multiline script steps (like the deploy SSH script), look for the `set -euo pipefail` error line — it will indicate which command failed and the exit code.

### Re-Running a Failed Job

On the failed run page, click *Re-run jobs → Re-run failed jobs*. This re-runs only the failed job, not the entire workflow. Useful after fixing a transient network error or after updating a secret.

### SSH Debugging on Oracle VM

If the deploy health check times out or the compose stack fails to start:

```bash
ssh ubuntu@<ORACLE_VM_IP>

# Check systemd service status
sudo systemctl status bmo-compose

# Tail compose logs
cd ~/ml-training-orchestrator
sudo docker compose -f infra/compose/compose.prod.yml logs --tail=100 -f

# Check individual service
sudo docker compose -f infra/compose/compose.prod.yml logs dagster --tail=50

# Manually restart
sudo systemctl restart bmo-compose
```

Common failure modes:

- **GHCR pull fails** — the `GITHUB_TOKEN` used during deploy is scoped to that run. If the image is private and the token expired, trigger a fresh deploy via `workflow_dispatch`.
- **Postgres healthcheck fails** — check disk space (`df -h`). Postgres refuses to start if the data volume is full.
- **Dagster parse error** — a code change introduced a Python import error. Check `docker logs bmo_dagster`.
- **cloud-init not finished** — only on first provision. SSH in and run `sudo cloud-init status --wait` manually, then re-trigger the deploy.
