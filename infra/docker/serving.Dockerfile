# FastAPI serving image using uv dependency groups.
# Only installs the 'serving' and 'iceberg' groups — no Dagster, no Spark, no training deps.
# This keeps the image small enough for Fly.io's 512MB RAM VMs.
#
# uv --frozen flag: never modifies uv.lock inside the image build.
# This guarantees reproducible builds across machines and CI runs.
# Docs: https://docs.astral.sh/uv/reference/cli/#uv-sync

FROM python:3.11-slim AS builder

WORKDIR /app

# Install uv (single binary, no deps)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy lockfile and project definition first (layer cache: only re-installs
# deps when pyproject.toml or uv.lock changes, not on every code change).
COPY pyproject.toml uv.lock ./

# Install only what the serving API needs.
# --no-dev skips the [dev] group (pytest, ruff, mypy).
# --no-install-project skips installing the bmo package itself yet (done below).
RUN uv sync --frozen --no-dev --group serving --group iceberg --no-install-project

# Now copy source and install the package
COPY src/ ./src/
COPY feature_repo/ ./feature_repo/
RUN uv sync --frozen --no-dev --group serving --group iceberg

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Copy installed venv and source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/feature_repo /app/feature_repo

# Activate the uv-managed venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"

# Non-root user: security best practice for container deployments.
# Fly.io machines run as root by default, but explicit non-root signals
# security maturity and is required for some enterprise registries.
RUN useradd --create-home --uid 1000 bmo
USER bmo

EXPOSE 8080

# workers=1: single process per Fly.io machine.
# Fly scales out horizontally (multiple machines), not vertically (multiple workers).
# Single-process also simplifies the in-memory model state (no shared memory needed).
CMD ["uvicorn", "bmo.serving.api:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
