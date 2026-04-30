FROM python:3.11-slim AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy lockfile first — this layer is cached until pyproject.toml/uv.lock changes.
# Without --no-install-project, uv would also install bmo here, before src/ exists.
# .python-version is gitignored (it's for local tooling only).
# Python version is already pinned by the FROM python:3.11-slim base image.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev \
    --group dagster \
    --group dbt \
    --group training \
    --group feast \
    --group iceberg \
    --group monitoring \
    --no-install-project

# Copy source and install bmo package
COPY src/ ./src/
COPY dagster_project/ ./dagster_project/
COPY dbt_project/ ./dbt_project/
COPY feature_repo/ ./feature_repo/

RUN uv sync --frozen --no-dev \
    --group dagster \
    --group dbt \
    --group training \
    --group feast \
    --group iceberg \
    --group monitoring

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Java 17 is required by PySpark / Spark 4.0 for its JVM gateway process.
# default-jdk-headless installs OpenJDK 17 on Debian bookworm and creates the
# arch-neutral /usr/lib/jvm/default-java symlink used by JAVA_HOME below.
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv      /app/.venv
COPY --from=builder /app/src        /app/src
COPY --from=builder /app/dagster_project  /app/dagster_project
COPY --from=builder /app/dbt_project      /app/dbt_project
COPY --from=builder /app/feature_repo     /app/feature_repo

COPY infra/docker/dagster-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
ENV DAGSTER_HOME=/dagster_home
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN mkdir -p /dagster_home

EXPOSE 3000

ENTRYPOINT ["/entrypoint.sh"]
