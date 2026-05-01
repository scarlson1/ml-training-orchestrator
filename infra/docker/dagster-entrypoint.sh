#!/bin/bash
# Bootstrap the dbt manifest, then start Dagster.
#
# WHY THIS EXISTS
# @dbt_assets (features_dbt.py) reads dbt_project/target/manifest.json at Python
# import time to register one Dagster asset per dbt model. If manifest.json is
# missing or stale, Dagster crashes on startup before serving any requests.
#
# WHY NOT IN THE DOCKERFILE
# manifest.json embeds source() and ref() paths that are resolved against env vars
# (DUCKDB_PATH, ICEBERG_CATALOG_URI, etc.) which are only available at runtime.
# Generating it at build time would bake in localhost defaults that break in prod.
#
# WHAT dbt parse DOES (AND DOESN'T DO)
# dbt parse resolves ref()/source() references and writes manifest.json.
# It does NOT execute any SQL or touch real data. The throwaway DuckDB file
# satisfies the adapter connection check without touching the real feature store.
set -euo pipefail

echo "Bootstrapping dbt manifest..."
cd /app/dbt_project

# dbt deps installs dbt packages (e.g. dbt_utils) declared in packages.yml into
# dbt_packages/. Required before parse because dbt resolves macros from packages.
# Runs on every startup so the image doesn't need to bake in package state.
DUCKDB_PATH=/tmp/dbt_parse.duckdb dbt deps --profiles-dir . --quiet

# dbt parse writes target/manifest.json. The throwaway DuckDB path keeps parse
# isolated from the real /dagster_home/bmo_features.duckdb feature store.
DUCKDB_PATH=/tmp/dbt_parse.duckdb dbt parse --profiles-dir . --quiet

echo "dbt manifest ready."
cd /app

# exec replaces this shell with the Dagster process so SIGTERM from
# `docker stop` goes directly to Dagster instead of bash. Without exec,
# Docker waits the full 10s stop timeout before force-killing the container.
exec dagster dev -m dagster_project.definitions --host 0.0.0.0 --port 3000
