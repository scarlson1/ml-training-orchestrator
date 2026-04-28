#!/bin/bash
# Bootstrap dbt manifest, then start Dagster.
#
# @dbt_assets reads dbt_project/target/manifest.json at Python import time.
# If the file doesn't exist, `dagster dev` crashes before serving any requests.
# We generate it here against an empty throwaway DuckDB file — dbt parse only
# resolves ref()/source() references, it doesn't execute any SQL.
set -euo pipefail

echo "Bootstrapping dbt manifest..."
cd /app/dbt_project
DUCKDB_PATH=/tmp/dbt_parse.duckdb dbt deps --profiles-dir . --quiet
DUCKDB_PATH=/tmp/dbt_parse.duckdb dbt parse --profiles-dir . --quiet
echo "dbt manifest ready."

cd /app

# exec replaces this shell with the dagster process so signals (SIGTERM on
# docker stop / systemctl stop) go directly to dagster, not to bash.
# Without exec, docker stop hangs 10s waiting for bash to forward the signal.
exec dagster dev -m dagster_project.definitions --host 0.0.0.0 --port 3000
