.PHONY: setup lint type test test-int test-det leakage dagster-dev serving-dev dbt dbt-docs dbt-deps dbt-parse feast-apply reproduce dbt-build

ENV := set -a && . .env && set +a &&

setup:
	uv sync --all-groups
	uv run pre-commit install

lint:
	uv run ruff check .
	uv run ruff format --check .

type:
	uv run mypy src dagster_project

test:
	uv run pytest tests/unit -q

test-int:
	uv run pytest tests/integration -q -m integration

test-det:
	uv run pytest tests/determinism -q -m determinism

leakage:  # must pass; runs the planted-future-value test
	uv run pytest tests/integration/test_leakage_planted_value.py -q

dagster-dev:
	$(ENV)uv run dagster dev -m dagster_project.definitions

serving-dev:
	$(ENV)uv run uvicorn bmo.serving.api:app --reload --port 8080

dbt-build:
	$(ENV)cd dbt_project && uv run dbt build --profiles-dir .

dbt:
	$(ENV)cd dbt_project && uv run dbt build --profiles-dir .

dbt-docs:
	$(ENV)cd dbt_project && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .

# run before dbt parse (parse validates {{ ref() }} and {{ source() }} references including macros from dbt_utils) => run `make dbt-bootstrap`
dbt-deps:
	$(ENV)cd dbt_project && uv run dbt deps --profiles-dir .

dbt-parse:
	$(ENV)cd dbt_project && uv run dbt parse --profiles-dir .

# dbt-bootstrap must run before `dagster dev` — it generates target/manifest.json
# which @dbt_assets reads at import time. If manifest.json doesn't exist,
# dagster dev will fail with a FileNotFoundError.
dbt-bootstrap: dbt-deps dbt-parse

# feast-apply:
# 	cd feature_repo && uv run feast apply

# After (runs from project root, passes config explicitly):
feast-apply:
	$(ENV)uv run feast -c feature_repo apply

feast-teardown:
	$(ENV)uv run feast -c feature_repo teardown

reproduce:
	$(ENV)uv run python -m bmo.training.reproduce $(RUN_ID)

serving-dev:
	uv run uvicorn bmo.serving.api:app --reload --port 8080

serving-build:
	docker build -f infra/docker/serving.Dockerfile -t bmo-serving:local .

serving-run:
	docker run --rm \
	  --env-file .env \
	  -p 8080:8080 \
	  bmo-serving:local

fly-deploy:
	fly deploy --config fly.toml

fly-reload:
	curl -s -X POST https://bmo-flight-delay.fly.dev/admin/reload \
	  -H "Authorization: Bearer $$(fly secrets list | grep ADMIN_TOKEN)" \
	  | jq .

test-serving:
	uv run pytest tests/unit/test_serving_feature_client.py -q
