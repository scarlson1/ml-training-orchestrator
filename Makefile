.PHONY: setup lint type test test-int test-det leakage dagster-dev serving-dev dbt dbt-docs feast-apply reproduce

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
	uv run dagster dev -m dagster_project.definitions

serving-dev:
	uv run uvicorn bmo.serving.api:app --reload --port 8080

dbt:
	cd dbt_project && uv run dbt build --profiles-dir .

dbt-docs:
	cd dbt_project && uv run dbt docs generate --profiles-dir . && uv run dbt docs serve --profiles-dir .

feast-apply:
	cd feature_repo && uv run feast apply

reproduce:
	uv run python -m bmo.training.reproduce $(RUN_ID)