from __future__ import annotations

from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

# The @dbt_assets decorator reads dbt_project/target/manifest.json and registers every dbt model as a Dagster asset. Dependencies between dbt models (via {{ ref() }}) are inferred automatically.

# The DagsterDbtTranslator handles the cross-boundary problem: dbt knows stg_flights depends on source iceberg_staging.staged_flights, but Dagster needs to know that this source is the Python asset staged_flights. Without the translator, the DAG edge from Python staging → dbt features is broken.

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / 'dbt_project'
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

# maps dbt source node identifiers -> Dagster Python asset keys
# every entry creates a DAG edge: Python asset -> dbt model
_SOURCE_TO_ASSET_KEY: dict[tuple[str, str], AssetKey] = {
    ('iceberg_staging', 'staged_flights'): AssetKey('staged_flights'),
    ('iceberg_staging', 'staged_weather'): AssetKey('staged_weather'),
    ('iceberg_staging', 'dim_airport'): AssetKey('dim_airport'),
    ('iceberg_staging', 'dim_route'): AssetKey('dim_route'),
    ('iceberg_staging', 'feat_cascading_delay'): AssetKey('feat_cascading_delay'),
}


class BmoDbtTranslator(DagsterDbtTranslator):
    """
    Overrides source node → Dagster asset key resolution.

    Called for every node in the dbt manifest (models, sources, seeds, tests).
    For source nodes, return the matching Python asset key so Dagster draws
    the cross-boundary edge in the asset graph. For everything else, fall
    through to the default behavior (model name → asset key).
    """

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props.get('resource_type') == 'source':
            source_name = dbt_resource_props['source_name']
            name = dbt_resource_props['name']
            asset_key = _SOURCE_TO_ASSET_KEY.get((source_name, name))
            if asset_key is not None:
                return asset_key
            return super().get_aset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=BmoDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def bmo_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> None:
    # `dbt build` runs models + tests + seeds in DAG order
    # yields Dagster events (AssetMaterialization, AssetCheckResult) as dbt progresses
    yield from dbt.cli(['build'], context=context).stream()


# dbt: DbtCliResource must exactly match the key used in definitions.py resources dict ('dbt'). If the names don't match, Dagster raises a missing resource error at startup.

# Partition mismatch: staged_flights is a MonthlyPartitionsDefinition asset; bmo_dbt_assets is unpartitioned. Dagster will show a warning that a partitioned asset feeds an unpartitioned one — this is expected and correct. The dbt feature models read the entire Iceberg table (all months) in one pass. You don't want dbt to run 84 times (once per month). Suppress the warning by adding `non_argument_deps=False`
