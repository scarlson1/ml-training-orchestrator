"""
Dagster Definitions object — the single entrypoint loaded by `dagster dev`.

Everything Dagster needs to know about the project is registered here:
assets, jobs, sensors, schedules, resources. If it's not in Definitions,
the UI won't show it and the daemon won't run it.
"""

from dotenv import load_dotenv

from dagster_project.schedules.feast_hourly import feast_hourly_schedule, feast_materialize_job

load_dotenv()  # loads .env from cwd — no-op if already set in environment

from dagster import (  # noqa: E402 (import top of file exception)
    Definitions,
    define_asset_job,
)
from dagster_dbt import DbtCliResource  # noqa E402

from dagster_project.asset_checks.schema_checks import (  # noqa: E402
    check_dim_airport,
    check_dim_route,
    check_staged_flights_nulls,
    check_staged_flights_schema_evolution,
    check_staged_weather_nulls,
)
from dagster_project.assets.feast_materialization import (  # noqa: E402
    feast_feature_export,
    feast_materialized_features,
)
from dagster_project.assets.features_dbt import DBT_PROJECT_DIR, bmo_dbt_assets  # noqa: E402
from dagster_project.assets.features_python import feat_cascading_delay  # noqa: E402
from dagster_project.assets.raw import (  # noqa: E402
    raw_bts_flights,
    raw_faa_airports,
    raw_noaa_weather,
    raw_openflights_routes,
    station_map,
)
from dagster_project.assets.staging import (  # noqa: E402
    dim_airport,
    dim_route,
    staged_flights,
    staged_weather,
)
from dagster_project.assets.training import (  # noqa: E402
    registered_model,
    trained_model,
    training_dataset,
)
from dagster_project.sensors.bts_new_month import bts_new_month_sensor  # noqa: E402

# A job is a named, executable subset of the asset graph.
# The sensor targets this job by name to kick off a single BTS partition run.
# partitions_def must match the asset's own definition so Dagster knows
# which partition to run when the sensor yields a partition_key.
ingest_bts_month_job = define_asset_job(
    name='ingest_bts_month',  # match name in sensor decorator
    selection=[raw_bts_flights, raw_noaa_weather],
    # partitions_def=MonthlyPartitionsDefinition(start_date='2018-01-01'),
)

# registers entities with cli ??
defs = Definitions(
    assets=[
        raw_faa_airports,
        raw_openflights_routes,
        station_map,
        raw_bts_flights,
        raw_noaa_weather,
        dim_airport,
        dim_route,
        staged_flights,
        staged_weather,
        feat_cascading_delay,
        bmo_dbt_assets,
        feast_feature_export,
        feast_materialized_features,
        training_dataset,
        trained_model,
        registered_model,
    ],
    asset_checks=[
        check_staged_flights_nulls,
        check_staged_flights_schema_evolution,
        check_staged_weather_nulls,
        check_dim_airport,
        check_dim_route,
    ],
    jobs=[ingest_bts_month_job, feast_materialize_job],
    schedules=[feast_hourly_schedule],
    sensors=[bts_new_month_sensor],
    resources={
        # Key ('dbt') must match the parameter name in bmo_dbt_assets(context, dbt: DbtCliResource)
        'dbt': DbtCliResource(project_dir=str(DBT_PROJECT_DIR))
    },
)
