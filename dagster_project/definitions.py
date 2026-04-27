"""
Dagster Definitions object — the single entrypoint loaded by `dagster dev`.

Everything Dagster needs to know about the project is registered here:
assets, jobs, sensors, schedules, resources. If it's not in Definitions,
the UI won't show it and the daemon won't run it.
"""

from dotenv import load_dotenv

from bmo.common.config import settings
from dagster_project.asset_checks.evaluation_gate import (
    check_auc_gate,
    check_calibration,
    check_leakage_sentinel,
    check_slice_parity,
)
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
    FEATURE_REPO_DIR,
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
from dagster_project.resources.duckdb_resource import DuckDBResource  # noqa: E402
from dagster_project.resources.feast_resource import FeastResource  # noqa: E402
from dagster_project.resources.mlflow_resource import MLflowResource  # noqa: E402
from dagster_project.resources.s3_resource import S3Resource  # noqa: E402
from dagster_project.schedules.nightly_retain import (  # noqa: E402
    nightly_retrain_schedule,
    retrain_job,
)
from dagster_project.sensors.bts_new_month import bts_new_month_sensor  # noqa: E402
from dagster_project.sensors.drift_retrain_sensor import drift_retrain_sensor  # noqa: E402
from dagster_project.sensors.run_failure_sensor import run_failure_sensor_fn  # noqa: E402

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
        # Raw layer (group: 'raw')
        raw_faa_airports,
        raw_openflights_routes,
        station_map,
        raw_bts_flights,
        raw_noaa_weather,
        # Staging layer (group: 'staging')
        dim_airport,
        dim_route,
        staged_flights,
        staged_weather,
        # Features layer (group: 'features' + 'feast')
        feat_cascading_delay,
        bmo_dbt_assets,
        feast_feature_export,
        feast_materialized_features,
        # Training layer (group: 'training')
        training_dataset,
        trained_model,
        registered_model,
    ],
    asset_checks=[
        # Schema contracts
        check_staged_flights_nulls,
        check_staged_flights_schema_evolution,
        check_staged_weather_nulls,
        check_dim_airport,
        check_dim_route,
        # Evaluation gate — blocking checks prevent registered_model from materializing if the model fails quality thresholds
        check_auc_gate,
        check_leakage_sentinel,
        check_calibration,
        check_slice_parity,
    ],
    jobs=[
        ingest_bts_month_job,  # sensor-triggered: one BTS month at a time
        feast_materialize_job,  # schedule-triggered: hourly Feast materialization
        retrain_job,  # schedule + sensor-triggered: full training pipeline
    ],
    schedules=[
        feast_hourly_schedule,  # top of every hour: push features to Redis
        nightly_retrain_schedule,  # 1am UTC nightly: training_dataset → trained_model → registered_model
    ],
    sensors=[
        bts_new_month_sensor,  # polls BTS site for new monthly releases (6h interval)
        drift_retrain_sensor,  # polls drift_metrics Postgres table for PSI > 0.2 (1h interval)
        run_failure_sensor_fn,  # posts to Discord on any run failure (event-driven)
    ],
    resources={
        # 'dbt' key must exactly match the parameter name in bmo_dbt_assets(context, dbt: DbtCliResource)
        'dbt': DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        # MLflow tracking server — used by training assets and evaluation gate checks
        'mlflow': MLflowResource(tracking_uri=settings.mlflow_tracking_uri),
        # S3-compatible object store (MinIO locally, Cloudflare R2 in production)
        # endpoint_url swap is the only config difference between MinIO and R2
        's3': S3Resource(
            endpoint_url=settings.s3_endpoint_url,
            access_key_id=settings.s3_access_key_id,
            secret_access_key=settings.s3_secret_access_key,
            region=settings.s3_region,
        ),
        # Feast feature store — pointed at the feature_repo/ directory
        # FeatureStore reads feature_store.yaml on construction; no network calls yet
        'feast': FeastResource(feature_repo_dir=str(FEATURE_REPO_DIR)),
        # DuckDB — query engine for dbt feature models and training dataset builder
        # s3_endpoint is HOST:PORT (no scheme) — DuckDB's httpfs format requirement
        'duckdb': DuckDBResource(
            duckdb_path=settings.duckdb_path,
            s3_endpoint=settings.s3_endpoint,  # computed property: strips http://
            s3_access_key_id=settings.s3_access_key_id,
            s3_secret_access_key=settings.s3_secret_access_key,
            s3_region=settings.s3_region,
        ),
    },
)
