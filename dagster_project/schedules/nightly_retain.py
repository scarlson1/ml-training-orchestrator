"""
Nightly retrain schedule.

Runs the full pipeline at 1am UTC every night:
  feast_feature_export → feast_materialized_features → training_dataset → trained_model → registered_model

Order matters:
  1. feast_feature_export re-exports dbt model outputs to S3 Parquet.
     This writes both data.parquet (Redis/online) and training.parquet
     (full history for the PIT join). Without this step, the training
     dataset would join against stale or missing feature snapshots.
  2. feast_materialized_features runs materialize_incremental into Redis.
  3. training_dataset runs build_dataset with a fresh PIT join.
  4. trained_model runs HPO and produces a new champion.
  5. registered_model registers the champion if it passes all gate checks.

DefaultScheduleStatus.STOPPED means the schedule is OFF when first deployed.
Enable it in the Dagster UI or flip to RUNNING for production.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

retrain_job = define_asset_job(
    name='nightly_retrain',
    selection=AssetSelection.assets(
        'feast_feature_export',
        'feast_materialized_features',
        'training_dataset',
        'trained_model',
        'registered_model',
        'mllib_baseline',
    ),
    description=(
        'Full training pipeline: Feast export → PIT dataset builder → XGBoost HPO → '
        'evaluation gate → MLflow registry → MLlib baseline. '
        'Triggered nightly at 1am UTC by nightly_retrain_schedule, '
        'and on-demand by drift_retrain_sensor when PSI > 0.2.'
    ),
)


@schedule(
    job=retrain_job,
    cron_schedule='0 1 * * *',  # 1am UTC every day
    name='nightly_retrain_schedule',
    default_status=DefaultScheduleStatus.STOPPED,
    description='Triggers the full training pipeline at 1am UTC nightly.',
)
def nightly_retrain_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest()
