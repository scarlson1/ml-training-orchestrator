"""
Nightly retrain schedule.

Runs the full training asset group at 1am UTC every night:
  training_dataset → trained_model → registered_model

1am was chosen (not midnight) because:
  1. The Feast hourly schedule runs at midnight (0am UTC).
  2. The training_dataset asset reads from feast_materialized_features.
  3. A 1h gap ensures the midnight Feast materialization completes before
     training starts reading features.

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
    # The + prefix means "and all upstream assets. Here we just want the three training assets, not entire upstream"
    selection=AssetSelection.assets(
        'training_dataset',
        'trained_model',
        'registered_model',
    ),
    description=(
        'Full training pipeline: PIT dataset builder → XGBoost → evaluation gate → MLflow registry. '
        'Triggered nightly at 1am UTC by nightly_retrain_schedule, '
        ' and on-demand by drift_retrain_sensor when PSI > 0.2.'
    ),
)


@schedule(
    job=retrain_job,
    cron_schedule='0 1 * * *',  # 1am UTC every day
    name='nightly_retrain_schedule',
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        'Triggers the training asset group at 1am UTC. '
        'Runs after feast hourly materialization at midnight.'
    ),
)
def nightly_retrain_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest()  # run with default config
