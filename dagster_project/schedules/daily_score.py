from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

batch_score_job = define_asset_job(
    name='daily_batch_score',
    selection=AssetSelection.keys('batch_predictions'),
    description="Daily batch scoring: load champion model, score today's flights, write predictions.",
)


@schedule(
    job=batch_score_job,
    cron_schedule='0 6 * * *',  # 6am UTC daily — after overnight Feast materialization
    name='daily_batch_score_schedule',
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        'Scores all flights scheduled for today at 6am UTC. '
        'Runs after the 5am feast_hourly_schedule has pushed fresh features to Redis.'
    ),
)
def daily_batch_score_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest(partition_key=context.scheduled_execution_time.strftime('%Y-%m-%d'))
