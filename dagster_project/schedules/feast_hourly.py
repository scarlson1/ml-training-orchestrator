from dagster import RunRequest, ScheduleEvaluationContext, define_asset_job, schedule

from dagster_project.assets.feast_materialization import feast_materialized_features

# Why hourly ?
#
# Demo project - dbt feature model includes 1 hour rolling windows. less frequently than 1 hour => stale
# Could benefit from more frequently. May want to opt for streaming in a real production system

feast_materialize_job = define_asset_job(
    name='feast_hourly_materialize',
    selection=[feast_materialized_features],
    description='Hourly Feast materialization: push latest features from S3 parquet to Redis',
)


@schedule(
    job=feast_materialize_job,
    cron_schedule='0 * * * *',  # top of every hour
    name='feast_hourly_materialization',
    description=(
        'Runs feast materialize-incremental every hour. '
        'Keeps the Redis online store within 1h of the dbt feature model outputs.'
    ),
)
def feast_hourly_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    return RunRequest()
