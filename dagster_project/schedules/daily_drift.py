"""
Daily drift report schedule: 8am UTC.

Runs 2 hours after daily_batch_score_schedule (6am UTC) to ensure predictions
are written before the drift report tries to read them.

The schedule runs the drift_report asset for YESTERDAY, not today.
Reason: batch scoring runs at 6am for today's flights, but today's flights
haven't all departed yet at 8am. Running drift against yesterday's complete
set is more stable than a partial day.

Dagster schedule docs: https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
"""

from datetime import timedelta

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

daily_drift_report_job = define_asset_job(
    name='daily_drift_report',
    selection=AssetSelection.keys('drift_report'),
    description='daily evidently drift report + PSI → Postgres for drift_retrain_sensor.',
)


@schedule(
    job=daily_drift_report_job,
    cron_schedule=' 0 8 * * *',  # 8am UTC - 2h after batch scoring
    name='daily_drift_report_schedule',
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        'Runs drift_report for yesterday at 8am UTC. '
        'Writes HTML to S3 (synced to GitHub Pages by evidently-reports.yml CI workflow) '
        'and PSI metrics to Postgres (polled by drift_retrain_sensor every hour).'
    ),
)
def daily_drift_report_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    yesterday = (context.scheduled_execution_time - timedelta(days=1)).strftime('%Y-%m-%d')
    return RunRequest(partition_key=yesterday)
