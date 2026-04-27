"""
Run failure sensor — posts a Discord embed when any Dagster run fails.

Dagster concept — @run_failure_sensor:
    Unlike a @sensor (polled on an interval), @run_failure_sensor is event-driven.
    The Dagster daemon calls this function automatically whenever a run in the
    code location transitions to the FAILURE state. No polling needed.

Setup:
    1. Create a Discord webhook in your server:
       Server Settings → Integrations → Webhooks → New Webhook → Copy URL
    2. Set DISCORD_WEBHOOK_URL in your .env file.
    3. If DISCORD_WEBHOOK_URL is not set, the sensor logs a warning and returns
       silently — it does NOT raise, to avoid a failure loop.

Discord Webhook API docs: https://discord.com/developers/docs/resources/webhook
Dagster run_failure_sensor docs: https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#run-failure-sensor
"""

from __future__ import annotations

import httpx
import structlog
from dagster import RunFailureSensorContext, run_failure_sensor

from bmo.common.config import settings

log = structlog.get_logger(__name__)

_MAX_ERROR_LEN = 1000  # discord embed field value limit is 1024 characters


@run_failure_sensor(
    name='run_failure_sensor',
    description=(
        'Posts a discord embed to DISCORD_WEBHOOK_URL when any Dagster run fails. '
        'Set DISCORD_WEBHOOK_URL in .env to activate. No-ops silently if env var is absent.'
    ),
)
def run_failure_sensor_fn(context: RunFailureSensorContext) -> None:
    if not settings.discord_webhook_url:
        context.log.warning('DISCORD_WEBHOOK_URL not configured - skipping failure notification.')
        return

    run = context.dagster_run

    error_message = (
        context.failure_event.message
        if context.failure_event and context.failure_event.message
        else 'No error message available'
    )

    if len(error_message) > _MAX_ERROR_LEN:
        error_message = error_message[:_MAX_ERROR_LEN] + '...'

    #  Discord embed docs: https://discord.com/developers/docs/resources/message#embed-object
    payload = {
        'embeds': [
            {
                'title': f'Dagster run failed `{run.job_name}`',
                'color': 0xFF3333,
                'fields': [
                    {
                        'name': 'Run ID',
                        'value': f'`{run.run_id[:12]}…`',
                        'inline': True,
                    },
                    {
                        'name': 'Job',
                        'value': run.job_name,
                        'inline': True,
                    },
                    {
                        'name': 'Error',
                        'value': error_message,
                        'inline': False,
                    },
                ],
            }
        ]
    }

    try:
        resp = httpx.post(settings.discord_webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        context.log.info(f'Discord failure alert sent for run {run.run_id[:12]}')
    except Exception as exc:
        context.log.warning(f'Discord notification failed: {exc}')
