"""
Drift-triggered retrain sensor.

Polls the drift_metrics Postgres table (populated in Phase 10 by the
monitoring/drift_report asset). When PSI > 0.2 on any top-10 feature,
triggers a nightly_retrain run.

Design decisions:
  - minimum_interval_seconds=3600 (hourly) — drift metrics are computed daily;
    checking more often wastes compute and produces duplicate triggers.
  - cursor stores the last-seen timestamp so the sensor only evaluates NEW rows,
    not reprocessing the full history on every tick.
  - run_key is date-based (e.g. 'drift-retrain-2026-04-26') — Dagster deduplicates
    by run_key, so even if the sensor fires twice in one day it won't retrain twice.
  - Graceful pre-Phase-10 behavior: catches any DB error and yields SkipReason
    instead of crashing the sensor daemon.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone

import structlog
from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from bmo.common.config import settings

log = structlog.get_logger(__name__)

_PSI_THRESHOLD = 0.2
_TOP_N_FEATURES = 10


def _query_drift_metrics(postgres_url: str, since: str) -> list[dict]:
    """
    Return drift_metrics rows since `since` for the top N features by importance rank.

    Returns an empty list if the table doesn't exist yet
    or if any database error occurs. Failures are logged, not raised.
    """
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT feature_name, psi_score, computed_at
                    FROM drift_metrics
                    WHERE rank <= :top_n
                        AND computed_at > :since
                    ORDER BY computed_at DESC
                    LIMIT 20
                """),
                {'top_n': _TOP_N_FEATURES, 'since': since},
            ).fetchall()

        return [
            {'feature_name': row[0], 'psi_score': float(row[1]), 'computed_at': str(row[2])}
            for row in rows
        ]
    except Exception as exc:
        log.warning('drift_metrics query failed', error=str(exc))
        return []


@sensor(
    job_name='nightly_retrain',
    minimum_interval_seconds=60 * 60,
    name='drift_retrain_sensor',
    description=(
        'Polls the drift_metrics postgres table for PSI > 0.2 on any top-10 feature. '
        'Triggers a nightly_retrain run when drift is detected. '
        'Pre-Phase-10: gracefully skips if the drift_metrics table does not exist yet.'
    ),
)
def drift_retrain_sensor(
    context: SensorEvaluationContext,
) -> Iterator[RunRequest | SkipReason]:
    since: str = context.cursor or '1970-01-01T00:00:00'

    rows = _query_drift_metrics(settings.postgres_url, since)

    if not rows:
        yield SkipReason(
            'No new drift_metrics rows since last evaluation '
            '(or drift_metrics table not yet created)'
        )
        return

    breached = [row for row in rows if row['psi_score'] > _PSI_THRESHOLD]

    # update rows to avoid processing rows that have already been seen
    latest_computed_at = max(row['computed_at'] for row in rows)
    context.update_cursor(latest_computed_at)

    if not breached:
        max_psi = max(row['psi_score'] for row in rows)
        yield SkipReason(
            f'No PSI breaches detected (threshold={_PSI_THRESHOLD}). '
            f'Highest PSI in top-{_TOP_N_FEATURES} features: {max_psi:.4f}.'
        )
        return

    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    run_key = f'drift-retrain-{today}'

    feature_summary = ', '.join(f'{r["feature_name"]}={r["psi_score"]:.3f}' for r in breached[:5])

    log.warning(
        'PSI drift detected - requesting retrain',
        n_breached=len(breached),
        features=feature_summary,
        threshold=_PSI_THRESHOLD,
    )

    yield RunRequest(
        run_key=run_key,
        tags={
            'trigger': 'drift_sensor',
            'n_breached_features': str(len(breached)),
            'breached_features': feature_summary[:200],
        },
    )
