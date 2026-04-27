"""
Monitoring-layer Dagster assets: drift_report and ground_truth_backfill.

drift_report (daily, DailyPartitionsDefinition):
  1. Load champion model version + dataset hash from MLflow registry
  2. Load reference feature distributions from the training dataset Parquet (S3)
  3. Load current feature values by re-retrieving from Feast for recent predictions
  4. Compute PSI + KL divergence per feature; build Evidently HTML report
  5. Write HTML report to s3://staging/monitoring/reports/date=YYYY-MM-DD/
  6. Write drift metrics Parquet to s3://staging/monitoring/metrics/date=YYYY-MM-DD/
     (this path is consumed by the mart_drift_metrics dbt model)
  7. Upsert DriftMetricsRow rows to the drift_metrics Postgres table
     (polled by drift_retrain_sensor every hour)

ground_truth_backfill (non-partitioned, runs after bmo_dbt_assets):
  - Queries mart_predictions, which LEFT JOINs batch predictions with staged_flights.
  - Rows where actual_is_delayed IS NOT NULL have BTS ground truth available.
  - Computes classification metrics per (score_date, model_version).
  - Upserts to the live_accuracy Postgres table.
  - BTS publishes with ~60 day lag, so this asset surfaces historical accuracy
    as actuals arrive. Re-running is idempotent (ON CONFLICT DO UPDATE).

Data flow:
  registered_model
       │
       └──► batch_predictions (daily) ──► s3://staging/predictions/
                │
                │          (also)
                ▼
         drift_report ──► s3://staging/monitoring/reports/ (HTML for GitHub Pages)
                │    └──► s3://staging/monitoring/metrics/ (Parquet for mart_drift_metrics)
                │    └──► drift_metrics (Postgres) ◄── drift_retrain_sensor polls
                │
       bmo_dbt_assets (mart_predictions)
                │
                ▼
         ground_truth_backfill ──► live_accuracy (Postgres)
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs as s3fs_lib
import structlog
from dagster import AssetExecutionContext, FreshnessPolicy, MaterializeResult, MetadataValue, asset
from feast import FeatureStore
from mlflow.tracking import MlflowClient
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)

from bmo.batch_scoring.score import BATCH_FEATURE_REFS, FEATURE_COLUMNS
from bmo.common.config import settings
from bmo.evaluation_gate.gate import MODEL_NAME
from bmo.monitoring.drift import PSI_MODERATE, DriftMetricsRow, DriftReportResult, compute_drift
from bmo.monitoring.retrain_trigger import should_retrain
from bmo.serving.partitions import DAILY_PARTITIONS
from dagster_project.resources import DuckDBResource
from dagster_project.resources.mlflow_resource import MLflowResource
from dagster_project.resources.s3_resource import S3Resource

log = structlog.get_logger(__name__)

FEATURE_REPO_DIR = Path(__file__).parent.parent.parent / 'feature_repo'

# How many days of predictions to use as the "current" distribution window.
# 7 days balances recency (capture recent drift) vs sample size (need enough
# data for reliable bin estimates). Adjust if scoring volume is low.
_CURRENT_WINDOW_DAYS = 7

# Sample at most this many rows from the training dataset as the reference.
# Training datasets can be 500K+ rows; we don't need all of them for distribution
# estimates. 20K rows gives stable percentile estimates for all features.
_REFERENCE_SAMPLE_SIZE = 20_000

# PSI threshold — must match drift_retrain_sensor._PSI_THRESHOLD
_PSI_THRESHOLD = PSI_MODERATE  # 0.2


@asset(
    group_name='monitoring',
    partitions_def=DAILY_PARTITIONS,
    deps=['batch_predictions', 'deployed_api'],
    freshness_policy=FreshnessPolicy.cron(
        deadline_cron='0 10 * * *', lower_bound_delta=timedelta(hours=1)
    ),
    description=(
        'Daily feature drift report using Evidently + PSI. '
        'Compares champion model training distribution (reference) to recent production '
        'feature values (current). Writes HTML to S3 for GitHub Pages and PSI metrics '
        'to Postgres for the drift_retrain_sensor.'
    ),
)
def drift_report(
    context: AssetExecutionContext,
    mlflow: MLflowResource,  # noqa: F821 - forward ref avoids circular import at module
    s3: S3Resource,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Compute daily drift report for the partition date.

    The asset is partitioned daily (matching batch_predictions). When you
    backfill 30 days of drift reports in the Dagster UI, Dagster queues 30
    independent runs — each compares the training distribution to a
    different 7-day production window anchored at the partition date.

    Idempotency:
        HTML and Parquet files are written with overwrite semantics (s3fs 'wb').
        Postgres rows use ON CONFLICT DO UPDATE. Re-running the same partition
        for the same state of the feature store and training dataset always
        produces identical results.
    """
    report_date_str: str = context.partition_key
    report_date = date.fromisoformat(report_date_str)

    mlflow_client = mlflow.get_client()

    try:
        champion = mlflow_client.get_model_version_by_alias(MODEL_NAME, 'champion')
    except Exception:
        context.log.warning('No champion model registered - skipping drift report')
        return MaterializeResult(
            metadata={'status': MetadataValue.text('skipped: no champion found')}
        )

    model_version = champion.version
    dataset_version_hash = champion.tags.get('dataset_version_hash', '')
    context.log.info(f'champion model version={model_version}, dataset_hash={dataset_version_hash}')

    fs = s3.get_s3fs()

    # load reference features (training dataset)
    reference_df = _load_reference_features(dataset_version_hash, context, fs)
    if reference_df.empty:
        context.log.warning('No reference features - skipping drift report')
        return MaterializeResult(metadata={'status': MetadataValue.text('skipped no reference df')})

    # load current features (Feast PIT retrieval for recent predictions)
    current_df = _load_current_features(report_date, context, fs)
    if current_df.empty:
        context.log.warning(f'No current features for window ending {report_date_str}')
        return MaterializeResult(
            metadata={'status': MetadataValue.text('skipped: no current data')}
        )

    context.log.info(f'reference rows={len(reference_df)}, current rows={len(current_df)}')

    # feature importance (for ranking psi results)
    feature_importance = _load_feature_importance(champion, mlflow_client, context)

    result: DriftReportResult = compute_drift(
        reference_df=reference_df,
        current_df=current_df,
        feature_columns=FEATURE_COLUMNS,
        feature_importance=feature_importance,
        report_date=report_date,
        psi_threshold=_PSI_THRESHOLD,
    )

    # assign model_version to each row
    for m in result.metrics:
        m.model_version = model_version

    # evaluate retrain trigger (log only)
    trigger, breached_features = should_retrain(result.metrics)
    if trigger:
        context.log.warning(
            f'PSI breach detected: {result.n_features_breached} features breached, '
            f'top={breached_features[:5]}'
        )

    # write report to s3
    html_path = _write_html_report(result.html_report, report_date, fs)
    context.log.info(f'HTML report written: {html_path}')

    # metrics parquet to S3 (mart_drift_metrics dbt model)
    metrics_path = _write_metrics_parquet(result.metrics, report_date, fs)
    context.log.info(f'metrics Parquet written: {metrics_path}')

    _upsert_drift_metrics(result.metrics, settings.postgres_url)
    context.log.info(f'upserted {len(result.metrics)} rows to drift_metrics')

    return MaterializeResult(
        metadata={
            'report_date': MetadataValue.text(report_date_str),
            'model_version': MetadataValue.text(model_version),
            'n_features_checked': MetadataValue.int(result.n_features_checked),
            'n_features_breached': MetadataValue.int(result.n_features_breached),
            'max_psi': MetadataValue.float(round(result.max_psi, 4)),
            'psi_threshold': MetadataValue.float(_PSI_THRESHOLD),
            'drift_detected': MetadataValue.text(str(trigger)),
            'breached_features': MetadataValue.text(', '.join(breached_features[:5])),
            'html_report_path': MetadataValue.url(html_path),
            'metrics_parquet_path': MetadataValue.url(metrics_path),
            'reference_rows': MetadataValue.int(len(reference_df)),
            'current_rows': MetadataValue.int(len(current_df)),
        }
    )


@asset(
    group_name='monitoring',
    deps=['bml_dbt_assets'],
    description=(
        'Ground-truth backfill: computes live accuracy metrics once BTS actuals '
        'become available (~60 days after scoring). '
        'Queries mart_predictions (which LEFT JOINs predictions + staged_flights actuals), '
        'groups by (score_date, model_version), and upserts classification metrics '
        'to the live_accuracy Postgres table. '
        'Idempotent: safe to re-run whenever new BTS data is ingested.'
    ),
)
def ground_truth_backfill(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> MaterializeResult:
    """
    Compute live accuracy metrics from mart_predictions.

    BTS timeline:
      Day 0:   Flights depart; batch_predictions scores them.
      Day ~60: BTS publishes actuals for that month.
      Day ~60: raw_bts_flights ingested → staged_flights updated → mart_predictions
               now has non-null actual_is_delayed for those flights.
      Day ~60: Running ground_truth_backfill picks up the new actuals and
               computes accuracy metrics for day 0's model version.

    This is the feedback loop that tells you whether PSI-triggered retrains
    actually improved production accuracy over time.

    The UNIQUE (score_date, model_version) constraint on live_accuracy means
    re-running this asset after new actuals arrive only updates rows for
    score_dates that now have more actuals, not all historical rows.
    """

    # get mart_predictions (dbt model joining prediction outputs with staged_flight actuals)
    with duckdb.get_connection(read_only=True) as con:
        df = con.execute("""
            SELECT
                score_date,
                model_version,
                predicted_is_delayed,
                predicted_delay_proba,
                actual_is_delayed
            FROM mart_predictions
            WHERE actual_is_delayed IS NOT NULL
        """).df()

    if df.empty:
        context.log.warning('no actuals available yet in mart_predictions')
        return MaterializeResult(metadata={'status': MetadataValue.text('no actuals available')})

    context.log.info(f'loaded {len(df)} rows with actuals from mart_predictions')

    accuracy_rows = []
    groups = df.groupby(['score_date', 'model_version'])

    for (score_date, model_version), group in groups:
        y_true = group['actual_is_delayed'].astype(int).values
        y_pred = group['predicted_is_delayed'].astype(int).values
        y_proba = group['predicted_delay_proba'].astype(float).values

        n = len(group)
        has_both_classes = len(set(y_true)) == 2

        accuracy_rows.append(
            {
                'score_date': score_date,
                'model_version': str(model_version),
                'n_flights': n,
                'n_with_actuals': n,
                'accuracy': float(accuracy_score(y_true, y_pred)),
                'precision_score': float(precision_score(y_true, y_pred, zero_division=0.0)),  # pyright: ignore[reportArgumentType]
                'recall_score': float(recall_score(y_true, y_pred, zero_division=0.0)),  # pyright: ignore[reportArgumentType]
                'f1': float(f1_score(y_true, y_pred, zero_division=0.0)),  # pyright: ignore[reportArgumentType]
                'roc_auc': float(roc_auc_score(y_true, y_proba)) if has_both_classes else None,
                'log_loss': float(log_loss(y_true, y_proba)) if has_both_classes else None,
                'brier_score': float(brier_score_loss(y_true, y_proba)),
                'positive_rate': float(y_pred.mean()),
                'actual_positive_rate': float(y_true.mean()),
            }
        )

    _upsert_live_accuracy(accuracy_rows, settings.postgres_url)
    context.log.info(f'upserted {len(accuracy_rows)} rows to live_accuracy')

    n_dates = len({r['score_date'] for r in accuracy_rows})
    avg_roc_auc = sum(r['roc_auc'] for r in accuracy_rows if r['roc_auc'] is not None) / max(
        1, sum(1 for r in accuracy_rows if r['roc_auc'] is not None)
    )

    return MaterializeResult(
        metadata={
            'n_score_dates': MetadataValue.int(n_dates),
            'n_model_versions': MetadataValue.int(len({r['model_version'] for r in accuracy_rows})),
            'total_flights_with_actuals': MetadataValue.int(len(df)),
            'avg_roc_auc': MetadataValue.float(round(avg_roc_auc, 4)),
        }
    )


def _load_reference_features(
    dataset_version_hash: str,
    context: AssetExecutionContext,
    fs: s3fs_lib.S3FileSystem,
) -> pd.DataFrame:
    """
    Load feature columns from the training dataset Parquet as the reference distribution.

    The training dataset path is {settings.dataset_s3_base}/{dataset_version_hash}/data.parquet.
    The dataset_version_hash is stored as an MLflow model version tag at registration time.

    We sample up to _REFERENCE_SAMPLE_SIZE rows — training datasets are large
    (500K+ rows) but PSI estimations stabilize well below 20K samples.
    Random seed is fixed so the reference sample is reproducible for a given
    dataset version.
    """
    if not dataset_version_hash:
        context.log.warning('no dataset_version_hash tag on champion model')
        return pd.DataFrame()

    dataset_path = f'{settings.dataset_s3_base}/{dataset_version_hash}/data.parquet'

    if not fs.exists(dataset_path):
        context.log.warning(f'training dataset not found at {dataset_path}')
        return pd.DataFrame()

    try:
        with fs.open(dataset_path, 'rb') as f:
            table = pq.read_table(f, columns=FEATURE_COLUMNS)
        df = table.to_pandas()
    except Exception as exc:
        context.log.warning(f'failed to read training dataset: {exc}')
        return pd.DataFrame()

    if len(df) > _REFERENCE_SAMPLE_SIZE:
        df = df.sample(n=_REFERENCE_SAMPLE_SIZE, random_state=42)

    context.log.info(f'loaded reference: {len(df)} rows from {dataset_path}')
    return df


def _load_current_features(
    report_date: date,
    context: AssetExecutionContext,
    fs: s3fs_lib.S3FileSystem,
) -> pd.DataFrame:
    """
    Load production feature values by re-retrieving from Feast for recent predictions.

    For each day in the lookback window:
      1. Read entity IDs and timestamps from the predictions Parquet.
      2. Call get_historical_features() with scored_at as the event_timestamp.
         This gives us the feature values that were available at prediction time —
         the same PIT logic used in batch_predictions and training.

    Why re-retrieve from Feast instead of reading cached feature values?
      - score.py doesn't write feature values to the output Parquet (only predictions).
      - Feast's offline store retains historical snapshots, so re-retrieval is
        possible and gives values identical to the originals.
      - This approach keeps score.py simple and avoids bloating the predictions output.
    """
    entity_frames: list[pd.DataFrame] = []

    for i in range(_CURRENT_WINDOW_DAYS):
        d = report_date - timedelta(days=i)
        path = f's3://{settings.s3_bucket_staging}/predictions/date={d.isoformat()}/data.parquet'
        if not fs.exists(path):
            continue
        try:
            with fs.open(path, 'rb') as f:
                df = pq.read_table(
                    f,
                    columns=[
                        'flight_id',
                        'origin',
                        'dest',
                        'carrier',
                        'tail_number',
                        'route_key',
                        'scored_at',
                    ],
                ).to_pandas()
            entity_frames.append(df)
        except Exception as exc:
            context.log.debug(f'count not read predictions for {d}: {exc}')

    if not entity_frames:
        return pd.DataFrame()

    # remove any duplicates flights that may have made it into multiple partitions
    entity_df = pd.concat(entity_frames, ignore_index=True).drop_duplicates('flight_id')

    entity_df['event_timestamp'] = pd.to_datetime(entity_df['scored_at'], utc=True)

    store = FeatureStore(repo_path=str(FEATURE_REPO_DIR))
    try:
        feature_df = store.get_historical_features(
            entity_df=entity_df[
                [
                    'flight_id',
                    'origin',
                    'dest',
                    'carrier',
                    'tail_number',
                    'route_key',
                    'event_timestamp',
                ]
            ],
            features=BATCH_FEATURE_REFS,
        ).to_df()
    except Exception as exc:
        context.log.warning(f'Feast feature retrieval failed: {exc}')
        return pd.DataFrame()

    valid_feature_cols = [c for c in FEATURE_COLUMNS if c in feature_df.columns]
    return feature_df[valid_feature_cols].dropna(how='all')


def _load_feature_importance(
    champion_version: object,
    mlflow_client: MlflowClient,
    context: AssetExecutionContext,
) -> dict[str, float]:
    """
    Load per-feature importance from the champion model's MLflow run artifacts.

    Expects a 'feature_importance.json' artifact logged by the training job.
    Format: {"origin_avg_dep_delay_1h": 0.142, "carrier_on_time_pct_7d": 0.089, ...}

    Falls back to uniform importance (all features score 1.0) if the artifact
    is missing or unreadable. With uniform importance, features are ranked by
    their position in FEATURE_COLUMNS — the order used in score.py.
    """
    try:
        run_id = champion_version.run_id  # type: ignore[union-attr]
        local_path = mlflow_client.download_artifacts(
            run_id=run_id,
            path='feature_importance.json',
        )
        with open(local_path) as f:
            importance: dict[str, float] = json.load(f)
        context.log.info(f'loaded feature importance: {len(importance)} features')
        return importance
    except Exception as exc:
        context.log.debug(f'feature_importance.json not found, using uniform: {exc}')
        # Uniform fallback: all features at 1.0, ranked by FEATURE_COLUMNS order
        return {col: 1.0 for col in FEATURE_COLUMNS}


def _write_html_report(
    html: str,
    report_date: date,
    fs: s3fs_lib.S3FileSystem,
) -> str:
    """
    Write Evidently HTML report to S3.

    Path: s3://staging/monitoring/reports/date=YYYY-MM-DD/drift_report.html

    The GitHub Actions evidently-reports.yml workflow syncs this path to
    GitHub Pages daily, making reports publicly browsable at:
      https://<user>.github.io/<repo>/evidently-reports/date=YYYY-MM-DD/drift_report.html
    """
    path = f's3://{settings.s3_bucket_staging}/monitoring/reports/date={report_date.isoformat()}/drift_report.html'
    with fs.open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    return path


def _write_metrics_parquet(
    metrics: list[DriftMetricsRow], report_date: date, fs: s3fs_lib.S3FileSystem
) -> str:
    """
    Write drift metrics as Parquet to S3.

    Path: s3://staging/monitoring/metrics/date=YYYY-MM-DD/drift_metrics.parquet

    This path is read by the mart_drift_metrics dbt model, which makes drift
    metrics queryable via DuckDB and visible in the Dagster lineage graph.
    """
    rows = [m.model_dump() for m in metrics]
    df = pd.DataFrame(rows)

    df['report_date'] = df['report_date'].astype(str)
    df['computed_at'] = df['computed_at'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    path = f's3://{settings.s3_bucket_staging}/monitoring/metrics/date={report_date.isoformat()}/drift_metrics.parquet'

    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, 'wb') as f:
        pq.write_table(table, f, compression='zstd')
    return path


def _upsert_drift_metrics(metrics: list[DriftMetricsRow], postgres_url: str) -> None:
    """
    Upsert drift metrics rows to Postgres.

    ON CONFLICT (report_date, feature_name) DO UPDATE SET ...
    makes this safe to re-run for the same partition: the row is updated
    rather than rejected or duplicated.

    SQLAlchemy docs: https://docs.sqlalchemy.org/en/20/core/dml.html
    """
    from sqlalchemy import create_engine, text

    rows = [
        {
            'report_date': m.report_date,
            'feature_name': m.feature_name,
            'psi_score': m.psi_score,
            'kl_divergence': m.kl_divergence,
            'rank': m.rank,
            'is_breached': m.is_breached,
            'model_version': m.model_version,
            'computed_at': m.computed_at,
        }
        for m in metrics
    ]

    engine = create_engine(postgres_url)
    with engine.begin() as conn:  # auto commits on exit
        conn.execute(
            text("""
                INSERT INTO drift_metrics (
                    report_date, feature_name, psi_score, kl_divergence,
                    rank, is_breached, model_version, computed_at
                )
                VALUES (
                    :report_date, :feature_name, :psi_score, :kl_divergence,
                    :rank, :is_breached, :model_version, :computed_at
                )
                ON CONFLICT (report_date, feature_name) DO UPDATE SET
                    psi_score     = EXCLUDED.psi_score,
                    kl_divergence = EXCLUDED.kl_divergence,
                    rank          = EXCLUDED.rank,
                    is_breached   = EXCLUDED.is_breached,
                    model_version = EXCLUDED.model_version,
                    computed_at   = EXCLUDED.computed_at
            """),
            rows,
        )


def _upsert_live_accuracy(rows: list[dict], postgres_url: str) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO live_accuracy (
                    score_date, model_version, n_flights, n_with_actuals,
                    accuracy, precision_score, recall_score, f1, roc_auc,
                    log_loss, brier_score, positive_rate, actual_positive_rate
                )
                VALUES (
                    :score_date, :model_version, :n_flights, :n_with_actuals,
                    :accuracy, :precision_score, :recall_score, :f1, :roc_auc,
                    :log_loss, :brier_score, :positive_rate, :actual_positive_rate
                )
                ON CONFLICT (score_date, model_version) DO UPDATE SET
                    n_flights            = EXCLUDED.n_flights,
                    n_with_actuals       = EXCLUDED.n_with_actuals,
                    accuracy             = EXCLUDED.accuracy,
                    precision_score      = EXCLUDED.precision_score,
                    recall_score         = EXCLUDED.recall_score,
                    f1                   = EXCLUDED.f1,
                    roc_auc              = EXCLUDED.roc_auc,
                    log_loss             = EXCLUDED.log_loss,
                    brier_score          = EXCLUDED.brier_score,
                    positive_rate        = EXCLUDED.positive_rate,
                    actual_positive_rate = EXCLUDED.actual_positive_rate,
                    computed_at          = NOW()
            """)
        )
