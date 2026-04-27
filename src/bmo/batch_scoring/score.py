from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import structlog
from pydantic import BaseModel

log = structlog.get_logger(__name__)

# These must match ALL_FEATURE_REFS in dagster_project/assets/training.py.
# Order matters: XGBoost's booster uses column-position-based indexing
# adding feature would corrupt existing model predictions (unless added to end)
BATCH_FEATURE_REFS: list[str] = [
    'origin_airport_features:origin_flight_count_1h',
    'origin_airport_features:origin_avg_dep_delay_1h',
    'origin_airport_features:origin_pct_delayed_1h',
    'origin_airport_features:origin_avg_dep_delay_24h',
    'origin_airport_features:origin_pct_cancelled_24h',
    'origin_airport_features:origin_avg_dep_delay_7d',
    'origin_airport_features:origin_pct_delayed_7d',
    'origin_airport_features:origin_congestion_score_1h',
    'dest_airport_features:dest_avg_arr_delay_1h',
    'dest_airport_features:dest_pct_delayed_1h',
    'dest_airport_features:dest_avg_arr_delay_24h',
    'dest_airport_features:dest_pct_diverted_24h',
    'carrier_features:carrier_on_time_pct_7d',
    'carrier_features:carrier_cancellation_rate_7d',
    'carrier_features:carrier_avg_delay_7d',
    'carrier_features:carrier_flight_count_7d',
    'route_features:route_avg_dep_delay_7d',
    'route_features:route_avg_arr_delay_7d',
    'route_features:route_pct_delayed_7d',
    'route_features:route_cancellation_rate_7d',
    'route_features:route_avg_elapsed_7d',
    'route_features:route_distance_mi',
    'aircraft_features:cascading_delay_min',
    'aircraft_features:turnaround_min',
]

FEATURE_COLUMNS: list[str] = [ref.split(':')[1] for ref in BATCH_FEATURE_REFS]

_METADATA_COLUMNS: set[str] = {
    'flight_id',
    'event_timestamp',
    'origin',
    'dest',
    'carrier',
    'tail_number',
    'route_key',
    'scheduled_departure_utc',
    'score_date',
    'scored_at',
    'model_name',
    'model_version',
    'predicted_delay_proba',
    'predicted_is_delayed',
}


class BatchScoreResult(BaseModel):
    """Immutable summary of one completed batch scoring partition."""

    # fmt: off
    score_date: str          # 'YYYY-MM-DD'
    model_name: str
    model_version: str
    row_count: int
    positive_rate: float     # fraction of flights predicted as delayed
    null_feature_rows: int   # rows where ≥1 feature was null (fillna(0) was applied)
    storage_path: str        # s3://staging/predictions/date=YYYY-MM-DD/data.parquet
    scored_at: str           # ISO-8601 UTC timestamp
    # fmt: on


def score_partition(
    score_date: date,
    model_uri: str,
    model_name: str,
    model_version: str,
    entity_df: pd.DataFrame,
    feature_store: Any,  # TODO: type
    s3_base: str,
    s3_endpoint_url: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    s3_region: str = 'us-east-1',
    delay_threshold: float = 0.5,
) -> BatchScoreResult:
    """
    Score all flights in entity_df for score_date.

    Caller (Dagster asset) is responsible for:
      - Building entity_df from staged_flights for the given date.
      - Setting entity_df['event_timestamp'] = min(scheduled_departure_utc, run_time)
        so that the PIT join doesn't look at future features for already-departed flights.

    Args:
        score_date:       Partition date — used in the output path and schema.
        model_uri:        MLflow model URI. Format: 'models:/bmo_flight_delay@champion'
                          or 'runs:/<run_id>/model'. The Dagster asset passes the
                          registry URI so this function is independent of MLflow client.
        model_name:       Registered model name (for output metadata).
        model_version:    Registry version string, e.g. '7'. Written to output so that
                          downstream drift analysis can correlate predictions to models.
        entity_df:        Columns: flight_id, origin, dest, carrier, tail_number,
                          route_key, scheduled_departure_utc, event_timestamp.
        feature_store:    feast.FeatureStore instance.
        s3_base:          Output root, e.g. 's3://staging/predictions'.
        delay_threshold:  Probability cutoff for predicted_is_delayed (default 0.5).

    Returns:
        BatchScoreResult — logged as Dagster asset metadata by the caller.
    """
    from mlflow.pyfunc import load_model

    scored_at = datetime.now(timezone.utc)
    log.info('batch score start', score_date=str(score_date), n_flights=len(entity_df))

    # PIT feature retrieval via Feast offline store
    # get_historical_features performs a point-in-time join:
    # for each row in entity_df, it finds the latest feature value where
    # feature_ts <= entity_df.event_timestamp. Identical to build_dataset() in training
    feature_df = feature_store.get_historical_features(
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

    log.info('feast feature retrieval complete', rows=len(feature_df))

    # Count null rows before fill (doesn't exist or removed bc of TTL)
    null_feature_rows = int(feature_df[FEATURE_COLUMNS].isna().any(axis=1).sum())
    if null_feature_rows > 0:
        log.warning(
            'null features before fill', count=null_feature_rows, score_date=str(score_date)
        )

    model = load_model(model_uri)
    X = feature_df[FEATURE_COLUMNS].fillna(0.0).values.astype(np.float32)

    # XGBoost pyfunc returns probabilities directly when the model was logged
    # with mlflow.xgboost.log_model and the booster objective is binary:logistic.
    raw_preds = model.predict(pd.DataFrame(X, columns=FEATURE_COLUMNS))

    probas: np.ndarray = raw_preds[:, 1] if raw_preds.ndim == 2 else raw_preds

    entity_aligned = feature_df.merge(
        entity_df[['flight_id', 'scheduled_departure_utc']],
        on='flight_id',
        how='left',
    )

    output_df = pd.DataFrame(
        {
            'flight_id': entity_aligned['flight_id'],
            'origin': entity_aligned['origin'],
            'dest': entity_aligned['dest'],
            'carrier': entity_aligned['carrier'],
            'tail_number': entity_aligned['tail_number'],
            'route_key': entity_aligned['route_key'],
            'scheduled_departure_utc': entity_aligned['scheduled_departure_utc'],
            'predicted_delay_proba': probas.astype(np.float32),
            'predicted_is_delayed': (probas >= delay_threshold).astype(np.int8),
            'model_name': model_name,
            'model_version': model_version,
            'score_date': str(score_date),
            'scored_at': scored_at.isoformat(),
        }
    )

    # Write to S3 (Parquet, Hive-partitioned by date)
    storage_path = _write_predictions(
        output_df=output_df,
        score_date=score_date,
        s3_base=s3_base,
        endpoint_url=s3_endpoint_url,
        access_key_id=s3_access_key_id,
        secret_access_key=s3_secret_access_key,
        region=s3_region,
    )

    positive_rate = float(output_df['predicted_is_delayed'].mean())
    log.info(
        'batch score complete',
        score_date=str(score_date),
        rows=len(output_df),
        positive_rate=round(positive_rate, 4),
        null_feature_rows=null_feature_rows,
        storage_path=storage_path,
    )

    return BatchScoreResult(
        score_date=score_date.isoformat(),
        model_name=model_name,
        model_version=model_version,
        row_count=len(output_df),
        positive_rate=positive_rate,
        null_feature_rows=null_feature_rows,
        storage_path=storage_path,
        scored_at=scored_at.isoformat(),
    )


def _write_predictions(
    output_df: pd.DataFrame,
    score_date: date,
    s3_base: str,
    endpoint_url: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
) -> str:
    """
    Write predictions Parquet to s3_base/date=YYYY-MM-DD/data.parquet.

    Uses Hive-style partitioning (date=YYYY-MM-DD directory) so that DuckDB
    and the Phase 10 dbt mart_predictions model can query it with predicate
    pushdown:
        SELECT * FROM read_parquet('s3://staging/predictions/**/data.parquet')
        WHERE date = '2024-06-15'

    Idempotency: s3fs.open with 'wb' (write-binary) overwrites the existing
    file at the same path. Re-running the same partition for the same
    (model_version, score_date) always produces the same bytes, making it
    safe to retry or re-backfill.

    NOTE: Hive-partitioned Parquet on S3 achieves the same partitioned overwrite semantics
    without the extra Iceberg catalog entry. Can migrate to Iceberg if time-travel
    or schema evolution is needed, as the S3 layout is compatible.
    Reason Hive/Parquet was used:
    Iceberg's overwrite filter is expression-based and requires a schema-registered partition transform
    """
    date_str = score_date.isoformat()
    output_path = f'{s3_base}/date={date_str}/data.parquet'

    fs = s3fs.S3FileSystem(
        key=access_key_id,
        secret=secret_access_key,
        endpoint_url=endpoint_url,
        client_kwargs={'region_name': region},
    )

    arrow_table = pa.Table.from_pandas(output_df, preserve_index=False)
    with fs.open(output_path, 'wb') as f:
        pq.write_table(arrow_table, f, compression='zstd')

    return output_path
