"""
Serving-layer Dagster assets: batch_predictions and deployed_api.

batch_predictions:
  - DailyPartitionsDefinition — one Iceberg partition per calendar day.
  - Reads champion model from MLflow registry (@champion alias).
  - Loads scheduled flights from DuckDB staged_flights for partition date.
  - Sets event_timestamp = min(scheduled_dep_utc, run_time) per row for PIT join.
  - Calls bmo.batch_scoring.score_partition() which uses Feast offline store.
  - Writes predictions to s3://staging/predictions/date=YYYY-MM-DD/data.parquet.
  - Idempotent: re-running the same partition overwrites the existing file.

deployed_api:
  - Writes model_config.json to s3://staging/serving/model_config.json.
  - The FastAPI service reads this on startup and on POST /admin/reload.
  - Provides zero-downtime model swap: new champion → Dagster asset materializes →
    model_config.json updated → operator calls /admin/reload → API hot-swaps.

Data flow:
  registered_model
       │
       ├──► batch_predictions (daily, reads staged_flights + Feast offline)
       │           │
       │           └──► s3://staging/predictions/date=.../data.parquet
       │
       └──► deployed_api
                   │
                   └──► s3://staging/serving/model_config.json
                                  ▲
                    FastAPI reads on startup + /admin/reload
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import mlflow
import pandas as pd
import s3fs
from dagster import AssetExecutionContext, FreshnessPolicy, MaterializeResult, MetadataValue, asset
from feast import FeatureStore
from mlflow.tracking import MlflowClient

from bmo.batch_scoring.score import score_partition
from bmo.common.config import settings
from bmo.evaluation_gate.gate import MODEL_NAME
from bmo.serving.partitions import DAILY_PARTITIONS

FEATURE_REPO_DIR = Path(__file__).parent.parent.parent / 'feature_repo'

# Staged flights query: load all flights scheduled for a given date.
# event_timestamp = min(scheduled_departure_utc, run_time) implements the
# PIT-correct scoring rule described in bmo/batch_scoring/score.py.
#
# Why LEAST(scheduled_departure_utc, :run_time)?
#   • For flights that haven't departed yet: use run_time as the PIT cutoff.
#     We can only use features available right now.
#   • For flights that already departed (historical backfill): use the actual
#     scheduled departure time so Feast returns features available at that time,
#     not features that were added by subsequent pipeline runs.
_FLIGHTS_SQL = """
SELECT
    flight_id,
    origin,
    dest,
    carrier,
    tail_number,
    route_key,
    scheduled_departure_utc,
    LEAST(scheduled_departure_utc, TIMESTAMPTZ '{run_time}') AS event_timestamp
FROM staged_flights
WHERE flight_date = '{score_date}'
"""


@asset(
    group_name='serving',
    partitions_def=DAILY_PARTITIONS,
    deps=['registered_model', 'feast_materialized_features'],
    freshness_policy=FreshnessPolicy.cron(
        deadline_cron=' 0 7 * * *', lower_bound_delta=timedelta(hours=1)
    ),
    description=(
        'Daily batch predictions for all scheduled flights. '
        'Loads champion model from MLflow registry, retrieves PIT-correct features '
        'from Feast offline store, and writes scored Parquet to '
        's3://staging/predictions/date=YYYY-MM-DD/. '
        'Idempotent: re-running the same partition overwrites cleanly.'
    ),
)
def batch_predictions(context: AssetExecutionContext) -> MaterializeResult:
    """
    Score all scheduled flights for the Dagster partition date.

    Partitioned execution:
      When you click "Materialize" for date 2024-06-15 in the Dagster UI,
      context.partition_key == '2024-06-15'. The asset runs once for that day.
      To backfill June, select all 30 June partitions and Dagster queues them.

    Idempotency:
      score_partition() writes to a deterministic S3 path per date and uses
      s3fs.open('wb') which overwrites. Re-running the same date produces the
      same output bytes if (model_version, staged_flights data) are unchanged.
    """

    score_date_str: str = context.partition_key  # 'YYYY-MM-DD'
    score_date = date.fromisoformat(score_date_str)
    run_time = datetime.now(timezone.utc)

    context.log.info(f'batch scoring for {score_date_str}, run_time={run_time.isoformat()}')

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = MlflowClient()

    champion = client.get_model_version_by_alias(MODEL_NAME, 'champion')
    model_uri = f'models:/{MODEL_NAME}/@champion'
    model_version = champion.version
    context.log.info(f'champion model: version={model_version}, uri={model_uri}')

    # read scheduled flights from DuckDB
    # duckdb connects to the same file used by dbt feature models
    # staged_flights was populated by the staging asset and contains partitioned flight records
    con = duckdb.connect(settings.duckdb_path, read_only=True)
    query = _FLIGHTS_SQL.format(score_date=score_date_str, run_time=run_time.isoformat())
    entity_df: pd.DataFrame = con.execute(query).df()
    con.close()

    if entity_df.empty:
        context.log.warning(f'No flights found for {score_date_str} - skipping')
        return MaterializeResult(
            metadata={
                'row_count': MetadataValue.int(0),
                'score_date': MetadataValue.text(score_date_str),
            }
        )

    entity_df['event_timestamp'] = pd.to_datetime(entity_df['event_timestamp'], utc=True)
    entity_df['scheduled_departure_utc'] = pd.to_datetime(
        entity_df['scheduled_departure_utc'], utc=True
    )
    context.log.info(f'loaded {len(entity_df)} flights from staged_flights')

    store = FeatureStore(repo_path=str(FEATURE_REPO_DIR))

    result = score_partition(
        score_date=score_date,
        model_uri=model_uri,
        model_name=MODEL_NAME,
        model_version=model_version,
        entity_df=entity_df,
        feature_store=store,
        s3_base='s3://staging/predictions',
        s3_endpoint_url=settings.s3_endpoint_url,
        s3_access_key_id=settings.s3_access_key_id,
        s3_secret_access_key=settings.s3_secret_access_key,
        s3_region=settings.s3_region,
    )

    return MaterializeResult(
        metadata={
            'score_date': MetadataValue.text(result.score_date),
            'model_name': MetadataValue.text(result.model_name),
            'model_version': MetadataValue.text(result.model_version),
            'row_count': MetadataValue.int(result.row_count),
            'positive_rate': MetadataValue.float(result.positive_rate),
            'null_feature_rows': MetadataValue.int(result.null_feature_rows),
            'storage_path': MetadataValue.url(result.storage_path),
            'scored_at': MetadataValue.text(result.scored_at),
        }
    )


@asset(
    group_name='serving',
    deps=['registered_model'],
    description=(
        'Writes the current champion model config to s3://staging/serving/model_config.json. '
        'The FastAPI service reads this file on startup and on POST /admin/reload. '
        'When a new champion is registered, materialize this asset and call /admin/reload '
        'on the Fly.io machine to hot-swap the model without a container restart.'
    ),
)
def deployed_api(context: AssetExecutionContext) -> MaterializeResult:
    """
    Publish champion model metadata to S3 for the FastAPI service to consume.

    This asset is the bridge between the Dagster training pipeline and the
    live serving API. It doesn't deploy a container — it writes a config file
    that the already-running FastAPI service reads when reloaded.

    Hot-swap workflow:
      1. new champion model registered → registered_model asset materializes
      2. deployed_api asset materializes (triggered by Dagster or manually)
      3. deployed_api writes model_config.json to S3
      4. operator (or automation) calls POST /admin/reload on the Fly.io machine
      5. FastAPI reads the new model_config.json and loads the model from MLflow
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = MlflowClient()

    champion = client.get_model_version_by_alias(MODEL_NAME, 'champion')

    config = {
        'model_name': MODEL_NAME,
        'model_version': champion.version,
        'model_uri': f'models:/{MODEL_NAME}@champion',
        'registered_at': champion.creation_timestamp,
        'tags': dict(champion.tags) if champion.tags else {},
        'published_at': datetime.now(timezone.utc).isoformat(),
    }

    fs = s3fs.S3FileSystem(
        key=settings.s3_access_key_id,
        secret=settings.s3_secret_access_key,
        endpoint_url=settings.se_endpoint_url,
        client_kwargs={'region_name': settings.s3_region},
    )

    config_path = 's3://staging/serving/model_config.json'
    with fs.open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    context.log.info(f'Published model config: version={champion.version}, path={config_path}')

    return MaterializeResult(
        metadata={
            'model_name': MetadataValue.text(MODEL_NAME),
            'model_version': MetadataValue.text(champion.version),
            'model_uri': MetadataValue.text(config['model_uri']),
            'config_path': MetadataValue.url(config_path),
        }
    )
