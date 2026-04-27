"""
Dagster asset: training_dataset.

Reads labels from the dbt mart, calls build_dataset() for PIT-correct features,
and surfaces the DatasetHandle as structured asset metadata.

The training_dataset asset is a thin orchestration wrapper. All business logic
lives in bmo.training_dataset_builder, which is testable without Dagster.
"""

# from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import duckdb
import mlflow
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from bmo.common.config import settings
from bmo.training.hpo import run_hpo
from bmo.training_dataset_builder import DatasetHandle, LeakageError, build_dataset
from bmo.training_dataset_builder.pit_join import default_feature_view_configs

# all features available from the 5 feature views
# remove a feature group, retrain, compare AUC
ALL_FEATURE_REFS = [
    # origin airport (1h and 24h windows)
    'origin_airport_features:origin_flight_count_1h',
    'origin_airport_features:origin_avg_dep_delay_1h',
    'origin_airport_features:origin_pct_delayed_1h',
    'origin_airport_features:origin_avg_dep_delay_24h',
    'origin_airport_features:origin_pct_cancelled_24h',
    'origin_airport_features:origin_avg_dep_delay_7d',
    'origin_airport_features:origin_pct_delayed_7d',
    'origin_airport_features:origin_congestion_score_1h',
    # destination airport
    'dest_airport_features:dest_avg_arr_delay_1h',
    'dest_airport_features:dest_pct_delayed_1h',
    'dest_airport_features:dest_avg_arr_delay_24h',
    'dest_airport_features:dest_pct_diverted_24h',
    # carrier (7d rolling)
    'carrier_features:carrier_on_time_pct_7d',
    'carrier_features:carrier_cancellation_rate_7d',
    'carrier_features:carrier_avg_delay_7d',
    'carrier_features:carrier_flight_count_7d',
    # route (7d rolling)
    'route_features:route_avg_dep_delay_7d',
    'route_features:route_avg_arr_delay_7d',
    'route_features:route_pct_delayed_7d',
    'route_features:route_cancellation_rate_7d',
    'route_features:route_avg_elapsed_7d',
    'route_features:route_distance_mi',
    # cascading delay (aircraft-level, 12h TTL)
    'aircraft_features:cascading_delay_min',
    'aircraft_features:turnaround_min',
]

# label columns in mart_training_dataset that are training targets
# calendar features from dbt mark are excluded - to be recomputed by builder
_LABEL_COLUMNS = [
    'dep_delay_min',
    'arr_delay_min',
    'is_dep_delayed',
    'is_arr_delayed',
    'cancelled',
    'diverted',
]

_ENTITY_COLUMNS = [
    'flight_id',
    'event_timestamp',
    'origin',
    'dest',
    'carrier',
    'tail_number',
    'route_key',
]

# override for fast local runs (DAGSTER_HPO_N_TRIALS=5 make dagster-dev) - TODO: move to pydantic ??
_HPO_N_TRIALS = int(os.getenv('DAGSTER_HPO_N_TRIALS', '50'))


@asset(
    group_name='training',
    deps=['feast_materialized_features'],
    freshness_policy=FreshnessPolicy.cron(
        deadline_cron='0 3 * * *', lower_bound_delta=timedelta(hours=2)
    ),
    description=(
        'Point-in-time correct training dataset. '
        'Reads labels from mar_training_dataset, joins features via DuckDB ASOF JOIN, '
        'runs leakage guards, and writes a content-addressed Parquet to S3. '
        'The DatasetHandle version_hash is emitted as asset metadata and should be '
        'logged as an MLflow parameter on every downstream training run.'
    ),
)
def training_dataset(context: AssetExecutionContext) -> MaterializeResult:
    """
    Build a PIT-correct training dataset from dbt mart labels + Feast features.

    Dependency chain:
      feast_materialized_features
        → (depends on) feast_feature_export
          → (depends on) bmo_dbt_assets (feat_* models)
            → (depends on) staged_flights, staged_weather, dim_airport

    The asset declares dep on feast_materialized_features (not directly on
    bmo_dbt_assets) because we need features in the Feast offline store (S3),
    not just the DuckDB tables. The feast_feature_export asset bridges DuckDB → S3.
    """
    con = duckdb.connect(settings.duckdb_path, read_only=True)

    # read label columns from dbt mart
    # strip feature cols from mart to let build_dataset retrieve via PIT join to ensure same temporal correctness logic as in production serving
    select_cols = (
        ', '.join(_entity_col_sql(col) for col in _ENTITY_COLUMNS)
        + ', '
        + ', '.join(_LABEL_COLUMNS)
    )

    raw = con.execute(f'SELECT {select_cols} FROM mart_training_dataset').df()  # noqa S608
    con.close()

    # event_timestamp must be tz aware for PIT join and leakage guards
    raw['event_timestamp'] = pd.to_datetime(raw['event_timestamp'], utc=True)
    context.log.info(f'loaded {len(raw)} label rows from mart_training_dataset')

    # as_of is pipeline run timestamp
    as_of = datetime.now(timezone.utc)

    try:
        handle: DatasetHandle = build_dataset(
            label_df=raw,
            feature_refs=ALL_FEATURE_REFS,
            as_of=as_of,
            output_base_path=settings.dataset_s3_base,
            feature_views=default_feature_view_configs(settings.feast_s3_base),
            skip_if_exists=True,
        )
    except LeakageError as exc:
        context.log.error(f'Leakage guard failure: {exc}')
        raise

    context.log.info(f'Dataset built: {handle.row_count} rows, hash={handle.version_hash[:12]}...')

    return MaterializeResult(
        metadata={
            'version_hash': MetadataValue.text(handle.version_hash),
            'row_count': MetadataValue.int(handle.row_count),
            'storage_path': MetadataValue.url(handle.storage_path),
            'schema_fingerprint': MetadataValue.text(handle.schema_fingerprint),
            'as_of': MetadataValue.text(as_of.isoformat()),
            'feature_views': MetadataValue.int(
                len({ref.split(':')[0] for ref in ALL_FEATURE_REFS})
            ),
            'feature_count': MetadataValue.int(len(ALL_FEATURE_REFS)),
            # Embed label distributions so they're visible in the Dagster UI
            # without opening MLflow. Useful for quick sanity checks.
            **{
                f'label_dist/{name}': MetadataValue.float(dist.positive_rate or dist.mean)
                for name, dist in handle.label_distribution.items()
            },
        }
    )


@asset(
    group_name='training',
    deps=['training_dataset'],
    description=(
        'XGBoost model trained via Optuna HPO (50 trials, TPE sampler, MedianPruner). '
        'Each trial is a nested MLflow child run with XGBoostPruningCallback for mid-trial pruning. '
        'Best params re-run has full artifact logging.'
    ),
)
def trained_model(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run HPO and log the best XGBoost model to MLflow.

    Reads DatasetHandle from the upstream training_dataset materialization's
    card.json sidecar on S3. This avoids a direct Python object dependency
    between assets while keeping the DatasetHandle as the authoritative record.
    """
    latest_event = context.instance.get_latest_materialization_event(AssetKey(['training_dataset']))
    if (
        latest_event is None or latest_event.asset_materialization is None
    ):  # not already handled by deps ??
        raise RuntimeError(
            'No training_dataset materialization found. '
            'Materialize training_dataset before running trained_model.'
        )

    metadata = latest_event.asset_materialization.metadata
    storage_path: str = str(metadata['storage_path'].value)
    version_hash: str = str(metadata['version_hash'].value)

    card_path = storage_path.replace('data.parquet', 'card.json')
    handle = _load_dataset_handle(card_path)

    context.log.info(
        f'Loaded DatasetHandle {version_hash[:12]}...'
        f'({handle.row_count:,} rows, {len(handle.feature_refs)} features)'
    )

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

    context.log.info(f'starting HPO sweep: {_HPO_N_TRIALS} trials')
    hpo_result = run_hpo(
        handle=handle,
        n_trials=_HPO_N_TRIALS,
        target_column='is_dep_delayed',
        run_mllib_baseline=True,
    )

    context.log.info(
        f'HPO complete. Best AUC: {hpo_result.best_auc:.4f} '
        f'({hpo_result.n_trials_completed} trials, {hpo_result.n_trials_pruned} pruned)'
    )

    return MaterializeResult(
        metadata={
            'mlflow_run_id': MetadataValue.text(hpo_result.best_run_id),
            'mlflow_parent_run_id': MetadataValue.text(hpo_result.parent_mlflow_run_id),
            'best_roc_auc': MetadataValue.float(hpo_result.best_auc),
            'n_trials_completed': MetadataValue.int(hpo_result.n_trials_completed),
            'n_trials_pruned': MetadataValue.int(hpo_result.n_trials_pruned),
            'dataset_version_hash': MetadataValue.text(hpo_result.dataset_version_hash),
            'hpo_sweep_duration_s': MetadataValue.float(
                (hpo_result.sweep_ended_at - hpo_result.sweep_started_at).total_seconds()
            ),
            **{
                f'best_param/{k}': (
                    MetadataValue.float(v) if isinstance(v, float) else MetadataValue.int(int(v))
                )
                for k, v in hpo_result.best_params.items()
                if isinstance(v, (int, float))
            },
        }
    )


def _entity_col_sql(col: str) -> str:
    """Map entity column names to their SQL equivalents in mart_training_dataset."""
    # scheduled_departure_utc is stored as event_timestamp in the mart
    if col == 'event_timestamp':
        return 'scheduled_departure_utc AS event_timestamp'
    return col


def _load_dataset_handle(card_path: str) -> DatasetHandle:
    if card_path.startswith('s3://'):
        import s3fs

        fs = s3fs.S3FileSystem(
            key=settings.s3_access_key_id,
            secret=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
        )
        with fs.open(card_path, 'rb') as f:
            data = json.loads(f.read())
    else:
        data = json.loads(open(card_path).read())  # noqa SIM115
    return DatasetHandle.model_validate(data)


@asset(
    group_name='training',
    deps=['trained_model'],
    freshness_policy=FreshnessPolicy.cron(
        deadline_cron='0 3 * * *', lower_bound_delta=timedelta(hours=2)
    ),
    description=(
        'Registers the champion XGBoost model in the MLflow Model Registry. '
        'Only materializes after all blocking @asset_check functions on trained_model pass. '
        'Sets the "challenger" alias unconditionally. '
        'Promotes to "champion" if the new model beats the current champion AUC, '
        'or if no champion exists. '
        'Generates an Evidently classification report and logs it as an MLflow artifact.'
    ),
)
def registered_model(context: AssetExecutionContext) -> MaterializeResult:
    """
    Register the trained model in the MLflow Model Registry.

    Asset dependency graph:
      training_dataset → trained_model → [eval gate checks] → registered_model

    The @asset_check(blocking=True) functions on trained_model prevent this
    asset from being auto-materialized if any blocking check failed.
    A defensive lightweight re-check here guards against manual forced runs.
    """
    from mlflow.tracking import MlflowClient

    from bmo.evaluation_gate.checks import AUCGateCheck, LeakageSentinelCheck
    from bmo.evaluation_gate.gate import MODEL_NAME, load_gate_input
    from bmo.evaluation_gate.reports import generate_classification_report

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

    # get champion run ID logged by trained_model
    latest_event = context.instance.get_latest_materialization_event(AssetKey(['trained_model']))
    if latest_event is None or latest_event.asset_materialization is None:
        raise RuntimeError(
            'No trained_model materialization found. '
            'Materialize trained_model before running registered_model.'
        )

    metadata = latest_event.asset_materialization.metadata
    run_id = str(metadata['mlflow_run_id'].value)
    dataset_version_hash = str(metadata['dataset_version_hash'].value)

    context.log.info(f'Registering model for MLflow run {run_id[:12]}...')

    gate_input = load_gate_input(run_id)
    for check_cls in (AUCGateCheck, LeakageSentinelCheck):
        result = check_cls().run(gate_input)
        if result.blocking:
            raise RuntimeError(
                f'Gate check {result.name} failed defensively in registered_model: '
                f'{result.message}, '
                'All blocking asset checks must pass before model registration'
            )

    client = MlflowClient()

    try:
        client.create_registered_model(
            name=MODEL_NAME,
            description='XGBoost flight delay classifier - BTS On-Time Performance data. ',
            tags={'project': 'bmo', 'target': 'is_dep_delayed'},
        )
        context.log.info(f'Created registered model: {MODEL_NAME}')
    except Exception:
        context.log.info(f'Registered model already exists: {MODEL_NAME}')

    # register run as a new model version
    model_version = mlflow.register_model(
        model_uri=f'runs:/{run_id}/model',
        name=MODEL_NAME,
        tags={
            'dataset_version_hash': dataset_version_hash,
            'auc': str(round(gate_input.metrics.get('test_roc_auc', 0.0), 4)),
        },
    )
    version_num = model_version.version
    context.log.info(f'Registered version {version_num} for run {run_id[:12]}...')

    # set challenger alias on the new version
    client.set_registered_model_alias(MODEL_NAME, 'challenger', version_num)
    context.log.info(f'set alias: challenger -> version {version_num}')

    # promote champion if better than current champion
    current_champion_version: str | None = None
    promoted_to_champion = False
    new_auc = gate_input.metrics.get('test_roc_auc', 0.0)

    if gate_input.prod_metrics is not None and gate_input.prod_run_id is not None:
        prod_auc = gate_input.prod_metrics.get('test_roc_auc', 0.0)
        should_promote = new_auc >= prod_auc
        context.log.info(
            f'Champion comparison: new={new_auc:.4f}, prod={prod_auc:.4f}, promote={should_promote}'
        )
    else:
        should_promote = True  # no champion yet
        context.log.info('No current champion - promoting immediately')

    if should_promote:
        # remove champion alias from old version before assigning to new one
        try:
            old_champion = client.get_model_version_by_alias(MODEL_NAME, 'champion')
            current_champion_version = old_champion.version
            client.delete_registered_model_alias(MODEL_NAME, 'champion')
            client.set_model_version_tag(MODEL_NAME, old_champion.version, 'status', 'archived')
            context.log.info(f'Archived old champion (version {old_champion.version})')
        except Exception:
            pass  # no existing champion alias to remove

        client.set_registered_model_alias(MODEL_NAME, 'champion', version_num)
        promoted_to_champion = True
        context.log.info(f'set alias: champion -> version {version_num}')

    # generate evidently report and log MLflow artifact
    report_path = generate_classification_report(
        mlflow_run_id=run_id,
        dataset_storage_path=gate_input.dataset_storage_path,
    )
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(report_path, artifact_path='reports')
    context.log.info(f'Evidently report logged: {report_path}')

    return MaterializeResult(
        metadata={
            'model_name': MetadataValue.text(MODEL_NAME),
            'model_version': MetadataValue.text(str(version_num)),
            'mlflow_run_id': MetadataValue.text(run_id),
            'promoted_to_champion': MetadataValue.text(str(promoted_to_champion)),
            'new_auc': MetadataValue.float(round(new_auc, 4)),
            'dataset_version_hash': MetadataValue.text(dataset_version_hash),
            'evidently_report': MetadataValue.text(report_path),
            **(
                {'archived_champion_version': MetadataValue.text(str(current_champion_version))}
                if current_champion_version
                else {}
            ),
        }
    )
