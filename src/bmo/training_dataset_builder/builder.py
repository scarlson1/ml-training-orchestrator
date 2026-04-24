"""
build_dataset() — the public API for Phase 5.

Orchestration:
  1. Validate inputs (label_df has required columns, feature_refs are known).
  2. Compute the dataset hash (fast, no I/O). Check if output already exists.
  3. Execute PIT join via PITJoiner. This is the expensive step.
  4. Run leakage guards. Raise on errors, log warnings.
  5. Write Parquet + JSON card to S3. Return DatasetHandle.

The function is idempotent: if the content-addressed output path already
exists, return the cached DatasetHandle without re-running the join.

Feast get_historical_features: https://docs.feast.dev/getting-started/concepts/point-in-time-joins
Feast offline store: https://docs.feast.dev/reference/offline-stores/file
DuckDB S3/httpfs: https://duckdb.org/docs/guides/network_and_cloud/s3_import.html
DuckDB ASOF JOIN: https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import structlog

from bmo.common.config import settings
from bmo.training_dataset_builder.dataset_handle import (
    DatasetHandle,
    compute_dataset_hash,
    compute_label_distributions,
    compute_schema_fingerprint,
)
from bmo.training_dataset_builder.leakage_guards import LeakageGuardResult, run_all_guards
from bmo.training_dataset_builder.pit_join import (
    FeatureViewConfig,
    PITJoiner,
    default_feature_view_configs,
)

log = structlog.get_logger(__name__)

# columns that must exist in label_df. required for PIT join matching. all other columns treated as labels
_REQUIRED_LABEL_COLUMNS = {'flight_id', 'event_timestamp', 'origin', 'dest', 'carrier'}

_METADATA_COLUMNS = {
    'flight_id',
    'event_timestamp',
    'origin',
    'dest',
    'carrier',
    'tail_number',
    'route_key',
}


class LeakageError(ValueError):
    """Raised when leakage guards detect critical violations."""


def build_dataset(
    label_df: pd.DataFrame,
    feature_refs: list[str],
    as_of: datetime | None = None,
    output_base_path: str = 's3://staging/datasets',
    feature_views: list[FeatureViewConfig] | None = None,
    code_version: str | None = None,
    feature_set_version: str | None = None,
    skip_if_exists: bool = True,
) -> DatasetHandle:
    """
    Build a point-in-time correct training dataset.

    Args:
        label_df:            DataFrame with columns: flight_id, event_timestamp
                             (= scheduled_departure_utc), origin, dest, carrier,
                             and one or more label columns (dep_delay_min,
                             is_dep_delayed, arr_delay_min, ...).
                             event_timestamp must be timezone-aware (UTC).

        feature_refs:        List of 'view_name:feature_name' strings identifying
                             which features to include. Example:
                             ['origin_airport_features:origin_avg_dep_delay_1h',
                              'carrier_features:carrier_on_time_pct_7d']

        as_of:               Upper bound on event_timestamp and feature data.
                             Features computed after as_of are excluded.
                             None means 'use all available data' (dev only).

        output_base_path:    S3 prefix where datasets are written. Each dataset
                             lands at {output_base_path}/{version_hash}/data.parquet
                             with a sidecar {output_base_path}/{version_hash}/card.json.

        feature_views:       Override the default feature view configs. If None,
                             uses default_feature_view_configs() which reads paths
                             from FEAST_S3_BASE env var.

        code_version:        Git SHA to embed in the handle. If None, attempts
                             `git rev-parse HEAD`. Falls back to 'unknown'.

        feature_set_version: Identifies the feature registry state. If None,
                             uses git log of feature_repo/ as a proxy.

        skip_if_exists:      If True and the content-addressed output already
                             exists, return the cached DatasetHandle without
                             re-executing the join. Makes the function idempotent.

    Returns:
        DatasetHandle with version_hash, row_count, label_distribution, etc.

    Raises:
        ValueError:   If label_df is missing required columns.
        LeakageError: If leakage guards detect critical violations.
    """
    _validate_label_df(label_df)

    if feature_views is None:
        feature_views = default_feature_view_configs(settings.feast_s3_base)

    if code_version is None:
        code_version = _get_git_sha()

    if feature_set_version is None:
        feature_set_version = _get_feature_repo_sha()

    label_columns = [c for c in label_df.columns if c not in _METADATA_COLUMNS]

    # compute hash to check cache and potentially skip join
    version_hash = compute_dataset_hash(
        label_df=label_df,
        feature_refs=feature_refs,
        as_of=as_of,
        feature_set_version=feature_set_version,
        code_version=code_version,
    )
    output_path = f'{output_base_path}/{version_hash}'
    parquet_path = f'{output_path}/data.parquet'
    card_path = f'{output_path}/card.json'

    log.info('build_dataset started', version_hash=version_hash, rows=len(label_df))

    if skip_if_exists and _s3_exists(parquet_path):
        log.info('dataset already exists, returning cached handle', path=parquet_path)
        return _load_handle_from_card(card_path)

    # filter feature_view to only those needed by feature_refs
    requested_views = _filter_feature_views(feature_views, feature_refs)
    log.info('joining feature views', views=[fv.name for fv in requested_views])

    # PIT join
    joiner = PITJoiner(
        feature_views=requested_views,
        as_of=pd.Timestamp(as_of, tz='UTC') if as_of else None,
        use_s3=output_base_path.startswith('s3'),
    )
    dataset_df = joiner.join(label_df)

    # leakage guards
    feature_ts_columns = [f'{fv.name}__feature_ts' for fv in requested_views]
    ttl_seconds = {fv.name: fv.ttl_seconds for fv in requested_views}

    guard_result: LeakageGuardResult = run_all_guards(
        label_df=label_df,
        dataset_df=dataset_df,
        feature_refs=feature_refs,
        label_columns=label_columns,
        feature_ts_columns=feature_ts_columns,
        ttl_seconds=ttl_seconds,
        as_of=as_of,
    )

    for warning in guard_result.warnings:
        log.warning('leakage guard warning', check=warning.check_name, details=warning.details)

    if not guard_result.passed:
        error_details = '; '.join(e.details for e in guard_result.errors)
        raise LeakageError(
            f'Leakage guard failed - refusing to write dataset. Errors: {error_details}'
        )

    # drop internal __feature_ts columns from final dataset (for leakage check)
    drop_cols = [c for c in dataset_df.columns if c.endswith('__feature_ts')]
    final_df = dataset_df.drop(columns=drop_cols)

    # compute stats
    label_distribution = compute_label_distributions(final_df, label_columns)
    schema_fingerprint = compute_schema_fingerprint(final_df)

    # write Parquet to S3
    _write_parquet_to_s3(final_df, parquet_path)
    log.info('dataset written', path=parquet_path, rows=len(final_df))

    handle = DatasetHandle(
        version_hash=version_hash,
        feature_refs=sorted(feature_refs),
        feature_set_version=feature_set_version,
        feature_ttls=ttl_seconds,
        as_of=as_of,
        row_count=len(final_df),
        label_distribution=label_distribution,
        schema_fingerprint=schema_fingerprint,
        created_at=datetime.now(timezone.utc),
        storage_path=parquet_path,
    )
    _write_card_to_s3(handle, card_path)

    log.info(
        'build_dataset complete',
        version_hash=version_hash,
        rows=len(final_df),
        guard_warnings=len(guard_result.warnings),
    )
    return handle


# ----- helpers ----- #


def _validate_label_df(df: pd.DataFrame) -> None:
    missing = _REQUIRED_LABEL_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f'label_df is missing required columns: {missing}, Required: {_REQUIRED_LABEL_COLUMNS}'
        )
    if 'event_timestamp' in df.columns:
        sample = df['event_timestamp'].iloc[0] if len(df) > 0 else None
        if sample is not None and sample.tzinfo is None:
            raise ValueError(
                'label_df.event_timestamp must be timezone-aware (UTC). '
                'Use pd.to_datetime(col, utc=True) before calling build_dataset().'
            )


def _filter_feature_views(
    feature_views: list[FeatureViewConfig],
    feature_refs: list[str],
) -> list[FeatureViewConfig]:
    """Keep only feature views that have at least one column in feature_refs."""
    requested_view_names = {ref.split(':')[0] for ref in feature_refs}
    return [fv for fv in feature_views if fv.name in requested_view_names]


def _get_s3fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.s3_access_key_id,
        secret=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        client_kwargs={'region_name': settings.s3_region},
    )


def _s3_exists(path: str) -> bool:
    if not path.startswith('s3://'):
        import pathlib

        return pathlib.Path(path).exists()
    try:
        s3 = _get_s3fs()
        return bool(s3.exists(path))
    except Exception:
        return False


def _write_parquet_to_s3(df: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression='zstd')
    buf.seek(0)
    if path.startswith('s3://'):
        s3 = _get_s3fs()
        with s3.open(path, 'wb') as f:
            f.write(buf.read())  # pyright: ignore[reportArgumentType]
    else:
        import pathlib

        p = pathlib.Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(buf.read())


def _write_card_to_s3(handler: DatasetHandle, path: str) -> None:
    card_bytes = handler.model_dump_json(indent=2).encode('utf-8')
    if path.startswith('s3://'):
        s3 = _get_s3fs()
        with s3.open(path, 'wb') as f:
            f.write(card_bytes)  # pyright: ignore[reportArgumentType]
    else:
        import pathlib

        pathlib.Path(path).write_bytes(card_bytes)


def _load_handle_from_card(card_path: str) -> DatasetHandle:
    if card_path.startswith('s3://'):
        s3 = _get_s3fs()
        with s3.open(card_path, 'rb') as f:
            data = json.loads(f.read())
    else:
        data = json.loads(open(card_path).read())  # noqa SIM115
    return DatasetHandle.model_validate(data)


def _get_git_sha() -> str:
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        log.warning('git SHA failed')
        return 'unknown'


def _get_feature_repo_sha() -> str:
    """Use the git tree hash of feature_repo/ as a proxy for feature set version."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD:feature_repo'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        return 'unknown'
