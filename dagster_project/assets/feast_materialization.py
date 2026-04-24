from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from feast import FeatureStore

from bmo.common.config import settings

# dbt models produce DuckDB tables. Feast needs Parquet on S3. This asset bridges the two.


FEATURE_REPO_DIR = Path(__file__).parent.parent.parent / 'feature_repo'


# // TODO: use pydantic for env vars ??
def _get_s3fs() -> s3fs.S3FileSystem:
    """Build an s3fs client using project env vars (MinIO or R2 compatible)."""
    return s3fs.S3FileSystem(
        key=settings.s3_access_key_id,
        secret=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        client_kwargs={'region_name': settings.s3_region},
    )


def _export_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    entity_col: str,
    feature_cols: list[str],
    s3: s3fs.S3FileSystem,
    s3_path: str,
) -> int:
    """
    Read a dbt feature table from DuckDB, select entity+timestamp+features,
    and write to Parquet on S3. Returns row count written.

    Select only the columns Feast needs — entity key, event_ts, and the
    feature values themselves. flight_id is deliberately excluded because
    it's an internal surrogate key that Feast doesn't need and can't use
    for entity resolution.
    """
    cols = ', '.join([entity_col, 'event_ts'] + feature_cols)
    df: pd.DataFrame = con.execute(f'SELECT {cols} FROM {table}').df()  # noqa: S608

    table_arrow = pa.Table.from_pandas(df, preserve_index=False)

    with s3.open(f'{s3_path}/data.parquet', 'wb') as f:
        pq.write_table(table_arrow, f, compression='zstd')

    return len(df)


def _export_cascading_delay(s3: s3fs.S3FileSystem, row_counts: dict) -> None:
    """
    The cascading delay feature lives in Iceberg (written by PySpark), not DuckDB.
    Read via PyArrow and re-export to Feast's expected path.
    """
    import pyiceberg.catalog

    catalog = pyiceberg.catalog.load_catalog(
        'default',
        **{
            'type': 'sql',
            'uri': settings.iceberg_catalog_uri,
            's3.endpoint': settings.s3_endpoint_url,
            's3.access-key-id': settings.s3_access_key_id,
            's3.secret-access-key': settings.s3_secret_access_key,
            's3.region': 'auto',
            's3.path-style-access': 'true',
        },
    )
    table = catalog.load_table('staging.feat_cascading_delay')
    df = table.scan(
        selected_fields=(
            'tail_number',
            'scheduled_departure_utc',
            'prev_arr_delay_min',
            'turnaround_min',
        ),
    ).to_pandas()
    df = df.rename(
        columns={
            'scheduled_departure_utc': 'event_ts',
            'prev_arr_delay_min': 'cascading_delay_min',
        }
    )

    dest_path = f'{settings.feast_s3_base}/aircraft'
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    with s3.open(f'{dest_path}/data.parquet', 'wb') as f:
        pq.write_table(arrow_table, f, compression='zstd')

    row_counts['feat_cascading_delay'] = len(df)


@asset(
    group_name='feast',
    deps=['bmo_dbt_assets', 'feat_cascading_delay'],
    description=(
        'Export dbt feature model outputs from DuckDB to S3 Parquet for Feast offline store. '
        'Runs after dbt build completes. Each entity type gets its own S3 prefix'
    ),
)
def feast_feature_export(context: AssetExecutionContext) -> MaterializeResult:
    con = duckdb.connect(settings.duckdb_path, read_only=True)
    s3 = _get_s3fs()
    row_counts: dict[str, int] = {}

    exports = [
        {
            'table': 'feat_origin_airport_windowed',
            'entity_col': 'origin',
            'features': [
                'origin_flight_count_1h',
                'origin_avg_dep_delay_1h',
                'origin_pct_delayed_1h',
                'origin_avg_dep_delay_24h',
                'origin_pct_cancelled_24h',
                'origin_avg_dep_delay_7d',
                'origin_pct_delayed_7d',
                'origin_congestion_score_1h',
            ],
            's3_path': f'{settings.feast_s3_base}/origin_airport',
        },
        {
            'table': 'feat_dest_airport_windowed',
            'entity_col': 'dest',
            'features': [
                'dest_avg_arr_delay_1h',
                'dest_pct_delayed_1h',
                'dest_avg_arr_delay_24h',
                'dest_pct_diverted_24h',
            ],
            's3_path': f'{settings.feast_s3_base}/dest_airport',
        },
        {
            'table': 'feat_carrier_rolling',
            'entity_col': 'carrier',
            'features': [
                'carrier_on_time_pct_7d',
                'carrier_cancellation_rate_7d',
                'carrier_avg_delay_7d',
                'carrier_flight_count_7d',
            ],
            's3_path': f'{settings.feast_s3_base}/carrier',
        },
        {
            'table': 'feat_route_rolling',
            'entity_col': 'route_key',
            'features': [
                'route_avg_dep_delay_7d',
                'route_avg_arr_delay_7d',
                'route_pct_delayed_7d',
                'route_cancellation_rate_7d',
                'route_avg_elapsed_7d',
                'route_distance_mi',
            ],
            's3_path': f'{settings.feast_s3_base}/route',
        },
    ]

    for spec in exports:
        count = _export_table(
            con=con,
            table=spec['table'],
            entity_col=spec['entity_col'],
            feature_cols=spec['features'],
            s3=s3,
            s3_path=spec['s3_path'],
        )
        row_counts[spec['table']] = count
        context.log.info(f'Exported {spec["table"]}: {count} rows -> {spec["s3_path"]}')

    # cascading delay comes from the iceberg table written by PySpark job,
    # not from DuckDB. Read via PyArrow/s3fs directly
    _export_cascading_delay(s3, row_counts)

    con.close()

    return MaterializeResult(
        metadata={name: MetadataValue.int(count) for name, count in row_counts.items()}
    )


@asset(
    group_name='feast',
    deps=['feast_feature_export'],
    description=(
        'Run feast materialize-incremental to push latest feature value from S3 Parquet '
        'into the Redis online store. Downstream serving code reads from Redis.'
    ),
)
def feast_materialized_features(context: AssetExecutionContext) -> MaterializeResult:
    store = FeatureStore(repo_path=str(FEATURE_REPO_DIR))
    end_date = datetime.now(timezone.utc)

    # materialize_incremental only processes data newer than the last materialization
    # feast tracks per feature view in the registry ==> idempotent
    store.materialize_incremental(end_date=end_date)

    # surface materialization timestamp as asset metadata for Dagster UI
    return MaterializeResult(
        metadata={
            'materialized_through': MetadataValue.text(end_date.isoformat()),
            'feature_views': MetadataValue.int(len(store.list_feature_views())),
        }
    )


# Why materialize_incremental instead of materialize?
#
# materialize takes explicit start/end timestamps and re-processes everything in that window. materialize_incremental tracks the high-water mark per feature view in the registry and only processes new data. For an hourly schedule, materialize_incremental is correct — it avoids re-sending features that are already in Redis.
#
# Docs: https://docs.feast.dev/reference/feast-cli-commands#feast-materialize-incremental
