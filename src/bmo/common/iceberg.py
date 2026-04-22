"""
PyIceberg catalog factory and per-partition overwrite helpers.

All staging writers share one catalog (SqlCatalog backed by SQLite). Data
files land in MinIO under s3://staging/iceberg/<table_name>/.

MinIO requires path-style access (not virtual-hosted), so s3.path-style-access
must be true in the catalog properties.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform


def make_catalog() -> SqlCatalog:
    """Return a SqlCatalog wired to the MinIO endpoint from environment variables."""
    # TODO: use pydantic to get env vars ??
    catalog_uri = os.environ.get('ICEBERG_CATALOG_URI', 'sqlite:////tmp/bmo_iceberg.db')
    endpoint = os.environ['S3_ENDPOINT_URL']
    access_key = os.environ['S3_ACCESS_KEY_ID']
    secret_key = os.environ['S3_SECRET_ACCESS_KEY']

    return SqlCatalog(
        'bmo',
        **{
            'uri': catalog_uri,
            's3.endpoint': endpoint,
            's3.access-key-id': access_key,
            's3.secret-access-key': secret_key,
            's3.region': 'auto',
            's3.path-style-access': 'true',  # required for MinIO
        },
    )


def get_or_create_table(
    catalog: SqlCatalog,
    identifier: str,  # e.g. "staging.staged_flights"
    arrow_schema: pa.Schema,
    location: str,
    partition_column: str | None = None,  # none for unpartitioned dim tables
) -> Table:
    """
    Return the Iceberg table, creating it (and its namespace) if needed.
    Partitioned tables use MonthTransform on the given column.
    """
    namespace, _ = identifier.split('.')
    if not catalog.namespace_exists(namespace):
        catalog.create_namespace(namespace)

    if catalog.table_exists(identifier):
        return catalog.load_table(identifier)

    iceberg_schema = pyarrow_to_schema(arrow_schema)

    if partition_column is not None:
        source_id = iceberg_schema.find_field(partition_column).field_id
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=source_id,
                field_id=1000,
                transform=MonthTransform(),
                name=f'{partition_column}_month',
            )
        )
    else:
        partition_spec = PartitionSpec()  # unpartitioned

    return catalog.create_table(
        identifier=identifier,
        schema=iceberg_schema,
        partition_spec=partition_spec,
        location=location,
    )


def overwrite_month_flights(table: Table, arrow_data: pa.Table, year: int, month: int) -> None:
    """Overwrite exactly one flight_date-based month partition. Idempotent."""
    start = date(year, month, 1).isoformat()
    # Use the first day of the NEXT month as the exclusive upper bound.
    # calendar.monthrange handles year-wrap (month=12 → next year Jan).
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)

    # filter to flights only within start and end dates
    row_filter = And(GreaterThanOrEqual('flight_date', start), LessThan('flight_date', end))

    table.overwrite(arrow_data, overwrite_filter=row_filter)


def overwrite_month_weather(table: Table, arrow_data: pa.Table, year: int, month: int) -> None:
    """Overwrite exactly one month partition. Idempotent."""
    start = datetime(year, month, 1, tzinfo=timezone.utc).isoformat()
    end = (
        datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    )

    row_filter = And(
        GreaterThanOrEqual('obs_time_utc', start),
        LessThan('obs_time_utc', end),
    )

    table.overwrite(arrow_data, overwrite_filter=row_filter)
