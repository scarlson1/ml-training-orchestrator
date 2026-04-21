"""
These run after materialization as separate Dagster checks. Two types:

    - Schema checks — column presence, null rates on critical columns, value ranges
    - Schema evolution check — detects new/missing BTS columns (WARN severity, not ERROR)
"""

import io

import pyarrow.parquet as pq
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from bmo.common.storage import make_object_store
from bmo.staging.contracts import STAGED_FLIGHTS_SCHEMA


@asset_check(asset='staged_flights', description='Critical columns must not be null')
def check_staged_flights_nulls(context) -> AssetCheckResult:
    year, month = (int(x) for x in context.partition_key.split('-'))
    store = make_object_store()

    key = f'bts/year={year}/month={month:02d}/flights/parquet'
    obj = store.client.get_object(Bucket='staging', Key=key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))

    critical = ['scheduled_departure_utc', 'origin', 'dest', 'flight_date']
    failures = {}
    for col in critical:
        if col not in table.column_names:
            failures[col] = 'column missing'
            continue
        null_rate = table[col].null_count / len(table)
        if null_rate > 0.01:  # if greater than 1%, nulls is a problem
            failures[col] = f'{null_rate:.1%} null'

    if failures:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={'failures': str(failures)},
        )

    return AssetCheckResult(passed=True, metadata={'rows_checked': len(table)})


@asset_check(asset='staged_flights', description='Detect schema changes vs expected BTS columns')
def check_staged_flights_schema_evolution(context) -> AssetCheckResult:
    """WARN (not ERROR) when BTS adds or removes columns — we want visibility, not a halt."""
    year, month = (int(x) for x in context.partition_key.split('-'))
    store = make_object_store()

    key = f'bts/year={year}/month={month:02d}/flights.parquet'
    obj = store.client.get_object(Bucket='staging', Key=key)
    schema = pq.read_schema(io.BytesIO(obj['Body'].read()))

    expected = {f.name for f in STAGED_FLIGHTS_SCHEMA}
    actual = set(schema.names)
    added = actual - expected
    removed = expected - actual

    if added or removed:
        return AssetCheckResult(
            passed=True,  # WARN doesn't block downstream
            severity=AssetCheckSeverity.WARN,
            metadata={'added_columns': str(sorted(added)), 'removed_columns': str(sorted(removed))},
        )

    return AssetCheckResult(passed=True)
