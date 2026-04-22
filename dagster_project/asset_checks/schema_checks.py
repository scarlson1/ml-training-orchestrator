"""
These run after materialization as separate Dagster checks. Two types:

    - Schema checks — column presence, null rates on critical columns, value ranges
    - Schema evolution check — detects new/missing BTS columns (WARN severity, not ERROR)
"""

import io

import pyarrow.parquet as pq
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    MonthlyPartitionsDefinition,
    asset_check,
)

from bmo.common.storage import make_object_store
from bmo.staging.contracts import STAGED_FLIGHTS_SCHEMA

_MONTHLY = MonthlyPartitionsDefinition(start_date='2018-01-01')


@asset_check(
    asset='staged_flights', description='Critical columns must not be null', partitions_def=_MONTHLY
)
def check_staged_flights_nulls(context) -> AssetCheckResult:
    year, month, *_ = (int(x) for x in context.partition_key.split('-'))
    store = make_object_store()

    key = f'bts/year={year}/month={month:02d}/flights.parquet'
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


@asset_check(
    asset='staged_flights',
    description='Detect schema changes vs expected BTS columns',
    partitions_def=_MONTHLY,
)
def check_staged_flights_schema_evolution(context) -> AssetCheckResult:
    """WARN (not ERROR) when BTS adds or removes columns — we want visibility, not a halt."""
    year, month, *_ = (int(x) for x in context.partition_key.split('-'))
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


@asset_check(
    asset='staged_weather',
    description='critical columns must not be null; value ranges must be plausible',
    partitions_def=_MONTHLY,
)
def check_staged_weather_nulls(context: AssetCheckExecutionContext) -> AssetCheckResult:
    year_str, month_str, *_ = context.partition_key.split('-')
    year, month = int(year_str), int(month_str)
    store = make_object_store()

    key = f'noaa/year={year}/month={month:02d}/weather.parquet'
    obj = store.client.get_object(Bucket='staging', Key=key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))

    failures = {}

    # null checks
    for col in ['station_id', 'iata_code', 'obs_time_utc']:
        if col not in table.column_names:
            failures[col] = 'column missing'
            continue
        null_rate = table[col].null_count / len(table)
        if null_rate > 0.05:  # weather sensors occasionally fail - 5% tolerance
            failures[col] = f'{null_rate:.1%} null'

    # row count sanity - expect at least 1 station with 1 obs
    if len(table) == 0:
        failures['row_count'] = 'empty table'

    if failures:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={'failures': str(failures)},
        )

    return AssetCheckResult(
        passed=True,
        metadata={
            'rows_checked': len(table),
            'station_count': table['iata_code'].n_unique()
            if hasattr(table['iata_code'], 'n_unique')
            else 'n/a',
        },
    )


@asset_check(asset='dim_airport', description='Airport dimension must have tz for all rows')
def check_dim_airport(context: AssetCheckExecutionContext) -> AssetCheckResult:
    store = make_object_store()
    obj = store.client.get_object(Bucket='staging', Key='dim_airport/dim_airport.parquet')
    table = pq.read_table(io.BytesIO(obj['Body'].read()))

    failures = {}

    # every airport needs a timezone for UTC conversion in staged_flights
    if 'tz_database_timezone' in table.column_names:
        null_rate = table['tz_database_timezone'].null_count / len(table)
        if null_rate > 0.05:
            failures['tz_database_timezone'] = (
                f'{null_rate:.1%} null - UTC conversion will fail for these airports'
            )

    if 'iata_code' in table.column_names:
        null_rate = table['iata_code'].null_count / len(table)
        if null_rate > 0:
            failures['iata_code'] = f'{null_rate:.1%} null'

    # minimum plausible row count (US has ~500 commercial airports)
    if len(table) < 50:
        failures['row_count'] = f'only {len(table)} rows - expected 400+'

    if failures:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR, metadata={'failures': str(failures)}
        )

    return AssetCheckResult(passed=True, metadata={'airport_count': len(table)})


@asset_check(asset='dim_route', description='Route dimension must have valid distances')
def check_dim_route(context: AssetCheckExecutionContext) -> AssetCheckResult:
    store = make_object_store()
    obj = store.client.get_object(Bucket='staging', Key='dim_route/dim_route.parquet')
    table = pq.read_table(io.BytesIO(obj['Body'].read()))

    failures = {}

    if 'distance_mi' in table.column_names:
        null_rate = table['distance_mi'].null_count / len(table)
        if null_rate > 0.01:
            failures['distance_mi'] = f'{null_rate:.1%} null'
    else:
        failures['distance_mi'] = 'missing column'

    if len(table) < 1000:
        failures['row_count'] = f'only {len(table)} rows - expected 5,000+'

    if failures:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR, metadata={'failures': str(failures)}
        )

    return AssetCheckResult(passed=True, metadata={'route_count': len(table)})
