"""
Staging-layer assets: raw Parquet → validated Parquet with UTC timestamps.

Dependency order:
    dim_airport  ←  raw_faa_airports, station_map
    dim_route    ←  raw_openflights_routes, dim_airport
    staged_flights (monthly) ← raw_bts_flights, dim_airport
    staged_weather (monthly) ← raw_noaa_weather
"""

from datetime import timedelta

from dagster import (
    AssetExecutionContext,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from bmo.common.storage import make_object_store
from bmo.serving.partitions import MONTHLY_PARTITIONS
from bmo.staging.dimensions import stage_airports, stage_routes
from bmo.staging.flights import stage_flights
from bmo.staging.weather import stage_weather


@asset(
    group_name='staging',
    deps=['raw_faa_airports', 'station_map'],
)
def dim_airport(context: AssetExecutionContext) -> MaterializeResult:
    store = make_object_store()
    count = stage_airports(store)
    return MaterializeResult(metadata={'row_count': MetadataValue.int(count)})


@asset(
    group_name='staging',
    deps=['raw_openflights_routes', 'dim_airport'],
)
def dim_route(context: AssetExecutionContext) -> MaterializeResult:
    store = make_object_store()
    count = stage_routes(store)
    return MaterializeResult(metadata={'row_count': MetadataValue.int(count)})


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name='staging',
    deps=['raw_bts_flights', 'dim_airport'],
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=timedelta(days=70),  # BTS 2-month publication lag + pipeline buffer
        warn_window=timedelta(days=40),
    ),
)
def staged_flights(context: AssetExecutionContext) -> MaterializeResult:
    year_str, month_str, *_ = context.partition_key.split('-')
    year, month = int(year_str), int(month_str)
    store = make_object_store()

    result = stage_flights(year=year, month=month, store=store)

    return MaterializeResult(
        metadata={
            'valid_count': MetadataValue.int(result.valid_count),
            'rejected_count': MetadataValue.int(result.rejected_count),
            'unknown_tz_count': MetadataValue.int(result.unknown_tz_count),
            'target_uri': MetadataValue.text(result.target_uri),
            'iceberg_snapshot_id': MetadataValue.int(result.snapshot_id),
        }
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name='staging',
    deps=['raw_noaa_weather'],
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=timedelta(days=70),
        warn_window=timedelta(days=40),
    ),
)
def staged_weather(context: AssetExecutionContext) -> MaterializeResult:
    year_str, month_str, *_ = context.partition_key.split('-')
    year, month = int(year_str), int(month_str)
    store = make_object_store()

    result = stage_weather(year=year, month=month, store=store)

    return MaterializeResult(
        metadata={
            'valid_count': MetadataValue.int(result.valid_count),
            'rejected_count': MetadataValue.int(result.rejected_count),
            'target_uri': MetadataValue.text(result.target_uri),
        }
    )
