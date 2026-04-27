"""
Raw-layer assets: ingestion from external sources into S3.

Asset graph:
    raw_faa_airports ──► station_map ──► raw_noaa_weather  (monthly partitioned)
    raw_openflights_routes
    raw_bts_flights                                         (monthly partitioned)
"""

import io
import json
from datetime import timedelta

import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    FreshnessPolicy,
    MaterializeResult,
    MetadataValue,
    asset,
)

from bmo.common.storage import make_object_store
from bmo.ingestion.bts import IngestResult, ingest_month
from bmo.ingestion.faa import ingest_airports, ingest_routes
from bmo.ingestion.noaa import NoaaIngestResult, build_station_map, ingest_noaa_month
from bmo.serving.partitions import MONTHLY_PARTITIONS


@asset(group_name='raw', metadata={'source': 'https://ourairports.com/data/airports.csv'})
def raw_faa_airports(context: AssetExecutionContext) -> MaterializeResult:
    """US commercial airports — ICAO/IATA codes, lat/lon, elevation."""
    store = make_object_store()
    table = ingest_airports(store)  # writes faa/airports.parquet to S3, returns table
    return MaterializeResult(metadata={'row_count': MetadataValue.int(table.num_rows)})


@asset(
    group_name='raw',
    metadata={
        'source': 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat'
    },
)
def raw_openflights_routes(context: AssetExecutionContext) -> MaterializeResult:
    """Nonstop route graph (airline, origin IATA, dest IATA)."""

    store = make_object_store()
    ingest_routes(store)  # writes openflights/routes.parquet to S3
    # still return materializeResult to tell Dagster we handled storing result
    return MaterializeResult()


@asset(
    # alternatively pass as arg to station_map (use when you want to use the result of upstream function)
    deps=[raw_faa_airports],
    group_name='raw',
)
def station_map(context: AssetExecutionContext) -> MaterializeResult:
    """
    {iata_code: lcd_station_id} mapping built from NOAA ISD history.
    Stored as JSON at raw/noaa/_station_map.json so partitioned NOAA runs
    can read it without re-downloading the ISD file each time.
    """
    store = make_object_store()

    existing = store.read_json_or_none('raw', 'noaa/_station_map.json')
    if existing is not None:
        context.log.info('Station map already exists in MinIO, skipping download')
        return MaterializeResult(metadata={'station_count': MetadataValue.int(len(existing))})

    # Read the already-stored FAA airports parquet to get the IATA code set.
    # We don't take raw_faa_airports as a function argument — we stored it in S3
    # and read it back. deps=[raw_faa_airports] just ensures ordering.
    obj = store.client.get_object(Bucket='raw', Key='faa/airports.parquet')
    airports_table = pq.read_table(io.BytesIO(obj['Body'].read()))
    iata_codes: set[str] = {
        c for c in airports_table.column('iata_code').to_pylist() if c is not None
    }

    mapping = build_station_map(iata_codes)

    store.put_bytes('raw', 'noaa/_station_map.json', json.dumps(mapping).encode())
    return MaterializeResult(metadata={'station_count': MetadataValue.int(len(mapping))})


# --------------------------------------------------------------------------#
#                       Monthly partitioned assets                          #
# --------------------------------------------------------------------------#


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    group_name='raw',
    metadata={'source': 'https://transtats.bts.gov/PREZIP/'},
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=timedelta(days=35), warn_window=timedelta(days=32)
    ),
)
def raw_bts_flights(context: AssetExecutionContext) -> MaterializeResult:
    """Monthly BTS on-time performance data, converted to Parquet."""
    partition_key = context.partition_key  # "2026-01-01"
    # dt = dg.TimeWindow  # not needed - parse directly
    year, month, *_ = (int(x) for x in partition_key.split('-'))

    store = make_object_store()
    result: IngestResult = ingest_month(year=year, month=month, store=store)

    return MaterializeResult(
        metadata={
            'row_count': MetadataValue.int(result.row_count),
            'skipped': MetadataValue.bool(result.skipped),
            'source_url': MetadataValue.url(result.source_url),
            'source_sha256': MetadataValue.text(result.source_sha256),
            'target_uri': MetadataValue.text(result.target_uri),
        }
    )


@asset(
    partitions_def=MONTHLY_PARTITIONS,
    deps=[station_map],
    group_name='raw',
    metadata={'source': 'https://www.ncei.noaa.gov/data/local-climatological-data'},
)
def raw_noaa_weather(context: AssetExecutionContext) -> MaterializeResult:
    """Monthly NOAA LCD FM-15 hourly observations for all BTS airports."""
    partition_key = context.partition_key
    year_str, month_str, *_ = partition_key.split('-')
    year, month = int(year_str), int(month_str)

    store = make_object_store()

    # Load the station map written by the station_map asset.
    # station_map is unpartitioned, so there's always exactly one version.
    obj = store.client.get_object(Bucket='raw', Key='noaa/_station_map.json')
    mapping: dict[str, str] = json.loads(obj['Body'].read())

    result: NoaaIngestResult = ingest_noaa_month(
        year=year, month=month, station_map=mapping, store=store
    )

    return MaterializeResult(
        metadata={
            'row_count': MetadataValue.int(result.row_count),
            'station_count': MetadataValue.int(result.station_count),
            # 'skipped': MetadataValue.bool(result.skipped),
            # 'source_url': MetadataValue.url(result.source_url),
            # 'source_sha256': MetadataValue.text(result.source_sha256),
            'target_uri': MetadataValue.text(result.target_uri),
        }
    )
