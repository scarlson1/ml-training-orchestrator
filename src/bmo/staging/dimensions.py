from __future__ import annotations

import io
import json
import math

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bmo.common.storage import ObjectStore

DIM_AIRPORT_SCHEMA = pa.schema(
    [
        ('iata_code', pa.string()),
        ('icao_code', pa.string()),
        ('name', pa.string()),
        ('latitude_deg', pa.float64()),
        ('longitude_deg', pa.float64()),
        ('elevation_ft', pa.float32()),
        ('iso_region', pa.string()),
        ('tz_database_timezone', pa.string()),
        ('lcd_station_id', pa.string()),  # null if no NOAA station matched
    ]
)

DIM_ROUTE_SCHEMA = pa.schema(
    [
        ('airline_iata', pa.string()),
        ('origin', pa.string()),
        ('dest', pa.string()),
        ('distance_mi', pa.float64()),
    ]
)


# as the crow flies distance between two coordinates (miles)
def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8  # earth radius (miles)
    # convert lat lon to radians
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    # calc difference between latitude and longitude (in radians)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    #
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    # angular distance in radians: 2 * arc tangent
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # return dist between the two coords in miles (c * earth's radius)
    return R * c


def stage_airports(
    store: ObjectStore, raw_bucket: str = 'raw', staging_bucket: str = 'staging'
) -> int:
    # load airports
    obj = store.client.get_object(Bucket=raw_bucket, Key='faa/airports.parquet')
    airports = pq.read_table(io.BytesIO(obj['Body'].read())).to_pandas()

    # load station map { [iata_code]: station_id }
    obj = store.client.get_object(Bucket=raw_bucket, Key='noaa/_station_map.json')
    station_map = json.loads(obj['Body'].read())

    # set noaa station id column for each airport (look up by iata code - calc in )
    airports['lcd_station_id'] = airports['iata_code'].map(station_map)

    table = pa.Table.from_pandas(airports, preserve_index=False)
    # only keep DIM_AIRPORT_SCHEMA
    keep = [f.name for f in DIM_AIRPORT_SCHEMA]
    table = table.select([c for c in keep if c in table.column_names])
    table = table.cast(DIM_AIRPORT_SCHEMA, safe=False)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression='zstd')
    store.put_bytes(staging_bucket, 'dim_airport/dim_airport.parquet', buf.getvalue())
    return len(table)


def stage_routes(
    store: ObjectStore, raw_bucket: str = 'raw', staging_bucket: str = 'staging'
) -> int:
    obj = store.client.get_object(Bucket=raw_bucket, Key='openflights/routes.parquet')
    routes = pq.read_table(io.BytesIO(obj['Body'].read())).to_pandas()

    obj = store.client.get_object(Bucket=staging_bucket, Key='dim_airport/dim_airport.parquet')
    airports = pq.read_table(
        io.BytesIO(obj['Body'].read()),
        columns=['iata_code', 'latitude_deg', 'longitude_deg'],
    ).to_pandas()

    coord_map = airports.set_index('iata_code')[['latitude_deg', 'longitude_deg']].to_dict('index')

    def dist(row: pd.Series) -> float:
        o = coord_map.get(row['origin'])
        d = coord_map.get(row['dest'])

        if o is None or d is None:
            return float('nan')
        return _haversine_mi(
            o['latitude_deg'], o['longitude_deg'], d['latitude_deg'], d['longitude_deg']
        )

    # calc flight distance; drop rows missing distance_mi
    routes['distance_mi'] = routes.apply(dist, axis=1)
    routes = routes[['airline_iata', 'origin', 'dest', 'distance_mi']].dropna(
        subset=['distance_mi']
    )

    table = pa.Table.from_pandas(routes, preserve_index=False).cast(DIM_ROUTE_SCHEMA, safe=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='zstd')
    store.put_bytes(staging_bucket, 'dim_route/dim_route.parquet', buf.getvalue())
    return len(table)
