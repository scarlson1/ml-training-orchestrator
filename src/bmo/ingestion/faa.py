# These are dimension tables (static / infrequently updated), so no partitioning needed.

# - FAA 5010: Download from the FAA website (it's an Access DB or CSV export — check current format). Key fields: ICAO_ID, IATA_ID, latitude, longitude, elevation, runway count. (faa.gov/airports/airport_safety/airportdata_5010)
# - OpenFlights routes: Download https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat (a flat CSV). Key fields: airline, source airport IATA, destination airport IATA. (Route dimension + great-circle distance) (openflights.org/data)

# The FAA 5010 database is the official US airport record, but the 5010 format is an ugly fixed-width text file. The practical alternative that carries the same data in a clean CSV is OurAirports (ourairports.com/data/airports.csv), which is derived from official FAA/ICAO sources and is far easier to work with.
# alternatives:
#   - https://adds-faa.opendata.arcgis.com/datasets/e747ab91a11045e8b3f8a3efd093d3b5_0/api

import io

import airportsdata
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from bmo.common.storage import ObjectStore

OURAIRPORTS_URL = 'https://ourairports.com/data/airports.csv'

AIRPORT_TYPES = {'medium_airport', 'large_airport'}
US_ISO_PREFIX = 'US-'

AIRPORTS_SCHEMA = pa.schema(
    [
        ('iata_code', pa.string()),
        ('icao_code', pa.string()),
        ('name', pa.string()),
        ('type', pa.string()),
        ('latitude_deg', pa.float64()),
        ('longitude_deg', pa.float64()),
        ('elevation_ft', pa.float32()),
        ('municipality', pa.string()),
        ('iso_region', pa.string()),  # e.g. "US-CA"
        ('tz_database_timezone', pa.string()),
    ]
)


def ingest_airports(store: ObjectStore, bucket: str = 'raw', prefix: str = 'faa') -> pa.Table:
    res = httpx.get(OURAIRPORTS_URL, timeout=30, follow_redirects=True)
    res.raise_for_status()

    table = pacsv.read_csv(
        io.BytesIO(res.content),
        convert_options=pacsv.ConvertOptions(
            include_columns=[
                'iata_code',
                'icao_code',
                'name',
                'type',
                'latitude_deg',
                'longitude_deg',
                'elevation_ft',
                'municipality',
                'iso_region',
            ],
            null_values=['', 'NA'],
            strings_can_be_null=True,
        ),
    )

    # filter to US commercial airports with IATA codes
    mask = pc.and_(
        pc.and_(
            pc.is_in(table['type'], value_set=pa.array(list(AIRPORT_TYPES))),
            pc.starts_with(table['iso_region'], pattern=US_ISO_PREFIX),
        ),
        pc.is_valid(table['iata_code']),
    )
    table = table.filter(mask)

    # OurAirports dropped the timezone column — join from airportsdata (IATA-keyed)
    _tz_lookup: dict[str, str] = {
        code: info['tz'] for code, info in airportsdata.load('IATA').items() if info.get('tz')
    }
    tz_col = pa.array(
        [_tz_lookup.get(code) for code in table['iata_code'].to_pylist()],
        type=pa.string(),
    )
    table = table.append_column('tz_database_timezone', tz_col).cast(AIRPORTS_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression='zstd')
    store.put_bytes(bucket, f'{prefix}/airports.parquet', buf.getvalue())
    return table  # return the table — noaa.py needs it immediately


# routes.dat is headerless -> need to supply column names manually

OPENFLIGHTS_URL = 'https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat'

COLUMN_NAMES = [
    'airline_iata',
    'airline_id',
    'origin',
    'origin_id',
    'dest',
    'dest_id',
    'codeshare',  # "Y" if codeshare, empty otherwise
    'stops',  # 0 = nonstop (almost always 0 in this dataset)
    'equipment',  # space-separated IATA aircraft codes e.g. "738 320"
]

ROUTES_SCHEMA = pa.schema(
    [
        ('airline_iata', pa.string()),
        ('origin', pa.string()),
        ('dest', pa.string()),
        ('codeshare', pa.bool_()),
        ('stops', pa.int8()),
        ('equipment', pa.string()),
    ]
)


def ingest_routes(store: ObjectStore, bucket: str = 'raw', prefix: str = 'openflights') -> None:
    res = httpx.get(OPENFLIGHTS_URL, timeout=30, follow_redirects=True)
    res.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(res.content),
        header=None,
        names=COLUMN_NAMES,
        na_values=['\\N'],  # OpenFlights uses \N for NULL (MySQL dump convention)
    )

    df = df[['airline_iata', 'origin', 'dest', 'codeshare', 'stops', 'equipment']]
    df['codeshare'] = df['codeshare'] == 'Y'
    df['stops'] = pd.to_numeric(df['stops'], errors='coerce').fillna(0).astype('int8')

    # drop rows without IATA codes
    df = df.dropna(subset=['airline_iata', 'origin', 'dest'])
    df = df[df['origin'].str.len() == 3]
    df = df[df['dest'].str.len() == 3]

    table = pa.Table.from_pandas(df, preserve_index=False).cast(ROUTES_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression='zstd')
    store.put_bytes(bucket, f'{prefix}/routes.parquet', buf.getvalue())
