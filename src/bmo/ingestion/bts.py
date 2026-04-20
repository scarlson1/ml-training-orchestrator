"""
BTS Reporting Carrier On-Time Performance ingestion.

Source: https://transtats.bts.gov/PREZIP/
Each monthly file follows this naming pattern (month has no leading zero):
    On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YYYY}_{M}.zip
Data is available from 1987 onward. BTS typically publishes with a ~2-month lag,
so in April 2026 the latest available month is usually January or February 2026.

Data model gotchas this module works around
-------------------------------------------
- CRSDepTime / DepTime are HHMM integers in **local airport time**, not UTC.
  UTC conversion requires a join to the airport dimension and is handled in the
  staging layer, not here.
- Cancelled flights have NULL DepTime / ArrTime but valid CRSDepTime / CRSArrTime.
  These rows are kept as-is; downstream code must not assume DepTime is non-null.
- BTS encodes boolean columns (Cancelled, Diverted, DepDel15, ArrDel15) as
  "1.00" / "0.00" float strings, not native booleans. We coerce them explicitly
  before the final schema cast.
- Historical CSVs sometimes have trailing commas on every row, producing a
  spurious unnamed column. PyArrow's include_columns filter drops it automatically.

Idempotency
-----------
We download the ZIP before checking the SHA because we need the bytes to compute
the hash. If the SHA matches the stored manifest, the Parquet is already current
and we return early without writing anything.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from tenacity import retry, stop_after_attempt, wait_exponential

from bmo.common.storage import ObjectStore, make_object_store

log = logging.getLogger(__name__)

BTS_BASE_URL = 'https://transtats.bts.gov/PREZIP'
BTS_FILE_TEMPLATE = 'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip'

# The raw CSV has ~110 columns. These are the ones we actually use downstream.
KEEP_COLUMNS = [
    'Year',
    'Month',
    'DayofMonth',
    'DayOfWeek',
    'FlightDate',
    'Reporting_Airline',
    'Tail_Number',
    'Flight_Number_Reporting_Airline',
    'Origin',
    'OriginCityName',
    'OriginState',
    'Dest',
    'DestCityName',
    'DestState',
    'CRSDepTime',
    'DepTime',
    'DepDelay',
    'DepDelayMinutes',
    'DepDel15',
    'CRSArrTime',
    'ArrTime',
    'ArrDelay',
    'ArrDelayMinutes',
    'ArrDel15',
    'Cancelled',
    'CancellationCode',
    'Diverted',
    'CRSElapsedTime',
    'ActualElapsedTime',
    'AirTime',
    'Distance',
    'CarrierDelay',
    'WeatherDelay',
    'NASDelay',
    'SecurityDelay',
    'LateAircraftDelay',
]

RAW_TO_TARGET = {
    'Year': 'year',
    'Month': 'month',
    'DayofMonth': 'day_of_month',
    'DayOfWeek': 'day_of_week',
    'FlightDate': 'flight_date',
    'Reporting_Airline': 'reporting_airline',
    'Tail_Number': 'tail_number',
    'Flight_Number_Reporting_Airline': 'flight_number',
    'Origin': 'origin',
    'OriginCityName': 'origin_city',
    'OriginState': 'origin_state',
    'Dest': 'dest',
    'DestCityName': 'dest_city',
    'DestState': 'dest_state',
    'CRSDepTime': 'crs_dep_time_hhmm',  # local airport time; UTC conversion in staging
    'DepTime': 'dep_time_hhmm',  # null on cancelled flights
    'DepDelay': 'dep_delay_min',  # signed: negative means early
    'DepDelayMinutes': 'dep_delay_min_nonneg',  # same delay, floored at 0
    'DepDel15': 'dep_del15',
    'CRSArrTime': 'crs_arr_time_hhmm',
    'ArrTime': 'arr_time_hhmm',
    'ArrDelay': 'arr_delay_min',
    'ArrDelayMinutes': 'arr_delay_min_nonneg',
    'ArrDel15': 'arr_del15',
    'Cancelled': 'cancelled',
    'CancellationCode': 'cancellation_code',
    'Diverted': 'diverted',
    'CRSElapsedTime': 'crs_elapsed_min',
    'ActualElapsedTime': 'actual_elapsed_min',
    'AirTime': 'air_time_min',
    'Distance': 'distance_mi',
    'CarrierDelay': 'carrier_delay_min',
    'WeatherDelay': 'weather_delay_min',
    'NASDelay': 'nas_delay_min',
    'SecurityDelay': 'security_delay_min',
    'LateAircraftDelay': 'late_aircraft_delay_min',
}

# Target schema after rename + type coercion.
PARQUET_SCHEMA = pa.schema(
    [
        ('year', pa.int16()),
        ('month', pa.int8()),
        ('day_of_month', pa.int8()),
        ('day_of_week', pa.int8()),
        ('flight_date', pa.date32()),
        ('reporting_airline', pa.string()),
        ('tail_number', pa.string()),
        ('flight_number', pa.int32()),
        ('origin', pa.string()),
        ('origin_city', pa.string()),
        ('origin_state', pa.string()),
        ('dest', pa.string()),
        ('dest_city', pa.string()),
        ('dest_state', pa.string()),
        ('crs_dep_time_hhmm', pa.int16()),
        ('dep_time_hhmm', pa.int16()),
        ('dep_delay_min', pa.float32()),
        ('dep_delay_min_nonneg', pa.float32()),
        ('dep_del15', pa.bool_()),
        ('crs_arr_time_hhmm', pa.int16()),
        ('arr_time_hhmm', pa.int16()),
        ('arr_delay_min', pa.float32()),
        ('arr_delay_min_nonneg', pa.float32()),
        ('arr_del15', pa.bool_()),
        ('cancelled', pa.bool_()),
        ('cancellation_code', pa.string()),
        ('diverted', pa.bool_()),
        ('crs_elapsed_min', pa.float32()),
        ('actual_elapsed_min', pa.float32()),
        ('air_time_min', pa.float32()),
        ('distance_mi', pa.float32()),
        ('carrier_delay_min', pa.float32()),
        ('weather_delay_min', pa.float32()),
        ('nas_delay_min', pa.float32()),
        ('security_delay_min', pa.float32()),
        ('late_aircraft_delay_min', pa.float32()),
    ]
)


@dataclass
class IngestResult:
    year: int
    month: int
    source_url: str
    source_sha256: str
    row_count: int
    target_uri: str
    manifest_uri: str
    skipped: bool


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    reraise=True,
)
def _download_zip(url: str) -> bytes:
    """Stream-download a ZIP with retries. BTS occasionally returns transient 5xx."""
    log.info('Downloading %s', url)
    with httpx.stream(
        'GET',
        url,
        timeout=httpx.Timeout(60.0, read=600.0),
        follow_redirects=True,
    ) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_bytes(chunk_size=1 << 20):
            buf.write(chunk)
        return buf.getvalue()


def _extract_csv(zip_bytes: bytes) -> bytes:
    """Extract the single CSV from the BTS ZIP. Fails if layout unexpected."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith('.csv')]
        if len(csv_names) != 1:
            raise ValueError(f'Expected exactly one CSV inside BTS ZIP, got {csv_names}')
        return zf.read(csv_names[0])


def _csv_to_parquet(csv_bytes: bytes) -> pa.Table:
    """Parse the BTS CSV, trim columns, rename to snake_case, cast to target schema."""
    read_options = pacsv.ReadOptions(use_threads=True)
    parse_options = pacsv.ParseOptions(newlines_in_values=False)
    convert_options = pacsv.ConvertOptions(
        include_columns=KEEP_COLUMNS,
        null_values=['', 'NA'],
        strings_can_be_null=True,
    )
    table = pacsv.read_csv(
        io.BytesIO(csv_bytes),
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    # Rename raw column names to snake_case.
    table = table.rename_columns([RAW_TO_TARGET[c] for c in table.column_names])

    # BTS encodes booleans as "1.00"/"0.00" float strings. PyArrow reads them as
    # float64; we compare != 0.0 to get bool before the final schema cast.
    for col in ('cancelled', 'diverted', 'dep_del15', 'arr_del15'):
        if col in table.column_names:
            idx = table.column_names.index(col)
            as_float = pc.cast(table[col], pa.float32())
            as_bool = pc.not_equal(as_float, pa.scalar(0.0, type=pa.float32()))
            table = table.set_column(idx, col, as_bool)

    # safe=False allows float64→float32 narrowing without raising on precision loss.
    return table.cast(PARQUET_SCHEMA, safe=False)


def ingest_month(
    year: int,
    month: int,
    store: ObjectStore,
    bucket: str = 'raw',
    prefix: str = 'bts',
    force: bool = False,
) -> IngestResult:
    """Ingest one month of BTS on-time performance data.

    Idempotent: reads the prior manifest (if any) and skips if the upstream
    ZIP's SHA-256 matches. Pass force=True to re-ingest regardless.
    """
    if not 1 <= month <= 12:
        raise ValueError(f'month must be 1–12, got {month}')
    if year < 1987:
        raise ValueError(f'BTS On-Time data starts in 1987, got {year}')

    filename = BTS_FILE_TEMPLATE.format(year=year, month=month)
    url = f'{BTS_BASE_URL}/{filename}'

    target_key = f'{prefix}/year={year}/month={month:02d}/data.parquet'
    manifest_key = f'{prefix}/_manifests/{year}-{month:02d}.json'
    target_uri = f's3://{bucket}/{target_key}'
    manifest_uri = f's3://{bucket}/{manifest_key}'

    existing_manifest = store.read_json_or_none(bucket, manifest_key)

    zip_bytes = _download_zip(url)
    source_sha256 = hashlib.sha256(zip_bytes).hexdigest()

    if (
        not force
        and existing_manifest is not None
        and existing_manifest.get('source_sha256') == source_sha256
        and store.exists(bucket, target_key)
    ):
        log.info('No-op: %d-%02d already ingested (sha match)', year, month)
        return IngestResult(
            year=year,
            month=month,
            source_url=url,
            source_sha256=source_sha256,
            row_count=existing_manifest['row_count'],
            target_uri=target_uri,
            manifest_uri=manifest_uri,
            skipped=True,
        )

    csv_bytes = _extract_csv(zip_bytes)
    table = _csv_to_parquet(csv_bytes)
    row_count = table.num_rows
    log.info('Parsed %d rows for %d-%02d', row_count, year, month)

    out_buf = io.BytesIO()
    pq.write_table(table, out_buf, compression='zstd', compression_level=3)
    store.put_bytes(bucket, target_key, out_buf.getvalue())

    manifest = {
        'year': year,
        'month': month,
        'source_url': url,
        'source_sha256': source_sha256,
        'ingested_at_utc': datetime.now(timezone.utc).isoformat(),
        'row_count': row_count,
        'target_uri': target_uri,
        'parquet_schema_fingerprint': hashlib.sha256(str(PARQUET_SCHEMA).encode()).hexdigest()[:16],
    }
    store.put_bytes(
        bucket,
        manifest_key,
        json.dumps(manifest, indent=2).encode(),
    )

    return IngestResult(
        year=year,
        month=month,
        source_url=url,
        source_sha256=source_sha256,
        row_count=row_count,
        target_uri=target_uri,
        manifest_uri=manifest_uri,
        skipped=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description='Ingest BTS on-time performance data')
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--month', type=int, required=True)
    parser.add_argument('--bucket', default='raw')
    parser.add_argument('--prefix', default='bts')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s — %(message)s',
    )

    store = make_object_store()
    result = ingest_month(
        year=args.year,
        month=args.month,
        store=store,
        bucket=args.bucket,
        prefix=args.prefix,
        force=args.force,
    )
    print(json.dumps(asdict(result), indent=2))


if __name__ == '__main__':
    main()
