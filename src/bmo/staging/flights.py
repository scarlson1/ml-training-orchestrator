"""
raw parquet (with HHMM local times)
  → join airport timezone from dim_airport
  → compute 4 UTC timestamps per flight
  → validate rows
  → write valid to staging/bts/year=YYYY/month=MM/
  → write rejected to staging/rejected/bts/year=YYYY/month=MM/
  → return StagingResult
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bmo.common.config import settings
from bmo.common.iceberg import get_or_create_table, make_catalog, overwrite_month_flights
from bmo.common.paths import Paths
from bmo.common.storage import ObjectStore
from bmo.staging.contracts import STAGED_FLIGHTS_SCHEMA, validate_flights
from bmo.staging.timezone import arrival_day_offset, local_hhmm_to_utc

log = logging.getLogger(__name__)


@dataclass
class StagingResult:
    year: int
    month: int
    valid_count: int
    rejected_count: int
    unknown_tz_count: int  # airports without a timezone in dim_airport
    target_uri: str
    rejected_uri: str
    snapshot_id: int


def _load_airport_tz() -> dict[str, str]:
    """Return {iata_code: tz_name} from the staged dim_airport."""

    # obj = store.client.get_object(Bucket='staging', Key='dim_airport/dim_airport.parquet')
    # tbl = pq.read_table(io.BytesIO(obj['Body'].read()), columns=['iata_code', 'tz_database_timezone'])

    catalog = make_catalog()
    tbl = (
        catalog.load_table('staging.dim_airport')
        .scan(selected_fields=('iata_code', 'tz_database_timezone'))
        .to_arrow()
    )
    return dict(zip(tbl['iata_code'].to_pylist(), tbl['tz_database_timezone'].to_pylist()))


def _add_utc_timestamps(df: pd.DataFrame, tz_map: dict[str, str]) -> pd.DataFrame:
    """
    Add four UTC timestamp columns. Uses vectorized airport lookup + per-row
    zoneinfo conversion. The zoneinfo lookup cannot be vectorized further —
    ~500K rows takes ~3-5s - acceptable for a monthly batch job.
    """
    df = df.copy()
    df['origin_tz'] = df['origin'].map(tz_map)
    df['dest_tz'] = df['dest'].map(tz_map)

    def to_utc(
        row: pd.Series, date_col: str, hhmm_col: str, tz_col: str, day_offset: int = 0
    ) -> datetime | None:
        tz = row[tz_col]
        hhmm = row[hhmm_col]
        flight_date = row[date_col]
        if pd.isna(tz) or pd.isna(hhmm) or pd.isna(flight_date):
            return None
        try:
            return local_hhmm_to_utc(
                flight_date=flight_date if isinstance(flight_date, date) else flight_date.date(),
                hhmm=int(hhmm),
                tz_name=tz,
                day_offset=day_offset,
            )
        except Exception:
            return None

    df['scheduled_departure_utc'] = df.apply(  # type: ignore[call-overload]
        lambda r: to_utc(r, 'flight_date', 'crs_dep_time_hhmm', 'origin_tz'), axis=1
    )
    df['actual_departure_utc'] = df.apply(  # type: ignore[call-overload]
        lambda r: to_utc(r, 'flight_date', 'dep_time_hhmm', 'origin_tz'), axis=1
    )

    # arrival day offset - compare CRS arr vs dep to detect overnight flights
    dep_hhmm = df['crs_dep_time_hhmm'].fillna(0).astype(int)
    arr_hhmm = df['crs_arr_time_hhmm'].fillna(0).astype(int)
    df['_arr_day_offset'] = [arrival_day_offset(d, a) for d, a in zip(dep_hhmm, arr_hhmm)]

    df['scheduled_arrival_utc'] = df.apply(  # type: ignore[call-overload]
        lambda r: to_utc(
            r, 'flight_date', 'crs_arr_time_hhmm', 'dest_tz', int(r['_arr_day_offset'])
        ),
        axis=1,
    )
    df['actual_arrival_utc'] = df.apply(  # type: ignore[call-overload]
        lambda r: to_utc(r, 'flight_date', 'arr_time_hhmm', 'dest_tz', int(r['_arr_day_offset'])),
        axis=1,
    )
    df = df.drop(columns=['_arr_day_offset'])
    return df


def stage_flights(
    year: int,
    month: int,
    store: ObjectStore,
    raw_bucket: str = 'raw',
    staging_bucket: Literal['staging'] = 'staging',
) -> StagingResult:
    # raw_key = f'bts/year={year}/month={month:02d}/data.parquet'
    # # target_key = f'bts/year={year}/month={month:02d}/flights.parquet'
    # iceberg_location = f's3://{staging_bucket}/iceberg/staged_flights'
    # rejected_key = f'rejected/bts/year={year}/month={month:02d}/rejected.parquet'
    raw_key = Paths('staged_flights').raw_key(year, month)
    iceberg_location = Paths('staged_flights').iceberg_location
    rejected_key = Paths('staged_flights').rejected_key(year, month)

    raw_obj = store.client.get_object(Bucket=raw_bucket, Key=raw_key)
    raw_table = pq.read_table(io.BytesIO(raw_obj['Body'].read()))

    tz_map = _load_airport_tz()
    unknown_tz = set(raw_table['origin'].to_pylist()) | set(raw_table['dest'].to_pylist())
    unknown_tz = {a for a in unknown_tz if a and a not in tz_map}
    if unknown_tz:
        log.warning(f'no timezone for {len(unknown_tz)} airports: {sorted(unknown_tz)[:10]}')

    df = raw_table.to_pandas()
    df = _add_utc_timestamps(df, tz_map)

    # trim only the columns in STAGED_FLIGHTS_SCHEMA
    keep = [f.name for f in STAGED_FLIGHTS_SCHEMA]
    for col in keep:
        if col not in df.columns:
            df[col] = None  # set nulls for optional columns missing values

    staged_table = pa.Table.from_pandas(df[keep], preserve_index=False)
    staged_table = staged_table.cast(STAGED_FLIGHTS_SCHEMA, safe=False)

    valid, rejected = validate_flights(staged_table)

    # write valid
    # buf = io.BytesIO()
    # pq.write_table(valid, buf, compression='zstd', compression_level=3)
    # store.put_bytes(staging_bucket, target_key, buf.getvalue())

    # valid → Iceberg (time-travel, ACID, schema evolution)
    catalog = make_catalog()
    iceberg_table = get_or_create_table(
        catalog,
        identifier=Paths('staged_flights').iceberg_identifier,
        arrow_schema=STAGED_FLIGHTS_SCHEMA,
        location=iceberg_location,
        partition_column='flight_date',
    )
    overwrite_month_flights(iceberg_table, valid, year, month)
    snapshot = iceberg_table.current_snapshot()
    assert snapshot is not None, 'no snapshot after overwrite'
    snapshot_id = snapshot.snapshot_id

    # write rejected -> Parquet instead of iceberg - not meant to be queried
    if len(rejected) > 0:
        buf = io.BytesIO()
        pq.write_table(rejected, buf, compression='zstd', compression_level=3)
        store.put_bytes(settings.s3_bucket_rejected, rejected_key, buf.getvalue())
        log.warning(f'rejected {len(rejected)}/{len(staged_table)} rows for {year}-{month:02d}')

    return StagingResult(
        year=year,
        month=month,
        valid_count=len(valid),
        rejected_count=len(rejected),
        unknown_tz_count=len(unknown_tz),
        # target_uri=f's3://{staging_bucket}/{target_key}',
        target_uri=f'{iceberg_location}',
        rejected_uri=f's3://{settings.s3_bucket_rejected}/{rejected_key}',
        snapshot_id=snapshot_id,
    )
