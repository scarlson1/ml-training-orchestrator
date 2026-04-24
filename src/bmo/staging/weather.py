from __future__ import annotations

import io
import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bmo.common.iceberg import get_or_create_table, make_catalog, overwrite_month_weather
from bmo.common.storage import ObjectStore

log = logging.getLogger(__name__)


@dataclass
class WeatherStagingResult:
    year: int
    month: int
    valid_count: int
    rejected_count: int
    target_uri: str
    snapshot_id: int


STAGED_WEATHER_SCHEMA = pa.schema(
    [
        ('station_id', pa.string()),
        ('iata_code', pa.string()),
        ('obs_time_utc', pa.timestamp('us', tz='UTC')),
        ('temp_f', pa.float32()),
        ('dew_point_f', pa.float32()),
        ('relative_humidity_pct', pa.float32()),
        ('wind_speed_kts', pa.float32()),
        ('wind_dir_deg', pa.float32()),
        ('precip_1h_in', pa.float32()),
        ('visibility_mi', pa.float32()),
        ('sky_conditions', pa.string()),
        ('present_weather', pa.string()),
        ('sea_level_pressure_hpa', pa.float32()),
    ]
)


def stage_weather(
    year: int,
    month: int,
    store: ObjectStore,
    raw_bucket: str = 'raw',
    staging_bucket: str = 'staging',
) -> WeatherStagingResult:
    raw_key = f'noaa/year={year}/month={month:02d}/weather.parquet'
    # target_key = f'noaa/year={year}/month={month:02d}/weather.parquet'
    rejected_key = f'noaa/year={year}/month={month:02d}/reject.parquet'

    # get raw table from S3 storage (read parquet file with pyarrow)
    obj = store.client.get_object(Bucket=raw_bucket, Key=raw_key)
    table = pq.read_table(io.BytesIO(obj['Body'].read()))

    # Domain rules: temp must be plausible (-100 to 150 °F), wind >= 0
    invalid_temp = pc.or_(
        pc.less(pc.cast(table['temp_f'], pa.float32()), pa.scalar(-100.0, pa.float32())),
        pc.greater(pc.cast(table['temp_f'], pa.float32()), pa.scalar(150.0, pa.float32())),
    )
    invalid_wind = pc.less(
        pc.cast(table['wind_speed_kts'], pa.float32()), pa.scalar(0.0, pa.float32())
    )
    reject_mask = pc.or_(
        pc.and_(pc.is_valid(table['temp_f']), invalid_temp),
        pc.and_(pc.is_valid(table['wind_speed_kts']), invalid_wind),
    )

    valid = table.filter(pc.invert(reject_mask)).cast(STAGED_WEATHER_SCHEMA, safe=False)
    rejected = table.filter(reject_mask)

    # # create buffer and write tables to S3
    # buf = io.BytesIO()
    # pq.write_table(valid, buf, compression='zstd', compression_level=3)
    # store.put_bytes(staging_bucket, target_key, buf.getvalue())

    # valid -> Iceberg
    catalog = make_catalog()
    iceberg_location = f's3://{staging_bucket}/iceberg/staged_weather'
    iceberg_table = get_or_create_table(
        catalog,
        identifier='staging.staged_weather',
        arrow_schema=STAGED_WEATHER_SCHEMA,
        location=iceberg_location,
        partition_column='obs_time_utc',
    )
    overwrite_month_weather(iceberg_table, valid, year, month)
    snapshot = iceberg_table.current_snapshot()
    assert snapshot is not None, 'no snapshot after overwrite'
    snapshot_id = snapshot.snapshot_id

    # rejected -> parquet
    if len(rejected) > 0:
        buf = io.BytesIO()
        pq.write_table(rejected, buf, compression='zstd', compression_level=3)
        store.put_bytes(staging_bucket, rejected_key, buf.getvalue())

    return WeatherStagingResult(
        year=year,
        month=month,
        valid_count=len(valid),
        rejected_count=len(rejected),
        # target_uri=f's3://{staging_bucket}/{target_key}',
        target_uri=iceberg_location,
        snapshot_id=snapshot_id,  # add this field
    )
