import os

from feast import FileSource
from feast.data_format import ParquetFormat

# base S3 path where feast_feature_export Dagster asset writes Parquet
# each subdirectory contains Parquet files for one entity type
# path must match what feast_feature_export writes
_FEAST_BASE = os.environ.get('FEAST_S3_BASE', 's3://staging/feast')

origin_airport_source = FileSource(
    name='origin_airport_source',
    path=f'{_FEAST_BASE}/origin_airport/',
    file_format=ParquetFormat(),
    timestamp_field='event_ts',  # During get_historical_features, Feast performs the PIT join using this column — it finds the latest row where event_ts <= requested_timestamp.
)

dest_airport_source = FileSource(
    name='dest_airport_source',
    path=f'{_FEAST_BASE}/dest_airport/',
    file_format=ParquetFormat(),
    timestamp_field='event_ts',
    description='Hourly rolling departure delay stats per origin airport - written by feast_feature_export asset',
)

carrier_source = FileSource(
    name='carrier_source',
    path='f{_FEAST_BASE}/carrier/',
    file_format=ParquetFormat(),
    timestamp_field='event_ts',
)

route_source = FileSource(
    name='route_source',
    path=f'{_FEAST_BASE}/route/',
    file_format=ParquetFormat(),
    timestamp_field='event_ts',
)

aircraft_source = FileSource(
    name='aircraft_source',
    path=f'{_FEAST_BASE}/aircraft/',
    file_format=ParquetFormat(),
    timestamp_field='event_ts',
)
