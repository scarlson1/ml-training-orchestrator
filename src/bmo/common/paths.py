from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

from bmo.common.config import settings

# TODO: better system for paths
# don't use "staged_flights", etc. for constructor
# confusing b/c same path returned for "raw_key" as staging key (but different manifest_key)

# add dock string descriptions


class IcebergId(StrEnum):
    DIM_ROUTE = 'staging.dim_route'
    STAGED_FLIGHTS = 'staging.staged_flights'
    DIM_AIRPORT = 'staging.dim_airport'
    STAGED_WEATHER = 'staging.staged_weather'
    FEAT_CASCADING_DELAY = 'staging.feat_cascading_delay'


# DELETE - USE CLASSES BELOW
@dataclass
class Paths:
    table: Literal[
        'staged_flights', 'staged_weather', 'dim_airport', 'dim_route', 'feat_cascading_delay'
    ]

    station_map_key: str = 'noaa/_station_map.json'
    staging_bucket: str = settings.s3_bucket_staging

    def _get_prefix(self) -> str | None:
        match self.table:
            case 'staged_flights':
                return 'bts'
            case 'staged_weather':
                return 'noaa'
            case _:
                return None

    def raw_key(self, year: int, month: int) -> str:
        """S3 path to raw ingestion partitioned parquet data"""
        prefix = self._get_prefix()
        if not prefix:
            raise Exception(f'prefix not set for {self.table}')
        return f'{prefix}/year={year}/month={month:02d}/data.parquet'

    def manifest_key(self, year: int, month: int) -> str:
        prefix = self._get_prefix()
        if not prefix:
            raise Exception(f'prefix not set for {self.table}')
        return f'{prefix}/_manifests/{year}-{month:02d}.json'

    def rejected_key(self, year: int, month: int) -> str:
        """S3 path to rejected parquet data that failed staging validation"""
        return f'{self._get_prefix}/year={year}/month={month:02d}/reject.parquet'

    @property
    def iceberg_location(self) -> str:
        proto = 's3a' if self.table == 'feat_cascading_delay' else 's3'
        return f'{proto}://{self.staging_bucket}/iceberg/{self.table}'

    @property
    def iceberg_identifier(self) -> str:
        return IcebergId[self.table.upper()]


# ---------------------------------------------------------------------------
# Raw bucket: sources with partitioned, time-series data
# ---------------------------------------------------------------------------


@dataclass
class BtsPaths:
    """BTS on-time performance raw + staging paths."""

    _prefix = 'bts'
    iceberg_id: IcebergId = field(default=IcebergId.STAGED_FLIGHTS, init=False)

    def raw_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/year={year}/month={month:02d}/data.parquet'

    def manifest_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/_manifests/{year}-{month:02d}.json'

    def rejected_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/year={year}/month={month:02d}/rejected.parquet'

    def iceberg_location(self, bucket: str = settings.s3_bucket_staging) -> str:
        return f's3://{bucket}/iceberg/staged_flights'


@dataclass
class NoaaPaths:
    """NOAA LCD weather raw + staging paths."""

    _prefix = 'noaa'
    station_map_key: str = field(default='noaa/_station_map.json', init=False)
    iceberg_id: IcebergId = field(default=IcebergId.STAGED_WEATHER, init=False)

    def raw_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/year={year}/month={month:02d}/data.parquet'

    def manifest_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/_manifests/{year}-{month:02d}.json'

    def rejected_key(self, year: int, month: int) -> str:
        return f'{self._prefix}/year={year}/month={month:02d}/rejected.parquet'

    def annual_prefix(self, year: int) -> str:
        return f'{self._prefix}/_annual/{year}/'

    def iceberg_location(self, bucket: str = settings.s3_bucket_staging) -> str:
        return f's3://{bucket}/iceberg/staged_weather'


# ---------------------------------------------------------------------------
# Raw bucket: sources with static (non-partitioned) files
# ---------------------------------------------------------------------------


class FaaPaths:
    """FAA airport reference data paths."""

    airports_key: str = 'faa/airports.parquet'
    iceberg_id: IcebergId = IcebergId.DIM_AIRPORT

    @staticmethod
    def iceberg_location(bucket: str = settings.s3_bucket_staging) -> str:
        return f's3://{bucket}/iceberg/dim_airport'


class OpenflightsPaths:
    """OpenFlights route reference data paths."""

    routes_key: str = 'openflights/routes.parquet'
    iceberg_id: IcebergId = IcebergId.DIM_ROUTE

    @staticmethod
    def iceberg_location(bucket: str = settings.s3_bucket_staging) -> str:
        return f's3://{bucket}/iceberg/dim_route'


# ---------------------------------------------------------------------------
# Staging bucket: derived feature tables (no raw source)
# ---------------------------------------------------------------------------


class FeatCascadingDelayPaths:
    """Cascading delay feature table paths."""

    iceberg_id: IcebergId = IcebergId.FEAT_CASCADING_DELAY

    @staticmethod
    def iceberg_location(bucket: str = settings.s3_bucket_staging) -> str:
        # s3a:// required for Spark/Hadoop compatibility
        return f's3a://{bucket}/iceberg/feat_cascading_delay'


# ---------------------------------------------------------------------------
# Convenience singletons
# ---------------------------------------------------------------------------

bts = BtsPaths()
noaa = NoaaPaths()
faa = FaaPaths()
openflights = OpenflightsPaths()
feat_cascading_delay = FeatCascadingDelayPaths()
