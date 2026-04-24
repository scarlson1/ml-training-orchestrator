from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from bmo.common.config import settings

# TODO: generalize into one class ??
# add descriptions


class IcebergId(StrEnum):
    DIM_ROUTE = 'staging.dim_route'
    STAGED_FLIGHTS = 'staging.staged_flights'
    DIM_AIRPORT = 'staging.dim_airport'
    STAGED_WEATHER = 'staging.staged_weather'
    FEAT_CASCADING_DELAY = 'staging.feat_cascading_delay'


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


# class BtsPaths:
#     """Keys and identifiers for BTS on-time performance data."""

#     prefix = 'bts'
#     iceberg_identifier = 'staging.staged_flights'

#     @staticmethod
#     def raw_key(year: int, month: int) -> str:
#         """S3 path to raw ingestion partitioned parquet data"""
#         return f'bts/year={year}/month={month:02d}/data.parquet'

#     @staticmethod
#     def manifest_key(year: int, month: int) -> str:
#         """"""
#         return f'bts/_manifests/{year}-{month:02d}.json'

#     @staticmethod
#     def rejected_key(year: int, month: int) -> str:
#         """S3 path to rejected parquet data that failed staging validation"""
#         return f'bts/year={year}/month={month:02d}/rejected.parquet'

#     @staticmethod
#     def iceberg_location(bucket=settings.s3_bucket_staging) -> str:
#         return f's3://{bucket}/iceberg/staged_flights'


# class NoaaPaths:
#     """Keys and identifiers for NOAA LCD weather data."""

#     prefix = 'noaa'
#     station_map_key = 'noaa/_station_map.json'
#     iceberg_identifier = 'staging.staged_weather'

#     @staticmethod
#     def raw_key(year: int, month: int) -> str:
#         return f'noaa/year={year}/month={month:02d}/weather.parquet'

#     @staticmethod
#     def manifest_key(year: int, month: int) -> str:
#         return f'noaa/_manifests/{year}-{month:02d}.json'

#     @staticmethod
#     def rejected_key(year: int, month: int) -> str:
#         return f'noaa/year={year}/month={month:02d}/reject.parquet'

#     @staticmethod
#     def iceberg_location(bucket=settings.s3_bucket_staging) -> str:
#         return f's3://{bucket}/iceberg/staged_weather'


# class DimAirportPaths:
#     """Keys and identifiers for the airport dimension."""

#     raw_key = 'faa/airports.parquet'
#     iceberg_identifier = 'staging.dim_airport'

#     @staticmethod
#     def iceberg_location(bucket=settings.s3_bucket_staging) -> str:
#         return f's3://{bucket}/iceberg/dim_airport'


# class DimRoutePaths:
#     """Keys and identifiers for the route dimension."""

#     raw_key = 'openflights/routes.parquet'
#     iceberg_identifier = 'staging.dim_route'

#     @staticmethod
#     def iceberg_location(bucket=settings.s3_bucket_staging) -> str:
#         return f's3://{bucket}/iceberg/dim_route'


# class FeatCascadingDelayPaths:
#     """Keys and identifiers for the cascading delay feature table."""

#     iceberg_identifier = 'staging.feat_cascading_delay'

#     @staticmethod
#     def iceberg_location(bucket=settings.s3_bucket_staging) -> str:
#         return f's3a://{bucket}/iceberg/feat_cascading_delay'
