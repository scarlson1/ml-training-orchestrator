"""
Dagster ConfigurableResource wrapping DuckDB.

DuckDB is used in this project as the query engine for dbt feature models.
It reads directly from Iceberg tables stored in MinIO/R2 via the httpfs and
iceberg extensions.

DuckDB's S3 config is session-level (SET s3_endpoint = '...'), not global.
This resource creates a fresh connection per call so each asset gets a clean
session. Connections are not thread-safe — do not share across threads.

DuckDB docs: https://duckdb.org/docs/guides/python/install
DuckDB S3 integration: https://duckdb.org/docs/guides/network_cloud_storage/s3_import.html
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import duckdb
from dagster import ConfigurableResource
from pydantic import Field


class DuckDBResource(ConfigurableResource):
    duckdb_path: str = Field(description='Path to the DuckDB file, or :memory: for in process')
    s3_endpoint: str | None = Field(
        default=None,
        description=(
            'S3 endpoint HOST:PORT (no scheme). If set, configures DuckDB httpfs '
            'to point to MinIO/R2 instead of AWS. '
            'Example: localhost:9000'
        ),
    )
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = 'us-east-1'

    @contextmanager
    def get_connection(self, read_only: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
        """
        Context manager that yields a DuckDB connection configured for S3/MinIO.

        Why a context manager? DuckDB connections hold a file lock on the
        .duckdb file. Using a context manager ensures the lock is released even
        if the asset raises an exception.

        Usage:
            with duckdb_resource.get_connection() as con:
                df = con.execute('SELECT * FROM feat_origin_airport_windowed').df()
        """
        con = duckdb.connect(self.duckdb_path, read_only=read_only)
        try:
            if self.s3_endpoint:
                con.execute('INSTALL httpfs; LOAD httpfs;')
                con.execute(f'SET s3_endpoint="{self.s3_endpoint}"')
                con.execute(f'SET s3_access_key_id="{self.s3_access_key_id}"')
                con.execute(f'SET s3_secret_access_key="{self.s3_secret_access_key}"')
                con.execute(f'SET s3_region="{self.s3_region}"')
                con.execute('SET s3_use_ssl=false')
                con.execute('SET s3_url_style="path"')
            yield con
        finally:
            con.close()
