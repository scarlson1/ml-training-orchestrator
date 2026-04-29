"""
Dagster ConfigurableResource wrapping S3 / MinIO / Cloudflare R2.

All three object stores are S3-compatible: they accept the same boto3 API
with an explicit endpoint_url to redirect away from AWS. The endpoint_url
is what makes this work against MinIO locally and R2 in production — the
rest of the code is identical.

Docs: https://docs.dagster.io/concepts/resources
boto3 S3 docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import boto3
from dagster import ConfigurableResource
from pydantic import Field

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


class S3Resource(ConfigurableResource):
    endpoint_url: str = Field(description='Full URL including scheme, e.g. http://localhost:9000')
    access_key_id: str
    secret_access_key: str
    region: str = Field(default='us-east-1')

    def get_client(self) -> S3Client:
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )

    def get_s3fs(self):
        """
        s3fs filesystem. Use when PyArrow, Feast, or pandas needs a file-like
        interface to S3 paths (e.g. pq.write_table(table, s3.open('s3://...'))).
        """
        import s3fs

        return s3fs.S3FileSystem(
            key=self.access_key_id,
            secret=self.secret_access_key,
            endpoint_url=self.endpoint_url,
            client_kwargs={'region_name': self.region},
        )
