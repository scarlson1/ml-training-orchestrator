"""
Object-storage helper used by ingestion and staging.

Location in project: src/bmo/common/storage.py

Thin wrapper over boto3's S3 client so the same code works against AWS S3,
MinIO (local dev), and Cloudflare R2 (cloud). Endpoint + credentials come
from environment variables.

Relevant env vars:
    S3_ENDPOINT_URL       MinIO / R2 endpoint. Unset = AWS.
    AWS_ACCESS_KEY_ID     credentials (or R2 token)
    AWS_SECRET_ACCESS_KEY
    AWS_REGION            default "auto" (R2 prefers "auto")
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


@dataclass
class ObjectStore:
    client: Any

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
                return False
            raise

    def read_json_or_none(self, bucket: str, key: str) -> dict[str, Any] | None:
        try:
            obj = self.client.get_object(Bucket=bucket, Key=key)
            result: dict[str, Any] = json.loads(obj['Body'].read())
            return result
        except ClientError as e:
            if e.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise

    def put_bytes(self, bucket: str, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=bucket, Key=key, Body=data)


# TODO: use pydantic env vars ??
def make_object_store() -> ObjectStore:
    endpoint = os.environ.get('S3_ENDPOINT_URL')  # MinIO / R2
    region = os.environ.get('AWS_REGION', 'auto')
    return ObjectStore(
        client=boto3.client(
            's3',
            endpoint_url=endpoint,
            region_name=region,
            config=Config(signature_version='s3v4'),
        )
    )
