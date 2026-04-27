from urllib.parse import quote

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    dagster_home: str

    # Object store
    s3_endpoint_url: str
    s3_access_key_id: str = Field(
        validation_alias=AliasChoices('s3_access_key_id', 'aws_access_key_id')
    )
    s3_secret_access_key: str = Field(
        validation_alias=AliasChoices('s3_secret_access_key', 'aws_secret_access_key')
    )
    s3_region: str = Field(
        default='us-east-1',
        validation_alias=AliasChoices('s3_region', 'aws_region', 'aws_default_region'),
    )
    s3_bucket_raw: str = 'raw'
    s3_bucket_staging: str = 'staging'
    s3_bucket_rejected: str = 'rejected'
    feast_s3_base: str = 's3://staging/feast'
    dataset_s3_base: str = 's3://staging/datasets'

    # dbt / DuckDB
    # DuckDB's S3 endpoint omits the http:// scheme — it only accepts host:port
    s3_endpoint_env: str | None = Field(
        default=None,
        validation_alias='DUCKDB_S3_ENDPOINT',
    )
    duckdb_path: str = '/tmp/bmo_features.duckdb'

    # Feast
    # TODO: change s3_... object store var names to aws_... ?? automatically picked up by boto3 & feast
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str = 'us-east-1'
    aws_endpoint_url: str

    # MLFlow
    mlflow_tracking_uri: str

    # Postgres (Dagster + MLFlow metadata)
    postgres_host: str
    postgres_port: str
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # Redis (Feast online store)
    redis_url: str

    # SQLite URI for the local Iceberg catalog - data files live in MinIO
    iceberg_catalog_uri_env: str | None = Field(
        default=None,
        validation_alias='ICEBERG_CATALOG_URI',
    )  # 'sqlite:////tmp/bmo_iceberg.db'

    discord_webhook_url: str | None = None

    # ===== Computed Properties ===== #

    @property
    def s3_endpoint(self) -> str:
        """stripped protocol from url"""
        endpoint_host = self.s3_endpoint_url.replace('http://', '').replace('https://', '')
        return endpoint_host

    @property
    def iceberg_catalog_uri(self) -> str:
        if self.iceberg_catalog_uri_env:
            return self.iceberg_catalog_uri_env
        else:
            user = quote(self.postgres_user, safe='')
            password = quote(self.postgres_password, safe='')
            host = quote(self.postgres_host, safe='')
            port = quote(self.postgres_port, safe='')
            # always postgresql+psycopg2 ??
            return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/iceberg'

    @property
    def postgres_url(self) -> str:
        user = quote(self.postgres_user, safe='')
        password = quote(self.postgres_password, safe='')
        host = quote(self.postgres_host, safe='')
        port = quote(self.postgres_port, safe='')

        return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{self.postgres_db}'


# ===== Singleton Instance =====
settings = Settings()  # pyright: ignore[reportCallIssue]  # pydantic-settings loads fields from env
