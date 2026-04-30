# Why PySpark for this specific feature?
#
# Cascading delay (the arrival delay of an aircraft's previous flight) requires a self-join on tail_number ordered by scheduled_departure_utc. On 58M rows this is a shuffle-heavy operation: all rows for the same tail number must reach the same executor in the right order. Spark handles this natively with partition-aware window functions; DuckDB would need to sort the entire dataset into memory first.


from __future__ import annotations

from pyspark.sql import SparkSession

from bmo.common.config import settings


def make_spark_session(app_name: str) -> SparkSession:
    """
    Return a SparkSession configured for Iceberg on MinIO.

    Uses Iceberg's JdbcCatalog (named 'bmo') pointing at the same PostgreSQL
    database that PyIceberg's SqlCatalog writes. Both tools share the
    iceberg_tables row and read/write the same physical metadata files in MinIO.

    Table references use two-part names (namespace.table, e.g.
    staging.staged_flights) with defaultCatalog=bmo. This avoids Spark 4.1's
    ResolveSQLOnFile rule, which intercepts two-part names whose first segment
    matches a registered datasource format — 'staging' is not a format so it
    passes through to the catalog lookup safely.

    fs.s3 is aliased to S3AFileSystem so that metadata_location values stored
    as s3:// by PyIceberg are resolved by the same S3A config as s3a:// paths.
    """
    jdbc_url = f'jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/iceberg'
    # Derive SSL from the endpoint URL so that local MinIO (http://) and
    # Cloudflare R2 (https://) are both handled correctly. Hardcoding 'false'
    # causes R2 to return a 301 HTTP→HTTPS redirect, which S3A misreads as an
    # AWS region redirect, producing the "region null" AWSRedirectException.
    ssl_enabled = str(settings.s3_endpoint_url.startswith('https://')).lower()

    return (
        SparkSession.builder.appName(app_name)
        # Iceberg + AWS + Postgres JDBC jars (downloaded to ~/.ivy2.5.2 on first run)
        .config(
            'spark.jars.packages',
            ','.join(
                [
                    'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1',
                    'org.apache.hadoop:hadoop-aws:3.4.2',
                    'org.postgresql:postgresql:42.7.3',
                ]
            ),
        )
        # Iceberg catalog: JdbcCatalog backed by the same PostgreSQL DB as PyIceberg
        .config('spark.sql.catalog.bmo', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.bmo.catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog')
        .config('spark.sql.catalog.bmo.uri', jdbc_url)
        .config('spark.sql.catalog.bmo.jdbc.user', settings.postgres_user)
        .config('spark.sql.catalog.bmo.jdbc.password', settings.postgres_password)
        .config('spark.sql.catalog.bmo.jdbc.schema-version', 'V1')
        .config('spark.sql.catalog.bmo.warehouse', f's3a://{settings.s3_bucket_staging}/iceberg')
        .config('spark.sql.defaultCatalog', 'bmo')
        # S3A / MinIO — for s3a:// paths
        .config('spark.hadoop.fs.s3a.endpoint', settings.s3_endpoint)
        .config('spark.hadoop.fs.s3a.access.key', settings.s3_access_key_id)
        .config('spark.hadoop.fs.s3a.secret.key', settings.s3_secret_access_key)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', ssl_enabled)
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        # Prevents S3A (hadoop-aws 3.4+) from attempting AWS region auto-discovery
        # via HeadBucket redirects. Non-AWS endpoints (R2, MinIO) return a 301 that
        # S3A's cross-region client cannot resolve, producing "region null" errors.
        .config('spark.hadoop.fs.s3a.endpoint.region', settings.s3_region)
        # Alias s3:// → S3AFileSystem so PyIceberg-stored metadata_location values
        # (written as s3://) resolve through the same S3A config.
        .config('spark.hadoop.fs.s3.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3.path.style.access', 'true')
        .config('spark.hadoop.fs.s3.connection.ssl.enabled', ssl_enabled)
        # Iceberg SQL extensions
        .config(
            'spark.sql.extensions',
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        )
        .getOrCreate()
    )
