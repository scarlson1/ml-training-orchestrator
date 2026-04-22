# Why PySpark for this specific feature?
#
# Cascading delay (the arrival delay of an aircraft's previous flight) requires a self-join on tail_number ordered by scheduled_departure_utc. On 58M rows this is a shuffle-heavy operation: all rows for the same tail number must reach the same executor in the right order. Spark handles this natively with partition-aware window functions; DuckDB would need to sort the entire dataset into memory first.


# Why HadoopCatalog instead of SqlCatalog?
#
# PySpark's Iceberg integration doesn't support SQLite or PostgreSQL catalogs directly — it expects a Hive Metastore, REST catalog, or HadoopCatalog.
# HadoopCatalog stores table metadata directly on the filesystem (s3a://staging/iceberg/<table_name>/metadata/), which is where PyIceberg's SqlCatalog writes when you set location=s3://staging/iceberg/<table_name>.
# Both tools read from and write to the same physical files; the catalog type difference doesn't matter because the Iceberg spec is the contract.


from __future__ import annotations

import os

from pyspark.sql import SparkSession


def make_spark_session(app_name: str) -> SparkSession:
    """
    Return a SparkSession configured for Iceberg on MinIO.

    Uses a HadoopCatalog named 'iceberg' with warehouse at s3a://staging/iceberg/.
    Table names: iceberg.staged_flights, iceberg.feat_cascading_delay, etc.
    The HadoopCatalog uses filesystem paths directly so PyIceberg-written tables
    (which set location=s3://staging/iceberg/<table_name>) are readable without
    any metastore — Spark finds metadata at s3a://staging/iceberg/<table_name>/metadata/.
    """
    endpoint = os.environ['S3_ENDPOINT_URL']
    access_key = os.environ['S3_ACCESS_KEY_ID']
    secret_key = os.environ['S3_SECRET_ACCESS_KEY']

    # strip http:// - s3a hadoop client expects host:port
    # or use the other environment var (DUCKDB_S3_ENDPOINT) ??
    endpoint_host = endpoint.replace('http://', '').replace('https://', '')

    return (
        SparkSession.builder.appName(app_name)
        # Iceberg + AWS jars (download on first run)
        .config(
            'spark.jars.packages',
            ','.join(
                [
                    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0',
                    'org.apache.hadoop:hadoop-aws:3.3.4',
                ]
            ),
        )
        # Iceberg catalog: HadoopCatalog at s3a://staging/iceberg
        .config('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.iceberg.type', 'hadoop')
        .config('spark.sql.catalog.iceberg.warehouse', 's3a://staging/iceberg')
        # S3 / MinIO connections
        .config('spark.hadoop.fs.s3a.endpoint', endpoint_host)
        .config('spark.hadoop.fs.s3a.access.key', access_key)
        .config('spark.hadoop.fs.s3a.secret.key', secret_key)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        # Iceberg SQL extensions (for MERGE, CREATE TABLE, ... USING iceberg etc.)
        .config(
            'spark.sql.extensions',
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        )
        .getOrCreate()
    )
