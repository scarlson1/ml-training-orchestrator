from __future__ import annotations

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from bmo.pyspark_jobs import make_spark_session
from bmo.pyspark_jobs.cascading_delay import compute_cascading_delay

# run pyspark job to compute cascading delay

# pyspark is able to handle sorted partitioning in window function more efficiently than duckDB
# partition by tail number; sort by scheduled departure
# pyspark doesn't need to read the entire dataset into memory and sort


@asset(
    group_name='features',
    deps=['staged_flights'],
    description='Per-aircraft cascading delay computed via PySpark LAG window',
)
def feat_cascading_delay(context: AssetExecutionContext) -> MaterializeResult:
    # SparkSession configured for Iceberg on MinIO
    spark = make_spark_session('bmo-cascading-delay')
    try:
        # run pyspark job
        row_count = compute_cascading_delay(spark)
    finally:
        spark.stop()

    return MaterializeResult(metadata={'row_count': MetadataValue.int(row_count)})
