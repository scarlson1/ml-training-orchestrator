from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def compute_cascading_delay(spark: SparkSession) -> int:
    """
    Compute per-aircraft cascading delay feature and write to Iceberg.

    For each flight, joins to the previous flight operated by the same aircraft
    (same tail_number) to get:
      - prev_arr_delay_min: how late the aircraft arrived on its previous leg
      - prev_dest: where the aircraft was before this flight
      - turnaround_min: minutes between previous actual arrival and this
                        scheduled departure (a proxy for buffer time)

    The HadoopCatalog maps `iceberg.staged_flights` to
    s3a://staging/iceberg/staged_flights — same location as PyIceberg writes.
    """
    flights = spark.table('staging.staged_flights')

    # compute same surrogate key as stg_flights.sql for clean joins on flight_id without mapping table
    # TODO: test to ensure key is the same ??
    flights = flights.withColumn(
        'flight_id',
        F.md5(
            F.concat(
                F.col('flight_date').cast('string'),
                F.col('reporting_airline'),
                F.col('flight_number').cast('string'),
                F.col('origin'),
                F.col('dest'),
            )
        ),
    )

    # window: per aircraft, ordered chronologically
    w = Window.partitionBy('tail_number').orderBy('scheduled_departure_utc')

    #
    result = flights.select(
        'flight_id',  # current flight ID
        'tail_number',
        'scheduled_departure_utc',  # current flight
        F.lag('arr_delay_min', 1)
        .over(w)
        .alias('prev_arr_delay_min'),  # previous flight's delay (arrival)
        F.lag('dest', 1).over(w).alias('prev_dest'),  # previous destination
        F.lag('actual_arrival_utc', 1)
        .over(w)
        .alias('prev_actual_arrival_utc'),  # previous flights actual arrival ts
    ).withColumn(
        'turnaround_min',
        (
            F.col('scheduled_departure_utc').cast('long')
            - F.col('prev_actual_arrival_utc').cast('long')
        )
        / 60,
    )  # turnaround: previous arrival - schedule departure

    # write result to iceberg, replacing previous (idempotent)
    result.writeTo('staging.feat_cascading_delay').createOrReplace()
    # For very large datasets: partition by month and use overwritePartitions() instead.

    return int(result.count())
