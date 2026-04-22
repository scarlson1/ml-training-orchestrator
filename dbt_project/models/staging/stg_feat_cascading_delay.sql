-- Thin pass-through from the PySpark-written Iceberg table. If the table doesn't exist yet, dbt run will fail at this model — that's fine. Run PySpark first.

select
    flight_id,
    tail_number,
    scheduled_departure_utc,
    prev_arr_delay_min,
    prev_dest,
    prev_actual_arrival_utc,
    turnaround_min
from {{ source('iceberg_staging', 'feat_cascading_delay') }}