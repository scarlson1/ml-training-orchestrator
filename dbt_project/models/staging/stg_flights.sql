select
    md5(
        cast(flight_date as varchar)
        || reporting_airline
        || cast(flight_number as varchar)
        || origin
        || dest
    ) as flight_id,
    
    flight_date,
    reporting_airline as carrier,
    tail_number,
    flight_number,
    origin,
    dest,
    scheduled_departure_utc,
    actual_departure_utc,
    scheduled_arrival_utc,
    actual_arrival_utc,
    dep_delay_min,
    arr_delay_min,
    dep_del15,
    arr_del15,
    cancelled,
    cancellation_code,
    diverted,
    distance_mi,
    crs_elapsed_min,
    actual_elapsed_min,
    carrier_delay_min,
    weather_delay_min,
    nas_delay_min,
    late_aircraft_delay_min,
    -- BTS day_of_week convention: 1=Mon, 7=Sun
    day_of_week,
    adapter.quote("month"),
    adapter.quote("year")

from {{ source('iceberg_staging', 'staged_flights') }}
where scheduled_departure_utc is not null