select
    md5(
        cast(flight_date as varchar)
        || reporting_airline
        || cast(flight_number as varchar)
        || origin
        || dest
    ) as flight_id,         -- unique ID
    
    -- flight info
    flight_date,
    reporting_airline as carrier,
    tail_number,
    flight_number,
    origin,
    dest,

    -- timestamps
    scheduled_departure_utc,
    actual_departure_utc,
    scheduled_arrival_utc,
    actual_arrival_utc,

    -- delay/performance data
    dep_delay_min,
    arr_delay_min,
    dep_del15,              -- boolean - dept delayed 15 mins
    arr_del15,              -- boolean - arrival delayed 15 mins
    cancelled,              -- boolean - cancelled
    cancellation_code,
    diverted,               -- boolean
    distance_mi,            -- float miles
    crs_elapsed_min,        -- 
    actual_elapsed_min,     -- 
    carrier_delay_min,      -- 
    weather_delay_min,      -- minutes delayed due to weather
    nas_delay_min,          -- ??
    late_aircraft_delay_min,

    -- dates
    day_of_week,  -- BTS day_of_week convention: 1=Mon, 7=Sun
    adapter.quote("month"),  -- 1-12 month
    adapter.quote("year")  -- 4 digit year

-- iceberg source connector
from {{ source('iceberg_staging', 'staged_flights') }}
where scheduled_departure_utc is not null