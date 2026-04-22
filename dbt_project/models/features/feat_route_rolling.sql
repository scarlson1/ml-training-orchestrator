-- distance_mi is already in stg_flights (from STAGED_FLIGHTS_SCHEMA). No join to dim_route needed — use max(distance_mi) within the partition to get the route's static distance.

-- TODO: where is distance_mi getting added to flights ??
-- it should be joined from route table to avoid recomputing ??

select

    flight_id,
    origin,
    dest,
    origin || '-' || dest               as route_key,
    scheduled_departure_utc             as event_ts,

    avg(dep_delay_min) over w_7d       as route_avg_dep_delay_7d,
    avg(arr_delay_min) over w_7d        as route_avg_arr_delay_7d,
    avg(dep_del15::double) over w_7d    as route_pct_delayed_7d,
    avg(cancelled::double) over w_7d    as route_cancellation_rate_7d,
    avg(actual_elapsed_min) over w_7d   as route_avg_elapsed_7d,

    -- distance_mi is constant per route; max() dives a scalar across the window
    max(distance_mi) over (partition by origin, dest) as route_distance_mi

from {{ ref('stg_flights') }}

window w_7d as (
    partition by origin, dest
    order by scheduled_departure_utc
    range between interval '7 days' preceding and current row
)