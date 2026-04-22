select

    flight_id,
    carrier,
    scheduled_departure_utc                 as event_ts,

    avg((not dep_del15)::double) over w_7d  as carrier_on_time_pct_7d,
    avg(cancelled::double) over w_7d        as carrier_cancellation_rate_7d,
    avg(dep_delay_min) over w_7d            as carrier_avg_delay_7d,
    count(*) over w_7d                      as carrier_flight_count_7d

from {{ ref('stg_flights') }}

window w_7d as (
    partition by carrier
    order by scheduled_departure_utc
    range between interval '7 days' preceding and current row
)