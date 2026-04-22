select

    flight_id,
    dest,
    scheduled_departure_utc             as event_ts,

    avg(arr_delay_min) over w_1h        as dest_avg_arr_delay_1h,
    avg(arr_del15::double) over w_1h    as dest_pct_delayed_1h,
    avg(arr_delay_min) over w_24h       as dest_avg_arr_delay_24h,
    avg(cancelled::double) over w_24h   as dest_pct_cancelled_24h,
    avg(diverted::double) over w_24h    as dest_pct_diverted_24h

from {{ ref('stg_flights') }}

window
    w_1h as (partition by dest order by scheduled_departure_utc
            range between interval '1 hour' preceding and current row),
    w_24h as (partition by dest order by scheduled_departure_utc 
            range between interval '24 hours' preceding and current row)