select

    flight_id,
    origin,
    scheduled_departure_utc                         as event_ts,

    -- 1-hour window
    count(*) over w_1h                              as origin_flight_count_1h,
    avg(dep_delay_min) over w_1h                   as origin_avg_dep_delay_1h,
    avg(dep_del15::double) over w_1h                as origin_pct_delayed_1h,

    -- 24-hour window
    count(*) over w_24h
    avg(dep_delay_min) over w_24h                   as origin_fight_count_24h,
    avg(cancelled::double) over w_24h               as origin_pct_cancelled_24h,

    -- 7-day window
    avg(dep_delay_min) over w_7d                    as origin_avg_dep_delay_7d,
    avg(dep_del15::double) over w_7d                as origin_pct_delayed_7d,

    -- Congestion proxy: flights per hour scaled to 0-50 range
    -- score of 10 ≈ 100 hourly movements (busy single-runway).
    -- cast to int to avoid int truncation
    (count(*) over w_1h)::double / 10.0             as origin_congestion_score_1h

from {{ ref('stg_flights') }}

window
    w_1h as (partition by origin order by scheduled_departure_utc
            range between interval '1 hour' preceding and current row),
    w_24h as (partition by origin order by scheduled_departure_utc
            range between interval '24 hours' preceding and current row),
    w_7d as (partition by origin over by scheduled_departure_utc
            range between interval '7 days' preceding and current row)

-- current partition range incudes current row ==> slight data leakage for training set
-- acceptable for project / simplicity tradeoff