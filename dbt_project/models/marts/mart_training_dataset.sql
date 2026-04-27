-- The feature models produce one row per flight each, but ML training needs all features on a single row. This mart assembles them.

select

    -- Identity
    e.flight_id,
    e.flight_date,
    e.carrier,
    e.origin,
    e.dest,
    e.tail_number,
    e.scheduled_departure_utc,

    -- Labels (what we're predicting)
    e.dep_delay_min,
    e.arr_delay_min,
    e.dep_del15           as is_dep_delayed,
    e.arr_del15           as is_arr_delayed,
    e.cancelled,
    e.diverted,

    -- Origin airport rolling features
    o.origin_flight_count_1h,
    o.origin_avg_dep_delay_1h,
    o.origin_pct_delayed_1h,
    o.origin_flight_count_24h,
    o.origin_avg_dep_delay_24h,
    o.origin_pct_cancelled_24h,
    o.origin_avg_dep_delay_7d,
    o.origin_pct_delayed_7d,
    o.origin_congestion_score_1h,

    -- Destination airport rolling features
    d.dest_avg_arr_delay_1h,
    d.dest_pct_delayed_1h,
    d.dest_avg_arr_delay_24h,
    d.dest_pct_cancelled_24h,
    d.dest_pct_diverted_24h,

    -- Carrier features
    c.carrier_on_time_pct_7d,
    c.carrier_cancellation_rate_7d,
    c.carrier_avg_delay_7d,
    c.carrier_flight_count_7d,

    -- Route features
    r.route_key,
    r.route_avg_dep_delay_7d,
    r.route_avg_arr_delay_7d,
    r.route_pct_delayed_7d,
    r.route_cancellation_rate_7d,
    r.route_avg_elapsed_7d,
    r.route_distance_mi,

    -- Weather at departure (from intermediate layer)
    e.origin_temp_f,
    e.origin_wind_kts,
    e.origin_precip_1h_in,
    e.origin_visibility_mi,
    e.origin_is_thunderstorm,
    e.origin_is_low_vis,
    e.origin_is_high_wind,
    e.dest_temp_f,
    e.dest_wind_kts,
    e.dest_is_thunderstorm,
    e.dest_is_low_vis,

    -- Static airport type
    -- s.origin_hub_size,
    coalesce(h.hub_size, 'small_regional') as origin_hub_size,

    -- Calendar
    cal.scheduled_hour_utc,
    cal.day_of_week,
    cal.month_of_year,
    cal.quarter,
    cal.is_weekend,
    cal.is_holiday,
    cal.is_rush_hour,

    -- Cascading delay (from PySpark job — null until pyspark has run)
    cd.prev_arr_delay_min    as cascading_delay_min,
    cd.turnaround_min

from {{ ref('int_flights_enriched') }} e
left join {{ ref('feat_origin_airport_windowed') }} o using (flight_id)
left join {{ ref('feat_dest_airport_windowed') }}   d using (flight_id)
left join {{ ref('feat_carrier_rolling') }}         c using (flight_id)
left join {{ ref('feat_route_rolling') }}           r using (flight_id)
left join {{ ref('hub_airports') }} h on h.iata_code = e.origin
left join {{ ref('feat_calendar') }}                cal using (flight_id)
left join {{ ref('stg_feat_cascading_delay') }}     cd using (flight_id)