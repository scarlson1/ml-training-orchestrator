with origin_weather as (
    select
        iata_code,
        obs_time_utc,
        temp_f          as origin_temp_f,
        wind_speed_kts  as origin_wind_kts,
        precip_1h_in    as origin_precip_1h_in,
        visibility_mi   as origin_visibility_mi,
        present_weather as origin_present_weather
    from {{ ref('stg_weather') }}
),

dest_weather as (
    select
        iata_code,
        obs_time_utc,
        temp_f          as dest_temp_f,
        wind_speed_kts  as dest_wind_kts,
        precip_1h_in    as dest_precip_1h_in,
        visibility_mi   as dest_visibility_mi,
        present_weather as dest_present_weather
    from {{ ref('stg_weather') }}
)

-- join origin weather. QUALIFY reduces to 1 row per flight
with_origin_wx as (
    select
        f.*,
        ow.obs_time_utc          as origin_obs_time_utc,
        ow.origin_temp_f,
        ow.origin_wind_kts,
        ow.origin_precip_1h_in,
        ow.origin_visibility_mi,
        ow.origin_present_weather
    from {{ ref('stg_flights') }}
    left join origin_weather ow
        on ow.iata_code = f.origin
        -- within 3 hours BEFORE scheduled departure
        and ow.obs_time_utc <= f.scheduled_departure_utc
        and ow.obs_time_utc >= f.scheduled_departure_utc - interval '3hours'
    qualify row_number() over (
        partition by f.flight_id
        order by ow.obs_time_utc desc nulls last
    ) = 1
)

-- join destination weather using same PIT anchor
-- with_origin_wx already has 1 row per flight ==> just join
with_dest_wx as (
    select
        f.*,
        wd.obs_time_utc         as dest_obs_time_utc,
        wd.dest_temp_f,
        wd.dest_wind_kts,
        wd.origin_precip_1h_in,
        wd.dest_visibility_mi,
        wd.dest_present_weather
    from with_origin_wx f
    left join dest_weather wd
        on wd.iata_code = f.dest
        and wd.obs_time_utc <= f.scheduled_departure_utc
        and wd.obs_time_utc >= f.scheduled_departure_utc - interval '6 hours'
    qualify row_number() over (
        partition by f.flight_id
        order by wd.obs_time_utc desc nulls last
    ) = 1
)

select
    *,
    origin_present_weather ilike '%TS%'         as origin_is_thunderstorm,
    (origin_present_weather ilike '%FG%'
     or origin_visibility_mi < 3.0)                        as origin_is_low_vis,
    origin_wind_kts > 25.0                                 as origin_is_high_wind,
    dest_present_weather ilike '%TS%'                      as dest_is_thunderstorm,
    dest_visibility_mi < 3.0                               as dest_is_low_vis

from with_dest_wx