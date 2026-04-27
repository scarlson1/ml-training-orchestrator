select
    station_id,
    iata_code,
    obs_time_utc,
    temp_f,
    dew_point_f,
    relative_humidity_pct,
    wind_speed_kts,
    wind_dir_deg,
    precip_1h_in,
    visibility_mi,
    sky_conditions,
    present_weather,
    sea_level_pressure_hpa
from {{ source('iceberg_staging', 'staged_weather' )}}
where obs_time_utc is not null