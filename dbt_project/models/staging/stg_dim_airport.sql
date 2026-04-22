select
    iata_code,
    icao_code,
    name          as airport_name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    iso_region,
    tz_database_timezone,
    lcd_station_id
from {{ source('iceberg_staging', 'dim_airport') }}
where iata_code is not null