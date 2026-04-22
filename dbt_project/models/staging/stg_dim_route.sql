select
    airline_iata,
    origin,
    dest,
    distance_mi
from {{ source('iceberg_staging', 'dim_route') }}
where distance_mi is not null
