from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float32, Int64

from data_sources import (
    aircraft_source,
    carrier_source,
    dest_airport_source,
    origin_airport_source,
    route_source,
)
from entities import (
    aircraft_tail,
    carrier,
    dest_airport,
    origin_airport,
    route,
)

# A FeatureView ties together: an entity, a data source, a TTL (time-to-live), and a schema. Think of it as a versioned, documented feature table.

# TTL is the maximum age of a feature value that Feast will return. For online retrieval (get_online_features), if the latest stored value is older than the TTL, Feast returns null

origin_airport_fv = FeatureView(
    name='origin_airport_features',
    entities=[origin_airport],
    ttl=timedelta(hours=26),  # 24 hour window + 2h buffer for lag
    schema=[
        Field(name='origin_flight_count_1h', dtype=Int64),
        Field(name='origin_avg_dep_delay_1h', dtype=Float32),
        Field(name='origin_pct_delayed_1h', dtype=Float32),
        Field(name='origin_avg_dep_delay_24h', dtype=Float32),
        Field(name='origin_pct_cancelled_24h', dtype=Float32),
        Field(name='origin_avg_dep_delay_7d', dtype=Float32),
        Field(name='origin_pct_delayed_7d', dtype=Float32),
        Field(name='origin_congestion_score_1h', dtype=Float32),
    ],
    source=origin_airport_source,
    tags={'entity': 'origin_airport', 'pipeline_stage': 'features'},
)

dest_airport_fv = FeatureView(
    name='dest_airport_features',
    entities=[dest_airport],
    ttl=timedelta(hours=26),
    schema=[
        Field(name='dest_avg_arr_delay_1h', dtype=Float32),
        Field(name='dest_pct_delayed_1h', dtype=Float32),
        Field(name='dest_avg_arr_delay_24h', dtype=Float32),
        Field(name='dest_pct_diverted_24h', dtype=Float32),
    ],
    source=dest_airport_source,
    tags={'entity': 'dest_airport', 'pipeline_stage': 'features'},
)

carrier_fv = FeatureView(
    name='carrier_features',
    entities=[carrier],
    ttl=timedelta(days=8),  # 7d window + 1d buffer
    schema=[
        Field(name='carrier_on_time_pct_7d', dtype=Float32),
        Field(name='carrier_cancellation_rate_7d', dtype=Float32),
        Field(name='carrier_avg_delay_7d', dtype=Float32),
        Field(name='carrier_flight_count_7d', dtype=Int64),
    ],
    source=carrier_source,
    tags={'entity': 'carrier', 'pipeline_stage': 'features'},
)

route_fv = FeatureView(
    name='route_features',
    entities=[route],
    ttl=timedelta(days=8),
    schema=[
        Field(name='route_avg_dep_delay_7d', dtype=Float32),
        Field(name='route_avg_arr_delay_7d', dtype=Float32),
        Field(name='route_pct_delayed_7d', dtype=Float32),
        Field(name='route_cancellation_rate_7d', dtype=Float32),
        Field(name='route_avg_elapsed_7d', dtype=Float32),
        Field(name='route_distance_mi', dtype=Float32),
    ],
    source=route_source,
    tags={'entity': 'route', 'pipeline_stage': 'features'},
)

aircraft_fv = FeatureView(
    name='aircraft_features',
    entities=[aircraft_tail],
    ttl=timedelta(hours=12),  # cascading delay is only relevant if close in time
    schema=[
        Field(name='cascading_delay_min', dtype=Float32),
        Field(name='turnaround_min', dtype=Float32),
    ],
    source=aircraft_source,
    tags={'entity': 'aircraft_tail', 'pipeline_stage': 'features'},
)
