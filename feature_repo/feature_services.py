from feast import FeatureService

from feature_repo.feature_views import (
    aircraft_fv,
    carrier_fv,
    dest_airport_fv,
    origin_airport_fv,
    route_fv,
)

# The full feature bundle used by the flight delay prediction model.
# At serving time, provide the entity keys (origin, dest, carrier, route_key,
# tail_number) and this service returns all features in one round-trip.
flight_delay_prediction_fs = FeatureService(
    name='flight_delay_prediction',
    features=[
        origin_airport_fv,
        dest_airport_fv,
        carrier_fv,
        route_fv,
        aircraft_fv,
    ],
    tags={'model': 'flight_delay', 'version': '1'},
)

# A lighter bundle used when only the origin airport and carrier are known
# (e.g., early prediction before aircraft assignment).
early_prediction_fs = FeatureService(
    name='flight_delay_early_prediction',
    features=[
        origin_airport_fv[['origin_avg_dep_delay_1h', 'origin_congestion_service_1h']],
        carrier_fv[['carrier_on_time_pct_7d', 'carrier_avg_delay_7d']],
    ],
    tags={'model': 'flight_delay_early', 'version': '1'},
)
