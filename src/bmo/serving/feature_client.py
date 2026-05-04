"""
Feast online feature retrieval with fail-closed semantics.

Why fail-closed?
  A model receiving stale or missing features produces silently wrong predictions.
  For flight delay prediction, a stale feature might be from 48 hours ago during
  a Redis restart — the model's 1h-window features would be nonsense. It's better
  to return a 503 to the caller and let them decide how to handle it (retry,
  fallback, etc.) than to return a confident but wrong probability.

"Stale" vs "missing":
  Feast enforces TTL at the FeatureView level (e.g., 26h for airport features).
  If Redis contains no record younger than TTL, Feast returns None for that feature.
  Our null-check catches both cases — missing and expired are indistinguishable
  at retrieval time, which is by design.

Feast get_online_features docs:
  https://docs.feast.dev/reference/feature-retrieval#get-online-features
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import structlog
from feast import FeatureStore

from bmo.serving.schemas import PredictRequest

log = structlog.get_logger(__name__)

# Must match BATCH_FEATURE_REFS in bmo/batch_scoring/score.py AND
# ALL_FEATURE_REFS in dagster_project/assets/training.py.
# The order determines the column positions in the feature matrix passed to XGBoost,
# so changing order here without retraining would corrupt predictions.
# TODO: import from one file ??
ONLINE_FEATURE_REFS: list[str] = [
    'origin_airport_features:origin_flight_count_1h',
    'origin_airport_features:origin_avg_dep_delay_1h',
    'origin_airport_features:origin_pct_delayed_1h',
    'origin_airport_features:origin_avg_dep_delay_24h',
    'origin_airport_features:origin_pct_cancelled_24h',
    'origin_airport_features:origin_avg_dep_delay_7d',
    'origin_airport_features:origin_pct_delayed_7d',
    'origin_airport_features:origin_congestion_score_1h',
    'dest_airport_features:dest_avg_arr_delay_1h',
    'dest_airport_features:dest_pct_delayed_1h',
    'dest_airport_features:dest_avg_arr_delay_24h',
    'dest_airport_features:dest_pct_diverted_24h',
    'carrier_features:carrier_on_time_pct_7d',
    'carrier_features:carrier_cancellation_rate_7d',
    'carrier_features:carrier_avg_delay_7d',
    'carrier_features:carrier_flight_count_7d',
    'route_features:route_avg_dep_delay_7d',
    'route_features:route_avg_arr_delay_7d',
    'route_features:route_pct_delayed_7d',
    'route_features:route_cancellation_rate_7d',
    'route_features:route_avg_elapsed_7d',
    'route_features:route_distance_mi',
    'aircraft_features:cascading_delay_min',
    'aircraft_features:turnaround_min',
]

FEATURE_COLUMNS: list[str] = [ref.split(':')[1] for ref in ONLINE_FEATURE_REFS]

# Aircraft features depend on tail_number, which callers may not know at serving time.
# When null, we impute 0.0 (no inbound delay assumed) rather than failing closed.
# All other features are expected to be in Redis — nulls there are a real data gap.
_SOFT_NULL_FEATURES: frozenset[str] = frozenset({'cascading_delay_min', 'turnaround_min'})


class FeatureClient:
    """
    Wraps Feast's online store retrieval and enforces fail-closed semantics.

    Feast's online store is Redis. In production, Feast materializes new feature
    values every hour (feast_materialized_features schedule). This client adds:
      1. Null detection: any None feature → return None (caller returns 503)
      2. Consistent entity_row format across all callers
      3. Structured logging of null events for monitoring

    Usage:
        client = FeatureClient(feature_repo_dir='feature_repo/')
        feature_df = client.get_features(request)
        if feature_df is None:
            raise HTTPException(503)  # fail closed
    """

    def __init__(self, feature_repo_dir: str | Path) -> None:
        self._store = FeatureStore(repo_path=str(feature_repo_dir))

    def get_features(self, request: PredictRequest) -> tuple[pd.DataFrame, bool] | None:
        """
        Retrieve online features for one flight entity.

        Returns None (fail-closed) if any non-aircraft feature is null.
        Aircraft features (cascading_delay_min, turnaround_min) are imputed as
        0.0 when tail_number is unknown — matching batch scoring's fillna(0) behaviour.

        Returns:
            (DataFrame, features_complete) where features_complete is False when
            aircraft features were imputed, or None if a hard null was encountered.
        """
        entity_row: dict[str, Any] = {
            'origin': request.origin,
            'dest': request.dest,
            'carrier': request.carrier,
            'route_key': request.route_key,
            'tail_number': request.tail_number,
        }

        try:
            response = self._store.get_online_features(
                features=ONLINE_FEATURE_REFS,
                entity_rows=[entity_row],
            )
        except Exception:
            log.exception('feast online feature retrieval failed', flight_id=request.flight_id)
            return None

        result_dict = response.to_dict()

        null_features = [col for col in FEATURE_COLUMNS if result_dict.get(col, [None])[0] is None]
        hard_nulls = [f for f in null_features if f not in _SOFT_NULL_FEATURES]

        if hard_nulls:
            log.warning(
                'null features - failing closed',
                flight_id=request.flight_id,
                origin=request.origin,
                null_features=hard_nulls,
            )
            return None

        if null_features:
            log.info(
                'aircraft features null - imputing 0.0',
                flight_id=request.flight_id,
                null_features=null_features,
            )

        feature_row = {
            col: [result_dict[col][0] if result_dict.get(col, [None])[0] is not None else 0.0]
            for col in FEATURE_COLUMNS
        }
        return pd.DataFrame(feature_row), len(null_features) == 0

    def ping_redis(self) -> bool:
        """
        Quick connectivity check for /health endpoint.
        Tries a minimal online feature lookup and considers Redis reachable
        if no exception is raised (even if all features are null).
        """
        try:
            self._store.get_online_features(
                features=[ONLINE_FEATURE_REFS[0]], entity_rows=[{'origin': '__health_probe__'}]
            )
            return True
        except Exception:
            return False
