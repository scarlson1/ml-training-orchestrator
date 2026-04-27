"""
Set required env vars at module load time — before any bmo import triggers
Settings() instantiation. Fixtures that need these values can rely on them
already being present in os.environ.
"""

import os

import pandas as pd
import pytest

os.environ.setdefault('S3_ENDPOINT_URL', 'http://localhost:9000')
os.environ.setdefault('S3_ACCESS_KEY_ID', 'admin')
os.environ.setdefault('S3_SECRET_ACCESS_KEY', 'password123')
os.environ.setdefault('MLFLOW_TRACKING_URI', 'http://localhost:5000')


"""
Fixtures for training dataset builder tests.

All fixtures are in-memory — no S3, no network. Integration tests that need
S3 use the @pytest.mark.integration marker and run against a live MinIO instance.
"""


@pytest.fixture
def sample_label_df() -> pd.DataFrame:
    """
    Minimal valid label DataFrame for testing build_dataset() and PITJoiner.

    event_timestamp is UTC-aware. The five flights cover different scenarios:
      - ORD→JFK: standard hub-to-hub route, DL carrier
      - ATL→LAX: long-haul, AA carrier
      - DEN→SFO: mountain west, UA carrier
      - ORD→MIA: carrier same as first flight (tests carrier join dedup)
      - BOS→ORD: small origin, tests sparse feature rows
    """
    return pd.DataFrame(
        {
            'flight_id': ['f001', 'f002', 'f003', 'f004', 'f005'],
            'event_timestamp': pd.to_datetime(
                [
                    '2024-03-15 14:00:00',
                    '2024-03-15 16:30:00',
                    '2024-03-15 09:00:00',
                    '2024-03-15 18:00:00',
                    '2024-03-16 07:00:00',
                ],
                utc=True,
            ),
            'origin': ['ORD', 'ATL', 'DEN', 'ORD', 'BOS'],
            'dest': ['JFK', 'LAX', 'SFO', 'MIA', 'ORD'],
            'carrier': ['DL', 'AA', 'UA', 'DL', 'B6'],
            'tail_number': ['N12345', 'N67890', 'N11111', 'N22222', 'N33333'],
            'route_key': ['ORD-JFK', 'ATL-LAX', 'DEN-SFO', 'ORD-MIA', 'BOS-ORD'],
            'dep_delay_min': [5.0, -2.0, 15.0, 0.0, 30.0],
            'is_dep_delayed': [False, False, True, False, True],
            'cancelled': [False, False, False, False, False],
        }
    )


@pytest.fixture
def sample_origin_feature_df() -> pd.DataFrame:
    """
    Simulated origin airport feature Parquet content.

    Three snapshots per airport, spaced 1 hour apart, all BEFORE the label
    event timestamps in sample_label_df. This tests correct ASOF JOIN behavior:
    the join should pick the snapshot immediately before the event timestamp.
    """
    return pd.DataFrame(
        {
            'origin': ['ORD', 'ORD', 'ORD', 'ATL', 'ATL', 'DEN', 'BOS'],
            'event_ts': pd.to_datetime(
                [
                    '2024-03-15 12:00:00',  # 2h before ORD flight at 14:00
                    '2024-03-15 13:00:00',  # 1h before — this should be selected
                    '2024-03-15 14:30:00',  # AFTER ORD flight — must NOT be selected
                    '2024-03-15 15:00:00',  # 1.5h before ATL flight at 16:30 — selected
                    '2024-03-15 14:00:00',  # 2.5h before ATL
                    '2024-03-15 08:00:00',  # 1h before DEN flight at 09:00 — selected
                    '2024-03-16 06:00:00',  # 1h before BOS flight at 07:00 — selected
                ],
                utc=True,
            ),
            'origin_avg_dep_delay_1h': [6.0, 9.8, 14.1, 3.2, 2.1, 5.5, 12.0],
            'origin_congestion_score_1h': [0.4, 0.6, 0.8, 0.2, 0.1, 0.3, 0.7],
        }
    )
