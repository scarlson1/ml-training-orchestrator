"""
Unit tests for bmo.serving.feature_client.FeatureClient.

These tests mock the Feast FeatureStore to exercise the fail-closed logic
without requiring a live Redis connection.

Why mock Feast here:
  FeatureClient's value proposition is the null-check and structured logging
  around Feast's raw API call. The Feast SDK itself is well-tested upstream —
  we're testing our wrapper logic, not Feast internals.

  The mock is scoped to FeatureStore.get_online_features so that we can
  simulate null returns (expired TTL), exceptions (Redis down), and full
  feature sets in a single, fast unit test module.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from bmo.serving.feature_client import FEATURE_COLUMNS, FeatureClient
from bmo.serving.schemas import PredictRequest

_SAMPLE_REQUEST = PredictRequest(
    flight_id='AA1234_20240615_0900',
    origin='ORD',
    dest='LAX',
    carrier='AA',
    tail_number='N12345',
    route_key='ORD-LAX',
)


def _make_full_response(value: float = 2.5) -> MagicMock:
    """Mock Feast response where all features have a value."""
    mock = MagicMock()
    mock.to_dict.return_value = {col: [value] for col in FEATURE_COLUMNS}
    return mock


def _make_partial_response(null_cols: list[str]) -> MagicMock:
    """Mock Feast response where some features are null (expired TTL)."""
    mock = MagicMock()
    result: dict[str, list[float | None]] = {col: [2.5] for col in FEATURE_COLUMNS}
    for col in null_cols:
        result[col] = [None]
    mock.to_dict.return_value = result
    return mock


@pytest.fixture
def client(tmp_path: Path) -> Generator[tuple[FeatureClient, MagicMock], None, None]:
    """FeatureClient with a fully mocked FeatureStore."""
    with patch('bmo.serving.feature_client.FeatureStore') as mock_cls:
        mock_store = MagicMock()
        mock_cls.return_value = mock_store
        yield FeatureClient(feature_repo_dir=str(tmp_path)), mock_store


class TestGetFeaturesSuccess:
    def test_returns_dataframe_on_full_response(
        self, client: tuple[FeatureClient, MagicMock]
    ) -> None:
        fc, mock_store = client
        mock_store.get_online_features.return_value = _make_full_response()
        result = fc.get_features(_SAMPLE_REQUEST)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == FEATURE_COLUMNS
        assert len(result) == 1

    def test_passes_correct_entity_row(self, client: tuple[FeatureClient, MagicMock]) -> None:
        fc, mock_store = client
        mock_store.get_online_features.return_value = _make_full_response()
        fc.get_features(_SAMPLE_REQUEST)

        call_kwargs = mock_store.get_online_features.call_args
        entity_rows = call_kwargs[1]['entity_rows']
        assert len(entity_rows) == 1
        row = entity_rows[0]
        assert row['origin'] == 'ORD'
        assert row['carrier'] == 'AA'
        assert row['route_key'] == 'ORD-LAX'


class TestFailClosed:
    def test_returns_none_when_single_feature_null(
        self, client: tuple[FeatureClient, MagicMock]
    ) -> None:
        """
        Expired TTL on one feature view (e.g., aircraft_features after 12h)
        should fail the entire request, not silently return 0.
        """
        fc, mock_store = client
        mock_store.get_online_features.return_value = _make_partial_response(
            null_cols=['cascading_delay_min']
        )
        result = fc.get_features(_SAMPLE_REQUEST)
        assert result is None

    def test_returns_none_when_all_features_null(
        self, client: tuple[FeatureClient, MagicMock]
    ) -> None:
        fc, mock_store = client
        mock_store.get_online_features.return_value = _make_partial_response(
            null_cols=FEATURE_COLUMNS
        )
        result = fc.get_features(_SAMPLE_REQUEST)
        assert result is None

    def test_returns_none_when_feast_raises(self, client: tuple[FeatureClient, MagicMock]) -> None:
        """Redis is down or unreachable — should fail closed, not raise."""
        fc, mock_store = client
        mock_store.get_online_features.side_effect = ConnectionError('Redis refused')
        result = fc.get_features(_SAMPLE_REQUEST)
        assert result is None

    def test_returns_none_when_key_missing_from_response(
        self, client: tuple[FeatureClient, MagicMock]
    ) -> None:
        """If Feast omits a feature key entirely (e.g., unknown entity), treat as null."""
        fc, mock_store = client
        mock_response = MagicMock()
        # Return only a subset of feature columns
        mock_response.to_dict.return_value = {'origin_avg_dep_delay_1h': [2.5]}
        mock_store.get_online_features.return_value = mock_response
        result = fc.get_features(_SAMPLE_REQUEST)
        assert result is None


class TestPingRedis:
    def test_returns_true_when_feast_succeeds(
        self, client: tuple[FeatureClient, MagicMock]
    ) -> None:
        fc, mock_store = client
        mock_store.get_online_features.return_value = MagicMock()
        assert fc.ping_redis() is True

    def test_returns_false_when_feast_raises(self, client: tuple[FeatureClient, MagicMock]) -> None:
        fc, mock_store = client
        mock_store.get_online_features.side_effect = Exception('connection refused')
        assert fc.ping_redis() is False
