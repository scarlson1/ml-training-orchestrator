# tests/integration/test_feast_roundtrip.py

"""
Integration smoke test for Feast feature store.

Split into three concerns:
  1. TestFeastApply       — verifies feast apply registered all objects.
                            Requires: feast apply already run (feature_repo/data/registry.db).
  2. TestHistoricalPITJoin — verifies point-in-time join correctness.
                            Self-contained: local Parquet only, no MinIO required.
  3. TestOnlineRoundtrip  — verifies materialize → get_online_features roundtrip.
                            Self-contained: SQLite online store, no Redis required.

Run with:
  uv run pytest tests/integration/test_feast_roundtrip.py -m integration -v
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.types import Float32, Int64

FEATURE_REPO_DIR = Path(__file__).parent.parent.parent / 'feature_repo'

pytestmark = pytest.mark.integration


@pytest.fixture(scope='module')
def store() -> FeatureStore:
    return FeatureStore(repo_path=str(FEATURE_REPO_DIR))


def _build_local_store(
    tmp_path: Path,
    feature_data: pd.DataFrame,
) -> FeatureStore:
    """
    Build a self-contained FeatureStore backed by local Parquet + SQLite.
    No MinIO, no Redis, no external dependencies.

    The caller writes whatever feature rows it wants to test; this function
    wires them into a minimal FeatureStore that can run materialize and
    get_historical_features against them.
    """
    from feast import RepoConfig

    data_dir = tmp_path / 'features'
    data_dir.mkdir(parents=True, exist_ok=True)
    feature_data.to_parquet(data_dir / 'data.parquet', index=False)

    entity = Entity(name='origin_airport', join_keys=['origin'])
    source = FileSource(
        path=str(data_dir) + '/',
        file_format=ParquetFormat(),
        timestamp_field='event_timestamp',
    )
    fv = FeatureView(
        name='test_origin_fv',
        entities=[entity],
        schema=[
            Field(name='origin_avg_dep_delay_1h', dtype=Float32),
            Field(name='origin_flight_count_1h', dtype=Int64),
        ],
        source=source,
    )

    # RepoConfig accepts dicts for offline_store and online_store —
    # Feast deserializes them via Pydantic using the 'type' discriminator.
    # Using dicts here is more stable across Feast minor versions than
    # importing internal *Config classes directly.
    config = RepoConfig(
        project='bmo_test',
        registry=str(tmp_path / 'registry.db'),
        provider='local',
        offline_store={'type': 'file'},
        online_store={'type': 'sqlite', 'path': str(tmp_path / 'online.db')},
        entity_key_serialization_version=2,
    )
    local = FeatureStore(config=config)
    local.apply([entity, source, fv])
    return local


class TestFeastApply:
    """Verify feast apply registered all expected objects in the project registry."""

    def test_all_feature_views_registered(self, store: FeatureStore) -> None:
        fv_names = {fv.name for fv in store.list_feature_views()}
        expected = {
            'origin_airport_features',
            'dest_airport_features',
            'carrier_features',
            'route_features',
            'aircraft_features',
        }
        assert expected.issubset(fv_names), f'Missing: {expected - fv_names}'

    def test_all_entities_registered(self, store: FeatureStore) -> None:
        entity_names = {e.name for e in store.list_entities()}
        assert 'origin_airport' in entity_names
        assert 'carrier' in entity_names
        assert 'aircraft_tail' in entity_names

    def test_feature_service_registered(self, store: FeatureStore) -> None:
        fs_names = {fs.name for fs in store.list_feature_services()}
        assert 'flight_delay_prediction' in fs_names


class TestHistoricalPITJoin:
    """
    Verify get_historical_features performs a real point-in-time join.
    Self-contained — writes controlled local Parquet, builds a local FeatureStore.
    """

    @pytest.fixture
    def pit_store(self, tmp_path: Path) -> FeatureStore:
        base = datetime(2024, 6, 1, tzinfo=timezone.utc)
        feature_data = pd.DataFrame(
            {
                'origin': ['ORD', 'ORD'],
                'event_timestamp': [
                    base.replace(hour=10, minute=0),  # early: delay=5.0
                    base.replace(hour=14, minute=0),  # rush:  delay=18.0
                ],
                'origin_avg_dep_delay_1h': [5.0, 18.0],
                'origin_flight_count_1h': [20, 95],
            }
        )
        return _build_local_store(tmp_path, feature_data)

    def test_pit_join_returns_correct_historical_value(self, pit_store: FeatureStore) -> None:
        """
        Scenario:
          t=10:00  ORD: avg_delay=5.0   (early morning, light traffic)
          t=14:00  ORD: avg_delay=18.0  (afternoon rush)

          Flight A: scheduled_dep=11:30 → should see 5.0 (from 10:00 snapshot)
          Flight B: scheduled_dep=15:00 → should see 18.0 (from 14:00 snapshot)

        A naive latest-value SELECT returns 18.0 for both. The PIT join returns 5.0 for A.
        """
        base = datetime(2024, 6, 1, tzinfo=timezone.utc)
        entity_df = pd.DataFrame(
            {
                'origin': ['ORD', 'ORD'],
                'event_timestamp': [
                    base.replace(hour=11, minute=30),  # Flight A
                    base.replace(hour=15, minute=0),  # Flight B
                ],
            }
        )

        result = (
            pit_store.get_historical_features(
                entity_df=entity_df,
                features=['test_origin_fv:origin_avg_dep_delay_1h'],
            )
            .to_df()
            .sort_values('event_timestamp')
            .reset_index(drop=True)
        )

        assert result['origin_avg_dep_delay_1h'].iloc[0] == pytest.approx(
            5.0, rel=1e-4
        ), 'Flight A got wrong delay — possible future leakage from the 14:00 snapshot'
        assert result['origin_avg_dep_delay_1h'].iloc[1] == pytest.approx(
            18.0, rel=1e-4
        ), 'Flight B got wrong delay'

    def test_get_historical_is_not_just_latest_value(self, pit_store: FeatureStore) -> None:
        """
        If Feast were doing SELECT latest WHERE entity=ORD, both flights return 18.0.
        Assert the values differ — proving PIT join is working, not a plain SELECT.
        """
        base = datetime(2024, 6, 1, tzinfo=timezone.utc)
        entity_df = pd.DataFrame(
            {
                'origin': ['ORD', 'ORD'],
                'event_timestamp': [
                    base.replace(hour=11, minute=30),
                    base.replace(hour=15, minute=0),
                ],
            }
        )
        result = (
            pit_store.get_historical_features(
                entity_df=entity_df,
                features=['test_origin_fv:origin_avg_dep_delay_1h'],
            )
            .to_df()
            .sort_values('event_timestamp')
        )

        values = result['origin_avg_dep_delay_1h'].tolist()
        assert values[0] != values[1], (
            'Both timestamps returned the same delay value — '
            'this suggests a latest-value SELECT instead of a PIT join. '
            'Check that event_timestamp is correctly populated in the Parquet source.'
        )


class TestOnlineRoundtrip:
    """
    Verify materialize → get_online_features roundtrip.
    Uses SQLite as the online store — no Redis required.
    """

    @pytest.fixture
    def online_store(self, tmp_path: Path) -> FeatureStore:
        now = datetime.now(timezone.utc)
        feature_data = pd.DataFrame(
            {
                'origin': ['ORD'],
                'event_timestamp': [now],
                'origin_avg_dep_delay_1h': [12.5],
                'origin_flight_count_1h': [42],
            }
        )
        local = _build_local_store(tmp_path, feature_data)
        # materialize() reads the FileSource Parquet and writes to the SQLite online store.
        # This is the correct replacement for the broken store.push() call — push() requires
        # a PushSource (streaming source), not a FileSource.
        local.materialize(
            start_date=now.replace(year=now.year - 1),
            end_date=now,
        )
        return local

    def test_write_then_read_online(self, online_store: FeatureStore) -> None:
        result = online_store.get_online_features(
            features=[
                'test_origin_fv:origin_avg_dep_delay_1h',
                'test_origin_fv:origin_flight_count_1h',
            ],
            entity_rows=[{'origin': 'ORD'}],
        ).to_df()

        assert result['origin_avg_dep_delay_1h'].iloc[0] == pytest.approx(12.5, rel=1e-4)
        assert result['origin_flight_count_1h'].iloc[0] == 42

    def test_missing_entity_returns_null(self, online_store: FeatureStore) -> None:
        """Unknown entities return null — not raise, not return 0. Serving code must handle this."""
        result = online_store.get_online_features(
            features=['test_origin_fv:origin_avg_dep_delay_1h'],
            entity_rows=[{'origin': 'ZZZ'}],  # nonexistent airport
        ).to_df()

        assert (
            result['origin_avg_dep_delay_1h'].isna().iloc[0]
        ), 'Unknown entity should return null — serving code must coalesce or impute'
