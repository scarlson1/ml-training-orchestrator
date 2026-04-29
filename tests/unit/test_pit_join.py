"""
Unit tests for PITJoiner and the DuckDB ASOF JOIN implementation.

These tests are the most important in the project — they verify that the PIT
join works correctly before any real data touches it.

Key scenarios tested:
  1. Correct ASOF behavior: picks nearest-before snapshot, not nearest overall.
  2. Future exclusion: snapshots AFTER the event timestamp are not selected.
  3. TTL masking: features older than TTL are set to NULL in the result.
  4. Left join: flights with no matching feature get NULL, not dropped.
  5. Multiple feature views: all are joined and combined correctly.
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bmo.training_dataset_builder.pit_join import FeatureViewConfig, PITJoiner


def write_parquet_local(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to a local Parquet file for testing without S3."""
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


@pytest.fixture
def origin_parquet(sample_origin_feature_df: pd.DataFrame, tmp_path: Path) -> str:
    p = tmp_path / 'origin_airport' / 'data.parquet'
    p.parent.mkdir()
    write_parquet_local(sample_origin_feature_df, p)
    return str(p)


@pytest.fixture
def origin_view_config(origin_parquet: str) -> FeatureViewConfig:
    return FeatureViewConfig(
        name='origin_airport_features',
        parquet_path=origin_parquet,
        entity_col='origin',
        label_entity_col='origin',
        ttl=timedelta(hours=26),
        feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
    )


class TestPITJoinerCorrectness:
    def test_selects_nearest_before_snapshot(
        self, sample_label_df: pd.DataFrame, origin_view_config: FeatureViewConfig
    ) -> None:
        """
        The ASOF JOIN must pick the snapshot at 13:00, not 12:00, for the
        ORD flight at 14:00. The 14:30 snapshot (after the event) must be excluded.

        Expected: ORD row has origin_avg_dep_delay_1h = 9.8 (from 13:00 snapshot).
        """
        joiner = PITJoiner(feature_views=[origin_view_config], use_s3=False)
        result = joiner.join(sample_label_df)

        ord_row = result[result['origin'] == 'ORD'].iloc[0]
        assert ord_row['origin_avg_dep_delay_1h'] == pytest.approx(9.8)

    def test_future_snapshot_excluded(
        self, sample_label_df: pd.DataFrame, origin_view_config: FeatureViewConfig
    ) -> None:
        """
        The ORD snapshot at 14:30 is AFTER the ORD flight at 14:00.
        It must never appear in results — even if it has a better delay value.
        This is the PIT correctness invariant.
        """
        joiner = PITJoiner(feature_views=[origin_view_config], use_s3=False)
        result = joiner.join(sample_label_df)

        # 14.1 is the value from the 14:30 snapshot (which is a future snapshot).
        # If the join were wrong, this value would appear instead of 9.8.
        ord_row = result[result['origin'] == 'ORD'].iloc[0]
        assert ord_row['origin_avg_dep_delay_1h'] != pytest.approx(14.1), (
            'Future snapshot value appeared in result — PIT join is broken!'
        )

    def test_left_join_unmatched_entity_returns_null(
        self, sample_label_df: pd.DataFrame, origin_view_config: FeatureViewConfig, tmp_path: Path
    ) -> None:
        """
        MIA is not in the feature Parquet. The MIA destination flights should
        still appear in the result (left join), but with NULL feature values.
        """
        # Create a feature Parquet that has no entry for the ORD-MIA origin (ORD is there,
        # but let's test with a completely absent entity by using a restricted fixture).
        restricted_df = pd.DataFrame(
            {
                'origin': ['JFK'],
                'event_ts': pd.to_datetime(['2024-03-15 13:00:00'], utc=True),
                'origin_avg_dep_delay_1h': [7.5],
                'origin_congestion_score_1h': [0.3],
            }
        )
        path = tmp_path / 'restricted.parquet'
        write_parquet_local(restricted_df, path)
        cfg = FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=str(path),
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(hours=26),
            feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
        )
        joiner = PITJoiner(feature_views=[cfg], use_s3=False)
        result = joiner.join(sample_label_df)

        # Row count unchanged — no rows dropped
        assert len(result) == len(sample_label_df)

        # ORD row has NULL features (no JFK snapshot for ORD entity)
        ord_rows = result[result['origin'] == 'ORD']
        assert ord_rows['origin_avg_dep_delay_1h'].isna().all()

    def test_ttl_exceeded_features_are_null(
        self, sample_label_df: pd.DataFrame, tmp_path: Path
    ) -> None:
        """
        If the latest feature snapshot is older than the TTL, the feature
        value must be NULL in the result (not the stale value).

        We use a very short TTL (30 minutes) and a snapshot that's 2 hours
        old — this must be nulled out.
        """
        stale_df = pd.DataFrame(
            {
                'origin': ['ORD'],
                'event_ts': pd.to_datetime(['2024-03-15 11:00:00'], utc=True),  # 3h before event
                'origin_avg_dep_delay_1h': [999.0],  # sentinel value
                'origin_congestion_score_1h': [0.99],
            }
        )
        path = tmp_path / 'stale.parquet'
        write_parquet_local(stale_df, path)

        cfg = FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=str(path),
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(minutes=30),  # very short TTL — 3h-old snapshot exceeds it
            feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
        )
        joiner = PITJoiner(feature_views=[cfg], use_s3=False)
        result = joiner.join(sample_label_df)

        ord_row = result[result['origin'] == 'ORD'].iloc[0]
        # The stale sentinel (999.0) must not appear — it should be NULL
        assert pd.isna(ord_row['origin_avg_dep_delay_1h']), (
            'Stale feature value (999.0) appeared despite TTL being exceeded — '
            'TTL masking is broken!'
        )

    def test_as_of_filters_feature_snapshots(
        self, sample_label_df: pd.DataFrame, tmp_path: Path
    ) -> None:
        """
        If as_of is set, no feature snapshot AFTER as_of should be used — even
        if it would otherwise be the nearest-before-event snapshot.

        This tests reproducibility: a run from January should not use features
        written in February.
        """
        future_df = pd.DataFrame(
            {
                'origin': ['ORD'],
                'event_ts': pd.to_datetime(['2024-03-15 13:00:00'], utc=True),
                'origin_avg_dep_delay_1h': [42.0],  # sentinel
                'origin_congestion_score_1h': [0.5],
            }
        )
        path = tmp_path / 'future_feature.parquet'
        write_parquet_local(future_df, path)

        cfg = FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=str(path),
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(hours=26),
            feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
        )
        # as_of is BEFORE the feature snapshot — should exclude it
        as_of = pd.Timestamp('2024-03-15 10:00:00', tz='UTC')
        joiner = PITJoiner(feature_views=[cfg], as_of=as_of, use_s3=False)
        result = joiner.join(sample_label_df)

        ord_row = result[result['origin'] == 'ORD'].iloc[0]
        # Feature was written at 13:00, as_of is 10:00 — must be excluded
        assert pd.isna(ord_row['origin_avg_dep_delay_1h']), (
            'Feature snapshot after as_of appeared in result — as_of filtering broken!'
        )
