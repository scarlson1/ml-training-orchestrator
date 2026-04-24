"""
Integration test: planted future value.

This test deliberately introduces data leakage and asserts it is caught.
It is the proof that this pipeline handles the hardest ML data engineering problem.

There are two failure modes this test covers:

  Mode A — Structural: The ASOF JOIN should NEVER return a future feature.
    Test: inject a feature with event_ts > event_timestamp. Assert the joined
    dataset does NOT contain that value. If it does, the PIT join is broken.

  Mode B — Guard: Even if Mode A somehow fails (e.g., wrong join logic),
    the leakage guard must catch it.
    Test: manually construct a dataset with a future feature_ts column.
    Assert run_all_guards() returns passed=False with an 'error' severity violation.

Both modes are tested because defense in depth is the right approach for
a critical safety check. The structural guarantee (ASOF JOIN) and the
explicit check (guard) are independent layers.
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bmo.training_dataset_builder.leakage_guards import run_all_guards
from bmo.training_dataset_builder.pit_join import FeatureViewConfig, PITJoiner

pytestmark = pytest.mark.integration


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


@pytest.fixture
def label_df() -> pd.DataFrame:
    """Single flight departing at 14:00 UTC from ORD."""
    return pd.DataFrame(
        {
            'flight_id': ['f001'],
            'event_timestamp': pd.to_datetime(['2024-03-15 14:00:00'], utc=True),
            'origin': ['ORD'],
            'dest': ['JFK'],
            'carrier': ['DL'],
            'tail_number': ['N12345'],
            'route_key': ['ORD-JFK'],
            'dep_delay_min': [5.0],
            'is_dep_delayed': [False],
        }
    )


@pytest.fixture
def feature_with_future_snapshot(tmp_path: Path) -> tuple[str, float]:
    """
    Feature Parquet containing two snapshots for ORD:
      - PAST snapshot  (13:00, delay=9.8)  — valid, should be selected
      - FUTURE snapshot (14:30, delay=999.0) — invalid, must be EXCLUDED

    The 999.0 value is a sentinel: if it appears in the joined dataset,
    the PIT join has a leakage bug and the test fails.
    """
    df = pd.DataFrame(
        {
            'origin': ['ORD', 'ORD'],
            'event_ts': pd.to_datetime(
                [
                    '2024-03-15 13:00:00',  # 1h BEFORE event — valid
                    '2024-03-15 14:30:00',  # 30min AFTER event — must be excluded
                ],
                utc=True,
            ),
            'origin_avg_dep_delay_1h': [9.8, 999.0],  # 999 is the leakage sentinel
            'origin_congestion_score_1h': [0.6, 0.99],
        }
    )
    path = tmp_path / 'origin_with_future.parquet'
    write_parquet(df, path)
    return str(path), 999.0  # path + sentinel value


class TestPITJoinExcludesFutureSnapshot:
    """Mode A: structural test — ASOF JOIN must not return future values."""

    def test_future_snapshot_not_in_joined_result(
        self, label_df: pd.DataFrame, feature_with_future_snapshot: tuple[str, float]
    ) -> None:
        parquet_path, sentinel = feature_with_future_snapshot
        cfg = FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=parquet_path,
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(hours=26),
            feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
        )
        joiner = PITJoiner(feature_views=[cfg], use_s3=False)
        result = joiner.join(label_df)

        assert len(result) == 1  # no rows dropped
        delay_value = result['origin_avg_dep_delay_1h'].iloc[0]

        assert delay_value != pytest.approx(sentinel), (
            f'FUTURE feature value ({sentinel}) appeared in joined result! '
            f'Got: {delay_value}. The ASOF JOIN has a leakage bug. '
            f'This means the model would be trained on a feature value that '
            f"didn't exist at the time of the flight's scheduled departure."
        )
        # Specifically: the correct value (9.8) should appear
        assert delay_value == pytest.approx(
            9.8
        ), f'Expected past snapshot value (9.8) in result, got: {delay_value}'

    def test_correct_past_snapshot_selected(
        self, label_df: pd.DataFrame, feature_with_future_snapshot: tuple[str, float]
    ) -> None:
        """Positive test: confirms the CORRECT value is selected, not just that the
        wrong value is absent. A join that returns NULL for all rows would pass
        the exclusion test but fail this one."""
        parquet_path, _ = feature_with_future_snapshot
        cfg = FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=parquet_path,
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(hours=26),
            feature_cols=['origin_avg_dep_delay_1h', 'origin_congestion_score_1h'],
        )
        joiner = PITJoiner(feature_views=[cfg], use_s3=False)
        result = joiner.join(label_df)

        assert result['origin_avg_dep_delay_1h'].iloc[0] == pytest.approx(9.8)
        assert result['origin_congestion_score_1h'].iloc[0] == pytest.approx(0.6)


class TestLeakageGuardCatchesFutureFeatureTs:
    """Mode B: guard test — if a future value somehow slips through, the guard catches it."""

    def test_guard_fails_on_future_feature_ts(self, label_df: pd.DataFrame) -> None:
        """
        Manually construct a dataset where the feature_ts column shows a future
        timestamp. This simulates a bug where the ASOF JOIN returned a future value.
        Assert that run_all_guards() returns passed=False.
        """
        # Simulate the JOINED result with a future feature timestamp
        dataset_with_leakage = label_df.copy()
        dataset_with_leakage['origin_avg_dep_delay_1h'] = 999.0
        # feature_ts AFTER event_timestamp = leakage
        dataset_with_leakage['origin_airport_features__feature_ts'] = pd.to_datetime(
            ['2024-03-15 14:30:00'], utc=True
        )

        result = run_all_guards(
            label_df=label_df,
            dataset_df=dataset_with_leakage,
            feature_refs=['origin_airport_features:origin_avg_dep_delay_1h'],
            label_columns=['dep_delay_min', 'is_dep_delayed'],
            feature_ts_columns=['origin_airport_features__feature_ts'],
            ttl_seconds={'origin_airport_features': 93600},  # 26h
            as_of=None,
        )

        assert (
            not result.passed
        ), 'Leakage guard should have FAILED — a future feature timestamp was present'
        error_checks = {v.check_name for v in result.errors}
        assert (
            'no_future_features' in error_checks
        ), f'Expected "no_future_features" error, got: {result.errors}'

    def test_guard_passes_on_valid_feature_ts(self, label_df: pd.DataFrame) -> None:
        """Confirm the guard PASSES when feature timestamps are all before event_timestamp."""
        dataset_valid = label_df.copy()
        dataset_valid['origin_avg_dep_delay_1h'] = 9.8
        dataset_valid['origin_airport_features__feature_ts'] = pd.to_datetime(
            ['2024-03-15 13:00:00'],
            utc=True,  # 1h BEFORE event
        )

        result = run_all_guards(
            label_df=label_df,
            dataset_df=dataset_valid,
            feature_refs=['origin_airport_features:origin_avg_dep_delay_1h'],
            label_columns=['dep_delay_min', 'is_dep_delayed'],
            feature_ts_columns=['origin_airport_features__feature_ts'],
            ttl_seconds={'origin_airport_features': 93600},
            as_of=None,
        )

        assert result.passed
        # There should be a warning about as_of=None, but no errors
        assert len(result.errors) == 0


class TestTargetLeakageGuard:
    """Test that guard_no_target_leakage catches label columns in feature_refs."""

    def test_direct_overlap_raises_error(self, label_df: pd.DataFrame) -> None:
        """dep_delay_min is a label — it must not appear in feature_refs."""
        dataset_df = label_df.copy()
        dataset_df['dep_delay_min_feature'] = 5.0

        result = run_all_guards(
            label_df=label_df,
            dataset_df=dataset_df,
            # dep_delay_min is in both feature_refs AND label_columns
            feature_refs=['origin_airport_features:dep_delay_min'],
            label_columns=['dep_delay_min'],
            feature_ts_columns=[],
            ttl_seconds={},
            as_of=None,
        )

        assert not result.passed
        assert any(
            v.check_name == 'no_target_leakage' and v.severity == 'error' for v in result.violations
        )
