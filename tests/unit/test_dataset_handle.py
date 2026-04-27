"""
Unit tests for DatasetHandle and hash computation.

These tests verify:
  1. The version_hash is stable (same inputs → same hash).
  2. Reordering rows in label_df does not change the hash (content-addressed).
  3. Changing any input changes the hash.
  4. compute_schema_fingerprint detects column name and dtype changes.
  5. compute_label_distributions correctly identifies binary vs. regression targets.
"""

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pytest

from bmo.training_dataset_builder.dataset_handle import (
    # DatasetHandle,
    LabelDistribution,
    compute_dataset_hash,
    compute_label_distributions,
    compute_schema_fingerprint,
)


@pytest.fixture
def base_label_df(sample_label_df: pd.DataFrame) -> pd.DataFrame:
    return sample_label_df


@pytest.fixture
def base_hash_inputs(base_label_df: pd.DataFrame) -> dict[str, Any]:
    return {
        'label_df': base_label_df,
        'feature_refs': ['origin_airport_features:origin_avg_dep_delay_1h'],
        'as_of': datetime(2024, 3, 16, 0, 0, tzinfo=timezone.utc),
        'feature_set_version': 'abc123',
        'code_version': 'def456',
    }


class TestComputeDatasetHash:
    def test_same_inputs_same_hash(self, base_hash_inputs: dict[str, Any]) -> None:
        h1 = compute_dataset_hash(**base_hash_inputs)
        h2 = compute_dataset_hash(**base_hash_inputs)
        assert h1 == h2

    def test_hash_is_64_char_hex(self, base_hash_inputs: dict[str, Any]) -> None:
        h = compute_dataset_hash(**base_hash_inputs)
        assert len(h) == 64
        assert all(c in '0123456789abcdef' for c in h)

    def test_row_order_does_not_affect_hash(
        self, base_hash_inputs: dict[str, Any], base_label_df: pd.DataFrame
    ) -> None:
        """The hash is content-addressed: shuffled rows produce the same hash."""
        shuffled = base_label_df.sample(frac=1, random_state=42).reset_index(drop=True)
        h1 = compute_dataset_hash(**base_hash_inputs)
        h2 = compute_dataset_hash(**{**base_hash_inputs, 'label_df': shuffled})
        assert h1 == h2

    def test_different_feature_refs_different_hash(self, base_hash_inputs: dict[str, Any]) -> None:
        h1 = compute_dataset_hash(**base_hash_inputs)
        h2 = compute_dataset_hash(
            **{**base_hash_inputs, 'feature_refs': ['carrier_features:carrier_avg_delay_7d']}
        )
        assert h1 != h2

    def test_different_as_of_different_hash(self, base_hash_inputs: dict[str, Any]) -> None:
        h1 = compute_dataset_hash(**base_hash_inputs)
        h2 = compute_dataset_hash(
            **{**base_hash_inputs, 'as_of': datetime(2024, 1, 1, tzinfo=timezone.utc)}
        )
        assert h1 != h2

    def test_different_label_data_different_hash(
        self, base_hash_inputs: dict[str, Any], base_label_df: pd.DataFrame
    ) -> None:
        modified = base_label_df.copy()
        modified.loc[0, 'dep_delay_min'] = 999.0  # change one label value
        h1 = compute_dataset_hash(**base_hash_inputs)
        h2 = compute_dataset_hash(**{**base_hash_inputs, 'label_df': modified})
        assert h1 != h2

    def test_none_as_of_produces_stable_hash(self, base_hash_inputs: dict[str, Any]) -> None:
        """None as_of (unbounded) should be stable, not use datetime.now()."""
        h1 = compute_dataset_hash(**{**base_hash_inputs, 'as_of': None})
        h2 = compute_dataset_hash(**{**base_hash_inputs, 'as_of': None})
        assert h1 == h2


class TestComputeSchemaFingerprint:
    def test_same_schema_same_fingerprint(self, base_label_df: pd.DataFrame) -> None:
        f1 = compute_schema_fingerprint(base_label_df)
        f2 = compute_schema_fingerprint(base_label_df.copy())
        assert f1 == f2

    def test_added_column_changes_fingerprint(self, base_label_df: pd.DataFrame) -> None:
        f1 = compute_schema_fingerprint(base_label_df)
        augmented = base_label_df.copy()
        augmented['new_feature'] = 0.0
        f2 = compute_schema_fingerprint(augmented)
        assert f1 != f2

    def test_dtype_change_changes_fingerprint(self, base_label_df: pd.DataFrame) -> None:
        f1 = compute_schema_fingerprint(base_label_df)
        cast = base_label_df.copy()
        cast['dep_delay_min'] = cast['dep_delay_min'].astype('int64')
        f2 = compute_schema_fingerprint(cast)
        assert f1 != f2


class TestComputeLabelDistributions:
    def test_binary_column_has_positive_rate(self, base_label_df: pd.DataFrame) -> None:
        dist = compute_label_distributions(base_label_df, ['is_dep_delayed'])
        assert 'is_dep_delayed' in dist
        result = dist['is_dep_delayed']
        assert isinstance(result, LabelDistribution)
        assert result.positive_rate is not None
        # 2 out of 5 flights are delayed in sample_label_df
        assert abs(result.positive_rate - 0.4) < 1e-6

    def test_continuous_column_has_no_positive_rate(self, base_label_df: pd.DataFrame) -> None:
        dist = compute_label_distributions(base_label_df, ['dep_delay_min'])
        result = dist['dep_delay_min']
        assert result.positive_rate is None
        assert result.mean == pytest.approx((5.0 + (-2.0) + 15.0 + 0.0 + 30.0) / 5)

    def test_missing_column_is_skipped(self, base_label_df: pd.DataFrame) -> None:
        dist = compute_label_distributions(base_label_df, ['nonexistent_col'])
        assert 'nonexistent_col' not in dist
