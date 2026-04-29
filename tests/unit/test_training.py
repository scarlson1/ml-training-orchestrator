"""
Unit tests for bmo.training.train.

Fast tests (no S3, no real MLflow server). Uses a local file:// MLflow
tracking directory and small in-memory DataFrames.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import mlflow
import numpy as np
import pandas as pd
import pytest
from mlflow.tracking import MlflowClient

from bmo.training_dataset_builder.dataset_handle import (
    DatasetHandle,
    LabelDistribution,
    compute_dataset_hash,
    compute_schema_fingerprint,
)


def _make_tiny_df(n: int = 120, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            'f1': rng.standard_normal(n).astype(np.float32),
            'f2': rng.standard_normal(n).astype(np.float32),
            'f3': rng.standard_normal(n).astype(np.float32),
            'is_dep_delayed': (rng.standard_normal(n) > 0).astype(float),
            'dep_delay_min': rng.uniform(-10, 120, n).astype(np.float32),
            'arr_delay_min': rng.uniform(-10, 120, n).astype(np.float32),
            'is_arr_delayed': (rng.standard_normal(n) > 0).astype(float),
            'cancelled': np.zeros(n, dtype=float),
            'diverted': np.zeros(n, dtype=float),
            'event_timestamp': pd.date_range('2024-01-01', periods=n, freq='1h', tz='UTC'),
            'flight_id': [f'FL{i:04d}' for i in range(n)],
            'origin': 'ORD',
            'dest': 'LAX',
            'carrier': 'AA',
            'tail_number': 'N12345',
            'route_key': 'ORD-LAX',
        }
    )


@pytest.fixture
def tiny_handle(tmp_path: Path) -> DatasetHandle:
    df = _make_tiny_df()
    parquet_path = tmp_path / 'data.parquet'
    df.to_parquet(parquet_path, index=False)
    version_hash = compute_dataset_hash(
        label_df=df,
        feature_refs=['view:f1'],
        as_of=None,
        feature_set_version='test',
        code_version='test',
    )
    return DatasetHandle(
        version_hash=version_hash,
        feature_refs=['view:f1'],
        feature_set_version='test',
        feature_ttls={'view': 3600},
        as_of=None,
        row_count=len(df),
        label_distribution={
            'is_dep_delayed': LabelDistribution(
                target_column='is_dep_delayed',
                mean=float(df['is_dep_delayed'].mean()),
                std=float(df['is_dep_delayed'].std()),
                min=0.0,
                max=1.0,
                positive_rate=float(df['is_dep_delayed'].mean()),
            )
        },
        schema_fingerprint=compute_schema_fingerprint(df),
        created_at=datetime.now(timezone.utc),
        storage_path=str(parquet_path),
    )


@pytest.fixture(autouse=True)
def local_mlflow(tmp_path: Path) -> Generator[None, Any, None]:
    mlflow.set_tracking_uri(f'file://{tmp_path / "mlflow"}')
    mlflow.end_run()
    yield
    mlflow.end_run()


def test_train_single_run_returns_result(tiny_handle: DatasetHandle) -> None:
    from bmo.training.train import TrainingResult, train_single_run

    result = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 2, 'n_estimators': 10, 'seed': 42, 'nthread': 1},
        target_column='is_dep_delayed',
    )

    assert isinstance(result, TrainingResult)
    assert result.mlflow_run_id != ''
    assert 0.0 <= result.metrics['test_roc_auc'] <= 1.0
    assert result.dataset_version_hash == tiny_handle.version_hash
    assert result.best_iteration > 0


def test_train_logs_to_mlflow(tiny_handle: DatasetHandle) -> None:
    from bmo.training.train import train_single_run

    result = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 2, 'n_estimators': 10, 'seed': 42, 'nthread': 1},
    )

    client = MlflowClient()
    run = client.get_run(result.mlflow_run_id)

    assert run.data.params['dataset_version_hash'] == tiny_handle.version_hash
    assert run.data.params['target_column'] == 'is_dep_delayed'
    assert 'xgb_max_depth' in run.data.params
    assert 'test_roc_auc' in run.data.metrics
    assert 'test_pr_auc' in run.data.metrics
    assert 'test_log_loss' in run.data.metrics
    assert 'test_brier_score' in run.data.metrics

    artifacts = [a.path for a in client.list_artifacts(result.mlflow_run_id)]
    assert 'dataset_card.json' in artifacts
    # MLflow 3.x stores named models outside the flat artifacts list; verify via URI instead
    import mlflow.xgboost

    assert mlflow.xgboost.load_model(result.model_uri) is not None


def test_feature_importance_normalized(tiny_handle: DatasetHandle) -> None:
    from bmo.training.train import train_single_run

    result = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 2, 'n_estimators': 10, 'seed': 42, 'nthread': 1},
    )

    total = sum(result.feature_importance.values())
    assert abs(total - 1.0) < 1e-5, f'importance sum={total:.6f}, expected ~1.0'
    assert all(v >= 0 for v in result.feature_importance.values())


def test_time_split_is_chronological() -> None:
    from bmo.training.train import _get_feature_columns, _time_split

    df = _make_tiny_df(n=200)
    feature_cols = _get_feature_columns(df)
    X_train, X_val, X_test, y_train, y_val, y_test = _time_split(df, feature_cols, 'is_dep_delayed')

    assert len(X_train) < len(df)
    assert len(X_test) > 0

    n = len(df)
    test_start_idx = int(n * 0.8)
    assert len(X_test) == n - test_start_idx


def test_train_with_missing_target_raises(tiny_handle: DatasetHandle) -> None:
    from bmo.training.train import train_single_run

    with pytest.raises(KeyError):
        train_single_run(
            handle=tiny_handle,
            target_column='nonexistent_column',
            params={'n_estimators': 5, 'nthread': 1},
        )
