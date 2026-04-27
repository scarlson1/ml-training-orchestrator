"""
Verify reproduce_run() produces byte-identical model output.

pytest marker: determinism — runs on nightly CI, not on every push.

Hardware caveat: byte equality requires the same OS and CPU architecture.
This test passes reliably within the same Docker image. x86 Linux vs Apple
Silicon may produce bit-different BLAS results — document this, don't hide it.
"""

import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import mlflow
import numpy as np
import pytest
import xgboost as xgb
from mlflow.tracking import MlflowClient
from pandas import DataFrame

from bmo.training_dataset_builder.dataset_handle import DatasetHandle

pytestmark = pytest.mark.determinism


@pytest.fixture
def tiny_dataset(tmp_path: Path) -> tuple[Path, list[str], DataFrame]:
    """
    100-row fixture. 100 rows (not 10) because XGBoost's early stopping
    needs enough data to form a meaningful validation set.
    Fixed numpy seed so the fixture is itself deterministic.
    """
    import pandas as pd

    rng = np.random.default_rng(seed=0)
    n = 100
    feature_names = ['f1', 'f2', 'f3', 'f4', 'f5']
    X = rng.standard_normal((n, 5)).astype(np.float32)
    y = (X[:, 0] + rng.standard_normal(n) > 0).astype(float)

    df = pd.DataFrame(X, columns=feature_names)
    df['is_dep_delayed'] = y
    df['event_timestamp'] = pd.date_range('2024-01-01', periods=n, freq='1h', tz='UTC')
    df['flight_id'] = [f'FL{i:04d}' for i in range(n)]
    df['origin'] = 'ORD'
    df['dest'] = 'LAX'
    df['carrier'] = 'AA'
    df['tail_number'] = 'N12345'
    df['route_key'] = 'ORD-LAX'

    parquet_path = tmp_path / 'data.parquet'
    df.to_parquet(parquet_path, index=False)
    return parquet_path, feature_names, df


@pytest.fixture
def tiny_handle(tiny_dataset: tuple[Path, list[str], DataFrame], tmp_path: Path) -> DatasetHandle:
    from bmo.training_dataset_builder.dataset_handle import (
        DatasetHandle,
        LabelDistribution,
        compute_dataset_hash,
        compute_schema_fingerprint,
    )

    parquet_path, feature_names, df = tiny_dataset
    version_hash = compute_dataset_hash(
        label_df=df,
        feature_refs=['mock_view:f1'],
        as_of=None,
        feature_set_version='test',
        code_version='test',
    )
    return DatasetHandle(
        version_hash=version_hash,
        feature_refs=['mock_view:f1'],
        feature_set_version='test',
        feature_ttls={'mock_view': 3600},
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
def local_mlflow(tmp_path_factory: pytest.TempPathFactory) -> Generator[None, Any, None]:
    mlflow_dir = tmp_path_factory.mktemp('mlflow')
    mlflow.set_tracking_uri(f'file://{mlflow_dir}')
    mlflow.end_run()  # close any accidentally open run from a prior test
    yield
    mlflow.end_run()


def test_reproduce_run_byte_equality(tiny_handle: DatasetHandle) -> None:
    from bmo.training.reproduce import reproduce_run
    from bmo.training.train import train_single_run

    result = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 3, 'learning_rate': 0.1, 'n_estimators': 50, 'seed': 42, 'nthread': 1},
        target_column='is_dep_delayed',
        mlflow_run_name='test_original',
    )

    success = reproduce_run(result.mlflow_run_id)

    assert success, (
        'reproduce_run() returned False — model bytes do not match. '
        'Check: same nthread=1, same seed, same data, same XGBoost version, same OS/arch.'
    )


def test_reproduce_fails_with_different_seed(tiny_handle: DatasetHandle) -> None:
    """Negative test: verifies the byte-equality check is not always True."""
    from bmo.training.train import train_single_run

    def get_sha(run_id: str) -> str:
        client = MlflowClient()
        with tempfile.TemporaryDirectory() as dl_dir:
            client.download_artifacts(run_id, 'model', dl_dir)
            b = xgb.Booster()
            b.load_model(str(Path(dl_dir) / 'model' / 'model.ubj'))
            return hashlib.sha256(bytes(b.save_raw(raw_format='ubj'))).hexdigest()

    r42 = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 3, 'n_estimators': 50, 'seed': 42, 'nthread': 1},
    )
    r99 = train_single_run(
        handle=tiny_handle,
        params={'max_depth': 3, 'n_estimators': 50, 'seed': 99, 'nthread': 1},
    )

    assert get_sha(r42.mlflow_run_id) != get_sha(
        r99.mlflow_run_id
    ), 'seed=42 and seed=99 produced identical bytes — seed has no effect.'
