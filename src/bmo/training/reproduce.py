"""
reproduce a past MLflow training run byte-for-byte.

Usage:
    uv run python -m bmo.training.reproduce <mlflow_run_id>

What it does:
  1. Loads the MLflow run's parameters (dataset_version_hash, XGBoost params, etc.)
  2. Loads the DatasetHandle from the run's dataset_card.json artifact
  3. Re-runs train_single_run with nthread=1 and the SAME params
  4. Compares SHA-256 of the new booster bytes vs. the original artifact
  5. Exits 0 on match, 1 on mismatch

Why nthread=1?
  With multiple threads, XGBoost uses parallel floating-point reductions whose
  order is non-deterministic (IEEE 754 is NOT associative for parallel sums).
  nthread=1 forces serial execution, guaranteeing identical floating-point ops.

  Hardware caveat: byte equality also requires the same OS and CPU architecture.
  x86 Linux and Apple Silicon can produce bit-different BLAS results. Run this
  test in the same Docker image used for training.
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

import structlog
import xgboost as xgb
from mlflow.tracking import MlflowClient

from bmo.training.train import train_single_run
from bmo.training_dataset_builder.dataset_handle import DatasetHandle

log = structlog.get_logger(__name__)


def reproduce_run(run_id: str) -> bool:
    """
    Reproduce a past training run and assert byte-equality.

    Returns:
        True if model bytes match, False otherwise.
    """
    client = MlflowClient()

    log.info('loading original run', run_id=run_id)
    original_run = client.get_run(run_id)
    params = original_run.data.params

    with tempfile.TemporaryDirectory() as tmpdir:
        client.download_artifacts(run_id, 'model', tmpdir)
        original_booster = xgb.Booster()
        original_booster.load_model(str(Path(tmpdir) / 'model' / 'model.ubj'))
        original_bytes = _booster_to_bytes(original_booster)

        client.download_artifacts(run_id, 'dataset_card.json', tmpdir)
        card_path = Path(tmpdir) / 'dataset_card.json'
        handle = DatasetHandle.model_validate_json(card_path.read_text())

    original_sha = hashlib.sha256(original_bytes).hexdigest()
    log.info(
        'loaded original run',
        dataset_hash=handle.version_hash[:12],
        original_model_sha=original_sha[:12],
    )

    xgb_params = {
        k.removeprefix('xgb_'): _coerce_param(v) for k, v in params.items() if k.startswith('xgb_')
    }
    xgb_params['nthread'] = 1

    target_column = params.get('target_column', 'is_dep_delayed')

    log.info('re-running training with nthread=1')
    reproduced = train_single_run(
        handle=handle,
        params=xgb_params,
        target_column=target_column,
        mlflow_run_name=f'reproduce_{run_id[:8]}',
        nthread=1,
    )

    with tempfile.TemporaryDirectory() as tmpdir2:
        client.download_artifacts(reproduced.mlflow_run_id, 'model', tmpdir2)
        new_booster = xgb.Booster()
        new_booster.load_model(str(Path(tmpdir2) / 'model' / 'model.ubj'))
        new_bytes = _booster_to_bytes(new_booster)

    new_sha = hashlib.sha256(new_bytes).hexdigest()

    if original_sha == new_sha:
        log.info('REPRODUCIBLE: model bytes match', sha=original_sha[:16])
        return True
    else:
        log.error(
            'NOT REPRODUCIBLE: model bytes differ', original=original_sha[:16], new=new_sha[:16]
        )
        log.error(
            'Common causes: different hardware (x86 vs ARM), different OS. '
            'Run reproduction in the same Docker image as training'
        )
        return False


def _booster_to_bytes(booster: xgb.Booster) -> bytes:
    return bytes(booster.save_raw(raw_format='ubj'))


def _coerce_param(value: str) -> int | float | str:
    """MLflow stores all params as strings; coerce numeric types"""
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def main() -> None:
    if len(sys.argv) != 2:
        print('Usage: python -m bmo.training.reproduce <run_id>', file=sys.stderr)
        sys.exit(1)
    success = reproduce_run(sys.argv[1])
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
