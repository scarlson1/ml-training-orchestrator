"""
train_single_run() — one full training execution.

Responsible for:
  1. Loading the dataset Parquet from the DatasetHandle storage path.
  2. Performing a time-based train/val/test split.
  3. Fitting XGBoost via models/xgboost_model.py.
  4. Logging everything to an MLflow run.
  5. Returning TrainingResult (immutable Pydantic model).

Called both by run_hpo() (50+ times per sweep, with Optuna pruning callbacks)
and directly from the Dagster trained_model asset.

# MLflow Tracking docs: https://mlflow.org/docs/latest/tracking.html
# Mlflow xgboost: https://mlflow.org/docs/latest/python_api/mlflow.xgboost.html
"""

from __future__ import annotations

import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any, cast

import matplotlib
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import structlog
from mlflow.xgboost import log_model as log_xgboost_model
from pydantic import BaseModel
from sklearn.calibration import CalibrationDisplay
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix

from bmo.common.config import settings
from bmo.training.models.xgboost_model import DEFAULT_PARAMS, XGBFitResult, fit_xgboost
from bmo.training_dataset_builder.dataset_handle import DatasetHandle

# support plots in headless env (CI, docker)
matplotlib.use('Agg')

log = structlog.get_logger(__name__)

DEFAULT_TARGET_COLUMN = 'is_dep_delayed'

# exclusion list for feature selection - remove columns from training (all other numeric columns are features)
_METADATA_COLUMNS = {
    'flight_id',
    'event_timestamp',
    'origin',
    'dest',
    'carrier',
    'tail_number',
    'route_key',
    'dep_delay_min',
    'arr_delay_min',
    'is_dep_delayed',
    'is_arr_delayed',
    'cancelled',
    'diverted',
}

_TEST_FRACTION = 0.2
_VAL_FRACTION = 0.15  # fraction of the training portion used for early stopping

MLFLOW_EXPERIMENT = 'bmo/flight_delay'


class TrainingResult(BaseModel):
    """Immutable summary of a completed training run"""

    mlflow_run_id: str
    model_uri: str
    metrics: dict[str, float]
    feature_importance: dict[str, float]
    params: dict[str, Any]
    dataset_version_hash: str
    feature_set_version: str
    git_sha: str
    best_iteration: int
    target_column: str
    train_rows: int
    test_rows: int
    trained_at: datetime


def train_single_run(
    handle: DatasetHandle,  # storage_path, as_of, version_hash, etc.
    params: dict[str, Any] | None = None,  # XGBoost overrides - missing keys use DEFAULT_PARAMS
    target_column: str = DEFAULT_TARGET_COLUMN,  # 'is_dep_delayed'
    mlflow_run_name: str | None = None,
    parent_run_id: str | None = None,  # set by Optuna for nested MLflow runs
    nthread: int = -1,  # -1 = all available CPUs; pass 1 for reproduce mode
    callbacks: list[Any] | None = None,  # XGBoostPruningCallback from Optuna
) -> TrainingResult:
    """
    Execute one XGBoost training run and log everything to MLflow.

    Args:
        handle:          DatasetHandle point-in-time training dataset with identity hash
        params:          XGBoost hyperparameters. Missing keys use DEFAULT_PARAMS.
        target_column:   Which label column to use as y.
        mlflow_run_name: Human-readable name in the MLflow UI.
        parent_run_id:   If set, logged as a nested child run. Optuna passes this
                         for each of the 50 trial runs.
        nthread:         XGBoost thread count. Pass nthread=1 in reproduce mode
                         to guarantee byte-for-byte determinism.
        callbacks:       XGBoost callbacks, forwarded to fit_xgboost. Optuna
                         passes XGBoostPruningCallback here to enable mid-trial
                         pruning. Without this, the MedianPruner can only prune
                         between complete trials, not mid-training.
    """
    merged_params = {**DEFAULT_PARAMS, **(params or {}), 'nthread': nthread}

    df = _load_dataset(handle.storage_path)
    feature_columns = _get_feature_columns(df)
    # split into training, validation, test (by time so later data isn't leaked into training dataset)
    X_train, X_val, X_test, y_train, y_val, y_test = _time_split(df, feature_columns, target_column)

    # scale_pos_weight balances the loss for imbalanced binary targets
    # count(negative) / count(positive): 70%  on time / 30% delayed = 2.33
    # up-weights the minority class (delayed flights) in the loss function (account for on time more common than delayed)
    if 'scale_pos_weight' not in (params or {}):
        neg = float((y_train == 0).sum())
        pos = float((y_train == 1).sum())
        merged_params['scale_pos_weight'] = neg / max(pos, 1.0)

    git_sha = _get_git_sha()
    run_name = mlflow_run_name or f'xgb_{handle.version_hash[:8]}_{target_column}'

    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    run_kwargs: dict[str, Any] = {'run_name': run_name}
    if parent_run_id:
        run_kwargs['nested'] = True

    training_result: TrainingResult | None = None
    with mlflow.start_run(**run_kwargs) as run:
        _log_provenance(handle, merged_params, git_sha, target_column)

        fit_result = fit_xgboost(
            X_train=X_train,
            y_train=y_train,
            X_val=X_val,
            y_val=y_val,
            X_test=X_test,
            y_test=y_test,
            feature_names=feature_columns,
            params=merged_params,
            callbacks=callbacks,
        )

        mlflow.log_metrics(fit_result.metrics)
        mlflow.log_metric('best_iteration', fit_result.best_iteration)
        mlflow.log_metric('train_rows', len(X_train))
        mlflow.log_metric('test_rows', len(X_test))

        # plot functions use pre-computed predictions from XGBFitResult - no redundant re-prediction passes needed
        _log_feature_importance(fit_result)
        _log_confusion_matrix(fit_result, y_test)
        _log_calibration_plot(fit_result, y_test)

        log_xgboost_model(fit_result.booster, 'model')
        mlflow.log_dict(handle.model_dump(mode='json'), 'dataset_card.json')

        model_uri = f'runs:/{run.info.run_id}/model'
        log.info(
            'training run complete',
            run_id=run.info.run_id,
            auc=fit_result.metrics['test_roc_auc'],
            best_iter=fit_result.best_iteration,
        )

        training_result = TrainingResult(
            mlflow_run_id=run.info.run_id,
            model_uri=model_uri,
            metrics=fit_result.metrics,
            feature_importance=fit_result.feature_importance,
            params=merged_params,
            dataset_version_hash=handle.version_hash,
            feature_set_version=handle.feature_set_version,
            git_sha=git_sha,
            best_iteration=fit_result.best_iteration,
            target_column=target_column,
            train_rows=len(X_train),
            test_rows=len(X_test),
            trained_at=datetime.now(timezone.utc),
        )

    assert training_result is not None
    return training_result


# ----- helpers ----- #


def _load_dataset(storage_path: str) -> pd.DataFrame:
    """
    Reads Parquet from S3 via s3fs (which wraps boto3 with a filesystem-like interface) or from local path. Uses PyArrow's pq.read_table then .to_pandas(). PyArrow columnar format maps directly to Parquet's physical layout.

    input:  's3://staging/datasets/abc123.../data.parquet'
    output: pd.DataFrame, shape ~(1_000_000, 60)
             columns: flight_id, event_timestamp, origin, dest, carrier,
                      feat_cascading_delay, origin_avg_dep_delay_1h, ..., is_dep_delayed
    """
    if storage_path.startswith('s3://'):
        fs = s3fs.S3FileSystem(
            key=settings.s3_access_key_id,
            secret=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
        )
        df: pd.DataFrame | None = None
        with fs.open(storage_path, 'rb') as f:
            df = pq.read_table(f).to_pandas()
        assert df is not None
        return df
    return cast(pd.DataFrame, pq.read_table(storage_path).to_pandas())


def _get_feature_columns(df: pd.DataFrame) -> list[str]:
    """Remove metadata columns (flight_id, dep_delay_min, is_dep_delayed, cancelled, etc), then filter to only numeric columns"""
    return [
        col
        for col in df.columns
        if col not in _METADATA_COLUMNS and pd.api.types.is_numeric_dtype(df[col])
    ]


def _time_split(
    df: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Chronological split: train 68% / val 12% / test 20%.

    WHY NOT RANDOM: Flight features include windowed aggregates like
    origin_avg_dep_delay_7d. A random split puts October flights in the test
    set while September flights are in training — the 7-day aggregate features
    on those September flights embed some October flight data, causing leakage
    through the feature pipeline. A time split avoids this entirely.

    val is used only for XGBoost early stopping — never for reported metrics.

    X_train/y_train:    fitting model weights
    X_val/y_val:        XGBoost early stopping - stops adding trees when val less stops improving
    X_test/y_test:      evaluation - test_roc_auc, test_f1, etc. model never sees these during training

    input:  df (1_000_000 rows), feature_columns (list of ~40 strings), 'is_dep_delayed'
    output: tuple of 6 np.ndarray

    With 1M rows and _TEST_FRACTION=0.2, _VAL_FRACTION=0.15:
      test_start  = 1_000_000 * 0.8 = 800_000
      val_start   = 800_000 * 0.85  = 680_000
          X_train shape: (680_000, 40)  dtype: float64
      X_val shape:   (120_000, 40)  dtype: float64
      X_test shape:  (200_000, 40)  dtype: float64
      y_train shape: (680_000,)     dtype: float64
      y_val shape:   (120_000,)     dtype: float64
      y_test shape:  (200_000,)     dtype: float64

    Example row:
    col index  feature name                     value
    ─────────────────────────────────────────────────
    0          origin_avg_dep_delay_1h          4.2
    1          origin_avg_dep_delay_24h         2.1
    2          carrier_on_time_pct_7d           0.78
    3          route_avg_dep_delay_30d          6.1
    4          feat_cascading_delay             12.0   ← PySpark window feature
    5          hour_of_day                      7.0
    6          day_of_week                      1.0
    ...
    39         weather_wind_speed_origin        14.3
    """
    df_sorted = df.sort_values('event_timestamp').reset_index(drop=True)
    n = len(df_sorted)
    test_start = int(n * (1 - _TEST_FRACTION))
    val_start = int(test_start * (1 - _VAL_FRACTION))

    train_df = df_sorted.iloc[:val_start]
    val_df = df_sorted.iloc[val_start:test_start]
    test_df = df_sorted.iloc[test_start:]

    X_train = train_df[feature_columns].fillna(0).values  # fillna(0) for null values
    X_val = val_df[feature_columns].fillna(0).values
    X_test = test_df[feature_columns].fillna(0).values
    y_train = train_df[target_column].to_numpy(dtype=float)
    y_val = val_df[target_column].to_numpy(dtype=float)
    y_test = test_df[target_column].to_numpy(dtype=float)

    return X_train, X_val, X_test, y_train, y_val, y_test


def _log_provenance(
    handle: DatasetHandle,
    params: dict[str, Any],
    git_sha: str,
    target_column: str,
) -> None:
    """
    Logs everything needed to reproduce the run as MLflow params (immutable key-value strings stored once at run start)
    """
    mlflow.log_params(
        {
            'dataset_version_hash': handle.version_hash,
            'dataset_row_count': handle.row_count,
            'dataset_feature_count': len(handle.feature_refs),
            'feature_set_version': handle.feature_set_version,
            'git_sha': git_sha,
            'target_column': target_column,
            **{f'xgb_{k}': v for k, v in params.items()},
        }
    )


def _log_feature_importance(fit_result: XGBFitResult) -> None:
    mlflow.log_dict(fit_result.feature_importance, 'feature_importance.json')

    sorted_imp = sorted(fit_result.feature_importance.items(), key=lambda x: x[1], reverse=True)
    top_n = sorted_imp[:20]
    if not top_n:
        return

    names = [item[0] for item in top_n]
    values = [item[1] for item in top_n]
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(names[::-1], values[::-1])
    ax.set_xlabel('Normalized Gain')
    ax.set_title('XGBoost Feature Importance (top 20, normalized gain)')
    ax.tight_layout()

    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
        fig.savefig(f.name, dpi=150, bbox_inches='tight')
        # → stored as: runs:/{run_id}/plots/tmp_abc123.png  (uses the tmpfile name)
        mlflow.log_artifact(f.name, 'plots')
    plt.close(fig)


def _log_confusion_matrix(fit_result: XGBFitResult, y_test: np.ndarray) -> None:
    # use pre-computed predictions - no re-prediction pass needed
    cm = confusion_matrix(y_test, fit_result.y_pred_test)
    fig, ax = plt.subplots(figsize=(6, 5))
    ConfusionMatrixDisplay(cm, display_labels=['On-time', 'Delayed']).plot(ax=ax)
    ax.set_title('Confusion Matrix - Test Set')

    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
        fig.savefig(f.name, dpi=150, bbox_inches='tight')
        mlflow.log_artifact(f.name, 'plots')
    plt.close(fig)


def _log_calibration_plot(fit_result: XGBFitResult, y_test: np.ndarray) -> None:
    """
    Reliability diagram. A well-calibrated model: when it predicts P(delay)=0.7,
    ~70% of those flights should actually be delayed. Poor calibration matters
    when choosing a probability threshold for alerting.
    """
    fig, ax = plt.subplots(figsize=(7, 6))
    # use pre-computed probs (fit_result.y_proba_test computed in fix_xgboost and stored on XGBFitResult)
    CalibrationDisplay.from_predictions(
        y_test, fit_result.y_proba_test, n_bins=10, ax=ax, name='XGBoost'
    )
    ax.set_title('Calibration Curve - Test Set')

    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
        fig.savefig(f.name, dpi=150, bbox_inches='tight')
        mlflow.log_artifact(f.name, 'plots/calibration_curve.png')
    plt.close(fig)


def _get_git_sha() -> str:
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        return 'unknown'
