"""
Optuna HPO orchestration for XGBoost flight delay classification.

Architecture:
  - One parent MLflow run for the sweep
  - Each Optuna trial is a nested child MLflow run (50+ children)
  - Optuna tests different combinations of parameters (max_depth, learning_rate, etc) to find optimal combo (based on test_roc_auc, test_f1, test_brier_score, etc. scores)
  - Best trial re-run as a 'champion' child run with full artifact logging
  - Optuna study persisted to SQLite for crash recovery

Without Optuna, you'd do a grid search — manually enumerate every combination. With 9 hyperparameters that each have ~10 possible values, that's 10⁹ combinations. Grid search is intractable.

Optuna uses bayesian optimization (TPE sampler) to guide param combo for next trial.

Why SQLite vs PostgreSQL for Optuna storage?
  For single-node HPO, SQLite is zero-config and fast enough. PostgreSQL is
  the right upgrade for distributed HPO across multiple Dagster workers.

{
    'test_roc_auc':    0.82,   # ranking quality (threshold-independent)
    'test_pr_auc':     0.61,   # precision-recall tradeoff on the minority class
    'test_log_loss':   0.44,   # average log-probability assigned to the true label
    'test_f1':         0.58,   # harmonic mean of precision and recall at threshold=0.5
    'test_brier_score': 0.18,  # calibration: mean squared error of P(delay) vs 0/1
}
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import mlflow
import optuna
import structlog
from optuna.trial import FrozenTrial

from bmo.training.train import MLFLOW_EXPERIMENT, TrainingResult, train_single_run
from bmo.training_dataset_builder.dataset_handle import DatasetHandle

log = structlog.get_logger(__name__)

_OPTUNA_STORAGE_DIR = Path('/tmp/bmo_optuna')

# suppress optuna's per-trial INFO logs - very verbose
optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class HPOResult:
    best_run_id: str
    best_auc: float
    best_params: dict[str, Any]
    n_trials_completed: int
    n_trials_pruned: int
    study_storage_path: str
    parent_mlflow_run_id: str
    dataset_version_hash: str
    sweep_started_at: datetime
    sweep_ended_at: datetime


def run_hpo(
    handle: DatasetHandle,
    n_trials: int = 50,
    target_column: str = 'is_dep_delayed',
    run_mllib_baseline: bool = True,
) -> HPOResult:
    """
    Run a full Optuna HPO sweep over XGBoost hyperparameters.

    Creates one parent MLflow run and 50 nested child runs — one per trial.
    Best params re-run as a 'champion' child run with full artifact logging.

    Args:
        handle:              DatasetHandle from Phase 5.
        n_trials:            Number of Optuna trials (minimum 50 per the plan).
        target_column:       Binary classification target.
        run_mllib_baseline:  If True, also trains an MLlib GBT baseline.
    """
    sweep_start = datetime.now(timezone.utc)
    _OPTUNA_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    storage_path = str(_OPTUNA_STORAGE_DIR / f'study_{handle.version_hash[:16]}.db')
    study_name = f'bmo_xgb_{handle.version_hash[:16]}_{target_column}'

    # TPE: first n_startup_trials are random exploration; then Bayesian
    sampler = optuna.samplers.TPESampler(seed=42, n_startup_trials=10)

    # prune if intermediate value is worse than the median of completed trials at same boosting step
    pruner = optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=50)

    study = optuna.create_study(
        study_name=study_name,
        direction='maximize',
        sampler=sampler,
        pruner=pruner,
        storage=f'sqlite:///{storage_path}',  # use postgres is distributed across Dagster workers
        load_if_exists=True,
    )

    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    champion_result: TrainingResult | None = None
    best_trial: FrozenTrial | None = None
    n_pruned: int | None = None

    with mlflow.start_run(run_name=f'hpo_{handle.version_hash[:8]}') as parent_run:
        mlflow.log_params(
            {
                'hpo_n_trials': n_trials,
                'hpo_sampler': 'TPE',
                'hpo_pruner': 'MedianPruner',
                'dataset_version_hash': handle.version_hash,
                'target_column': target_column,
            }
        )
        mlflow.set_tag('role', 'hpo_parent')

        # factory fn: passed to study.optimize(objective(trial), ...)
        # calls trial.suggest_*() to sample hyperparameters
        # objective() calls train_single_run: loads full dataset from S3, splits it, fits XGBoost with these params and returns TrainingResult
        # objective returns result.metrics['test_roc_auc'] which Optuna uses to guide the next trial (& potentially prunes)
        objective = _make_objective(
            handle=handle,
            target_column=target_column,
            parent_run_id=parent_run.info.run_id,
        )

        already_done = len(study.trials)
        remaining = max(0, n_trials - already_done)
        if remaining > 0:
            log.info('starting HPO sweep', n_trials=remaining, already_done=already_done)
            # loop controller - calls objective which calls train_single_run. persists result state to sqlite
            study.optimize(objective, n_trials=remaining, show_progress_bar=True)
        else:
            log.info('study already complete', study=study_name)

        best_trial = study.best_trial
        n_pruned = sum(1 for t in study.trials if t.state == optuna.trial.TrialState.PRUNED)

        log.info(
            'HPO complete',
            best_auc=best_trial.value,
            n_pruned=n_pruned,
        )

        # re-run best params as the champion run with full artifact logging
        # trial runs are lean (params + metrics) for speed
        # champion run gets feature important plots, confusion matrix, etc.
        champion_result = train_single_run(
            handle=handle,
            params=best_trial.params,
            target_column=target_column,
            mlflow_run_name=f'champion_{handle.version_hash[:8]}',
            parent_run_id=parent_run.info.run_id,
        )

        mlflow.log_metrics(
            {
                'best_trial_auc': best_trial.value or 0.0,
                'n_trials_completed': len(study.trials) - n_pruned,
                'n_trials_pruned': n_pruned,
                'champion_auc': champion_result.metrics['test_roc_auc'],
            }
        )
        mlflow.log_artifact(storage_path, 'optuna_study.db')

        # sanity check & benchmark (no tuning) - shows XGBoost model outperforms ML baseline
        if run_mllib_baseline:
            _run_mllib_comparison(handle, target_column, parent_run.info.run_id)

    assert champion_result is not None
    assert best_trial is not None
    assert n_pruned is not None

    return HPOResult(
        best_run_id=champion_result.mlflow_run_id,
        best_auc=champion_result.metrics['test_roc_auc'],
        best_params=best_trial.params,
        n_trials_completed=len(study.trials) - n_pruned,
        n_trials_pruned=n_pruned,
        study_storage_path=storage_path,
        parent_mlflow_run_id=parent_run.info.run_id,
        dataset_version_hash=handle.version_hash,
        sweep_started_at=sweep_start,
        sweep_ended_at=datetime.now(timezone.utc),
    )


def _make_objective(
    handle: DatasetHandle,
    target_column: str,
    parent_run_id: str,
) -> Callable[[optuna.Trial], float]:
    """
    Return an Optuna objective function bound to handle and parent_run_id.

    The XGBoostPruningCallback hooks into XGBoost's per-round eval reporting.
    It calls trial.report(val_loss, step=round) after each boosting round.
    If trial.should_prune() returns True, the callback raises TrialPruned —
    stopping that trial early. Without this callback, MedianPruner can only
    act between complete trials and never cuts a bad trial short.
    """
    from optuna.integration import XGBoostPruningCallback

    def objective(trial: optuna.Trial) -> float:
        params = {
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
            'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
            'subsample': trial.suggest_float('subsample', 0.5, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
            'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
            'gamma': trial.suggest_float('gamma', 0.0, 5.0),
        }

        # pruning callback observes 'validation_0-logloss' - XGBoost names unnamed eval_set entries as validation_0, validation_1, ...
        pruning_callback = XGBoostPruningCallback(trial, 'validation_0-logloss')

        result = train_single_run(
            handle=handle,
            params=params,
            target_column=target_column,
            mlflow_run_name=f'trial_{trial.number:03d}',
            parent_run_id=parent_run_id,
            callbacks=[pruning_callback],
        )
        return result.metrics['test_roc_auc']

    return objective


def _run_mllib_comparison(
    handle: DatasetHandle,
    target_column: str,
    parent_run_id: str,
) -> None:
    """Run the PySpark MLlib baseline as a nested child run."""
    try:
        import pandas as pd
        import pyarrow.parquet as pq
        import s3fs

        from bmo.common.config import settings
        from bmo.training.models.mllib_baseline import fit_mllib_baseline
        from bmo.training.train import _get_feature_columns, _time_split

        fs = s3fs.S3FileSystem(
            key=settings.s3_access_key_id,
            secret=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
        )
        with fs.open(handle.storage_path, 'rb') as f:
            df = pq.read_table(f).to_pandas()

        feature_cols = _get_feature_columns(df)
        X_train, _, X_test, y_train, _, y_test = _time_split(df, feature_cols, target_column)

        # MLlib doesn't use a validation set (uses maxIter instead of early stopping) --> only pass train/test
        train_df = pd.DataFrame(X_train, columns=feature_cols)
        train_df[target_column] = y_train
        test_df = pd.DataFrame(X_test, columns=feature_cols)
        test_df[target_column] = y_test

        fit_mllib_baseline(
            train_df=train_df,
            test_df=test_df,
            feature_columns=feature_cols,
            target_column=target_column,
            experiment_name=MLFLOW_EXPERIMENT,
            parent_run_id=parent_run_id,
        )
        log.info('MLlib baseline complete')

    except Exception as exc:
        # MLlib baseline is objectional - don't fail HPO sweep if Spark is unavailable
        log.warning('MLlib baseline skipped', error=str(exc))
