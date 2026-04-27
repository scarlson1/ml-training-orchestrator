"""
XGBoost binary classifier wrapper.

Design contracts:
  1. All randomness goes through the 'seed' parameter — no global state mutation.
  2. 'nthread=1' in reproduce mode guarantees byte-for-byte determinism.
  3. Returns the native xgb.Booster so callers can save via mlflow.xgboost.log_model().
  4. Stores y_proba_test so callers don't need to re-predict for plots/metrics.

XBoost Parameters: https://xgboost.readthedocs.io/en/stable/parameter.html
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    log_loss,
    roc_auc_score,
)

DEFAULT_PARAMS: dict[str, Any] = {
    'objective': 'binary:logistic',
    'eval_metric': ['logloss', 'auc'],
    'max_depth': 6,
    'learning_rate': 0.05,
    'n_estimators': 500,
    'min_child_weight': 5,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'reg_alpha': 0.1,
    'reg_lambda': 1.0,
    'gamma': 0.0,
    'scale_pos_weight': 1.0,
    'seed': 42,
    'nthread': -1,
}

EARLY_STOPPING_ROUNDS = 50


@dataclass
class XGBFitResult:
    """output of fit_xgboost"""

    # fmt: off
    booster: xgb.Booster
    metrics: dict[str, float]
    feature_importance: dict[str, float]    # feature_name -> normalized gain
    best_iteration: int                     # which tree the model stopped at
    model_bytes: bytes                      # for byte equality checks in reproduce.py 
    y_proba_test: np.ndarray                # P(class=1) on test set - stored to avoid re-predicting # shape (200_000,), dtype float
    y_pred_test: np.ndarray                 # threshold=0.5 binary predictions # shape (200_000,), dtype int 
    # fmt: on

    @property
    def roc_auc(self) -> float:
        return self.metrics['test_roc_auc']


def fit_xgboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    feature_names: list[str],
    params: dict[str, Any] | None = None,
    callbacks: list[Any] | None = None,
) -> XGBFitResult:
    """
    Train on XGBoost binary classifier

    Why XGBClassifier over the low-level xgb.train() API?
    XGBClassifier integrates with scikit-learn's ecosystem (cross_val_score,
    calibration), used in the evaluation gate. The .get_booster() method
    retrieves the underlying Booster for MLflow logging and byte comparison.

    Args:
        X_train, y_train: Training features and labels.
        X_val, y_val:     Validation set for early stopping ONLY. Not used for
                          reported metrics — that would bias the estimate.
        X_test, y_test:   Held-out test set for final metric computation.
        feature_names:    Column names in same order as X_train columns.
        params:           XGBoost params; missing keys fall back to DEFAULT_PARAMS.
        callbacks:        Optional callback list (e.g. Optuna XGBoostPruningCallback).
    """
    merged_params = {**DEFAULT_PARAMS, **(params or {})}

    model = xgb.XGBClassifier(
        **{k: v for k, v in merged_params.items() if k != 'eval_metric'},
        eval_metric=merged_params.get('eval_metric', ['logloss', 'auc']),
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        callbacks=callbacks or [],
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    booster = model.get_booster()
    booster.feature_names = feature_names

    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= 0.5).astype(int)

    metrics = _compute_metrics(y_test, y_pred, y_proba)

    # 'gain' = average gain of splits using this feature
    # preferred over 'weight' (biased toward high-cardinality features) and
    # 'cover' (average samples affected). Gain is used for the leakage sentinel
    # in the phase 7 evaluation gate: no single feature should dominate.
    importance_raw = cast(dict[str, float], booster.get_score(importance_type='gain'))
    total = sum(importance_raw.values(), 0.0) or 1.0
    feature_importance = {k: v / total for k, v in importance_raw.items()}

    return XGBFitResult(
        booster=booster,
        metrics=metrics,
        feature_importance=feature_importance,
        best_iteration=model.best_iteration,
        model_bytes=_booster_to_bytes(booster),
        y_proba_test=y_proba,
        y_pred_test=y_pred,
    )


def _compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: np.ndarray,
) -> dict[str, float]:
    """
    Why Brier score?
    measures probability calibration - a model predicting P(delay)=0.9 for flights
    that are only delayed 50% of the time has poor calibration.
    The gate checks for Brier score <= 0.25

    Why PR-AUC alongside ROC-AUC?
    On imbalanced data (most flights are on-time), PR-AUC reveals the floor a naive all-negative classifier achieves
    """
    return {
        'test_roc_auc': float(roc_auc_score(y_true, y_proba)),
        'test_pr_auc': float(average_precision_score(y_true, y_proba)),
        'test_log_loss': float(log_loss(y_true, y_proba)),
        'test_f1': float(f1_score(y_true, y_pred, zero_division=0.0)),  # pyright: ignore[reportArgumentType]
        'test_brier_score': float(brier_score_loss(y_true, y_proba)),
    }


def _booster_to_bytes(booster: xgb.Booster) -> bytes:
    return bytes(booster.save_raw(raw_format='ubj'))
