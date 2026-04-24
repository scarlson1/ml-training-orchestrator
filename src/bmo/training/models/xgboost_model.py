"""
XGBoost binary classifier wrapper.

Design contracts:
  1. All randomness goes through the 'seed' parameter — no global state mutation.
  2. 'nthread=1' in reproduce mode guarantees byte-for-byte determinism.
  3. Returns the native xgb.Booster so callers can save via mlflow.xgboost.log_model().
  4. Stores y_proba_test so callers don't need to re-predict for plots/metrics.
"""

# from __future__ import annotations

# import io
# from dataclasses import dataclass
# from typing import Any

# import numpy as np
# import xgboost as xgb
# from sklearn.metrics import (
#     average_precision_score,
#     brier_score_loss,
#     f1_score,
#     log_loss,
#     roc_auc_score,
# )


# DEFAULT_PARAMS: dict[str, Any] = {
#     'objective': 'binary:logistic',
#     'eval_metric': ['logloss', 'auc'],
#     'max_depth': 6,
#     'learning_rate': 0.05,
#     'n_estimators': 500,
#     'min_child_weight': 5,
#     'subsample': 0.8,
#     'colsample_bytree': 0.8,
#     'reg_alpha': 0.1,
#     'reg_lambda': 1.0,
#     'gamma': 0.0,
#     'scale_pos_weight': 1.0,
#     'seed': 42,
#     'nthread': -1,
# }

# EARLY_STOPPING_ROUNDS = 50


#############################################
############ SEE UPDATED VERSION ############
#############################################
