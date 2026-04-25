"""
bmo.training — XGBoost training, HPO, and reproducibility.

Public API:
  train_single_run  — one training run, returns TrainingResult, logs to MLflow
  run_hpo           — Optuna HPO over 50+ trials, returns best TrainingResult
  reproduce_run     — re-run a historical MLflow run, assert byte-equality
  TrainingResult    — Pydantic model returned by train_single_run / run_hpo
"""

from bmo.training.hpo import HPOResult, run_hpo
from bmo.training.reproduce import reproduce_run
from bmo.training.train import TrainingResult, train_single_run

__all__ = [
    'TrainingResult',
    'HPOResult',
    'train_single_run',
    'run_hpo',
    'reproduce_run',
]
