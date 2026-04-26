"""
Evidently HTML classification report for trained model evaluation.

Loads the test split, scores it with the logged model, and produces an
HTML report surfaced as Dagster asset metadata and an MLflow artifact.

  - The HTML is self-contained (no external JS CDN) — good for CI artifacts.
  - It provides confusion matrix, calibration curve, PR curve, and class balance
    in one shot without custom matplotlib code.

Evidently docs: https://docs.evidentlyai.com/
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import structlog
import xgboost as xgb
from evidently import BinaryClassification, DataDefinition, Dataset, Report
from evidently.presets import ClassificationPreset
from mlflow.xgboost import load_model

# from bmo.evaluation_gate.checks import _load_dataset_for_slicing
from bmo.training.train import _get_feature_columns, _time_split

log = structlog.get_logger(__name__)


def generate_classification_report(
    mlflow_run_id: str,
    dataset_storage_path: str,
    output_dir: str | None = None,
) -> str:
    """
    Generate an Evidently classification quality report for a trained model run.

    Reproduces the same test split used during training, scores it with the
    logged XGBoost booster, and writes an interactive HTML file.

    Args:
        mlflow_run_id:          Champion MLflow run ID (from trained_model asset).
        dataset_storage_path:   Local or S3 path to the training dataset Parquet.
        output_dir:             Write HTML here. Defaults to /tmp.

    Returns:
        Absolute path to the HTML report file.
    """
    log.info('generating evidently report', run_id=mlflow_run_id)

    # df = _load_dataset_for_slicing(dataset_storage_path)
    # n = len(df)
    # test_start = int(n * (1 - _TEST_FRACTION))
    # test_df = df.sort_values('event_timestamp').iloc[test_start:].copy().reset_index(drop=True)

    # feature_cols = _get_feature_columns(test_df)
    # X_test = test_df[feature_cols].fillna(0).values
    # y_test = test_df['is_dep_delayed'].to_numpy(dtype=float)

    # load test slice
    df = cast(pd.DataFrame, pq.read_table(dataset_storage_path).to_pandas())
    feature_cols = _get_feature_columns(df)
    _, _, X_test, _, _, y_test = _time_split(df, feature_cols, 'is_dep_delayed')

    # load model from MLflow and predict
    model: xgb.Booster = load_model(f'runs:/{mlflow_run_id}/model')
    y_proba = model.predict(xgb.DMatrix(X_test, feature_names=feature_cols))
    y_pred: np.ndarray = (y_proba >= 0.5).astype(int)

    eval_df = pd.DataFrame(
        {
            'target': y_test.astype(int),
            'prediction': y_pred,
            'score': y_proba,
        }
    )

    data_definition = DataDefinition(
        classification=[  # pyright: ignore[reportArgumentType]
            BinaryClassification(
                target='target',
                prediction_labels='prediction',
                prediction_probas='score',
                pos_label=1,  # pyright: ignore
            )
        ]
    )
    dataset = Dataset.from_pandas(eval_df, data_definition=data_definition)

    report = Report(metrics=[ClassificationPreset()])
    snapshot = report.run(current_data=dataset, reference_data=None)

    out_dir = Path(output_dir) if output_dir else Path(tempfile.mkdtemp())
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = str(out_dir / 'classification_report.html')
    snapshot.save_html(html_path)

    log.info('evidently report saved', path=str(html_path))
    return str(html_path)
