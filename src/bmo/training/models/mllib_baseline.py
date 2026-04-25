"""
PySpark MLlib GBTClassifier baseline.

Purpose: single comparison run to demonstrate distributed ML competency.
Default params are intentional — this is a baseline, not a tuned competitor.

This module creates its own SparkSession so it can run as a standalone job
or be called from a Dagster asset. It does NOT depend on a shared Spark
resource injected by Dagster — that coupling makes it untestable in isolation.

# pyspark MLlib: https://spark.apache.org/docs/latest/ml-guide.html
# GBTClassifier: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.GBTClassifier.html
"""


#############################################
############ SEE UPDATED VERSION ############
#############################################

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

import mlflow
import numpy as np
import pandas as pd
import structlog
from mlflow.spark import log_model
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import average_precision_score, log_loss

log = structlog.get_logger(__name__)


@dataclass
class MLlibBaselineResult:
    mlflow_run_id: str
    metrics: dict[str, float]
    model_uri: str


def fit_mllib_baseline(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    experiment_name: str,
    parent_run_id: str | None = None,
) -> MLlibBaselineResult:
    """
    Train a PySpark GBTClassifier and log as an MLflow run.

    Receives Pandas DataFrames and converts internally to Spark DataFrames.
    This keeps the interface identical to the XGBoost path and avoids
    requiring the caller to manage a SparkSession.

    Args:
        train_df:        Pandas DataFrame with feature + target columns.
        test_df:         Hold-out test set (same split as XGBoost for fair comparison).
        feature_columns: Column names to use as features.
        target_column:   Binary target column name.
        experiment_name: MLflow experiment to log under.
        parent_run_id:   If set, logged as a nested child of this parent run.
    """
    # lazy pyspark import - installed only in the spark dependency group
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ImportError('PySpark is not installed. Run uv sync --group spark') from exc

    spark = (
        SparkSession.builder.appName('bmo_mllib_baseline')
        .master('local[*]')  # local[*] uses all available cores on current machine
        .config('spark.ui.port', '4041')
        .config(
            'spark.sql.shuffle.partitions', '8'
        )  # reduce shuffle for single-node runs - 200 partitions on 1M rows creates tiny files and slow shuffles
        .config('spark.sql.shuffle.partitions', '8')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    log.info('PySpark session started', master=spark.sparkContext.master)

    try:
        return _run_mllib_training(
            spark=spark,
            train_pd=train_df,
            test_pd=test_df,
            feature_columns=feature_columns,
            target_column=target_column,
            experiment_name=experiment_name,
            parent_run_id=parent_run_id,
        )
    finally:
        spark.stop()
        log.info('PySpark session stopped')


def _run_mllib_training(
    spark: SparkSession,
    train_pd: pd.DataFrame,
    test_pd: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    experiment_name: str,
    parent_run_id: str | None,
) -> MLlibBaselineResult:
    # MLlib expects the label column named 'label' by convention
    train_spark = spark.createDataFrame(
        train_pd[feature_columns + [target_column]].rename(columns={target_column: 'label'})
    )
    test_spark = spark.createDataFrame(
        test_pd[feature_columns + [target_column]].rename(columns={target_column: 'label'})
    )

    # VectorAssembler combines feature columns into a single 'features' vector - MLlib's equivalent to a 2D numpy feature matrix
    assembler = VectorAssembler(
        inputCols=feature_columns, outputCol='features', handleInvalid='keep'
    )

    # GBTClassifier default params: maxIter=20, maxDepth=5, stepSize=0.1
    # intentionally not tuned - serves as baseline
    gbt = GBTClassifier(labelCol='label', featuresCol='features', maxIter=20, maxDepth=5, seed=42)

    # Pipeline: fit() only on train, transform() on test
    # prevents leakage if any stages were stateful (e.g. StandardScaler)
    pipeline = Pipeline(stages=[assembler, gbt])

    mlflow.set_experiment(experiment_name)

    # re-enter parent run context in order to nest this run as child
    result: MLlibBaselineResult | None = None
    if parent_run_id:
        with mlflow.start_run(run_id=parent_run_id):
            result = _log_and_fit(
                pipeline, train_spark, test_spark, feature_columns, experiment_name, nested=True
            )
    else:
        result = _log_and_fit(
            pipeline, train_spark, test_spark, feature_columns, experiment_name, nested=False
        )
    assert result is not None
    return result


def _log_and_fit(
    pipeline: Pipeline,
    train_spark: SparkDataFrame,
    test_spark: SparkDataFrame,
    feature_columns: list[str],
    experiment_name: str,
    nested: bool,
) -> MLlibBaselineResult:
    baseline_result: MLlibBaselineResult | None = None
    with mlflow.start_run(
        nested=nested, tags={'model_type': 'mllib_gbt', 'role': 'baseline'}
    ) as run:
        mlflow.log_params(
            {
                'model_type': 'mllib_gbt',
                'max_iter': 20,
                'max_depth': 5,
                'step_size': 0.1,
                'seed': 42,
                'n_features': len(feature_columns),
            }
        )

        log.info('fitting PySpark GBT pipeline')
        fitted_pipeline = pipeline.fit(train_spark)

        predictions = fitted_pipeline.transform(test_spark)
        evaluator = BinaryClassificationEvaluator(labelCol='label', metricName='areaUnderROC')
        spark_auc = evaluator.evaluate(predictions)

        # convert to pandas for sklearn metrics
        pred_pd = predictions.select('label', 'probability').toPandas()
        y_true = pred_pd['label'].values
        # DenseVector -> extract P(class=1)
        y_proba = np.array([float(row[1]) for row in pred_pd['probability']])

        metrics = {
            'test_roc_auc': float(spark_auc),
            'test_pr_auc': float(average_precision_score(y_true, y_proba)),
            'test_log_loss': float(log_loss(y_true, y_proba)),
        }
        mlflow.log_metrics(metrics)
        log_model(fitted_pipeline, 'model')

        log.info('MLlib baseline logged', run_id=run.info.run_id, auc=spark_auc)
        baseline_result = MLlibBaselineResult(
            mlflow_run_id=run.info.run_id,
            metrics=metrics,
            model_uri=f'runs:/{run.info.run_id}/model',
        )

    assert baseline_result is not None
    return baseline_result
