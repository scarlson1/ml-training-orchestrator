"""
Dagster @asset_check wrappers for the evaluation gate.

These are thin wrappers — all logic lives in bmo.evaluation_gate.
Each check:
  1. Reads the trained_model's last materialization to get the MLflow run ID.
  2. Calls load_gate_input() to load metrics, feature importance, prod metrics.
  3. Runs one specific check.
  4. Returns AssetCheckResult with typed metadata.

Four separate @asset_check functions (vs. one function that runs all four) so
Dagster can display each check independently in the UI and so SliceParityCheck
(the expensive one) doesn't block the fast checks from completing.
"""

from __future__ import annotations

from typing import Any

import mlflow
import structlog
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    MetadataValue,
    asset_check,
)

from bmo.common.config import settings
from bmo.evaluation_gate.checks import (
    AUCGateCheck,
    CalibrationCheck,
    LeakageSentinelCheck,
    SliceParityCheck,
)
from bmo.evaluation_gate.gate import load_gate_input

log = structlog.get_logger(__name__)


def _get_trained_model_run_id(context: AssetCheckExecutionContext) -> str:
    """
    Pull the MLflow champion run ID from trained_model's last materialization metadata.

    This is the same pattern used in trained_model to read training_dataset's
    metadata (see dagster_project/assets/training.py). The metadata key
    'mlflow_run_id' is set by the trained_model @asset.
    """
    event = context.instance.get_latest_materialization_event(AssetKey(['trained_model']))
    if event is None or event.asset_materialization is None:
        raise RuntimeError(
            'No trained_model materialization found. '
            'Materialize trained_model before running evaluation get checks.'
        )

    metadata = event.asset_materialization.metadata
    if 'mlflow_run_id' not in metadata:
        raise RuntimeError(
            'trained_model metadata missing "mlflow_run_id". '
            'Re-materialize trained_model to regenerate metadata.'
        )
    return str(metadata['mlflow_run_id'].value)


def _to_dagster_metadata(raw: dict[str, Any]) -> dict[str, MetadataValue]:
    result: dict[str, MetadataValue] = {}
    for k, v in raw.items():
        if isinstance(v, float):
            result[k] = MetadataValue.float(v)
        elif isinstance(v, int):
            result[k] = MetadataValue.int(v)
        elif isinstance(v, str):
            result[k] = MetadataValue.text(v)
        else:
            result[k] = MetadataValue.text(str(v))
    return result


def _severity_to_dagster(passed: bool, result_severity: str) -> AssetCheckSeverity:
    if not passed and result_severity == 'error':
        return AssetCheckSeverity.ERROR
    return AssetCheckSeverity.WARN


@asset_check(
    asset='trained_model',
    blocking=True,
    description=(
        'AUC must be >= 70 (absolute floor) AND not regress vs. '
        'current champion by more than 0.01. '
        'ERROR: blocks registered_model materialization.'
    ),
)
def check_auc_gate(context: AssetCheckExecutionContext) -> AssetCheckResult:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    run_id = _get_trained_model_run_id(context)
    gate_input = load_gate_input(run_id)
    result = AUCGateCheck().run(gate_input)
    context.log.info(f'AUC gate: {result.message}')
    return AssetCheckResult(
        passed=result.passed,
        severity=_severity_to_dagster(result.passed, result.severity),
        metadata=_to_dagster_metadata(result.metadata),
        description=result.message,
    )


@asset_check(
    asset='trained_model',
    blocking=True,
    description=(
        'No single feature importance should exceed 0.70 of total normalized gain. '
        'A dominant feature almost always indicates data leakage or a trivially '
        'correlated feature. '
        'ERROR: blocks registered_model materialization.'
    ),
)
def check_leakage_sentinel(context: AssetCheckExecutionContext) -> AssetCheckResult:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    run_id = _get_trained_model_run_id(context)
    gate_input = load_gate_input(run_id)
    result = LeakageSentinelCheck().run(gate_input)
    context.log.info(f'Leakage sentinel: {result.message}')
    return AssetCheckResult(
        passed=result.passed,
        severity=_severity_to_dagster(result.passed, result.severity),
        metadata=_to_dagster_metadata(result.metadata),
        description=result.message,
    )


@asset_check(
    asset='trained_model',
    blocking=False,
    description=(
        'Brier score must be <=0.25. '
        'WARN: surfaces recalibration recommendation but does not block promotion.'
    ),
)
def check_calibration(context: AssetCheckExecutionContext) -> AssetCheckResult:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    run_id = _get_trained_model_run_id(context)
    gate_input = load_gate_input(run_id)
    result = CalibrationCheck().run(gate_input)
    context.log.info(f'Calibration: {result.message}')
    return AssetCheckResult(
        passed=result.passed,
        severity=AssetCheckSeverity.WARN,
        metadata=_to_dagster_metadata(result.metadata),
        description=result.message,
    )


@asset_check(
    asset='trained_model',
    blocking=True,
    description=(
        'Model must not degrade severely on subgroups: '
        'carrier, origin hub size, time-of-day bucket, weather condition. '
        'Any slice with AUC < 0.60 or a drop > 0.10 vs. overall fails. '
        'Expensive check (~30s): loads test dataset from S3 and model from MLflow. '
        'ERROR: blocks registered_model materialization.'
    ),
)
def check_slice_parity(context: AssetCheckExecutionContext) -> AssetCheckResult:
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    run_id = _get_trained_model_run_id(context)
    gate_input = load_gate_input(run_id)
    result = SliceParityCheck().run(gate_input)
    context.log.info(f'Slice parity: {result.message}')
    return AssetCheckResult(
        passed=result.passed,
        severity=_severity_to_dagster(result.passed, result.severity),
        metadata=_to_dagster_metadata(result.metadata),
        description=result.message,
    )
