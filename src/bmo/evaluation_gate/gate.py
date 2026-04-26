"""
Gate orchestration: loads a GateInput from MLflow, runs all checks.

This is the only I/O layer in bmo.evaluation_gate. checks.py is pure;
reports.py does its own I/O; nothing else in this package touches the network.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Sequence
from pathlib import Path

import structlog
from mlflow.tracking import MlflowClient

from bmo.evaluation_gate.base import EvalCheck, GateInput, GateResult
from bmo.evaluation_gate.checks import DEFAULT_CHECKS

log = structlog.get_logger(__name__)

# MLflow registered model name. Used by load_gate_input() to find the
# current champion model and by registered_model asset to register new versions.
MODEL_NAME = 'bmo_flight_delay'


def load_gate_input(mlflow_run_id: str) -> GateInput:
    """
    Build a GateInput from an MLflow champion run ID.

    Loads metrics from the run, downloads feature_importance.json and
    dataset_card.json artifacts, and queries the registry for the current
    production model's metrics.

    Args:
        mlflow_run_id: The MLflow run ID logged by the trained_model Dagster asset.
                       This is the champion run from the Optuna sweep.

    Returns:
        GateInput ready to pass to run_gate() or individual check.run() calls.
    """
    client = MlflowClient()
    run = client.get_run(mlflow_run_id)
    metrics = dict(run.data.metrics)

    tmp_dir = tempfile.mkdtemp(prefix='bmo_gate_')

    client.download_artifacts(mlflow_run_id, 'feature_importance.json', tmp_dir)
    with (Path(tmp_dir) / 'feature_importance.json').open() as f:
        feature_importance: dict[str, float] = json.load(f)

    client.download_artifacts(mlflow_run_id, 'dataset_card.json', tmp_dir)
    with (Path(tmp_dir) / 'dataset_card.json').open() as f:
        card = json.load(f)

    dataset_version_hash: str = card['version_hash']
    dataset_storage_path: str = card['storage_path']

    prod_run_id, prod_metrics = _load_prod_metrics(client)

    return GateInput(
        mlflow_run_id=mlflow_run_id,
        metrics=metrics,
        feature_importance=feature_importance,
        dataset_version_hash=dataset_version_hash,
        dataset_storage_path=dataset_storage_path,
        prod_run_id=prod_run_id,
        prod_metrics=prod_metrics,
    )


def run_gate(
    gate_input: GateInput,
    checks: Sequence[EvalCheck] | None = None,
) -> GateResult:
    """
    Run all checks and aggregate results.

    Args:
        gate_input: Pre-loaded GateInput from load_gate_input().
        checks:     Override the default check list. Useful in tests.
                    Defaults to DEFAULT_CHECKS (AUC, leakage, calibration, slice parity).

    Returns:
        GateResult with per-check results and overall_passed status.
    """
    active_checks = checks if checks is not None else DEFAULT_CHECKS
    results = []
    for check in active_checks:
        log.info('running gate check', check=check.name)
        result = check.run(gate_input)
        log.info(
            'gate check complete',
            check=check.name,
            passed=result.passed,
            severity=result.severity,
            message=result.message,
        )
        results.append(result)
    return GateResult(checks=results)


def _load_prod_metrics(
    client: MlflowClient,
) -> tuple[str | None, dict[str, float] | None]:
    """
    Return (run_id, metrics) for the current champion model, or (None, None).

    Uses the MLflow alias API (mlflow>=2.3.2). The 'champion' alias points to
    the version currently serving in production. If no champion exists yet
    (first training run), both return values are None — the AUC gate will
    skip the regression check in that case.

    MLflow aliases docs:
    https://mlflow.org/docs/latest/model-registry.html#deploy-and-organize-models-with-aliases-and-tags
    """
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, 'champion')

        if not version or not version.run_id:
            raise Exception('version not found for {MODEL_NAME}')

        prod_run = client.get_run(version.run_id)
        return version.run_id, dict(prod_run.data.metrics)
    except Exception:
        log.info('no champion model in registry', model=MODEL_NAME)
        return None, None
