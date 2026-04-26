"""
Shared types for the evaluation gate.

Pure data types only — no I/O, no framework imports. Keeps the protocol
importable across layers without dependency bloat.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """
    Maps to Dagster's AssetCheckSeverity and to gate blocking behavior.

    ERROR:  check failure blocks downstream registered_model materialization.
    WARN:   check failure surfaces in the UI but does not block promotion.
            Use for checks where a miss is important to know but not critical
            enough to stop the pipeline (e.g., mild miscalibration).
    """

    WARN = 'warn'
    ERROR = 'error'


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    severity: Severity
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def blocking(self) -> bool:
        return not self.passed and self.severity == Severity.ERROR


@dataclass(frozen=True)
class GateInput:
    """
    Everything a check needs, loaded once and shared across all checks.

    Frozen (immutable) so checks cannot mutate shared state. The heavy
    data — test DataFrame and XGBoost Booster — is loaded lazily inside
    SliceParityCheck, not here. This avoids paying the S3 + MLflow load
    cost for checks that only need metrics and feature importance.

    Built by gate.load_gate_input(mlflow_run_id).
    """

    mlflow_run_id: str
    metrics: dict[str, float]
    feature_importance: dict[str, float]  # normalized gain, sums to 1.0
    dataset_version_hash: str
    dataset_storage_path: str  # S3 or local path to data.parquet
    prod_run_id: str | None  # none when no champion exists yet
    prod_metrics: dict[str, float] | None  # none when no champion exists yet


@dataclass
class GateResult:
    """Aggregated result of running all checks against a GateInput."""

    checks: list[CheckResult]

    @property
    def overall_passed(self) -> bool:
        """True when no ERROR-severity checks failed."""
        return all(c.passed or c.severity != Severity.ERROR for c in self.checks)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.blocking]


class EvalCheck(ABC):
    name: str
    severity: Severity

    @abstractmethod
    def run(self, gate_input: GateInput) -> CheckResult:
        raise NotImplementedError
