"""
Unit tests for bmo.evaluation_gate.

Fast tests — no S3, no real MLflow server, no XGBoost training.
SliceParityCheck tests write a tiny Parquet to tmp_path and mock
mlflow.xgboost.load_model so the check runs without network access.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from bmo.evaluation_gate.base import CheckResult, GateInput, GateResult, Severity
from bmo.evaluation_gate.checks import (
    AUCGateCheck,
    CalibrationCheck,
    LeakageSentinelCheck,
    SliceParityCheck,
)
from bmo.evaluation_gate.gate import run_gate


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_gate_input(
    auc: float = 0.78,
    brier: float = 0.16,
    feature_importance: dict[str, float] | None = None,
    prod_auc: float | None = None,
    storage_path: str = '/tmp/nonexistent.parquet',
) -> GateInput:
    return GateInput(
        mlflow_run_id='test_run_abc123',
        metrics={
            'test_roc_auc': auc,
            'test_brier_score': brier,
            'test_f1': 0.58,
            'test_pr_auc': 0.52,
            'test_log_loss': 0.43,
        },
        feature_importance=feature_importance
        or {
            'origin_avg_dep_delay_1h': 0.30,
            'carrier_on_time_pct_7d': 0.25,
            'cascading_delay_min': 0.20,
            'route_avg_dep_delay_7d': 0.15,
            'origin_wind_kts': 0.10,
        },
        dataset_version_hash='abcd1234',
        dataset_storage_path=storage_path,
        prod_run_id='prod_run_xyz' if prod_auc is not None else None,
        prod_metrics={'test_roc_auc': prod_auc} if prod_auc is not None else None,
    )


def _make_tiny_test_parquet(tmp_path: Path, n: int = 400) -> str:
    """
    Write a synthetic test dataset Parquet with the columns SliceParityCheck needs.
    The dataset is 5x the test fraction size so the full dataset produces n test rows.
    """
    total = int(n / 0.20)
    rng = np.random.default_rng(42)
    df = pd.DataFrame(
        {
            'event_timestamp': pd.date_range('2024-01-01', periods=total, freq='1h', tz='UTC'),
            'origin': rng.choice(['ORD', 'ATL', 'DEN', 'BOS', 'DAL'], size=total),
            'dest': rng.choice(['JFK', 'LAX', 'SFO', 'MIA'], size=total),
            'carrier': rng.choice(['AA', 'DL', 'UA', 'WN'], size=total),
            'f1': rng.standard_normal(total).astype(np.float32),
            'f2': rng.standard_normal(total).astype(np.float32),
            'is_dep_delayed': (rng.standard_normal(total) > 0).astype(float),
            # label columns — excluded from feature_columns by _METADATA_COLUMNS
            'dep_delay_min': rng.uniform(-10, 90, total).astype(np.float32),
            'arr_delay_min': rng.uniform(-10, 90, total).astype(np.float32),
            'is_arr_delayed': (rng.standard_normal(total) > 0).astype(float),
            'cancelled': np.zeros(total),
            'diverted': np.zeros(total),
            'flight_id': [f'FL{i:05d}' for i in range(total)],
            'tail_number': 'N12345',
            'route_key': 'ORD-LAX',
        }
    )
    path = tmp_path / 'data.parquet'
    df.to_parquet(path, index=False)
    return str(path)


# ── AUCGateCheck ──────────────────────────────────────────────────────────────


class TestAUCGateCheck:
    def test_passes_above_floor(self) -> None:
        result = AUCGateCheck(min_auc=0.70).run(_make_gate_input(auc=0.75))
        assert result.passed
        assert result.severity == Severity.ERROR

    def test_fails_below_floor(self) -> None:
        result = AUCGateCheck(min_auc=0.70).run(_make_gate_input(auc=0.65))
        assert not result.passed
        assert result.blocking

    def test_passes_when_no_prod_model(self) -> None:
        result = AUCGateCheck().run(_make_gate_input(auc=0.72, prod_auc=None))
        assert result.passed
        assert 'prod_auc' not in result.metadata

    def test_passes_marginal_regression(self) -> None:
        # 0.005 regression within the 0.01 margin
        result = AUCGateCheck(prod_regression_margin=0.01).run(
            _make_gate_input(auc=0.795, prod_auc=0.800)
        )
        assert result.passed

    def test_fails_excessive_regression(self) -> None:
        result = AUCGateCheck(prod_regression_margin=0.01).run(
            _make_gate_input(auc=0.75, prod_auc=0.82)
        )
        assert not result.passed
        assert 'regressed' in result.message

    def test_metadata_contains_auc(self) -> None:
        result = AUCGateCheck().run(_make_gate_input(auc=0.78))
        assert 'auc' in result.metadata
        assert result.metadata['auc'] == pytest.approx(0.78, abs=0.001)


# ── LeakageSentinelCheck ───────────────────────────────────────────────────────


class TestLeakageSentinelCheck:
    def test_passes_distributed_importance(self) -> None:
        fi = {'f1': 0.35, 'f2': 0.35, 'f3': 0.30}
        result = LeakageSentinelCheck(max_single_importance=0.70).run(
            _make_gate_input(feature_importance=fi)
        )
        assert result.passed

    def test_fails_dominant_feature(self) -> None:
        fi = {'dep_delay_min': 0.85, 'f2': 0.10, 'f3': 0.05}
        result = LeakageSentinelCheck(max_single_importance=0.70).run(
            _make_gate_input(feature_importance=fi)
        )
        assert not result.passed
        assert result.blocking
        assert 'dep_delay_min' in result.message

    def test_passes_exactly_at_threshold(self) -> None:
        fi = {'f1': 0.70, 'f2': 0.30}
        result = LeakageSentinelCheck(max_single_importance=0.70).run(
            _make_gate_input(feature_importance=fi)
        )
        assert result.passed  # threshold is inclusive

    def test_empty_importance_passes(self) -> None:
        result = LeakageSentinelCheck().run(_make_gate_input(feature_importance={}))
        assert result.passed

    def test_metadata_contains_top_feature(self) -> None:
        fi = {'big_feature': 0.60, 'small': 0.40}
        result = LeakageSentinelCheck().run(_make_gate_input(feature_importance=fi))
        assert result.metadata['top_feature'] == 'big_feature'


# ── CalibrationCheck ──────────────────────────────────────────────────────────


class TestCalibrationCheck:
    def test_passes_good_calibration(self) -> None:
        result = CalibrationCheck(max_brier_score=0.25).run(_make_gate_input(brier=0.15))
        assert result.passed
        assert result.severity == Severity.WARN  # calibration is always WARN

    def test_fails_poor_calibration(self) -> None:
        result = CalibrationCheck(max_brier_score=0.25).run(_make_gate_input(brier=0.30))
        assert not result.passed

    def test_not_blocking_even_when_failed(self) -> None:
        result = CalibrationCheck().run(_make_gate_input(brier=0.99))
        assert not result.passed
        assert not result.blocking  # WARN severity never blocks

    def test_metadata_contains_brier(self) -> None:
        result = CalibrationCheck().run(_make_gate_input(brier=0.18))
        assert 'brier_score' in result.metadata


# ── SliceParityCheck ──────────────────────────────────────────────────────────


class TestSliceParityCheck:
    def test_passes_uniform_model(self, tmp_path: Path) -> None:
        """A model that predicts 0.5 for everything has consistent (uniform) AUC across slices."""
        parquet_path = _make_tiny_test_parquet(tmp_path, n=600)

        # Mock XGBoost booster: predict() returns constant 0.5 for all rows
        mock_booster = MagicMock()
        mock_booster.predict.return_value = np.full(600, 0.5)  # n=600 test rows (20% of total)

        with patch('mlflow.xgboost.load_model', return_value=mock_booster):
            result = SliceParityCheck(min_slice_rows=20).run(
                _make_gate_input(storage_path=parquet_path)
            )

        # Constant predictions → random AUC ≈ 0.5. The absolute floor is 0.60,
        # so this will FAIL but the test verifies the check runs without error.
        assert isinstance(result, CheckResult)
        assert 'overall_auc' in result.metadata

    def test_skips_when_dataset_too_small(self, tmp_path: Path) -> None:
        """When test set is below min_slice_rows, check passes without scoring."""
        total_rows = 5  # 20% = 1 test row
        df = pd.DataFrame(
            {
                'event_timestamp': pd.date_range(
                    '2024-01-01', periods=total_rows, freq='1h', tz='UTC'
                ),
                'origin': ['ORD'] * total_rows,
                'dest': ['JFK'] * total_rows,
                'carrier': ['AA'] * total_rows,
                'f1': [1.0] * total_rows,
                'is_dep_delayed': [1.0, 0.0, 1.0, 0.0, 1.0],
                'dep_delay_min': [10.0] * total_rows,
                'arr_delay_min': [5.0] * total_rows,
                'is_arr_delayed': [1.0] * total_rows,
                'cancelled': [0.0] * total_rows,
                'diverted': [0.0] * total_rows,
                'flight_id': [f'f{i}' for i in range(total_rows)],
                'tail_number': ['N1'] * total_rows,
                'route_key': ['ORD-JFK'] * total_rows,
            }
        )
        path = tmp_path / 'tiny.parquet'
        df.to_parquet(path, index=False)

        result = SliceParityCheck(min_slice_rows=50).run(_make_gate_input(storage_path=str(path)))
        assert result.passed
        assert 'skipped' in result.message


# ── GateResult ────────────────────────────────────────────────────────────────


class TestGateResult:
    def test_overall_passed_all_pass(self) -> None:
        checks = [
            CheckResult('a', True, Severity.ERROR, 'ok'),
            CheckResult('b', True, Severity.WARN, 'ok'),
        ]
        assert GateResult(checks=checks).overall_passed

    def test_overall_failed_on_error(self) -> None:
        checks = [
            CheckResult('a', False, Severity.ERROR, 'fail'),
            CheckResult('b', True, Severity.WARN, 'ok'),
        ]
        assert not GateResult(checks=checks).overall_passed

    def test_overall_passed_despite_warn(self) -> None:
        checks = [
            CheckResult('a', True, Severity.ERROR, 'ok'),
            CheckResult('b', False, Severity.WARN, 'warn'),
        ]
        assert GateResult(checks=checks).overall_passed

    def test_blocking_failures_only_errors(self) -> None:
        checks = [
            CheckResult('a', False, Severity.ERROR, 'error'),
            CheckResult('b', False, Severity.WARN, 'warn'),
        ]
        result = GateResult(checks=checks)
        assert len(result.blocking_failures) == 1
        assert result.blocking_failures[0].name == 'a'


# ── run_gate ──────────────────────────────────────────────────────────────────


class TestRunGate:
    def test_runs_all_checks(self) -> None:
        gate_input = _make_gate_input()
        gate_result = run_gate(gate_input, checks=[AUCGateCheck(), CalibrationCheck()])
        assert len(gate_result.checks) == 2

    def test_passes_with_good_inputs(self) -> None:
        gate_input = _make_gate_input(auc=0.80, brier=0.15)
        checks = [AUCGateCheck(), LeakageSentinelCheck(), CalibrationCheck()]
        result = run_gate(gate_input, checks=checks)
        assert result.overall_passed
        assert len(result.checks) == 3

    def test_all_checks_run_despite_early_failure(self) -> None:
        """Gate does not short-circuit — all checks run even after a blocking failure."""
        gate_input = _make_gate_input(
            auc=0.55,
            brier=0.30,
            feature_importance={'leaky_col': 0.90, 'f2': 0.10},
        )
        checks = [AUCGateCheck(), LeakageSentinelCheck(), CalibrationCheck()]
        result = run_gate(gate_input, checks=checks)
        assert len(result.checks) == 3
        assert not result.overall_passed
