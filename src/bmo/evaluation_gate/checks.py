"""
Gate checks for trained model evaluation.

Lightweight checks (AUC, leakage sentinel, calibration) touch only
gate_input.metrics and gate_input.feature_importance — no I/O.

SliceParityCheck loads the test dataset from S3 and the model from MLflow.
It runs as a separate @asset_check (see dagster_project/asset_checks/evaluation_gate.py)
so its ~30s runtime doesn't delay the fast checks.
"""

from __future__ import annotations

from typing import Any, cast

import pandas as pd
import structlog

from bmo.evaluation_gate.base import CheckResult, EvalCheck, GateInput, Severity

log = structlog.get_logger(__name__)

# Large-hub airports by FAA NPIAS classification.
# Used in SliceParityCheck to bucket the 'origin' column.
# Source: https://www.faa.gov/airports/planning_capacity/passenger_allcargo_stats/categories
_LARGE_HUB_AIRPORTS = frozenset(
    {
        'ATL',
        'LAX',
        'ORD',
        'DFW',
        'DEN',
        'JFK',
        'SFO',
        'SEA',
        'LAS',
        'MCO',
        'CLT',
        'PHX',
        'MIA',
        'IAH',
        'EWR',
        'BOS',
        'MSP',
        'DTW',
        'PHL',
        'LGA',
        'BWI',
        'SLC',
        'SAN',
        'TPA',
        'PDX',
        'HNL',
        'MDW',
        'DAL',
    }
)
_MEDIUM_HUB_AIRPORTS = frozenset(
    {
        'OAK',
        'SMF',
        'SJC',
        'RDU',
        'PIT',
        'STL',
        'MEM',
        'BNA',
        'IND',
        'CMH',
        'CLE',
        'MKE',
        'MSY',
        'AUS',
        'SAT',
        'JAX',
        'OMA',
        'OKC',
        'TUL',
        'ABQ',
        'ELP',
        'BHM',
        'RIC',
        'ORF',
        'RSW',
        'BUF',
        'GRR',
        'LEX',
        'MHT',
        'TUS',
    }
)

# Must match _TEST_FRACTION in bmo.training.train. If you change the split
# there, change it here. The gate is an independent auditor — it must
# reproduce the exact test rows that training evaluated on. # TODO: move to env var ??
_TEST_FRACTION = 0.20


def _hub_size(iata: str) -> str:
    if iata in _LARGE_HUB_AIRPORTS:
        return 'large_hub'
    if iata in _MEDIUM_HUB_AIRPORTS:
        return 'medium_hub'
    return 'small_hub'


class AUCGateCheck(EvalCheck):
    """
    Gate check 1: AUC must exceed an absolute floor AND not regress vs. current
    production model.

    Why two conditions?
    - Absolute floor (0.70) catches models trained on accidentally tiny or wrong
      datasets that happen to score higher than nothing.
    - Regression margin (0.01) allows for natural variation between training runs
      (different random seeds, slightly different data windows) but blocks a model
      that genuinely got worse. 1% AUC regression on a 0.80 baseline is ~12.5%
      relative degradation — that's a real regression, not noise.

    Severity: ERROR — a model that fails the AUC floor must not reach production.
    """

    name = 'auc_gate'
    severity = Severity.ERROR

    def __init__(
        self,
        min_auc: float = 0.7,
        prod_regression_margin: float = 0.01,
    ) -> None:
        self.min_auc = min_auc
        self.prod_regression_margin = prod_regression_margin

    def run(self, gate_input: GateInput) -> CheckResult:
        auc = gate_input.metrics.get('test_roc_auc', 0.0)
        failures: list[str] = []
        meta: dict[str, Any] = {'auc': round(auc, 4), 'min_auc_floor': self.min_auc}

        if auc < self.min_auc:
            failures.append(f'AUC {auc:.4f} < floor {self.min_auc}')

        if gate_input.prod_metrics is not None:
            prod_auc = gate_input.prod_metrics.get('test_roc_auc', 0.0)
            regression = prod_auc - auc
            meta['prod_auc'] = round(prod_auc, 4)
            meta['regression_vs_prod'] = round(regression, 4)
            if regression > self.prod_regression_margin:
                failures.append(
                    f'AUC regressed {regression:.4f} vs prod ({prod_auc:.4f}): '
                    f'max allowed {self.prod_regression_margin}'
                )

        passed = len(failures) == 0
        message = (
            '; '.join(failures) if failures else f'AUC={auc:.4f} passes floor and prod comparison'
        )

        return CheckResult(
            name=self.name,
            passed=passed,
            severity=self.severity,
            message=message,
            metadata=meta,
        )


class LeakageSentinelCheck(EvalCheck):
    """
    Gate check 2: no single feature should dominate the model's decisions.

    In a healthy feature pipeline, the model learns from many sources. If one
    feature has >70% of total normalized gain, it is almost certainly either:
      a) Directly related to the target (e.g. dep_delay_min predicting is_dep_delayed)
      b) Leaked from the future (e.g. an aggregation that included the event itself)

    Normalized gain = (gain of this feature) / (sum of all gains). XGBoost's
    'gain' importance is the average gain per split — preferred over 'weight'
    (biased toward high-cardinality) and 'cover' (average samples per split).

    The 0.70 threshold is generous. In practice, even a strong legitimate feature
    like prev_flight_arrival_delay rarely exceeds 0.50. Adjust down once you have
    a stable baseline.

    Severity: ERROR — a leaky model must not reach production.
    """

    name = 'leakage_sentinel'
    severity = Severity.ERROR

    def __init__(self, max_single_importance: float = 0.70) -> None:
        self.max_single_importance = max_single_importance

    def run(self, gate_input: GateInput) -> CheckResult:
        fi = gate_input.feature_importance
        if not fi:
            return CheckResult(
                name=self.name,
                passed=True,
                severity=self.severity,
                message='no feature importance recorded',
                metadata={},
            )

        top_feature, top_value = max(fi.items(), key=lambda x: x[1])
        top_5 = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]

        meta: dict[str, Any] = {
            'top_feature': top_feature,
            'top_importance': round(top_value, 4),
            'threshold': self.max_single_importance,
            **{f'top5/{k}': round(v, 4) for k, v in top_5},
        }
        passed = top_value <= self.max_single_importance
        message = (
            f'{top_feature}={top_value:.3f}: OK'
            if passed
            else (
                f'LEAKAGE SIGNAL: {top_feature}={top_value:.3f} '
                f'> threshold {self.max_single_importance}'
            )
        )
        return CheckResult(
            name=self.name,
            passed=passed,
            severity=self.severity,
            message=message,
            metadata=meta,
        )


class CalibrationCheck(EvalCheck):
    """
    Gate check: predicted probabilities must be calibrated.

    Brier score = mean squared error of P(event) against true 0/1 labels.
    Lower is better. Reference values:
      - Perfect calibration: 0.0 (never achievable)
      - All-negative classifier (30% delay rate): 0.21  (not useful)
      - Random classifier (50/50): 0.25
      - Well-calibrated XGBoost: typically 0.12–0.18

    Why calibration matters: if the model says P(delay)=0.9 but only 60% of
    those flights are delayed, downstream systems that use the probability
    (alert thresholds, expected delay calculations) will be wrong.

    Severity: WARN — poor calibration is important to know but doesn't
    invalidate the model's ranking ability (AUC). The fix is
    sklearn.calibration.CalibratedClassifierCV, not retraining.
    """

    name = 'calibration'
    severity = Severity.WARN

    def __init__(self, max_brier_score: float = 0.25) -> None:
        self.max_brier_score = max_brier_score

    def run(self, gate_input: GateInput) -> CheckResult:
        brier = gate_input.metrics.get('test_brier_score', 0.0)
        passed = brier <= self.max_brier_score
        meta: dict[str, Any] = {
            'brier_score': round(brier, 4),
            'threshold': self.max_brier_score,
        }
        message = (
            f'brier={brier:.4f}: OK'
            if passed
            else f'brier={brier:.4f} > {self.max_brier_score}: recalibration recommended'
        )
        return CheckResult(
            name=self.name,
            passed=passed,
            severity=self.severity,
            message=message,
            metadata=meta,
        )


class SliceParityCheck(EvalCheck):
    """
    Gate check: model must not degrade severely on specific subgroups.

    A model can achieve 0.82 overall AUC while performing terribly on 5AM
    regional flights or storm-day departures. This check forces the model
    to generalize across:
      - Time-of-day buckets (late night / morning / afternoon / evening)
      - Individual carriers (AA, DL, UA, WN, B6, ...)
      - Origin hub size (large / medium / small hub)
      - Weather condition (bad weather vs clear)

    'Bad weather' is derived from precip and wind columns in the dataset.
    These column names are searched by substring match since the exact name
    varies by feature view naming convention.

    This check loads the full test split from S3 (~200k rows) and scores it
    with the model loaded from MLflow. It takes ~30s and runs as a separate
    @asset_check so it doesn't slow the lightweight checks.

    A slice fails if:
      - AUC < min_slice_auc (0.60 absolute floor), OR
      - AUC drops more than max_drop_vs_overall (0.10) vs. overall test AUC

    Slices with < min_slice_rows (200) are skipped — AUC is unreliable on
    tiny samples. Single-class slices (all on-time or all delayed) are also
    skipped since ROC AUC is undefined.

    Severity: ERROR — a model that fails on a major subgroup must not ship.
    """

    name = 'slice_parity'
    severity = Severity.ERROR

    def __init__(
        self,
        min_slice_auc: float = 0.60,
        min_slice_rows: int = 200,
        max_drop_vs_overall: float = 0.10,
    ) -> None:
        self.min_slice_auc = min_slice_auc
        self.min_slice_rows = min_slice_rows
        self.max_drop_vs_overall = max_drop_vs_overall

    def run(self, gate_input: GateInput) -> CheckResult:
        # import mlflow
        import xgboost as xgb
        from mlflow.xgboost import load_model
        from sklearn.metrics import roc_auc_score

        from bmo.training.train import _get_feature_columns

        log.info('slice_parity: loading dataset', path=gate_input.dataset_storage_path)
        df = _load_dataset_for_slicing(gate_input.dataset_storage_path)

        # reproduce the exact test split from train.py
        df_sorted = df.sort_values('event_timestamp').reset_index(drop=True)
        test_start = int(len(df_sorted) * (1 - _TEST_FRACTION))  # index of 80th percentile row
        test_df = df_sorted.iloc[test_start:].copy()  # test set = last 20%

        if len(test_df) < self.min_slice_rows:
            return CheckResult(
                name=self.name,
                passed=True,
                severity=self.severity,
                message=f'test too small ({len(test_df)} rows, slice check skipped)',
                metadata={'test_rows': len(test_df)},
            )

        feature_cols = _get_feature_columns(
            test_df
        )  # filter to numeric features only (rolling avg, etc.)
        X_test = test_df[feature_cols].fillna(0).values  # xgboost requires numbers
        y_test = test_df['is_dep_delayed'].to_numpy(dtype=float)

        log.info('slice_parity: loading model', run_id=gate_input.mlflow_run_id)
        booster: xgb.Booster = load_model(f'runs://{gate_input.mlflow_run_id}/model')
        # convert to XGBoost data type for booster.predict
        dmatrix = xgb.DMatrix(X_test, feature_names=feature_cols)
        # used trained model to generate probabilities (inference on test set)
        y_proba = booster.predict(dmatrix)

        overall_auc = float(roc_auc_score(y_test, y_proba))

        # attach derived slice columns to a working copy of test_df
        sliced = test_df.copy()
        sliced['_proba'] = y_proba
        sliced['_hub_size'] = sliced['origin'].map(_hub_size)
        sliced['_hour_bucket'] = pd.cut(
            sliced['event_timestamp'].dt.hour,
            bins=[-1, 5, 11, 17, 23],
            labels=['late_night', 'morning', 'afternoon', 'evening'],
        )
        # derive bad weather flag from whatever precip/wind columns survived the feature pipeline
        precip_col = next(  # next() finds first instance where condition is met
            (c for c in sliced.columns if 'precip' in c.lower() and 'origin' in c.lower())
        )
        wind_col = next(
            (c for c in sliced.columns if 'wind' in c.lower() and 'origin' in c.lower()),
        )
        bad_weather = pd.Series(False, index=sliced.index)  # initialize series default to False
        if precip_col:
            # merge current bad weather series with True if wind is above 0.1
            bad_weather = bad_weather | (sliced[wind_col].fillna(0) > 0.1)
        if wind_col:
            bad_weather = bad_weather | (sliced[wind_col].fillna(0) > 25)
        sliced['_weather'] = bad_weather.map({True: 'bad_weather', False: 'clear'})

        slice_columns = [
            ('carrier', 'carrier'),
            ('hub_size', '_hub_size'),
            ('hour_bucket', '_hour_bucket'),
            ('weather', '_weather'),
        ]
        slice_results: dict[str, dict[str, Any]] = {}
        # group by slice category -> compute AUC by group
        for dimension_name, col in slice_columns:
            for name, group in sliced.groupby(col, observed=True):
                # name = group name ?? 'UA' or 'AA' for carrier ? 'evening' or 'morning' for hour_bucket ??
                if len(group) < self.min_slice_rows:
                    continue
                if group['is_dep_delayed'].nunique() < 2:
                    continue  # skip if all delayed values are the same (all on time or all delayed)
                # calc AUC for group
                slice_auc = float(
                    roc_auc_score(
                        group['is_dep_delayed'].to_numpy(dtype=float),
                        group['_proba'].to_numpy(),
                    )
                )
                slice_results[f'{dimension_name}={name}'] = {
                    'auc': round(slice_auc, 4),
                    'rows': len(group),
                    'drop_vs_overall': round(overall_auc - slice_auc, 4),
                }

        # check for group AUC below threshold
        failures = {
            k: v
            for k, v in slice_results.items()
            if (v['auc'] < self.min_slice_auc or v['drop_vs_overall'] > self.max_drop_vs_overall)
        }

        passed = len(failures) == 0
        message = (
            f'{len(failures)} slice(s) failed parity: {sorted(failures)}'
            if failures
            else (f'all {len(slice_results)} slices OK (overall_auc={overall_auc:.4f})')
        )
        return CheckResult(
            name=self.name,
            passed=passed,
            severity=self.severity,
            message=message,
            metadata={
                'overall_auc': round(overall_auc, 4),
                'slices_evaluated': len(slice_results),
                'slices_failed': len(failures),
                'test_rows': len(test_df),
                **{f'slice/{k}/auc': v['auc'] for k, v in slice_results.items()},
                **{f'slice/{k}/rows': v['rows'] for k, v in slice_results.items()},
            },
        )


def _load_dataset_for_slicing(storage_path: str) -> pd.DataFrame:
    import pyarrow.parquet as pq

    if storage_path.startswith('s3://'):
        import s3fs

        from bmo.common.config import settings

        fs = s3fs.S3FileSystem(
            key=settings.s3_access_key_id,
            secret=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
        )
        dataset: pd.DataFrame | None = None
        with fs.open(storage_path, 'rb') as f:
            dataset = pq.read_table(f).to_pandas()
        assert dataset is not None
        return dataset

    return cast(pd.DataFrame, pq.read_table(storage_path).to_pandas())


DEFAULT_CHECKS: list[EvalCheck] = [
    AUCGateCheck(),
    LeakageSentinelCheck(),
    CalibrationCheck(),
    SliceParityCheck(),
]
