"""
Drift detection: PSI, KL divergence, and Evidently HTML report generation.

Concepts
--------
PSI (Population Stability Index)
    The standard production drift metric from credit risk modeling.
    Bins both reference (training) and current (production) distributions,
    then computes a signed log-ratio sum across bins.

    Thresholds:
        PSI < 0.1   → stable
        PSI 0.1-0.2 → moderate shift
        PSI > 0.2   → significant drift — trigger retrain

KL Divergence D_KL(current || reference)
    Information-theoretic measure. Unbounded; use for relative comparison only.

Evidently
    Generates a self-contained HTML report with distribution histograms and
    per-column drift test results. We specify PSI as the statistical test so
    the HTML report and the Postgres rows use the same algorithm.

    Docs: https://docs.evidentlyai.com/reference/all-metrics
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import structlog
from pydantic import BaseModel

log = structlog.get_logger(__name__)

PSI_NO_CHANGE = 0.1
PSI_MODERATE = 0.2  # sensor threshold
PSI_SEVERE = 0.4


class DriftMetricsRow(BaseModel):
    """One row written to the drift_metrics Postgres table per feature per day."""

    report_date: date
    feature_name: str
    psi_score: float
    kl_divergence: float | None
    rank: int  # 1 = most important feature by training importance
    is_breached: bool  # PSI > PSI_MODERATE
    model_version: str | None = None
    computed_at: datetime


class DriftReportResult(BaseModel):
    """Return value from compute_drift()."""

    report_date: date
    n_features_checked: int
    n_features_breached: int
    max_psi: float
    html_report: str  # full standalone Evidently HTML — written to S3 for GitHub Pages
    metrics: list[DriftMetricsRow]


def compute_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    feature_columns: list[str],
    feature_importance: dict[str, float],
    report_date: date,
    psi_threshold: float = PSI_MODERATE,
) -> DriftReportResult:
    """
    Compute feature distribution drift between reference (training) and current
    (production) datasets.

    Args:
        reference_df:       Training dataset features. Defines what "normal" looks like.
        current_df:         Recent production feature values (last 7 days of predictions).
        feature_columns:    Ordered list of feature column names.
        feature_importance: {feature_name: importance_score} from the trained model.
                            Used to rank features so the sensor checks top-N by importance.
        report_date:        Partition date for this report (written to Postgres + S3 path).
        psi_threshold:      PSI above which a feature is "breached" (default 0.2).

    Returns:
        DriftReportResult with Evidently HTML and per-feature PSI/KL rows.
    """
    valid_cols = [
        c for c in feature_columns if c in reference_df.columns and c in current_df.columns
    ]

    if not valid_cols:
        log.warning('no valid feature columns for drift computation')
        return DriftReportResult(
            report_date=report_date,
            n_features_checked=0,
            n_features_breached=0,
            max_psi=0.0,
            html_report='<html><body>No features available.</body></html>',
            metrics=[],
        )

    ref_feat = reference_df[valid_cols].copy()
    cur_feat = current_df[valid_cols].copy()

    sorted_features = sorted(valid_cols, key=lambda f: feature_importance.get(f, 0.0), reverse=True)

    # ── Evidently HTML report ──────────────────────────────────────────────────
    # DataDriftPreset runs PSI for every column and renders an HTML report with:
    #   - overall dataset drift (share of drifted columns)
    #   - per-column distribution histograms (reference vs current)
    #   - per-column drift score and test result
    #
    # stattest='psi' uses PSI as the statistical test for all numeric columns.
    # stattest_threshold=psi_threshold marks a column as "drifted" when PSI >= threshold.
    html_report = _generate_evidently_html(ref_feat, cur_feat, psi_threshold)

    # recompute PSI independently - no dependency on evidently
    computed_at = datetime.now(timezone.utc)
    metrics: list[DriftMetricsRow] = []

    for rank, feature_name in enumerate(sorted_features, start=1):
        ref_vals = ref_feat[feature_name].dropna().to_numpy()
        cur_vals = cur_feat[feature_name].dropna().to_numpy()

        if len(ref_vals) < 10 or len(cur_vals) < 10:
            log.debug('feature_name - insufficient data', feature=feature_name)
            continue

        psi = _compute_psi(ref_vals, cur_vals)
        kl = _compute_kl_divergence(ref_vals, cur_vals)

        metrics.append(
            DriftMetricsRow(
                report_date=report_date,
                feature_name=feature_name,
                psi_score=round(psi, 6),
                kl_divergence=round(kl, 6),
                rank=rank,
                is_breached=psi > psi_threshold,
                computed_at=computed_at,
            )
        )

    n_breached = sum(1 for m in metrics if m.is_breached)
    max_psi = max((m.psi_score for m in metrics), default=0.0)

    log.info(
        'drift computed',
        n_features=len(metrics),
        n_breached=n_breached,
        max_psi=round(max_psi, 4),
        report_date=str(report_date),
    )

    return DriftReportResult(
        report_date=report_date,
        n_features_checked=len(metrics),
        n_features_breached=n_breached,
        max_psi=max_psi,
        html_report=html_report,
        metrics=metrics,
    )


# ── Private helpers ────────────────────────────────────────────────────────────


def _compute_psi(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """
    Population Stability Index.

    PSI = Σ (A_i − E_i) × ln(A_i / E_i)

    Bin edges are derived from reference percentiles so that each bin contains
    roughly equal probability mass under the reference distribution.
    This is better than fixed-width bins because it's invariant to the scale
    of the feature and gives uniform sensitivity across the distribution.

    Edge cases handled:
      - Duplicate bin edges (constant or near-constant features): merged via np.unique
      - Empty bins: replaced with epsilon before log to avoid ln(0)
      - Fewer than 2 unique edges (perfectly constant feature): returns 0.0

    Reference: https://www.listendata.com/2015/05/population-stability-index.html
    """
    # Percentile-based bin edges from reference distribution
    percentiles = np.linspace(0, 100, n_bins + 1)
    bin_edges = np.unique(np.percentile(reference, percentiles))

    if len(bin_edges) < 2:
        return 0.0  # constant feature — no drift possible

    # Clip current values to reference bin range to avoid out-of-range bins
    # (values outside the training range are assigned to the nearest boundary bin)
    current_clipped = np.clip(current, bin_edges[0], bin_edges[-1])

    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current_clipped, bins=bin_edges)

    n_bins_actual = len(ref_counts)
    epsilon = 1e-4  # small constant to avoid ln(0) when a bin is empty

    ref_pct = (ref_counts + epsilon) / (len(reference) + n_bins_actual * epsilon)
    cur_pct = (cur_counts + epsilon) / (len(current) + n_bins_actual * epsilon)

    psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))
    return max(psi, 0.0)  # PSI is always non-negative; small negatives are floating-point noise


def _compute_kl_divergence(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """
    KL divergence D_KL(current || reference).

    D_KL(P || Q) = Σ P(x) × log(P(x) / Q(x))

    Asymmetric: measures how much information is lost when using the reference
    distribution Q as an approximation for the current distribution P.

    Note: KL divergence is unbounded above. It should NOT be used for absolute
    alerting thresholds (use PSI for that). It is logged alongside PSI to enable
    relative comparisons between features.
    """
    bin_edges = np.unique(np.percentile(reference, np.linspace(0, 100, n_bins + 1)))

    if len(bin_edges) < 2:
        return 0.0

    current_clipped = np.clip(current, bin_edges[0], bin_edges[-1])

    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current_clipped, bins=bin_edges)

    n_bins_actual = len(ref_counts)
    epsilon = 1e-4

    ref_pct = (ref_counts + epsilon) / (len(reference) + n_bins_actual * epsilon)
    cur_pct = (cur_counts + epsilon) / (len(current) + n_bins_actual * epsilon)

    kl = float(np.sum(cur_pct * np.log(cur_pct / ref_pct)))
    return kl


def _generate_evidently_html(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    psi_threshold: float,
) -> str:
    """
    Generate a standalone Evidently drift report HTML string.

    Uses DataDriftPreset with PSI as the statistical test. The report includes:
      - Overall drift share (fraction of columns that drifted)
      - Per-column: reference vs current distribution histogram
      - Per-column: drift score and whether threshold was exceeded

    Evidently >= 0.4 is required. Uses save_html() to a temp file rather than
    get_html() for compatibility across minor versions.

    Docs: https://docs.evidentlyai.com/presets/data-drift
    """
    from evidently.metric_preset import DataDriftPreset  # pyright: ignore[reportMissingImports]
    from evidently.report import Report  # pyright: ignore[reportMissingImports]

    report = Report(metrics=[DataDriftPreset(stattest='psi', stattest_threshold=psi_threshold)])
    report.run(reference_data=reference_df, current_data=current_df)

    with tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w') as f:
        tmp_path = f.name

    try:
        report.save_html(tmp_path)
        with open(tmp_path) as f:
            html = f.read()
    finally:
        os.unlink(tmp_path)

    return html
