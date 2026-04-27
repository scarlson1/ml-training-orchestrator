"""
Threshold evaluation for drift-triggered retraining.

This module is intentionally thin. The business rule (PSI > 0.2 → retrain)
is evaluated here so that it can be unit-tested in isolation from Dagster
and from the sensor. The sensor imports this function so the same logic
runs whether drift is detected via the asset or queried from Postgres.
"""

from __future__ import annotations

from bmo.monitoring.drift import PSI_MODERATE, DriftMetricsRow


def should_retain(
    metrics: list[DriftMetricsRow], top_n: int = 10, psi_threshold: float = PSI_MODERATE
) -> tuple[bool, list[str]]:
    """
    Determine whether drift metrics warrant triggering a retrain.

    Args:
        metrics:        List of DriftMetricsRow for one report_date.
        top_n:          Only consider features ranked <= top_n by importance.
                        Lower-ranked features are informational but don't trigger retrains.
        psi_threshold:  PSI above which a feature is "breached". Defaults to PSI_MODERATE (0.2).

    Returns:
        (should_retrain: bool, breached_features: list[str])
        breached_features lists feature names that exceeded the threshold (for logging).
    """
    top_metrics = [m for m in metrics if m.rank <= top_n]
    breached = [m.feature_name for m in top_metrics if m.psi_score > psi_threshold]
    return len(breached) > 0, breached
