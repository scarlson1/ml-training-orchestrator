"""
Redundant data leakage test to ensure data that cannot be present during inference is not present during training.

Complement the PIT join — they don't replace it. The PIT join
prevents leakage structurally (by never looking forward in time). The guards
catch cases where the structural prevention failed or where non-temporal
leakage could be present.

Each guard returns a list of LeakageViolation objects. An empty list means
no violations detected. Guards are independent; all run regardless of
whether earlier guards found violations.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

import pandas as pd
from pydantic import BaseModel


class LeakageViolation(BaseModel):
    """One detected leakage violation"""

    check_name: str
    severity: Literal['error', 'warning']
    details: str
    affected_rows: int = 0
    affected_columns: list[str] = []


class LeakageGuardResult(BaseModel):
    """Aggregated result from running all guards"""

    passed: bool
    violations: list[LeakageViolation]
    rows_checked: int

    @property
    def errors(self) -> list[LeakageViolation]:
        return [v for v in self.violations if v.severity == 'error']

    @property
    def warnings(self) -> list[LeakageViolation]:
        return [v for v in self.violations if v.severity == 'warning']


def guard_event_timestamps_bounded(
    label_df: pd.DataFrame, as_of: datetime | None
) -> list[LeakageViolation]:
    """
    Guard 1: Event timestamps must not exceed as_of.

    If as_of is None, this guard is skipped (unbounded datasets are valid
    for development but should always have as_of set in production).

    Why: if a training label has event_timestamp = tomorrow, the model is
    being trained on an event that hasn't happened yet. This usually means
    a timezone bug (e.g., UTC vs. local time confusion converting timestamps).
    """
    if as_of is None:
        return [
            LeakageViolation(
                check_name='event_timestamps_bounded',
                severity='warning',
                details='as_of is None - dataset has no temporal upper bound. '
                'Set as_of in production to ensure reproducibility',
            )
        ]

    as_of_ts = pd.Timestamp(as_of, tz='UTC')
    event_ts = pd.to_datetime(label_df['event_timestamp'], utc=True)
    future_mask = event_ts > as_of_ts
    future_count = int(future_mask.sum())

    if future_count == 0:
        return []

    return [
        LeakageViolation(
            check_name='event_timestamps_bounded',
            severity='error',
            details=f'{future_count} label rows have event_timestamp > as_of ({as_of.isoformat()}). '
            f'These flights have not yet departed and must be excluded from training.',
            affected_rows=future_count,
        )
    ]


def guard_no_future_features(
    dataset_df: pd.DataFrame,
    feature_ts_columns: list[str],
) -> list[LeakageViolation]:
    """
    Guard 2: Feature snapshot timestamps must not exceed event_timestamp.

    The ASOF JOIN should guarantee this, but this guard provides an explicit
    check. If it fires, it means either:
      a) The ASOF JOIN implementation has a bug.
      b) A feature Parquet was written with incorrect timestamps.
      c) The as_of filter was not applied correctly.

    The feature_ts_columns are the '{view_name}__feature_ts' columns appended
    by PITJoiner.join(). If the PITJoiner didn't add them, this guard is skipped.
    """
    violations = []
    event_ts = pd.to_datetime(dataset_df['event_timestamp'], utc=True)

    for fts_col in feature_ts_columns:
        if fts_col not in dataset_df.columns:
            continue

        fts = pd.to_datetime(dataset_df[fts_col], utc=True, errors='coerce')
        non_null_mask = fts.notna()
        # Compare only rows where a feature was actually retrieved
        future_mask = (fts > event_ts) & non_null_mask
        future_count = int(future_mask.sum())

        if future_count > 0:
            view_name = fts_col.replace('__feature_ts', '')
            violations.append(
                LeakageViolation(
                    check_name='no_future_features',
                    severity='error',
                    details=f'Feature view "{view_name}": {future_count} rows have '
                    f'feature_ts > event_timestamp. The PIT join returned future values. '
                    f'This is a critical bug — investigate the ASOF JOIN logic.',
                    affected_rows=future_count,
                    affected_columns=[fts_col],
                )
            )

    return violations


def guard_ttl_compliance(
    dataset_df: pd.DataFrame,
    feature_ts_columns: list[str],
    ttl_seconds: dict[str, int],
) -> list[LeakageViolation]:
    """
    Guard 3: Feature snapshot age must not exceed TTL.

    Feast's TTL applies to ONLINE serving (get_online_features returns null if
    stale). For OFFLINE retrieval (get_historical_features / our ASOF JOIN),
    the join returns the nearest value even if it's ancient. Without this guard,
    you could train a model using a carrier's on-time rate from 3 months ago
    for a flight that departed today — the feature was 'available' but stale.

    This guard WARNS (not errors) because stale features are acceptable in
    some periods (e.g., sparse data at the beginning of a backfill). Training
    code can optionally impute NULLs or leave them for the model to handle.

    The view_name is extracted from the '{view_name}__feature_ts' column name.
    The TTL dict is keyed by view_name.
    """
    violations = []
    event_ts = pd.to_datetime(dataset_df['event_timestamp'], utc=True)

    for fts_col in feature_ts_columns:
        if fts_col not in dataset_df.columns:
            continue

        view_name = fts_col.replace('__feature_ts', '')
        if view_name not in ttl_seconds:
            continue

        fts = pd.to_datetime(dataset_df[fts_col], utc=True, errors='coerce')
        non_null_mask = fts.notna()
        age_seconds = (event_ts - fts).dt.total_seconds()
        ttl = ttl_seconds[view_name]
        stale_mask = (age_seconds > ttl) & non_null_mask
        stale_count = int(stale_mask.sum())
        stale_pct = stale_count / len(dataset_df) * 100

        if stale_count > 0:
            violations.append(
                LeakageViolation(
                    check_name='ttl_compliance',
                    severity='warning',
                    details=f'Feature view "{view_name}": {stale_count} rows ({stale_pct:.1f}%) '
                    f'have feature age > TTL ({ttl}s). These features were set to NULL '
                    f'by the PITJoiner TTL mask. If this percentage is high (>10%), '
                    f'investigate whether the feature pipeline has gaps.',
                    affected_rows=stale_count,
                    affected_columns=[fts_col],
                )
            )

    return violations


# Column name patterns that are strongly associated with leakage.
# These are outcome columns that describe what HAPPENED to the flight,
# not what was KNOWN before the flight departed.
_KNOWN_LEAKY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r'^actual_(dep|arr)_.*'),
    re.compile(r'^(dep|arr)_delay_min$'),  # the actual delay — this IS the label
    re.compile(r'^cancelled$'),
    re.compile(r'^diverted$'),
    re.compile(r'^wheels_(off|on).*'),
    re.compile(r'^taxi_(out|in).*'),
    re.compile(r'^air_time.*'),
    re.compile(r'^elapsed_time.*'),
]


def guard_no_target_leakage(
    feature_refs: list[str],
    label_columns: list[str],
) -> list[LeakageViolation]:
    """
    Guard 4: Feature columns must not overlap with label columns or known outcome columns.

    This catches two kinds of mistakes:
      a) Direct overlap: 'dep_delay_min' appears in both feature_refs and label_columns.
         This is obvious once spotted but easy to introduce via copy-paste.
      b) Pattern match: a feature column matches a known-leaky pattern even if it's
         not in label_columns (e.g., 'actual_dep_delay_ma7' — a moving average of
         the actual delay, computed from historical data, sounds innocent but encodes
         outcome information if computed incorrectly).

    Pattern matching errs on the side of false positives and uses 'warning' severity
    to avoid breaking pipelines on legitimate feature names that happen to match.
    """
    violations = []

    # Strip the 'view:' prefix from feature refs to get bare column names
    bare_feature_names = {ref.split(':')[-1] for ref in feature_refs}
    label_set = set(label_columns)

    # Check direct overlap
    overlap = bare_feature_names & label_set
    if overlap:
        violations.append(
            LeakageViolation(
                check_name='no_target_leakage',
                severity='error',
                details=f'Feature columns overlap with label columns: {sorted(overlap)}. '
                f'These features directly encode the training target. Remove them '
                f'from feature_refs or rename the label columns.',
                affected_columns=sorted(overlap),
            )
        )

    # Check pattern match for known leaky patterns
    suspicious = [
        name
        for name in bare_feature_names
        if any(pat.match(name) for pat in _KNOWN_LEAKY_PATTERNS)
        and name not in label_set  # direct overlaps already reported above
    ]
    if suspicious:
        violations.append(
            LeakageViolation(
                check_name='no_target_leakage',
                severity='warning',
                details=f'Feature columns match known-leaky patterns: {sorted(suspicious)}. '
                f'Review these carefully to confirm they are computed from data '
                f'available BEFORE the scheduled departure time.',
                affected_columns=sorted(suspicious),
            )
        )

    return violations


def run_all_guards(
    label_df: pd.DataFrame,
    dataset_df: pd.DataFrame,
    feature_refs: list[str],
    label_columns: list[str],
    feature_ts_columns: list[str],
    ttl_seconds: dict[str, int],
    as_of: datetime | None,
) -> LeakageGuardResult:
    """
    Run all four guards and aggregate results.

    All guards run regardless of whether earlier guards found violations.
    This gives you a complete picture of all problems in one pass rather than
    fixing one issue and discovering the next on the next run.
    """
    all_violations: list[LeakageViolation] = []

    all_violations.extend(guard_event_timestamps_bounded(label_df, as_of))
    all_violations.extend(guard_no_future_features(dataset_df, feature_ts_columns))
    all_violations.extend(guard_ttl_compliance(dataset_df, feature_ts_columns, ttl_seconds))
    all_violations.extend(guard_no_target_leakage(feature_refs, label_columns))

    has_errors = any(v.severity == 'error' for v in all_violations)

    return LeakageGuardResult(
        passed=not has_errors,
        violations=all_violations,
        rows_checked=len(dataset_df),
    )
