"""
Dataset handle: content-addressed identifier for a training dataset.

A DatasetHandle is the receipt you get after build_dataset() completes.
It encodes exactly what went into the dataset so that:
  - Future training runs can verify they used identical inputs.
  - MLflow run parameters include the hash → traceable from model to data.
  - Any discrepancy in the hash reveals which input changed.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field


class LabelDistribution(BaseModel):
    """Descriptive statistics for one target column."""

    target_column: str
    mean: float
    std: float
    min: float
    max: float
    positive_rate: float | None = None  # binary targets only; None for regression


class DatasetHandle(BaseModel):
    """
    Immutable, content-addressed descriptor for a point-in-time training dataset.

    The version_hash encodes: sorted feature refs + as_of + label content +
    feature registry version + code version. Identical inputs → identical hash.

    Store this as a JSON sidecar next to the Parquet file and log it as an
    MLflow artifact on every training run.
    """

    version_hash: str = Field(description='SHA-256 of all dataset inputs; stable identifier')
    feature_refs: list[str] = Field(description='Sorted list of "view:feature" strings')
    feature_set_version: str = Field(description='Feast registry hash or git SHA of feature_repo/')
    feature_ttls: dict[str, int] = Field(description='feature_view_name -> TTL in seconds')
    as_of: datetime | None = Field(
        default=None, description='Upper bound on feature and label timestamps. None = unbound'
    )
    row_count: int = Field(description='Number of rows in the final dataset Parquet')
    label_distribution: dict[str, LabelDistribution] = Field(
        description='Per-target descriptive stats; logged to MLflow for quick sanity checks'
    )
    schema_fingerprint: str = Field(
        description='SHA-256 of column names + dtypes; changes when feature schema evolves'
    )
    created_at: datetime
    storage_path: str = Field(description='S3 URI of the dataset Parquet file')


def compute_dataset_hash(
    label_df: pd.DataFrame,
    feature_refs: list[str],
    as_of: datetime | None,
    feature_set_version: str,
    code_version: str,
) -> str:
    """
    Compute a stable SHA-256 hash identifying this exact dataset configuration.

    The hash is a function of WHAT was requested, not WHAT was returned. This
    means you can compute the hash before running the join, and use it as the
    output path (write the dataset to s3://datasets/{hash}/data.parquet). If
    the path already exists, skip the expensive join — the dataset is cached.

    Inputs:
        label_df         — the label events (entity keys + event_ts + targets)
        feature_refs     — the feature columns requested (sorted for stability)
        as_of            — the data cutoff timestamp
        feature_set_version — hash of the Feast registry or feature_repo/ git tree
        code_version     — git SHA of HEAD; pins the pipeline code version
    """
    sorted_refs = sorted(feature_refs)

    # Hash the label data by content, not by object identity.
    # Sort all columns so row-order differences don't produce different hashes.
    # We hash the Parquet bytes (not JSON) because Parquet encoding is stable
    # for the same data, whereas JSON float representation can vary by platform.
    label_cols = sorted(label_df.columns.tolist())
    label_bytes = label_df[label_cols].sort_values(label_cols).to_parquet(index=False)
    label_hash = hashlib.sha256(label_bytes).hexdigest()

    payload = {
        'feature_refs': sorted_refs,
        'as_of': as_of.isoformat() if as_of else 'unbounded',
        'feature_set_version': feature_set_version,
        'label_hash': label_hash,
        'code_version': code_version,
    }

    payload_bytes = json.dumps(payload, sort_keys=True).encode('utf-8')
    return hashlib.sha256(payload_bytes).hexdigest()


def compute_schema_fingerprint(df: pd.DataFrame) -> str:
    """
    Hash column names and their dtypes.

    This fingerprint changes whenever:
      - A feature column is added or removed
      - A feature dtype changes (e.g., Float32 → Float64 after schema migration)

    Log it alongside the dataset hash so you can quickly detect when a
    'same hash' dataset was accidentally re-exported with a different schema.
    This should never happen, but it's cheap insurance.
    """
    # repr() of a numpy dtype is stable across platforms
    schema = {col: repr(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    schema_bytes = json.dumps(schema, sort_keys=True).encode('utf-8')
    return hashlib.sha256(schema_bytes).hexdigest()


def compute_label_distributions(
    df: pd.DataFrame, label_columns: list[str]
) -> dict[str, LabelDistribution]:
    """
    Compute descriptive statistics for each label column.

    Why this matters: label distribution shift is one of the earliest signals
    of a data pipeline bug. If 'positive_rate' drops from 20% to 2%, something
    upstream changed (rejection filter too aggressive, timezone bug, join failure).
    Logging this on every build_dataset() call makes drift visible immediately.
    """
    distributions: dict[str, LabelDistribution] = {}
    for col in label_columns:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        is_binary = set(series.unique()).issubset({0, 1, True, False, 0.0, 1.0})
        distributions[col] = LabelDistribution(
            target_column=col,
            mean=float(series.mean()),
            std=float(series.std()),
            min=float(series.min()),
            max=float(series.max()),
            positive_rate=float(series.mean()) if is_binary else None,
        )
    return distributions
