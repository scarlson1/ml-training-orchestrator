"""
Dagster ConfigurableResource wrapping Feast FeatureStore.

Feast's FeatureStore is initialized from a repo directory containing
feature_store.yaml. The YAML specifies the offline store (S3 Parquet),
online store (Redis), and registry (S3 file or Postgres).

Feast docs: https://docs.feast.dev/reference/feature-repository/feature-store-yaml
"""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource
from feast import FeatureStore
from pydantic import Field


class FeastResource(ConfigurableResource):
    feature_repo_dir: str = Field(
        description='Absolute path to the feature_repo/ directory containing feature_store.yaml'
    )

    def get_store(self) -> FeatureStore:
        """
        Instantiate a Feast FeatureStore pointed at the configured repo.

        FeatureStore reads feature_store.yaml on construction. It is cheap
        to construct — no network calls happen until you call get_online_features
        or materialize.
        """
        return FeatureStore(repo_path=str(Path(self.feature_repo_dir).resolve()))
