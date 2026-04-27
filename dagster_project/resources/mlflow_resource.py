"""
Dagster ConfigurableResource wrapping MLflow.

MLflow's tracking URI is the single config value that tells the client
where to send metrics, artifacts, and model registry calls. By centralizing
it here, every asset that logs to MLflow gets the same tracking server
without each one needing to call mlflow.set_tracking_uri().

MLflow docs: https://mlflow.org/docs/latest/tracking.html
Dagster + MLflow: https://docs.dagster.io/integrations/mlflow
"""

from __future__ import annotations

import mlflow
from dagster import ConfigurableResource
from mlflow.tracking import MlflowClient
from pydantic import Field


class MLflowResource(ConfigurableResource):
    tracking_uri: str = Field(description='MLflow tracking server URI, e.g. http://localhost:5000')

    def get_client(self) -> MlflowClient:
        mlflow.set_tracking_uri(self.tracking_uri)
        return MlflowClient()

    def configure(self) -> None:
        """
        Call this at the top of any asset that logs runs directly via mlflow.*.
        Sets the global tracking URI so mlflow.start_run() and friends know
        where to send data.
        """
        mlflow.set_tracking_uri(self.tracking_uri)
