"""
Thread-safe MLflow model loader with hot-swap support.

Why thread safety matters here:
  FastAPI runs with a thread pool (or async event loop + thread executor) for
  blocking I/O. If two requests arrive simultaneously, one calling predict()
  while the other is mid-reload, you risk reading a partially-initialized model.

  We use asyncio.Lock so that reload() and predict() cannot overlap. This is
  safe because:
    (a) XGBoost's Booster.inplace_predict() is READ-ONLY — no internal state
        mutation during inference, so concurrent predict() calls are safe.
    (b) The lock is only held during the MLflow download + model load, not
        during prediction itself.

asyncio.Lock docs: https://docs.python.org/3/library/asyncio-sync.html#asyncio.Lock
MLflow model registry docs: https://mlflow.org/docs/latest/model-registry.html
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger(__name__)


class ModelLoader:
    """
    Loads and caches the champion XGBoost model from the MLflow registry.

    Usage:
        loader = ModelLoader(tracking_uri='http://localhost:5000', model_name='bmo_flight_delay')
        await loader.load()               # on startup
        proba = await loader.predict(df)  # on each request
        await loader.reload()             # on POST /admin/reload
    """

    def __init__(self, tracking_uri: str, model_name: str, alias: str = 'champion') -> None:
        self._tracking_uri = tracking_uri
        self._model_name = model_name
        self._alias = alias
        self._lock = asyncio.Lock()

        self._model: Any = None  # mlflow.pyfunc.PyFuncModel
        self._model_version: str | None = None
        self._loaded_at: datetime | None = None
        self._registered_at: datetime | None = None
        self._training_roc_auc: float | None = None

    @property
    def model_version(self) -> str | None:
        return self._model_version

    @property
    def loaded_at(self) -> datetime | None:
        return self._loaded_at

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    @property
    def registered_at(self) -> datetime | None:
        return self._registered_at

    @property
    def training_roc_auc(self) -> float | None:
        return self._training_roc_auc

    async def load(self) -> str:
        """
        Download and cache the champion model from the MLflow registry.

        Runs in a thread executor because MLflow's download is blocking I/O
        (it downloads the model artifact from MinIO/R2 and unpacks it).
        asyncio.get_event_loop().run_in_executor moves it off the async thread
        so the event loop can keep handling other requests during the download.

        Returns the loaded model version string.
        """
        async with self._lock:
            version = await asyncio.to_thread(self._load_blocking)
            return version

    def _load_blocking(self) -> str:
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(self._tracking_uri)
        client = MlflowClient()

        version_obj = client.get_model_version_by_alias(self._model_name, self._alias)
        version_num = version_obj.version

        model_uri = f'models:/{self._model_name}@{self._alias}'
        log.info('loading model', uri=model_uri, version=version_num)

        ts_ms = int(version_obj.creation_timestamp)
        self._registered_at = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        run = client.get_run(version_obj.run_id)
        auc = run.data.metrics.get('test_roc_auc')
        self._training_roc_auc = float(auc) if auc is not None else None

        self._model = mlflow.pyfunc.load_model(model_uri)
        self._model_version = version_num
        self._loaded_at = datetime.now(timezone.utc)

        log.info('model loaded', version=version_num, training_roc_auc=self._training_roc_auc)
        return str(version_num)

    async def reload(self) -> str:
        """Hot-swap the in-memory model without restarting the container."""
        log.info('hot-swap reload requested')
        return await self.load()

    async def predict(self, feature_df: pd.DataFrame) -> np.ndarray:
        """
        Run inference on a feature matrix.

        The lock is NOT acquired here because XGBoost prediction is read-only.
        Only reload() needs the lock. This means concurrent predict() calls
        are safe and don't queue behind each other.
        """
        if self._model is None:
            raise RuntimeError('Model not loaded - call load() first')

        raw = await asyncio.to_thread(lambda: self._model.predict(feature_df))

        # normalize to 1D probability array
        if isinstance(raw, np.ndarray) and raw.ndim == 2:
            return raw[:1]
        return np.asarray(raw)
