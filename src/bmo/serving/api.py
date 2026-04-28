"""
FastAPI inference service — online prediction for flight delay probability.

Endpoints:
  GET  /health          — liveness + readiness probe (used by Fly.io health checks)
  GET  /model-info      — current champion model metadata
  POST /predict         — single flight prediction
  POST /admin/reload    — hot-swap champion model without container restart
  GET  /metrics         — Prometheus metrics (scraped by Grafana Cloud)

Shadow deploy:
  Set env var SHADOW_MODEL_VERSION=<registry_version> to activate.
  The primary response is returned immediately. The shadow model runs in a
  FastAPI BackgroundTask so it never adds latency to the caller. Shadow
  predictions are logged to stdout as structured JSON for offline comparison.
  This is a simplified blue/green: one traffic copy, one log comparison step,
  no traffic split infrastructure required.

Fail-closed contract:
  If Feast returns any null feature, the API returns 503 (not 200 with a guess).
  This is enforced in FeatureClient.get_features(). The caller can retry,
  fall back to a heuristic, or surface the degraded state to users.

FastAPI docs: https://fastapi.tiangolo.com/
Prometheus client docs: https://github.com/prometheus/client_python
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import structlog
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    Info,
    generate_latest,
)

from bmo.common.config import settings
from bmo.serving.feature_client import FeatureClient
from bmo.serving.model_loader import ModelLoader
from bmo.serving.schemas import (
    HealthResponse,
    ModelInfoResponse,
    PredictRequest,
    PredictResponse,
    ShadowPrediction,
)

log = structlog.get_logger(__name__)

# ── Environment variables ──────────────────────────────────────────────────────
_MLFLOW_URI = settings.mlflow_tracking_uri
_MODEL_NAME = settings.model_name
_FEATURE_REPO_DIR = settings.feature_repo_dir
_ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN', '')  # required; empty = disabled
_SHADOW_MODEL_VERSION = settings.shadow_model_version  # e.g. '5'

# ── Prometheus metrics ─────────────────────────────────────────────────────────
# Counter: always increases, never decreases. Suitable for "number of X that happened."
# Histogram: samples observations into configurable buckets. Suitable for latency.
# Info: static key-value pairs. Suitable for version labels.
_predict_requests = Counter(
    'bmo_predict_requests_total',
    'Total prediction requests',
    labelnames=['model_version', 'features_complete'],
)
_predict_latency = Histogram(
    'bmo_predict_latency_seconds',
    'End-to-end prediction latency',
    labelnames=['model_version'],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)
_feature_latency = Histogram(
    'bmo_feature_retrieval_latency_seconds',
    'Feast online feature retrieval latency',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)
_fail_closed_count = Counter(
    'bmo_fail_closed_total', 'Requests returned 503 due to missing/stale features'
)
_model_info = Info('bmo_model', 'Current loaded model metadata')

# ----- Application State ----- #
# state shared across requests is stored in app.state - read-only after startup

model_loader: ModelLoader | None = None
shadow_loader: ModelLoader | None = None
feature_client: FeatureClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Runs once on startup (before any requests), once on shutdown.
    Equivalent to Docker's ENTRYPOINT setup + teardown.
    """
    global model_loader, shadow_loader, feature_client

    log.info('serving startup', model_name=_MODEL_NAME, mlflow_uri=_MLFLOW_URI)

    model_loader = ModelLoader(
        tracking_uri=_MLFLOW_URI,
        model_name=_MODEL_NAME,
        alias='champion',
    )
    await model_loader.load()

    _model_info.info(
        {
            'model_name': _MODEL_NAME,
            'version': model_loader.model_version or 'unknown',
        }
    )

    if _SHADOW_MODEL_VERSION:
        log.info('shadow deploy active', shadow_version=_SHADOW_MODEL_VERSION)
        shadow_loader = ModelLoader(
            tracking_uri=_MLFLOW_URI,
            model_name=_MODEL_NAME,
            alias=f'v{_SHADOW_MODEL_VERSION}',  # Feast alias format; or use 'version' below
        )
        # Simpler: load by version number directly
        import mlflow

        mlflow.set_tracking_uri(_MLFLOW_URI)
        shadow_model_uri = f'models:/{_MODEL_NAME}/{_SHADOW_MODEL_VERSION}'
        import mlflow.pyfunc

        shadow_loader._model = mlflow.pyfunc.load_model(shadow_model_uri)
        shadow_loader._model_version = _SHADOW_MODEL_VERSION

    feature_client = FeatureClient(feature_repo_dir=_FEATURE_REPO_DIR)
    log.info('serving ready', model_version=model_loader.model_version)

    yield  # application runs here

    log.info('serving shutdown')


app = FastAPI(
    title='BMO Flight Delay Prediction API',
    description='Online inference service for BMO batch ML training orchestrator.',
    version='1.0.0',
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],  # ["https://my-app.com"],  # or ["*"] for open access
    allow_methods=['POST', 'GET'],
    allow_headers=['Content-Type', 'Authorization'],
)

# ----- Dependency injection helpers ----- #


def get_model_loader() -> ModelLoader:
    if model_loader is None or not model_loader.is_loaded:
        raise HTTPException(status_code=503, detail='Model not loaded')
    return model_loader


def get_feature_client() -> FeatureClient:
    if feature_client is None:
        raise HTTPException(status_code=503, detail='Feature client not initialized')
    return feature_client


def verify_admin_token(authorization: str | None = Header(default=None)) -> None:
    """
    Bearer token guard for /admin endpoints.
    Set ADMIN_TOKEN env var; empty string disables the check (dev mode only).
    """
    if not _ADMIN_TOKEN:
        return  # disabled in dev
    if authorization != f'Bearer {_ADMIN_TOKEN}':
        raise HTTPException(status_code=401, detail='Unauthorized')


# ----- ROUTES ----- #


@app.get('/health', response_model=HealthResponse, tags=['ops'])
async def health(
    loader: ModelLoader = Depends(get_model_loader),
    client: FeatureClient = Depends(get_feature_client),
) -> HealthResponse:
    """
    Liveness + readiness check.
    Fly.io uses this for health checks — if it returns non-2xx, the machine is
    replaced. Redis reachability is checked but not fatal (degraded, not unhealthy).
    """
    redis_ok = await asyncio.to_thread(client.ping_redis)
    return HealthResponse(
        status='health' if redis_ok else 'degraded',
        model_loaded=loader.is_loaded,
        redis_reachable=redis_ok,
        model_version=loader.model_version,
    )


@app.get('/model-info', response_model=ModelInfoResponse, tags=['ops'])
async def model_info(loader: ModelLoader = Depends(get_model_loader)) -> ModelInfoResponse:
    return ModelInfoResponse(
        model_name=_MODEL_NAME,
        model_version=loader.model_version or 'unknown',
        champion_alias='champion',
        loaded_at=loader.loaded_at.isoformat() if loader.loaded_at else '',
        feature_service='flight_delay_predictions',
        shadow_version=_SHADOW_MODEL_VERSION,
    )


@app.post('/predict', response_model=PredictResponse, tags=['inference'])
async def predict(
    request: PredictRequest,
    background_tasks: BackgroundTasks,
    loader: ModelLoader = Depends(get_model_loader),
    client: FeatureClient = Depends(get_feature_client),
) -> PredictResponse:
    """
    Predict departure delay probability for a single flight.

    Flow:
      1. Retrieve online features from Feast/Redis (fail-closed on null)
      2. Run champion model inference
      3. If SHADOW_MODEL_VERSION set: enqueue shadow inference as background task
      4. Return prediction to caller

    The shadow task runs AFTER the response is sent — zero latency impact.
    """
    t_start = time.perf_counter()

    t_feat = time.perf_counter()
    feature_df = await asyncio.to_thread(lambda: client.get_features(request))
    _feature_latency.observe(time.perf_counter() - t_feat)

    if feature_df is None:
        _fail_closed_count.inc()
        _predict_requests.labels(
            model_version=loader.model_version or 'unknown', features_complete='false'
        ).inc()
        raise HTTPException(
            status_code=503,
            detail=(
                f'Features unavailable for flight {request.flight_id}. '
                'One or more Feast feature views returned null. '
                'Retry after the next Feast materialization cycle (hourly).'
            ),
        )

    probas = await loader.predict(feature_df)
    primary_proba = float(probas[0])
    primary_is_delayed = primary_proba >= 0.5

    model_version = loader.model_version or 'unknown'
    _predict_requests.labels(model_version=model_version, features_complete='true').inc()
    _predict_latency.labels(model_version=model_version).observe(time.perf_counter() - t_start)

    if shadow_loader is not None and shadow_loader.is_loaded:
        background_tasks.add_task(
            _run_shadow_prediction,
            request=request,
            feature_df=feature_df.copy(),
            primary_proba=primary_proba,
            primary_version=model_version,
        )

    return PredictResponse(
        flight_id=request.flight_id,
        predicted_is_delayed=primary_is_delayed,
        delay_probability=round(primary_proba, 4),
        model_name=_MODEL_NAME,
        model_version=model_version,
        features_complete=True,
    )


@app.post('/admin/reload', tags=['ops'])
async def reload_model(
    loader: ModelLoader = Depends(get_model_loader),
    _: None = Depends(verify_admin_token),
) -> dict[str, str]:
    """
    Hot-swap the champion model without restarting the container.

    When a new champion is registered (registered_model Dagster asset materializes
    and deployed_api writes a new model_config.json), call this endpoint to load it.

    Protected by Bearer token (ADMIN_TOKEN env var).

    Returns the new model version string.
    """
    new_version = await loader.reload()
    _model_info.info({'model_name': _MODEL_NAME, 'version': new_version})
    log.info('hot-swap complete', new_version=new_version)
    return {'status': 'reloaded', 'model_version': new_version}


@app.get('/metrics', tags=['ops'])
async def metrics() -> Response:
    """Prometheus metrics scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ----- Shadow prediction background task ----- #


async def _run_shadow_prediction(
    request: PredictRequest,
    feature_df: Any,
    primary_proba: float,
    primary_version: str,
) -> None:
    """
    Runs shadow model inference and logs the result as structured JSON.

    Never raises — any exception is caught and logged, so a shadow failure
    can never cause the primary response to fail retroactively.

    The output is a ShadowPrediction logged to stdout. In production,
    Fly.io ships logs to a log aggregator (e.g., Papertrail, Loki).
    Run `SELECT * FROM shadow_predictions WHERE agreed = false ORDER BY scored_at DESC`
    in your log query tool to find disagreements between primary and shadow.
    """
    try:
        if shadow_loader is None:
            return
        shadow_probas = await shadow_loader.predict(feature_df)
        shadow_proba = float(shadow_probas[0])

        result = ShadowPrediction(
            flight_id=request.flight_id,
            primary_version=primary_version,
            shadow_version=shadow_loader.model_version or _SHADOW_MODEL_VERSION or 'unknown',
            primary_proba=round(primary_proba, 4),
            shadow_proba=round(shadow_proba, 4),
            primary_is_delayed=primary_proba >= 0.5,
            shadow_is_delayed=shadow_proba >= 0.5,
            agreed=(primary_proba >= 0.5) == (shadow_proba >= 0.5),
        )
        log.info('shadow_prediction', **result.model_dump())
    except Exception:
        log.exception('shadow prediction failed', flight_id=request.flight_id)
