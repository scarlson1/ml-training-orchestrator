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
from datetime import date
from typing import TYPE_CHECKING, Any, cast

import duckdb

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
from sqlalchemy import Engine, create_engine, text

from bmo.common.config import settings
from bmo.serving.feature_client import FeatureClient
from bmo.serving.model_loader import ModelLoader
from bmo.serving.schemas import (
    AccuracyResponse,
    AccuracyRow,
    DriftHeatmapResponse,
    DriftHeatmapRow,
    DriftMetricRow,
    DriftResponse,
    DriftSummaryResponse,
    DriftSummaryRow,
    HealthResponse,
    ModelInfoResponse,
    ModelRow,
    ModelStatsResponse,
    PredictionRow,
    PredictionsDayResponse,
    PredictionsResponse,
    PredictRequest,
    PredictResponse,
    PsiResponse,
    PsiRow,
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
    try:
        await model_loader.load()
    except Exception as exc:
        log.warning(
            'no champion model found at startup — serving will be degraded until a model is registered',
            error=str(exc),
        )

    _model_info.info(
        {
            'model_name': _MODEL_NAME,
            'version': model_loader.model_version or 'unknown',
        }
    )

    if _SHADOW_MODEL_VERSION:
        try:
            log.info('shadow deploy active', shadow_version=_SHADOW_MODEL_VERSION)
            shadow_loader = ModelLoader(
                tracking_uri=_MLFLOW_URI,
                model_name=_MODEL_NAME,
                alias=f'v{_SHADOW_MODEL_VERSION}',
            )
            # Load by version number directly; shadow is optional in dev.
            import mlflow
            import mlflow.pyfunc

            mlflow.set_tracking_uri(_MLFLOW_URI)
            shadow_model_uri = f'models:/{_MODEL_NAME}/{_SHADOW_MODEL_VERSION}'
            shadow_loader._model = mlflow.pyfunc.load_model(shadow_model_uri)
            shadow_loader._model_version = _SHADOW_MODEL_VERSION
        except Exception as exc:
            shadow_loader = None
            log.warning(
                'shadow model unavailable at startup — continuing without shadow deploy',
                shadow_version=_SHADOW_MODEL_VERSION,
                error=str(exc),
            )

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


# ----- Postgres dependency ----- #
_pg_engine: Engine | None = None


def get_db() -> Engine:
    global _pg_engine
    if _pg_engine is None:
        _pg_engine = create_engine(settings.postgres_url, pool_pre_ping=True)
    return _pg_engine


# Don't pool DuckDB connections — they hold a file lock.
# Open read-only per request; Dagster's write connection can coexist.
def get_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(settings.duckdb_path, read_only=True)


# ----- React API ----- #


@app.get('/api/drift/summary', response_model=DriftSummaryResponse, tags=['api'])
async def driftsummary(db: Engine = Depends(get_db)) -> DriftSummaryResponse:
    """Calc drift aggregates for breach banner"""

    _query = text("""
        SELECT
            report_date::text,
            COUNT(*) FILTER (WHERE is_breached) AS n_breached,
            COUNT(*)                            AS n_features,
            ROUND(MAX(psi_score)::numeric, 4)   AS max_psi,
            model_version
        FROM drift_metrics
        WHERE report_date = (SELECT MAX(report_date) FROM drift_metrics)
        GROUP BY report_date, model_version
    """)

    with db.connect() as conn:
        rows = conn.execute(_query).mappings().all()

        data = [DriftSummaryRow(**r) for r in rows]
        return DriftSummaryResponse(rows=data)


@app.get('/api/drift/metrics', response_model=DriftResponse, tags=['api'])
async def drift(
    start: date | None = None, end: date | None = None, db: Engine = Depends(get_db)
) -> DriftResponse:
    """Drift metrics for React dashboard. Defaults to 30 most recent report dates when no range is provided. Query monitoring table (drift_metrics) in Postgres"""

    _query = text("""
                SELECT
                    report_date::text,
                    feature_name,
                    psi_score,
                    kl_divergence,
                    rank,
                    is_breached,
                    model_version
                FROM drift_metrics
                WHERE report_date BETWEEN
                    COALESCE(:start, (SELECT MAX(report_date) FROM drift_metrics) - INTERVAL '30 days')
                    AND
                    COALESCE(:end, (SELECT MAX(report_date) FROM drift_metrics))
                ORDER BY report_date DESC, rank ASC
            """)

    with db.connect() as conn:
        rows = (
            conn.execute(
                _query,
                {'start': start, 'end': end},
            )
            .mappings()
            .all()
        )

    data = [DriftMetricRow(**r) for r in rows]
    latest = data[0].report_date if data else str(date.today())
    return DriftResponse(
        rows=data,
        report_date=latest,
        n_breached=sum(1 for r in data if r.is_breached and r.report_date == latest),
    )


@app.get('/api/model-stats', response_model=ModelStatsResponse, tags=['api'])
async def modelstats(db: Engine = Depends(get_db)) -> ModelStatsResponse:
    """All versions of models with AUC Query monitoring table (live_accuracy) in Postgres"""

    _query = text("""
                SELECT
                    model_version,
                    AVG(roc_auc) AS avg_roc_auc,
                    MAX(score_date) AS last_scored,
                    SUM(n_flights) AS total_flights_scored
                FROM live_accuracy
                GROUP BY model_version
                ORDER BY MAX(score_date) DESC
                """)

    with db.connect() as conn:
        rows = conn.execute(_query).mappings().all()

        data = [
            ModelRow(
                model_version=r.model_version,
                avg_roc_auc=r.avg_roc_auc,
                last_scored=r.last_scored,
                n_flights=r.total_flights_scored,
            )
            for r in rows
        ]
        return ModelStatsResponse(rows=data)


@app.get('/api/drift/heatmap', response_model=DriftHeatmapResponse, tags=['api'])
async def driftheatmap(
    start_date: date | None = None, end_date: date | None = None, db: Engine = Depends(get_db)
) -> DriftHeatmapResponse:
    """features x dates. Query monitoring table (drift_metrics) in Postgres"""

    _query = text("""
                SELECT
                    report_date,
                    feature_name,
                    psi_score,
                    kl_divergence,
                    rank,
                    is_breached,
                    model_version
                FROM drift_metrics
                WHERE report_date BETWEEN :start_date AND :end_date
                ORDER BY report_date DESC, rank ASC
                """)

    with db.connect() as conn:
        rows = (
            conn.execute(
                _query,
                {'start_date': start_date, 'end_date': end_date},
            )
            .mappings()
            .all()
        )

        data = [DriftHeatmapRow(**r) for r in rows]
        return DriftHeatmapResponse(rows=data)


@app.get('/api/psi/:feature_name', response_model=PsiResponse, tags=['api'])
async def psi(feature_name: str, db: Engine = Depends(get_db)) -> PsiResponse:
    """per-feature PSI time series. Query monitoring table (drift_metrics) in Postgres"""

    _query = text("""
                SELECT
                    report_date,
                    psi_score,
                    kl_divergence,
                    is_breached
                FROM drift_metrics
                WHERE feature_name = :feature_name
                ORDER BY report_date
                """)

    with db.connect() as conn:
        rows = (
            conn.execute(
                _query,
                {'feature_name': feature_name},
            )
            .mappings()
            .all()
        )

        data = [PsiRow(**r) for r in rows]
        return PsiResponse(rows=data)


@app.get('/api/accuracy', response_model=AccuracyResponse, tags=['api'])
async def accuracy(db: Engine = Depends(get_db)) -> AccuracyResponse:
    """Live accuracy time series. Query monitoring table (live_accuracy) in Postgres"""

    _query = text("""
                SELECT
                    score_date,
                    model_version,
                    roc_auc,
                    f1,
                    precision_score,
                    recall_score,
                    brier_score,
                    positive_rate,
                    actual_positive_rate,
                    n_with_actuals
                FROM live_accuracy
                WHERE score_date >= NOW() - INTERVAL '90 days'
                ORDER BY score_date, model_version
                """)

    with db.connect() as conn:
        rows = conn.execute(_query).mappings().all()

        data = [AccuracyRow(**r) for r in rows]
        return AccuracyResponse(rows=data)


@app.get('/api/predictions', response_model=PredictionsResponse, tags=['api'])
async def predictions(
    days: int = 30, con: duckdb.DuckDBPyConnection = Depends(get_duckdb)
) -> PredictionsResponse:
    """
    mart_predictions is a DuckDB view, so this needs DuckDB, not Postgres.
    Queries mart_predictions in S3 storage.

    DuckDB connections hold a file lock and aren't thread-safe, so read_only=True and asyncio.to_thread are used to avoid blocking the event loop (DuckDB api is synchronous ==> will block fast api event loop).
    """

    def _query() -> list[dict[str, Any]]:
        try:
            # to_dict('records') returns list[dict[Hashable, Any]] in pandas stubs
            # but column names are always strings, so cast is safe.
            rows = cast(
                list[dict[str, Any]],
                con.execute(
                    """
                    SELECT
                        score_date::text                               AS score_date,
                        model_version,
                        COUNT(*)                                       AS n_flights,
                        AVG(predicted_is_delayed::int)                 AS positive_rate,
                        COUNT(*) FILTER (WHERE actual_is_delayed IS NOT NULL) AS n_with_actuals
                    FROM mart_predictions
                    WHERE score_date >= CURRENT_DATE - INTERVAL (? || ' days')
                    GROUP BY score_date, model_version
                    ORDER BY score_date DESC
                    """,
                    [days],
                )
                .df()
                .to_dict('records'),
            )
        finally:
            con.close()
        return rows

    rows = await asyncio.to_thread(_query)
    return PredictionsResponse(rows=[PredictionRow(**r) for r in rows])


@app.get('/api/predictions/today', response_model=PredictionsDayResponse, tags=['api'])
async def preditionstoday(
    duck: duckdb.DuckDBPyConnection = Depends(get_duckdb),
) -> PredictionsDayResponse:
    """Get predictions for today for dashboard"""

    def _query() -> tuple[int, float] | None:
        try:
            row = duck.execute(
                """
                SELECT
                    COUNT(*)                               AS n_flights,
                    AVG(predicted_is_delayed::int)         AS positive_rate,
                FROM mart_predictions
                WHERE score_date = CURRENT_DATE
                """,
            ).fetchone()
            return (row[0], row[1]) if row is not None else None
        finally:
            duck.close()

    today = await asyncio.to_thread(_query)

    return PredictionsDayResponse(
        model_version=model_loader.model_version
        if model_loader and model_loader.model_version
        else None,
        model_loaded_at=model_loader.loaded_at.isoformat()
        if model_loader and model_loader.loaded_at
        else '',
        n_flights_today=today[0] if today else 0,
        positive_rate_today=round(today[1], 4) if today and today[1] is not None else None,
    )
