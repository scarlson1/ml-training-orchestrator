"""
Request/response Pydantic models for the FastAPI serving API.

These models serve as the API contract — they validate inputs and shape outputs.
Using Pydantic v2 here means FastAPI generates accurate OpenAPI docs automatically.

FastAPI docs: https://fastapi.tiangolo.com/tutorial/body/
Pydantic v2 docs: https://docs.pydantic.dev/latest/
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    """
    Entity keys for a single flight prediction request.

    All entity keys are required. The API fails closed if any required key is
    missing — partial entity keys produce inconsistent feature lookups in Feast,
    so it's safer to reject the request than to silently omit a feature group.

    field examples below are used by FastAPI to generate OpenAPI docs.
    """

    flight_id: str = Field(json_schema_extra={'example': 'AA123_20240406_0900'})
    origin: str = Field(
        description='IATA origin airport code', json_schema_extra={'example': 'ORD'}
    )
    dest: str = Field(
        description='IATA destination airport code', json_schema_extra={'example': 'LAX'}
    )
    carrier: str = Field(description='BTS carrier code', json_schema_extra={'example': 'AA'})
    tail_number: str = Field(
        description='Aircraft tail number', json_schema_extra={'example': 'N12345'}
    )
    route_key: str = Field(
        description='Composite route key: "{origin}-{dest}"',
        json_schema_extra={'example': 'ORD-LAX'},
    )


class PredictResponse(BaseModel):
    flight_id: str
    predicted_is_delayed: bool
    delay_probability: float = Field(ge=0.0, le=1.0)
    model_name: str
    model_version: str
    features_complete: bool  # false when any feature was null


class ShadowPrediction(BaseModel):
    """Logged asynchronously when SHADOW_MODEL_VERSION is set. Never returned to caller."""

    flight_id: str
    primary_version: str
    shadow_version: str
    primary_proba: float
    shadow_proba: float
    primary_is_delayed: bool
    shadow_is_delayed: bool
    agreed: bool


class ModelInfoResponse(BaseModel):
    model_name: str
    model_version: str
    champion_alias: str
    loaded_at: str  # iso utc
    registered_at: str  # iso utc
    training_roc_auc: float | None
    feature_service: str
    shadow_version: str | None  # SHADOW_MODEL_VERSION if set


class HealthResponse(BaseModel):
    status: str  # 'health' | 'degraded' | 'unhealthy'
    model_loaded: bool
    redis_reachable: bool
    model_version: str | None


# ----- API responses ----- #


# class DriftSummaryRow(BaseModel):
#     report_date: str
#     n_breached: int
#     n_features: int
#     max_psi: float
#     model_version: str


# class DriftSummaryResponse(BaseModel):
#     rows: list[DriftSummaryRow]
class DriftFeatureSummary(BaseModel):
    name: str
    psi: float
    severity: Literal['green', 'amber', 'red']


class DriftSummaryResponse(BaseModel):
    report_date: str
    psi_breaches: int
    n_features: int
    max_psi: float
    model_version: str | None
    features: list[DriftFeatureSummary]


class DriftMetricRow(BaseModel):
    report_date: str
    feature_name: str
    psi_score: float
    kl_divergence: float | None
    rank: int
    is_breached: bool
    model_version: str | None


class DriftResponse(BaseModel):
    rows: list[DriftMetricRow]
    report_date: str  # the most recent date returned
    n_breached: int


class ModelRow(BaseModel):
    model_version: str
    avg_roc_auc: float
    last_scored: float
    n_flights: int


class ModelStatsResponse(BaseModel):
    rows: list[ModelRow]


class DriftHeatmapRow(BaseModel):
    report_date: str
    feature_name: str
    psi_score: float
    kl_divergence: float | None
    rank: int
    is_breached: bool
    model_version: str | None


class DriftHeatmapResponse(BaseModel):
    rows: list[DriftHeatmapRow]


class PsiRow(BaseModel):
    report_date: str
    psi_score: float
    kl_divergence: float | None
    is_breached: bool


class PsiResponse(BaseModel):
    rows: list[PsiRow]


class AccuracyRow(BaseModel):
    score_date: str
    model_version: str
    roc_auc: float
    f1: float
    precision_score: float
    recall_score: float
    brier_score: float
    positive_rate: float
    actual_positive_rate: float
    n_with_actuals: float


class AccuracyResponse(BaseModel):
    rows: list[AccuracyRow]


class PredictionRow(BaseModel):
    score_date: str
    model_version: str
    n_flights: int
    avg_proba: float
    positive_rate: float
    n_with_actuals: int


class PredictionsResponse(BaseModel):
    rows: list[PredictionRow]


class PredictionsDayResponse(BaseModel):
    model_version: str | None
    model_loaded_at: str
    registered_at: str
    n_flights_today: int
    positive_rate_today: float | None
    days_since_retrain: int | None
