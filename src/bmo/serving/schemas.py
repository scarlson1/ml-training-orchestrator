"""
Request/response Pydantic models for the FastAPI serving API.

These models serve as the API contract — they validate inputs and shape outputs.
Using Pydantic v2 here means FastAPI generates accurate OpenAPI docs automatically.

FastAPI docs: https://fastapi.tiangolo.com/tutorial/body/
Pydantic v2 docs: https://docs.pydantic.dev/latest/
"""

from __future__ import annotations

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
    feature_service: str
    shadow_version: str | None  # SHADOW_MODEL_VERSION if set


class HealthResponse(BaseModel):
    status: str  # 'health' | 'degraded' | 'unhealthy'
    model_loaded: bool
    redis_reachable: bool
    model_version: str | None
