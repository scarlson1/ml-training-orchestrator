# Serving

## Overview

## FastAPI Endpoints

### `GET /health`

### `GET /metrics` (Prometheus)

### `POST /predict`

#### Request Schema

#### Response Schema

#### Feature Lookup Flow

#### Latency Budget

### `POST /admin/reload`

## Feature Retrieval at Inference Time

### Redis (Upstash) Online Store

### Feature Client (`feature_client.py`)

### Handling Missing / Stale Features

## Model Loading

### `model_loader.py`

### Loading Champion Model from MLflow Registry

### Hot-Swap Without Downtime (`/admin/reload`)

## Deployment on Fly.io

### `fly.toml` Configuration

### Auto-Scaling Settings

### Health Check Integration

### Environment Variables at Runtime

## Observability

### Prometheus Metrics Exposed

### Logging Format

## Local Development

### Running `make serving-dev`

### Testing the `/predict` Endpoint Locally
