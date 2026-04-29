# Testing

## Overview

## Test Structure

```
tests/
├── unit/
├── integration/
└── determinism/
```

## Unit Tests

### What Gets Mocked (S3, Redis, HTTP)

### Fixtures (`conftest.py`)

### Key Test Files & What They Cover

#### `test_ingestion_bts.py`

#### `test_ingestion_noaa.py`

#### `test_staging_timezone.py`

#### `test_pit_join.py`

#### `test_training.py`

#### `test_evaluation_gate.py`

#### `test_dataset_handle.py`

#### `test_serving_feature_client.py`

## Integration Tests

### Prerequisites (Docker Compose Stack)

### `test_feast_roundtrip.py`

### `test_leakage_planted_value.py`

#### How the Planted-Value Test Works

#### What It Catches That Unit Tests Miss

## Determinism Tests

### `test_reproduce_run.py`

### Byte-Equality Assertions

### When to Run

## Pytest Markers

### `@pytest.mark.integration`

### `@pytest.mark.determinism`

### Running a Subset of Tests

## CI Test Matrix

## Writing New Tests

### Unit Test Checklist

### Integration Test Checklist

### When to Add a Determinism Test
