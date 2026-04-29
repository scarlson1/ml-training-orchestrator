# Model Training

## Overview

## Training Dataset Builder

### Content Addressing (SHA-256)

### What Goes Into the Hash

### Leakage Guards

#### Guard 1: Event Timestamp Bounds

#### Guard 2: No Future Features

#### Guard 3: TTL Compliance

#### Guard 4: No Target Leakage

### `DatasetHandle` — Dataset Metadata Card

### PIT Join via DuckDB ASOF JOIN

## Hyperparameter Optimization (Optuna)

### Search Space

### Objective Function

### Trial Count & Pruning

### Determinism (Optuna Seed)

## XGBoost Training

### Model Configuration

### Early Stopping

### Label Definition (Departure Delay Threshold)

## MLflow Integration

### What Gets Logged (Params, Metrics, Artifacts)

### `version_hash` as a Tracked Parameter

### Champion Model Selection Logic

## Reproducibility

### Reproducing a Model from a Run ID

### Byte-Equality Guarantee

### `reproduce.py` Usage

## Nightly Retrain Schedule

### Trigger Conditions

### Partition Selection

### Fallback Behavior on Failure

## Extending the Model

### Swapping XGBoost for Another Algorithm

### Adding New Input Features
