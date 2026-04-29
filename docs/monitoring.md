# Monitoring

## Overview

## Drift Detection

### Evidently HTML Reports

### PSI (Population Stability Index)

#### PSI Threshold (0.2) — Rationale

### Feature-Level vs. Prediction Drift

### `drift_report.py` Asset

### Postgres Storage Schema (`scripts/create_monitoring_tables.sql`)

### GitHub Pages Publishing

## Ground Truth Backfill

### `ground_truth_backfill.py`

### Matching Predictions to Actuals

### Live Accuracy Tracking

## Drift Retrain Sensor

### `drift_retrain_sensor` — How It Works

### Polling Interval

### Trigger Conditions

### Auto-Retrain Flow

### Preventing Retrain Loops

## Alerts

### `run_failure_alerts` Sensor

### Alert Destination Configuration

## Dashboards

### What to Watch (Key Metrics)

### Interpreting PSI Values

### Prediction Volume Anomalies

## Operational Procedures

### Investigating a Drift Alert

### Manually Triggering a Retrain

### Resetting the Ground Truth Baseline
