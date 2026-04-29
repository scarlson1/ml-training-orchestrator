# Feature Store

## Overview

## Entities

### `origin_airport`

### `dest_airport`

### `carrier`

### `route`

### `aircraft`

## Feature Views

### Design Patterns

### Mapping Feature Views to dbt / Parquet Sources

### TTL Semantics

## Feature Services

### Pre-Defined Feature Sets for Serving

## Offline Store

### Iceberg-Backed Parquet

### Point-in-Time Join at Training Time

### `feast historical_features` Usage

## Online Store (Redis via Upstash)

### Materialization Workflow

### Feast Hourly Materialization Schedule (Dagster)

### Sub-Millisecond Lookup at Inference Time

## Train–Serve Consistency

### How Feast Enforces It

### What Can Still Go Wrong

## Registry

### `feature_store.yaml` Options

### Applying Changes (`feast apply`)

### Idempotency Guarantees

## Operating the Feature Store

### Inspecting the Registry

### Re-Materializing Online Features

### Debugging Missing Features at Serve Time
