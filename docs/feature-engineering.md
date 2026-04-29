# Feature Engineering

## Overview

## dbt Models (DuckDB)

### Staging Views

### Intermediate: `int_flights_enriched`

#### Point-in-Time Weather Join

### Feature Models

#### `feat_origin_airport_windowed` (1h / 24h / 7d)

#### `feat_dest_airport_windowed` (1h / 24h)

#### `feat_carrier_rolling` (7d)

#### `feat_route_rolling` (7d)

#### `feat_calendar` (Hour, Day-of-Week, Holiday)

### Marts

#### `mart_training_dataset`

#### `mart_predictions`

### Dagster–dbt Integration

#### `BmoDbtTranslator` and Asset Key Mapping

#### Auto-Loading dbt Models as Dagster Assets

## PySpark: `feat_cascading_delay`

### What It Computes

### LAG Window per `tail_number`

### HadoopCatalog → Iceberg on S3

### Running Locally vs. Production

## Adding a New Feature

### dbt Feature Model Checklist

### PySpark Feature Checklist

## Feature Naming Conventions

## Known Limitations & Edge Cases
