# Staging & Validation

## Overview

## Pydantic Validation

### Flight Records

#### Invalid-Row Guards

#### UTC Timestamp Conversion

### Weather Observations (FM-15)

### Airport & Route Dimensions

#### Haversine Distance Computation

## Schema Contracts

### How `contracts.py` Works

### Null-Rate Checks

### Asset Checks in Dagster

### Blocking vs. Warning Checks

## Iceberg Table Format

### Why Iceberg

### Catalog Setup (JdbcCatalog → Postgres)

### Partition Strategy

### Schema Evolution

### Partition Overwrite Semantics (Idempotency)

## Staging Asset Dependencies

## Timezone Handling

### UTC Conversion Utilities

### Edge Cases (DST, Midnight Crossings)

## What Happens When Validation Fails
