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

All staging assets (`dim_airport`, `dim_route`, `staged_flights`, `staged_weather`) return `MaterializeResult` and write their output directly to S3 via `ObjectStore`. Dagster's file IO manager never stores a value for these assets.

### `deps` vs `ins`

Because no asset value is stored by the IO manager, upstream assets must be declared using `deps` or `AssetDep` — never `ins` / `AssetIn`. Using `ins` tells Dagster to load the upstream asset's value from IO manager storage at execution time, which raises a `FileNotFoundError` since nothing is written there.

For unpartitioned dependencies (e.g. `dim_airport`), use a plain string in `deps`:

```python
deps=['dim_airport']
```

For partitioned dependencies where you need to express a partition mapping, use `AssetDep` with `partition_mapping=`:

```python
from dagster import AssetDep, TimeWindowPartitionMapping

deps=[
    AssetDep(
        'raw_bts_flights',
        partition_mapping=TimeWindowPartitionMapping(start_offset=0, end_offset=0),
    )
]
```

This preserves the partition dependency wiring in the asset graph without triggering an IO manager load.

## Timezone Handling

### UTC Conversion Utilities

### Edge Cases (DST, Midnight Crossings)

## What Happens When Validation Fails
