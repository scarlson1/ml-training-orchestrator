# Runbooks

## Backfilling a Historical Partition

### When to Use

### Step-by-Step

### Verifying the Backfill

## Hot-Swapping the Production Model

### When to Use

### Step-by-Step (MLflow Registry → `/admin/reload`)

### Rollback Procedure

## Re-Materializing Online Features

### When to Use

### Step-by-Step

### Verifying Redis Is Up-to-Date

## Debugging a Failed Dagster Run

### Reading the Run Log

### Common Failure Points by Phase

### Re-Running a Specific Asset

## Investigating Data Leakage

### Symptoms

### Running the Planted-Value Leakage Test

### Tracing Leakage to Its Source

## Recovering from a Schema Migration

### Iceberg Schema Evolution Commands

### dbt Model Recompile

### Feast `feast apply` After Schema Change

## Rotating Secrets / Credentials

### Updating GitHub Secrets

### Propagating to Oracle VM

### Updating `.env.prod`

## Rebuilding the Dagster Code Server

### When to Rebuild the Docker Image

### Restarting the Code Server Without Full Stack Restart

## Inspecting Iceberg Table State

### Querying via DuckDB

### Listing Snapshots

### Rolling Back to a Previous Snapshot

## Disaster Recovery

### Full Stack Rebuild from Scratch

### Restoring from R2 Backups
