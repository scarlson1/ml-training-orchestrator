# Infrastructure

## Overview

## Local Development Stack (Docker Compose)

### Services (`compose.dev.yml`)

#### Dagster (User-Code Server + Webserver)

#### PostgreSQL (Metadata + Iceberg Catalog)

#### MinIO (Object Store)

#### MLflow (Tracking Server)

#### Redis (Feast Online Store)

### Starting & Stopping

### Persisted Volumes

## Production Stack

### `compose.prod.yml` Differences

### Environment Variable Injection

## Docker Images

### `dagster.Dockerfile`

#### Multi-Stage Build

#### uv Dependency Installation

### `serving.Dockerfile`

### Building Locally

### Multi-Arch Builds (amd64 + arm64)

### Image Registry (GHCR)

## Terraform

### Oracle Cloud (Always Free VM)

#### VM Provisioning (`infra/terraform/oracle/`)

#### `cloud-init.sh` — Docker & systemd Setup

#### `bmo-compose` systemd Service

### Cloudflare R2

#### Bucket Creation (`infra/terraform/r2/`)

#### Zero-Egress Pricing Rationale

### Variables & Secrets (`terraform.tfvars`)

### Applying Changes

## Networking

### Port Mapping (Local vs. Prod)

### Fly.io → Redis (Upstash) Connectivity

### Oracle VM Firewall Rules

## Cost Breakdown

### Oracle Always Free Tier Limits

### Cloudflare R2 Pricing

### Fly.io Free Tier

### Upstash Redis Free Tier
