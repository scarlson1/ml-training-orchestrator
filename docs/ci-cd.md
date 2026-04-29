# CI/CD

## Overview

## GitHub Actions Workflows

### `ci.yml` — Lint & Test

#### Lint Stage (ruff + mypy)

#### Unit Test Stage

#### Feast Apply Idempotency Check

#### Triggers

### `build-images.yml` — Docker Image Builds

#### Multi-Arch Strategy (amd64 + arm64)

#### Caching

#### Pushing to GHCR

#### Triggers

### `deploy.yml` — Production Deployment

#### Prerequisites (Secrets, Oracle VM)

#### Deployment Steps

#### Waiting for cloud-init

#### `bmo-compose` systemd Service Restart

#### Rollback on Failure

### `evidently-reports.yml` — Drift Report Publishing

#### Generating Reports

#### Publishing to GitHub Pages

## Required GitHub Secrets

## Branch & Merge Strategy

## Making a Change That Requires a New Docker Build

## Adding a New Workflow

## Debugging a Failed Workflow Run

### Reading GitHub Actions Logs

### Re-Running a Failed Job

### SSH Debugging on Oracle VM
