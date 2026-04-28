# System Architecture

## Infrastructure Tiers

```mermaid
graph TB
    subgraph ext["External Data Sources"]
        bts["BTS On-Time Performance\ntranstats.bts.gov"]
        noaa["NOAA GHCN / LCD\nncei.noaa.gov"]
        faa["FAA Airport Master\nfaa.gov"]
        ofl["OpenFlights Routes\nopenflights.org"]
    end

    subgraph oracle["Control Plane — Oracle Cloud Always Free"]
        direction LR
        dagster["Dagster\nWebUI + Daemon + User-Code gRPC"]
        mlflow_srv["MLflow\nTracking + Registry"]
        feast_reg["Feast\nRegistry"]
        evidently["Evidently\nDrift Reports"]
        postgres[("PostgreSQL\nDagster metadata\nIceberg catalog\nDrift metrics")]
        minio[("MinIO — dev\nArtifact store")]
        dagster -.->|metadata| postgres
        mlflow_srv -.->|backend| postgres
        mlflow_srv -.->|artifacts| minio
    end

    subgraph r2["Object Store — Cloudflare R2 — $0 egress"]
        raw_b["raw/\nParquet — ingested"]
        staging_b["staging/\nIceberg — validated"]
        feast_b["staging/feast/\nParquet — per entity"]
        datasets_b["staging/datasets/\ncontent-addressed"]
        preds_b["staging/predictions/\nDailyPartition"]
        mlflow_b["mlflow-artifacts/\nModel files"]
    end

    subgraph fly["Serving — Fly.io + Upstash"]
        fastapi["FastAPI\n/predict  /health  /metrics\n/admin/reload"]
        redis[("Upstash Redis\nFeast Online Store")]
        fastapi <-->|feature lookup| redis
    end

    ext -->|HTTP download| oracle
    oracle -->|orchestrates| r2
    r2 -->|Feast materialize| redis
    mlflow_b -->|champion model| fastapi

    style oracle fill:#fff3e0,stroke:#e65100
    style r2 fill:#e3f2fd,stroke:#1565c0
    style fly fill:#e8f5e9,stroke:#2e7d32
    style ext fill:#fce4ec,stroke:#880e4f
```

## End-to-End Data Flow

```mermaid
flowchart TD
    subgraph ingest["Phase 1 — Raw Ingestion  ·  group: raw"]
        R1["raw_bts_flights\nMonthlyPartition"]
        R2["raw_noaa_weather\nMonthlyPartition"]
        R3["raw_faa_airports\ndimension"]
        R4["raw_openflights_routes\ndimension"]
        SM["station_map\nairport → NOAA station"]
    end

    subgraph stage["Phase 2 — Staging + Schema Contracts  ·  group: staging"]
        S1["staged_flights\nUTC timestamps\nPydantic validation"]
        S2["staged_weather\nFM-15 observations"]
        S3["dim_airport\nIATA + timezone + station"]
        S4["dim_route\nhaversine distances"]
        AC["5× @asset_check\nnull rates · schema evolution"]
    end

    subgraph feat["Phase 3 — Feature Engineering  ·  group: features"]
        PY["feat_cascading_delay\nPySpark LAG window\nprev flight delay per tail #"]
        DBT["bmo_dbt_assets  ·  15 dbt models\nfeat_origin · feat_dest · feat_carrier\nfeat_route · feat_calendar\nmart_training_dataset"]
    end

    subgraph fs["Phase 4 — Feature Store  ·  group: feast"]
        FE["feast_feature_export\nDuckDB → S3 Parquet\n5 entity types"]
        FM["feast_materialized_features\nS3 → Redis  ·  hourly @schedule"]
    end

    subgraph train["Phase 5–7 — Training  ·  group: training"]
        TD["training_dataset\nPIT ASOF Join\nLeakage Guards\ncontent-addressed"]
        TM["trained_model\nXGBoost + Optuna\n50-trial HPO"]
        EG{"Evaluation Gate\n4× @asset_check"}
        RM["registered_model\nMLflow challenger → champion"]
    end

    subgraph serve["Phase 9 — Serving  ·  group: serving"]
        BP["batch_predictions\nDailyPartition"]
        API["deployed_api\nFastAPI on Fly.io"]
    end

    subgraph mon["Phase 10 — Monitoring  ·  group: monitoring"]
        DR["drift_report\nEvidently PSI"]
        GT["ground_truth_backfill\nlive accuracy tracking"]
        SENSOR["drift_retrain_sensor\nPSI > 0.2 → retrain"]
    end

    R1 --> S1
    R2 --> S2
    R3 --> SM --> S3
    R4 --> S4
    S3 --> S4
    S1 --> AC
    S2 --> AC
    S3 --> AC
    S4 --> AC

    S1 --> PY
    S1 --> DBT
    S2 --> DBT
    S3 --> DBT
    S4 --> DBT
    PY --> DBT

    DBT --> FE
    PY --> FE
    FE --> FM

    FM --> TD
    TD --> TM
    TM --> EG
    EG -->|all checks pass| RM
    EG -->|any ERROR check fails| HALT["⛔ halt + Discord alert"]

    RM --> BP
    RM --> API

    BP --> DR
    API --> DR
    DR --> GT
    DR --> SENSOR
    SENSOR -->|RunRequest| TD

    style EG fill:#fff9c4,stroke:#f9a825
    style HALT fill:#ffebee,stroke:#c62828
    style TD fill:#e8f5e9,stroke:#2e7d32
    style SENSOR fill:#e3f2fd,stroke:#1565c0
```

## Dagster Orchestration Layer

```mermaid
graph LR
    subgraph sensors["Sensors"]
        S1["bts_new_month_sensor\npolls BTS every 6h"]
        S2["drift_retrain_sensor\nhourly PSI check"]
        S3["run_failure_sensor\n→ Discord webhook"]
    end

    subgraph schedules["Schedules"]
        SC1["feast_hourly_schedule\ncron: 0 * * * *"]
        SC2["nightly_retrain_schedule\ncron: 0 1 * * *"]
        SC3["daily_score_schedule\ncron: 0 6 * * *"]
        SC4["daily_drift_schedule\ncron: 0 8 * * *"]
    end

    subgraph resources["Resources — wired to all assets"]
        RES1["MLflowResource"]
        RES2["S3Resource\nMinIO / R2"]
        RES3["FeastResource"]
        RES4["DuckDBResource"]
        RES5["SparkResource"]
    end

    S1 -->|RunRequest + partition_key| raw_bts["raw_bts_flights\nraw_noaa_weather"]
    S2 -->|RunRequest| training_dataset["training_dataset"]
    SC1 -->|RunRequest| feast_mat["feast_materialized_features"]
    SC2 -->|RunRequest| training_dataset
    SC3 -->|RunRequest + date| batch_preds["batch_predictions"]
    SC4 -->|RunRequest + date| drift["drift_report"]

    style resources fill:#f3e5f5,stroke:#6a1b9a
```

## Storage Layout

```
Cloudflare R2 / MinIO
├── raw/
│   ├── bts/year=YYYY/month=MM/data.parquet      ← BTS flights (monthly)
│   ├── noaa/year=YYYY/month=MM/data.parquet     ← NOAA weather (monthly)
│   ├── faa/airports.parquet                     ← FAA airport master
│   └── openflights/routes.parquet               ← OpenFlights routes
├── staging/                                     ← Iceberg tables (ACID, partitioned)
│   ├── iceberg/staged_flights/                  ← month-partitioned
│   ├── iceberg/staged_weather/
│   ├── iceberg/dim_airport/
│   ├── iceberg/dim_route/
│   ├── iceberg/feat_cascading_delay/
│   ├── feat_*/                                  ← dbt feature tables (DuckDB writes)
│   ├── feast/                                   ← Feast offline store
│   │   ├── origin_airport/data.parquet
│   │   ├── dest_airport/data.parquet
│   │   ├── carrier/data.parquet
│   │   ├── route/data.parquet
│   │   └── aircraft/data.parquet
│   ├── datasets/{version_hash}/
│   │   ├── data.parquet                         ← content-addressed training dataset
│   │   └── card.json                            ← DatasetHandle metadata
│   └── predictions/date=YYYY-MM-DD/data.parquet ← batch scoring output
├── rejected/
│   ├── bts/...                                  ← rows failing Pydantic validation
│   └── noaa/...
└── mlflow-artifacts/                            ← model binaries, Evidently reports
```
