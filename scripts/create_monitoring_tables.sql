-- scripts/create_monitoring_tables.sql
-- Run once to create monitoring tables in Postgres.
-- Safe to re-run: all statements use IF NOT EXISTS.
--
-- Why Postgres for drift metrics?
-- The drift_retrain_sensor already exists and queries Postgres via SQLAlchemy.
-- Writing metrics here means the sensor can detect drift within seconds of
-- the drift_report asset completing, with no additional infrastructure.

CREATE TABLE IF NOT EXISTS drift_metrics (
    id      SERIAL PRIMARY KEY,

    -- partition key: one row per feature per report_date
    -- UNIQUE constraint makes upserts idempotent (re-running the same
    -- partition for the same data always produces identical rows)
    report_date         DATE    NOT NULL,
    feature_name        TEXT    NOT NULL,

    -- drift scores
    psi_score           DOUBLE PRECISION NOT NULL,
    feature_name        TEXT    NOT NULL,

    -- feature importance rant (1 most important)
    rank                INTEGER     NOT NULL,

    is_breached         BOOLEAN     NOT NULL,
    model_version       TEXT,
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_drift_metrics_date_feature UNIQUE (report_date, feature_name)
);

-- Indexes that the sensor query hits:
--   WHERE rank <= :top_n AND computed_at > :since
CREATE INDEX IF NOT EXISTS idx_drift_metrics_rank
    ON drift_metrics (rank);
CREATE INDEX IF NOT EXISTS idx_drift_metrics_computed_at
    ON drift_metrics (computed_at);
CREATE INDEX IF NOT EXISTS idx_drift_metrics_report_date
    ON drift_metrics (report_date);

-- live_accuracy: ground-truth backfill results.
-- Populated by the ground_truth_backfill Dagster asset after BTS actuals are published.
-- BTS publishes with ~60-day lag, so rows here trail predictions by 2 months.
CREATE TABLE IF NOT EXISTS live_accuracy (
    id                  SERIAL PRIMARY KEY,

    score_date          DATE        NOT NULL,
    model_version       TEXT        NOT NULL,

    -- SMAPLE COUNTS
    n_flights           INTEGER     NOT NULL,
    n_with_actuals      INTEGER     NOT NULL,

    -- classification metrics
    accuracy             DOUBLE PRECISION,
    precision_score      DOUBLE PRECISION,
    recall_score         DOUBLE PRECISION,
    f1                   DOUBLE PRECISION,
    roc_auc              DOUBLE PRECISION,
    log_loss             DOUBLE PRECISION,
    brier_score          DOUBLE PRECISION,

    -- distribution summary
    positive_rate      DOUBLE PRECISION, -- predicted
    actual_positive_rate    DOUBLE PRECISION, -- actual 

    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_live_accuracy_date_version UNIQUE (score_date, model_version)
);

CREATE INDEX IF NOT EXISTS idx_live_accuracy_score_date
    ON live_accuracy (score_date);
CREATE INDEX IF NOT EXISTS idx_live_accuracy_model_version
    ON live_accuracy (model_version);

