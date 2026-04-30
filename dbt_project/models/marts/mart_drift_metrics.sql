-- Drift metrics history from the monitoring/drift_report Dagster asset.
--
-- The drift_report asset writes daily Parquet files to:
--   s3://staging/monitoring/metrics/date=*/drift_metrics.parquet
--
-- DuckDB's read_parquet() with a glob pattern reads all partitions in one
-- scan with predicate pushdown on the date= directory prefix:
--   WHERE report_date = '2024-06-15'  → reads only that partition
--
-- This model is the bridge between the Python monitoring code and the
-- dbt lineage graph. It makes drift trends queryable via dbt and visible
-- in the Dagster asset graph alongside all feature models.
--
-- Usage examples:
--   -- Features drifting most over the past 30 days
--   SELECT feature_name, AVG(psi_score) AS avg_psi, COUNT(*) AS n_breached
--   FROM mart_drift_metrics
--   WHERE report_date >= CURRENT_DATE - INTERVAL '30 days' AND is_breached
--   GROUP BY feature_name ORDER BY avg_psi DESC;

{{
    config(
        materialized='table',
        tags=['monitoring']
    )
}}

{% set metrics_glob = "s3://" ~ env_var("S3_BUCKET_STAGING") ~ "/monitoring/metrics/**/drift_metrics.parquet" %}

{% if execute %}
    {% set file_check %}
        SELECT count(*) AS n FROM glob('{{ metrics_glob }}')
    {% endset %}
    {% set n_files = run_query(file_check).columns[0].values()[0] %}
{% else %}
    {% set n_files = 1 %}
{% endif %}

{% if n_files > 0 %}
SELECT
    report_date::DATE                               AS report_date,
    feature_name,
    psi_score::DOUBLE                               AS psi_score,
    kl_divergence::DOUBLE                           AS kl_divergence,
    rank::INTEGER                                   AS importance_rank,
    is_breached::BOOLEAN                            AS is_breached,
    model_version,
    computed_at::TIMESTAMPTZ                        AS computed_at
FROM read_parquet('{{ metrics_glob }}')
{% else %}
SELECT
    NULL::DATE          AS report_date,
    NULL::VARCHAR       AS feature_name,
    NULL::DOUBLE        AS psi_score,
    NULL::DOUBLE        AS kl_divergence,
    NULL::INTEGER       AS importance_rank,
    NULL::BOOLEAN       AS is_breached,
    NULL::VARCHAR       AS model_version,
    NULL::TIMESTAMPTZ   AS computed_at
WHERE 1=0
{% endif %}


