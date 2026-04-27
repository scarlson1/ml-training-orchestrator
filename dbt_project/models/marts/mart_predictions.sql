-- mart_predictions: join batch prediction outputs with actuals for drift analysis.
--
-- Why this lives in dbt:
--   The predictions Parquet (written by batch_predictions Dagster asset) lives
--   in s3://staging/predictions/date=*/. DuckDB can query it directly via
--   read_parquet with glob patterns. Joining with staged_flights (actuals) here
--   keeps the lineage visible in the dbt DAG and exposes it as a native Dagster
--   asset via @dbt_assets.
--
-- This model is consumed by Phase 10's drift_report asset (Evidently).

{{
    config(
        materialized='table',
        tags=['serving', 'monitoring']
    )
}}

{% set predictions_glob = env_var("PREDICTIONS_S3_GLOB", "s3://staging/predictions/**/data.parquet") %}

{# Check at compile time whether any prediction files exist yet. #}
{# read_parquet hard-errors on a no-match glob, so we emit an empty typed   #}
{# SELECT when batch_predictions hasn't run yet.                             #}
{% if execute %}
    {% set file_check %}
        SELECT count(*) AS n FROM glob('{{ predictions_glob }}')
    {% endset %}
    {% set n_files = run_query(file_check).columns[0].values()[0] %}
{% else %}
    {% set n_files = 1 %}
{% endif %}

WITH predictions AS (
    {% if n_files > 0 %}
        SELECT * FROM read_parquet('{{ predictions_glob }}')
    {% else %}
        SELECT
            NULL::VARCHAR       AS flight_id,
            NULL::VARCHAR       AS origin,
            NULL::VARCHAR       AS dest,
            NULL::VARCHAR       AS carrier,
            NULL::VARCHAR       AS tail_number,
            NULL::VARCHAR       AS route_key,
            NULL::TIMESTAMPTZ   AS scheduled_departure_utc,
            NULL::FLOAT         AS predicted_delay_proba,
            NULL::TINYINT       AS predicted_is_delayed,
            NULL::VARCHAR       AS model_name,
            NULL::VARCHAR       AS model_version,
            NULL::VARCHAR       AS score_date,
            NULL::VARCHAR       AS scored_at
        WHERE 1=0
    {% endif %}
),

actuals as (
    SELECT
        flight_id,
        dep_delay_min,
        arr_delay_min,
        dep_del15       AS is_dep_delayed,
        cancelled,
        actual_departure_utc
    FROM {{ ref('stg_flights') }}
    WHERE actual_departure_utc IS NOT NULL 
),

joined AS (
    SELECT
        p.flight_id,
        p.origin,
        p.dest,
        p.carrier,
        p.tail_number,
        p.route_key,
        p.scheduled_departure_utc,
        p.predicted_delay_proba,
        p.predicted_is_delayed,
        p.model_name,
        p.model_version,
        p.score_date,
        p.scored_at,
        a.dep_delay_min         AS actual_dep_delay_min,
        a.is_dep_delayed        AS actual_is_delayed,
        a.actual_departure_utc,
        a.cancelled
    FROM predictions AS p
    LEFT JOIN actuals AS a USING (flight_id)
)

SELECT * FROM joined