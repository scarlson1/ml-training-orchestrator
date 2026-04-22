-- The test_ prefix makes it a generic test — it can be applied to any model column via _schema.yml. 
-- dbt's test framework expects the macro to return a query; if the query returns zero rows, the test passes.

-- wired in _int_schema.yml to test int_flights_enriched

{% macro test_no_future_leakage(model, column_name, event_ts_column) %}

select count(*) as leaking_rows
from {{ model }}
where {{ column_name }} > {{ event_ts_column }}

{% endmacro %}