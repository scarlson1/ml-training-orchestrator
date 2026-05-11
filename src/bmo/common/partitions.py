"""
Partition definitions shared across Dagster assets.

MonthlyPartitionsDefinition: one partition per calendar month, used by raw ingestion.
DailyPartitionsDefinition:   one partition per calendar day, used by batch scoring.

Centralizing these here avoids duplicate instantiation (which would cause
Dagster to show the same partition key twice in the UI for the same asset).

Docs: https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions
"""

from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

# Start date matches the earliest BTS data we ingest.
MONTHLY_PARTITIONS = MonthlyPartitionsDefinition(start_date='2018-01-01')

# Batch predictions start when training data is available.
# Dagster renders this as a calendar selector in the UI — one tile per day.
DAILY_PARTITIONS = DailyPartitionsDefinition(start_date='2024-01-01')
