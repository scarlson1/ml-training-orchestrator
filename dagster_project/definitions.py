"""
Dagster Definitions object — the single entrypoint loaded by `dagster dev`.

Everything Dagster needs to know about the project is registered here:
assets, jobs, sensors, schedules, resources. If it's not in Definitions,
the UI won't show it and the daemon won't run it.
"""

from dagster import Definitions, MonthlyPartitionsDefinition, define_asset_job

from dagster_project.assets.raw import (
    raw_bts_flights,
    raw_faa_airports,
    raw_noaa_weather,
    raw_openflights_routes,
    station_map,
)
from dagster_project.sensors.bts_new_month import bts_new_month_sensor

# A job is a named, executable subset of the asset graph.
# The sensor targets this job by name to kick off a single BTS partition run.
# partitions_def must match the asset's own definition so Dagster knows
# which partition to run when the sensor yields a partition_key.
ingest_bts_month_job = define_asset_job(
    name='ingest_bts_month',  # match name in sensor decorator
    selection=[raw_bts_flights],
    partitions_def=MonthlyPartitionsDefinition(start_date='2018-01-01'),
)

# registers entities with cli ??
defs = Definitions(
    assets=[
        raw_faa_airports,
        raw_openflights_routes,
        station_map,
        raw_bts_flights,
        raw_noaa_weather,
    ],
    jobs=[ingest_bts_month_job],
    sensors=[bts_new_month_sensor],
)
