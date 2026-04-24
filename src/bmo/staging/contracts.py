"""
Schema contracts for the staging layer.

Each source has:
  - A target PyArrow schema (structural contract)
  - A set of domain rules (value-level contract)
  - A `validate()` function that splits a table into (valid, rejected)

Rejected rows are written to s3://staging/rejected/{source}/ with an added
`rejection_reason` column. This is intentionally not a hard failure — we keep
bad rows visible for debugging rather than silently dropping them.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

# The staged flights schema — note dual UTC timestamps, not in raw.
STAGED_FLIGHTS_SCHEMA = pa.schema(
    [
        ('year', pa.int16()),
        ('month', pa.int8()),
        ('day_of_month', pa.int8()),
        ('day_of_week', pa.int8()),
        ('flight_date', pa.date32()),
        ('reporting_airline', pa.string()),
        ('tail_number', pa.string()),
        ('flight_number', pa.int32()),
        ('origin', pa.string()),
        ('dest', pa.string()),
        ('origin_tz', pa.string()),
        ('dest_tz', pa.string()),
        # UTC timestamps — the whole point of staging
        ('scheduled_departure_utc', pa.timestamp('us', tz='UTC')),
        ('actual_departure_utc', pa.timestamp('us', tz='UTC')),  # null if cancelled
        ('scheduled_arrival_utc', pa.timestamp('us', tz='UTC')),
        ('actual_arrival_utc', pa.timestamp('us', tz='UTC')),  # null if cancelled/diverted
        # Raw delay fields preserved for feature engineering
        ('dep_delay_min', pa.float32()),
        ('arr_delay_min', pa.float32()),
        ('dep_del15', pa.bool_()),
        ('arr_del15', pa.bool_()),
        ('cancelled', pa.bool_()),
        ('cancellation_code', pa.string()),
        ('diverted', pa.bool_()),
        ('crs_elapsed_min', pa.float32()),
        ('actual_elapsed_min', pa.float32()),
        ('distance_mi', pa.float32()),  # should be joined from route table ??
        ('carrier_delay_min', pa.float32()),
        ('weather_delay_min', pa.float32()),
        ('nas_delay_min', pa.float32()),
        ('late_aircraft_delay_min', pa.float32()),
    ]
)


def validate_flights(table: pa.Table) -> tuple[pa.Table, pa.Table]:
    """
    Split a flights table into (valid_rows, rejected_rows).

    rejected_rows has an extra string column `rejection_reason`.
    A single row can only have one reason (first match wins).
    """

    # the reason_col loop is O(n×rules). For 500K rows and 4 rules, this is fine. If you add many more rules, switch to a numpy boolean array for the mask.
    reasons: list[tuple[pa.Array, str]] = [
        # origin/dest must be 3-char IATA codes
        (
            pc.or_(
                pc.not_equal(pc.utf8_length(table['origin']), pa.scalar(3)),
                pc.not_equal(pc.utf8_length(table['dest']), pa.scalar(3)),
            ),
            'invalid_iata_code',
        ),
        # distance must be positive and plausible (<= 6000 mi for domestic US)
        (
            pc.or_(
                pc.less_equal(
                    pc.cast(table['distance_mi'], pa.float32()), pa.scalar(0.0, pa.float32())
                ),
                pc.greater(
                    pc.cast(table['distance_mi'], pa.float32()), pa.scalar(6000.0, pa.float32())
                ),
            ),
            'implausible_distance',
        ),
        # scheduled departure UTC must not be null (means timezone lookup failed)
        (
            pc.is_null(table['scheduled_departure_utc']),
            'missing_scheduled_departure_utc',
        ),
        # non-cancelled flights must have actual_departure_utc
        (
            pc.and_(
                pc.invert(table['cancelled']),
                pc.is_null(table['actual_departure_utc']),
            ),
            'missing_actual_departure_for_operated_flight',
        ),
    ]

    # Build a boolean mask: True = this row should be rejected
    reject_mask = pa.array([False] * len(table))
    reason_col = [''] * len(table)

    for condition, reason in reasons:
        # only flag rows not already rejected
        newly_rejected = pc.and_(condition, pc.invert(reject_mask))
        reject_mask = pc.or_(reject_mask, newly_rejected)
        # assign reason string to newly-rejected rows
        newly_rejected_list = newly_rejected.to_pylist()
        for i, flag in enumerate(newly_rejected_list):
            if flag:
                reason_col[i] = reason

    valid = table.filter(pc.invert(reject_mask))
    rejected_base = table.filter(reject_mask)
    rejected = rejected_base.append_column(
        'rejection_reason', pa.array([r for r, f in zip(reason_col, reject_mask.to_pylist()) if f])
    )
    return valid, rejected
