"""
Convert BTS HHMM local times to UTC datetimes.

BTS encodes all times as HHMM integers in local *airport* time, not UTC.
The only source of truth for the timezone is the airport dimension.

Key edge cases:
  - 2400 = midnight of the next calendar day (not an error, BTS uses this)
  - Overnight arrivals: arr_hhmm < dep_hhmm because the clock wraps past midnight
  - DST transitions: zoneinfo handles these correctly; never use pytz.localize() here
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from datetime import timezone as stdlib_tz
from zoneinfo import ZoneInfo


def hhmm_to_td(hhmm: int) -> timedelta:
    """Convert HHMM integer to timedelta. 2400 → 24h (midnight next day)."""
    if hhmm == 2400:
        return timedelta(hours=24)
    if not 0 <= hhmm <= 2400:
        raise ValueError(f'invalid HHMM value: {hhmm}')
    hours, minutes = divmod(hhmm, 100)

    if minutes >= 60:
        raise ValueError(f'invalid minutes in HHMM {hhmm}: {minutes}')

    return timedelta(hours=hours, minutes=minutes)


def local_hhmm_to_utc(
    flight_date: date,
    hhmm: int,
    tz_name: str,
    day_offset: int = 0,
) -> datetime:
    """
    Convert a BTS HHMM local time to a UTC-aware datetime.

    flight_date: the departure date (BTS FlightDate column)
    hhmm:        the HHMM integer (e.g. 1430 = 14:30 local)
    tz_name:     IANA timezone string from airport dimension (e.g. "America/Chicago")
    day_offset:  +1 for overnight arrivals that land the next calendar day
    """
    tz = ZoneInfo(tz_name)
    base = datetime(flight_date.year, flight_date.month, flight_date.day)
    local_naive = base + hhmm_to_td(hhmm) + timedelta(days=day_offset)
    local_aware = local_naive.replace(tzinfo=tz)
    return local_aware.astimezone(stdlib_tz.utc)


def arrival_day_offset(dep_hhmm: int, arr_hhmm: int) -> int:
    """
    Infer whether a flight arrives the next calendar day.

    The heuristic: if the scheduled arrival HHMM is more than 60 minutes
    *earlier* in clock time than the scheduled departure, the flight crosses
    midnight. (Shortest nonstop US flights are ~45 min, so a 60-min gap
    catches all overnight cases without false positives.)
    """

    def to_minutes(hhmm: int) -> int:
        return (hhmm // 100) * 60 + (hhmm % 100)

    dep_min = to_minutes(dep_hhmm)
    arr_min = to_minutes(arr_hhmm % 2400)  # treat 2400 as 0 for comparison
    if arr_min < dep_min - 60:
        return 1
    return 0
