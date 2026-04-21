from datetime import date, datetime, timezone

import pytest

from bmo.staging.timezone import arrival_day_offset, hhmm_to_td, local_hhmm_to_utc


def test_hhmm_basic() -> None:
    from datetime import timedelta

    assert hhmm_to_td(1430) == timedelta(hours=14, minutes=30)
    assert hhmm_to_td(0) == timedelta(0)
    assert hhmm_to_td(2400) == timedelta(hours=24)


def test_hhmm_invalid() -> None:
    with pytest.raises(ValueError):
        hhmm_to_td(2500)
    with pytest.raises(ValueError):
        hhmm_to_td(1260)  # 60 minutes


def test_local_to_utc_chicago_standard() -> None:
    # 2024-01-15 14:30 CST (UTC-6) = 20:30 UTC
    result = local_hhmm_to_utc(date(2024, 1, 15), 1430, 'America/Chicago')
    assert result == datetime(2024, 1, 15, 20, 30, tzinfo=timezone.utc)


def test_local_to_utc_chicago_dst() -> None:
    # 2024-07-15 14:30 CDT (UTC-5) = 19:30 UTC
    result = local_hhmm_to_utc(date(2024, 7, 15), 1430, 'America/Chicago')
    assert result == datetime(2024, 7, 15, 19, 30, tzinfo=timezone.utc)


def test_overnight_arrival() -> None:
    # Flight departs 22:00, scheduled to arrive 01:30 next day
    assert arrival_day_offset(2200, 130) == 1


def test_same_day_arrival() -> None:
    # Flight departs 09:00, arrives 11:30 same day
    assert arrival_day_offset(900, 1130) == 0


def test_2400_midnight() -> None:
    # BTS encodes midnight as 2400 on the departure date
    result = local_hhmm_to_utc(date(2024, 1, 15), 2400, 'America/New_York')
    # 2400 = midnight = 00:00 on Jan 16 EST (UTC-5) = 05:00 UTC Jan 16
    assert result == datetime(2024, 1, 16, 5, 0, tzinfo=timezone.utc)
