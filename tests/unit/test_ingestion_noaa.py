"""
Unit tests for NOAA ingestion.

These tests mock the HTTP layer and exercise the CSV parsing + schema coercion
against small in-memory fixtures. For integration tests that hit a real
MinIO instance, see tests/integration/.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
import pytest

from bmo.ingestion import noaa
from bmo.ingestion.noaa import (
    LCD_SCHEMA,
    _parse_lcd_csv,
    _strip_quality_flag,
    build_station_map,
    ingest_noaa_month,
)

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

# Minimal LCD CSV with five rows covering all parsing branches:
#   Row 1 — normal FM-15, January 2024
#   Row 2 — FM-15 with quality-flag suffixes on numeric fields
#   Row 3 — FM-15 with trace precipitation ("T")
#   Row 4 — FM-16 special obs (must be filtered out)
#   Row 5 — FM-15 in February 2024 (filtered when month=1)
SAMPLE_CSV = (
    'STATION,DATE,REPORT_TYPE,'
    'HourlyDryBulbTemperature,HourlyDewPointTemperature,HourlyRelativeHumidity,'
    'HourlyWindSpeed,HourlyWindDirection,HourlyPrecipitation,'
    'HourlyVisibility,HourlySkyConditions,HourlyPresentWeatherType,HourlySeaLevelPressure\n'
    '720530-14733,2024-01-15T12:00:00Z,FM-15,'
    '72,55,55,'
    '12,270,0.00,'
    '10.00,FEW050,RA,1014.2\n'
    '720530-14733,2024-01-15T13:00:00Z,FM-15,'
    '75s,57s,52s,'
    '10s,280s,0.00,'
    '10.00,SCT040,,1013.8*\n'
    '720530-14733,2024-01-15T14:00:00Z,FM-15,'
    '68,50,52,'
    '8,260,T,'
    '9.00,BKN030,,1015.0\n'
    '720530-14733,2024-01-15T14:30:00Z,FM-16,'
    '68,50,52,'
    '8,260,0.00,'
    '9.00,BKN030,,1015.0\n'
    '720530-14733,2024-02-01T00:00:00Z,FM-15,'
    '40,30,70,'
    '15,310,0.10,'
    '7.00,OVC020,,1008.5\n'
)

SAMPLE_ISD_CSV = (
    'USAF,WBAN,STATION NAME,CTRY,ST,CALL,ICAO,LAT,LON,ELEV(M),BEGIN,END\n'
    '720530,14733,CHICAGO OHARE INTL,US,IL,KORD,KORD,41.98,-87.90,205,19730101,20991231\n'
    '722950,23174,LOS ANGELES INTL,US,CA,KLAX,KLAX,33.94,-118.41,30,19730101,20991231\n'
    '911820,22521,HONOLULU INTL,US,HI,PHNL,PHNL,21.34,-157.93,4,19730101,20991231\n'
    '031590,99999,LONDON HEATHROW,UK,,EGLL,EGLL,51.48,-0.45,25,19730101,20991231\n'
    '725300,94846,DETROIT CITY,US,MI,KDET,KDET,42.41,-83.01,189,19730101,20100101\n'
)


# ------------------------------------------------------------------
# _strip_quality_flag
# ------------------------------------------------------------------


def test_strip_quality_flag_removes_letter_suffix() -> None:
    s = pd.Series(['75s', '10s', '280s'])
    result = _strip_quality_flag(s)
    assert result.tolist() == ['75', '10', '280']


def test_strip_quality_flag_removes_star_suffix() -> None:
    s = pd.Series(['1014.2*', '1013.8*'])
    result = _strip_quality_flag(s)
    assert result.tolist() == ['1014.2', '1013.8']


def test_strip_quality_flag_leaves_clean_values_unchanged() -> None:
    s = pd.Series(['72', '0.00', '-5.5'])
    result = _strip_quality_flag(s)
    assert result.tolist() == ['72', '0.00', '-5.5']


def test_strip_quality_flag_handles_negative() -> None:
    s = pd.Series(['-10s', '-0.5*'])
    result = _strip_quality_flag(s)
    assert result.tolist() == ['-10', '-0.5']


# ------------------------------------------------------------------
# _parse_lcd_csv — shape and filtering
# ------------------------------------------------------------------


def test_parse_lcd_csv_keeps_only_fm15() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    assert len(df) == 3


def test_parse_lcd_csv_filters_to_target_month() -> None:
    df_jan = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    df_feb = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 2)
    assert len(df_jan) == 3
    assert len(df_feb) == 1


def test_parse_lcd_csv_returns_empty_for_missing_month() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 6)
    assert df.empty


def test_parse_lcd_csv_column_names() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    expected = {
        'station_id',
        'iata_code',
        'obs_time_utc',
        'temp_f',
        'dew_point_f',
        'relative_humidity_pct',
        'wind_speed_kts',
        'wind_dir_deg',
        'precip_1h_in',
        'visibility_mi',
        'sky_conditions',
        'sea_level_pressure_hpa',
    }
    assert expected.issubset(set(df.columns))


def test_parse_lcd_csv_station_and_iata_populated() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    assert (df['station_id'] == '720530-14733').all()
    assert (df['iata_code'] == 'ORD').all()


# ------------------------------------------------------------------
# _parse_lcd_csv — numeric coercion
# ------------------------------------------------------------------


def test_parse_lcd_csv_quality_flags_stripped() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    assert df['temp_f'].iloc[1] == pytest.approx(75.0)


def test_parse_lcd_csv_trace_precip_coerced() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    assert df['precip_1h_in'].iloc[2] == pytest.approx(0.001)


def test_parse_lcd_csv_numeric_types_are_float32() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    for col in ('temp_f', 'wind_speed_kts', 'precip_1h_in', 'sea_level_pressure_hpa'):
        assert df[col].dtype == 'float32', f'{col} should be float32'


def test_parse_lcd_csv_obs_time_is_utc() -> None:
    df = _parse_lcd_csv(SAMPLE_CSV.encode(), '720530-14733', 'ORD', 2024, 1)
    assert str(df['obs_time_utc'].dt.tz) == 'UTC'


# ------------------------------------------------------------------
# build_station_map
# ------------------------------------------------------------------


def test_build_station_map_continental_airport() -> None:
    with patch('bmo.ingestion.noaa.httpx.get') as mock_get:
        mock_get.return_value = MagicMock(content=SAMPLE_ISD_CSV.encode(), status_code=200)
        mock_get.return_value.raise_for_status = lambda: None
        mapping = build_station_map({'ORD', 'LAX'})

    assert mapping['ORD'] == '72053014733'
    assert mapping['LAX'] == '72295023174'


def test_build_station_map_noncontinental_airport() -> None:
    with patch('bmo.ingestion.noaa.httpx.get') as mock_get:
        mock_get.return_value = MagicMock(content=SAMPLE_ISD_CSV.encode(), status_code=200)
        mock_get.return_value.raise_for_status = lambda: None
        mapping = build_station_map({'HNL'})

    assert 'HNL' in mapping


def test_build_station_map_excludes_foreign_stations() -> None:
    with patch('bmo.ingestion.noaa.httpx.get') as mock_get:
        mock_get.return_value = MagicMock(content=SAMPLE_ISD_CSV.encode(), status_code=200)
        mock_get.return_value.raise_for_status = lambda: None
        mapping = build_station_map({'ORD', 'LAX', 'LHR'})

    assert 'LHR' not in mapping


def test_build_station_map_excludes_inactive_stations() -> None:
    with patch('bmo.ingestion.noaa.httpx.get') as mock_get:
        mock_get.return_value = MagicMock(content=SAMPLE_ISD_CSV.encode(), status_code=200)
        mock_get.return_value.raise_for_status = lambda: None
        mapping = build_station_map({'DET'})

    assert 'DET' not in mapping


# ------------------------------------------------------------------
# ingest_noaa_month
# ------------------------------------------------------------------


def _make_download_mock(csv_text: str) -> Callable[[str, int], bytes]:
    def _download(station_id: str, year: int) -> bytes:
        return csv_text.encode()

    return _download


def test_ingest_noaa_month_writes_parquet_and_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noaa, '_download_lcd_year', _make_download_mock(SAMPLE_CSV))
    store = MagicMock()

    result = ingest_noaa_month(year=2024, month=1, station_map={'ORD': '720530-14733'}, store=store)

    assert result.skipped is False
    assert result.row_count == 3
    assert result.station_count == 1
    assert store.put_bytes.call_count == 2


def test_ingest_noaa_month_manifest_contents(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noaa, '_download_lcd_year', _make_download_mock(SAMPLE_CSV))
    store = MagicMock()

    ingest_noaa_month(year=2024, month=1, station_map={'ORD': '720530-14733'}, store=store)

    manifest_call = store.put_bytes.call_args_list[1]
    payload = json.loads(manifest_call.args[2])
    assert payload['year'] == 2024
    assert payload['month'] == 1
    assert payload['row_count'] == 3
    assert payload['station_count'] == 1


def test_ingest_noaa_month_parquet_conforms_to_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    import pyarrow.parquet as pq

    monkeypatch.setattr(noaa, '_download_lcd_year', _make_download_mock(SAMPLE_CSV))
    store = MagicMock()

    ingest_noaa_month(year=2024, month=1, station_map={'ORD': '720530-14733'}, store=store)

    parquet_bytes = store.put_bytes.call_args_list[0].args[2]
    table = pq.read_table(io.BytesIO(parquet_bytes))
    assert table.schema == LCD_SCHEMA
    assert table.num_rows == 3


def test_ingest_noaa_month_skips_404_station(monkeypatch: pytest.MonkeyPatch) -> None:
    def download_side_effect(station_id: str, year: int) -> bytes:
        if station_id == 'MISSING':
            raise httpx.HTTPStatusError(
                '404', request=MagicMock(), response=MagicMock(status_code=404)
            )
        return SAMPLE_CSV.encode()

    monkeypatch.setattr(noaa, '_download_lcd_year', download_side_effect)
    store = MagicMock()

    result = ingest_noaa_month(
        year=2024, month=1, station_map={'ORD': '720530-14733', 'ZZZ': 'MISSING'}, store=store
    )

    assert result.row_count == 3
    assert result.station_count == 1


def test_ingest_noaa_month_raises_when_no_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noaa, '_download_lcd_year', _make_download_mock(SAMPLE_CSV))
    store = MagicMock()

    with pytest.raises(RuntimeError, match='No LCD data'):
        ingest_noaa_month(year=2024, month=6, station_map={'ORD': '720530-14733'}, store=store)


def test_ingest_noaa_month_multi_station_combines_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(noaa, '_download_lcd_year', _make_download_mock(SAMPLE_CSV))
    store = MagicMock()

    result = ingest_noaa_month(
        year=2024,
        month=1,
        station_map={'ORD': '720530-14733', 'LAX': '722950-23174'},
        store=store,
    )

    assert result.row_count == 6
    assert result.station_count == 2
