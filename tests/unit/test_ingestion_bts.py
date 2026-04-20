"""
Unit tests for BTS ingestion.

Location in project: tests/unit/test_ingestion_bts.py

These tests mock the HTTP layer and exercise the CSV parsing + schema coercion
against small in-memory fixtures. For integration tests that hit a real
MinIO instance, see tests/integration/.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from unittest.mock import MagicMock

import pytest

from bmo.ingestion import bts
from bmo.ingestion.bts import (
    PARQUET_SCHEMA,
    _csv_to_parquet,
    _extract_csv,
    ingest_month,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------
SAMPLE_ROWS = (
    'Year,Month,DayofMonth,DayOfWeek,FlightDate,'
    'Reporting_Airline,Tail_Number,Flight_Number_Reporting_Airline,'
    'Origin,OriginCityName,OriginState,'
    'Dest,DestCityName,DestState,'
    'CRSDepTime,DepTime,DepDelay,DepDelayMinutes,DepDel15,'
    'CRSArrTime,ArrTime,ArrDelay,ArrDelayMinutes,ArrDel15,'
    'Cancelled,CancellationCode,Diverted,'
    'CRSElapsedTime,ActualElapsedTime,AirTime,Distance,'
    'CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay\n'
    # normal on-time flight
    '2024,1,15,1,2024-01-15,'
    'AA,N123AA,100,'
    'JFK,"New York, NY",NY,'
    'LAX,"Los Angeles, CA",CA,'
    '0800,0805,5.00,5.00,0.00,'
    '1100,1058,-2.00,0.00,0.00,'
    '0.00,,0.00,'
    '360.00,353.00,340.00,2475.00,'
    ',,,,\n'
    # cancelled flight — nulls in actual fields
    '2024,1,15,1,2024-01-15,'
    'UA,N456UA,200,'
    'ORD,"Chicago, IL",IL,'
    'SFO,"San Francisco, CA",CA,'
    '0900,,,,,'
    '1200,,,,,'
    '1.00,B,0.00,'
    '270.00,,,1846.00,'
    ',,,,\n'
    # delayed flight with delay breakdown
    '2024,1,15,1,2024-01-15,'
    'DL,N789DL,300,'
    'ATL,"Atlanta, GA",GA,'
    'JFK,"New York, NY",NY,'
    '1000,1045,45.00,45.00,1.00,'
    '1230,1335,65.00,65.00,1.00,'
    '0.00,,0.00,'
    '150.00,170.00,155.00,760.00,'
    '20.00,0.00,15.00,0.00,30.00\n'
)


def make_fake_zip(csv_text: str, csv_name: str = 'On_Time.csv') -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_text)
    return buf.getvalue()


# ------------------------------------------------------------------
# _extract_csv
# ------------------------------------------------------------------
def test_extract_csv_returns_bytes() -> None:
    csv_text = 'Year,Month\n2024,1\n'
    out = _extract_csv(make_fake_zip(csv_text))
    assert out.startswith(b'Year,Month')


def test_extract_csv_rejects_multiple_csvs() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr('a.csv', 'x')
        zf.writestr('b.csv', 'y')
    with pytest.raises(ValueError, match='Expected exactly one CSV'):
        _extract_csv(buf.getvalue())


def test_extract_csv_rejects_no_csv() -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr('readme.txt', 'hi')
    with pytest.raises(ValueError):
        _extract_csv(buf.getvalue())


# ------------------------------------------------------------------
# _csv_to_parquet
# ------------------------------------------------------------------
def test_csv_to_parquet_shapes() -> None:
    table = _csv_to_parquet(SAMPLE_ROWS.encode())
    assert table.num_rows == 3
    assert set(table.column_names) == set(PARQUET_SCHEMA.names)
    assert table.schema == PARQUET_SCHEMA


def test_csv_to_parquet_cancelled_bool_coerces() -> None:
    table = _csv_to_parquet(SAMPLE_ROWS.encode())
    cancelled = table['cancelled'].to_pylist()
    assert cancelled == [False, True, False]


def test_csv_to_parquet_preserves_airport_codes_as_strings() -> None:
    table = _csv_to_parquet(SAMPLE_ROWS.encode())
    origins = table['origin'].to_pylist()
    assert origins == ['JFK', 'ORD', 'ATL']


def test_csv_to_parquet_delay_breakdown_nullable() -> None:
    table = _csv_to_parquet(SAMPLE_ROWS.encode())
    carrier_delays = table['carrier_delay_min'].to_pylist()
    # first two rows have no delay breakdown, third does
    assert carrier_delays[0] is None
    assert carrier_delays[1] is None
    assert carrier_delays[2] == pytest.approx(20.0)


# ------------------------------------------------------------------
# ingest_month — idempotency
# ------------------------------------------------------------------
def test_ingest_month_skips_when_sha_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    zip_bytes = make_fake_zip(SAMPLE_ROWS)
    expected_sha = hashlib.sha256(zip_bytes).hexdigest()

    monkeypatch.setattr(bts, '_download_zip', lambda url: zip_bytes)

    store = MagicMock()
    store.read_json_or_none.return_value = {
        'source_sha256': expected_sha,
        'row_count': 3,
    }
    store.exists.return_value = True

    result = ingest_month(year=2024, month=1, store=store)

    assert result.skipped is True
    store.put_bytes.assert_not_called()


def test_ingest_month_writes_when_no_prior_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    zip_bytes = make_fake_zip(SAMPLE_ROWS)
    monkeypatch.setattr(bts, '_download_zip', lambda url: zip_bytes)

    store = MagicMock()
    store.read_json_or_none.return_value = None

    result = ingest_month(year=2024, month=1, store=store)

    assert result.skipped is False
    assert result.row_count == 3
    # one call for the parquet, one for the manifest
    assert store.put_bytes.call_count == 2

    manifest_call = store.put_bytes.call_args_list[1]
    manifest_payload = json.loads(manifest_call.args[2])
    assert manifest_payload['row_count'] == 3
    assert manifest_payload['year'] == 2024
    assert manifest_payload['month'] == 1


def test_ingest_month_rejects_invalid_month() -> None:
    store = MagicMock()
    with pytest.raises(ValueError, match='month must be 1'):
        ingest_month(year=2024, month=13, store=store)


def test_ingest_month_rejects_pre_1987() -> None:
    store = MagicMock()
    with pytest.raises(ValueError, match='1987'):
        ingest_month(year=1986, month=12, store=store)
