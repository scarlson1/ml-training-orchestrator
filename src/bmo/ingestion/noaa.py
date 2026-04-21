"""
NOAA Local Climatological Data (LCD) ingestion.

Source: https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/{station_id}.csv
LCD provides hourly ASOS (Automated Surface Observing System) observations from
weather stations physically located at airports. We use LCD instead of GHCN Daily
because flight delay prediction requires weather at departure time — using daily
aggregates (GHCN) would join end-of-day totals to morning flights, a form of
temporal leakage.

Station mapping
---------------
LCD station IDs are derived from NOAA's ISD history file:
    https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv
This maps ICAO codes to ISD/LCD station IDs (USAF 6-digit + WBAN 5-digit),
so no spatial join is needed — on-airport ASOS stations map directly to ICAO codes.

For US continental airports: ICAO = "K" + IATA (KORD → ORD).
Alaska/Hawaii/territories use different ICAO prefixes (PA*, PH*, TJ*); those
airports are handled via a small explicit mapping and logged as warnings if absent.

Report type filtering
---------------------
LCD CSVs mix several report types per station per day:
    FM-15  routine hourly METAR        ← the one we want
    FM-16  special (SPECI) observation
    SOD    summary-of-day record
    SOM    summary-of-month record
We keep only FM-15 so downstream features get one observation per hour.

Annual-file download strategy
------------------------------
NCEI publishes LCD in annual files (one per station per year), not monthly.
We download the full year and filter to the target month. For a monthly
backfill this means re-downloading the same annual file for each month of
that year, but avoids an API key (the CDO API requires one and is rate-limited).
If download volume becomes a concern, cache annual files in S3 at
noaa/_annual/{year}/{station_id}.csv before filtering.

Trace precipitation
-------------------
NOAA encodes trace precipitation (< 0.005 in) as the string "T".
We coerce "T" → 0.001 so the column stays numeric while preserving
the signal that some precipitation occurred.

Quality flags
-------------
LCD numeric fields sometimes have a trailing quality-flag character
(e.g. "75s", "10.5*"). We strip non-numeric suffixes before coercion.
"""

from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import retry, stop_after_attempt, wait_exponential

from bmo.common.storage import ObjectStore

log = logging.getLogger(__name__)

ISD_HISTORY_URL = 'https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv'
LCD_BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access'

# ICAO prefixes used by US airports outside the continental US.
# Continental US: ICAO = "K" + IATA (e.g. KORD → ORD).
# These non-K airports appear in BTS data and need explicit mapping.
_NONCONTINENTAL_ICAO_TO_IATA: dict[str, str] = {
    'PANC': 'ANC',
    'PAFA': 'FAI',
    'PAJN': 'JNU',
    'PABT': 'BET',
    'PADQ': 'KDK',
    'PAFB': 'FBK',
    'PAKN': 'AKN',
    'PAOM': 'OME',
    'PAHO': 'HOM',
    'PADK': 'ADK',
    'PHNL': 'HNL',
    'PHOG': 'OGG',
    'PHKO': 'KOA',
    'PHTO': 'ITO',
    'TJSJ': 'SJU',
    'TJBQ': 'BQN',
    'TJPS': 'PSE',
    'PGUM': 'GUM',
}

# LCD columns we retain from the ~120-column raw file.
_LCD_KEEP = {
    'STATION',
    'DATE',
    'REPORT_TYPE',
    'HourlyDryBulbTemperature',
    'HourlyDewPointTemperature',
    'HourlyRelativeHumidity',
    'HourlyWindSpeed',
    'HourlyWindDirection',
    'HourlyPrecipitation',
    'HourlyVisibility',
    'HourlySkyConditions',
    'HourlyPresentWeatherType',
    'HourlySeaLevelPressure',
}

# Maps raw LCD column names to our snake_case names.
_NUMERIC_RENAME: dict[str, str] = {
    'HourlyDryBulbTemperature': 'temp_f',
    'HourlyDewPointTemperature': 'dew_point_f',
    'HourlyRelativeHumidity': 'relative_humidity_pct',
    'HourlyWindSpeed': 'wind_speed_kts',
    'HourlyWindDirection': 'wind_dir_deg',
    'HourlyPrecipitation': 'precip_1h_in',
    'HourlyVisibility': 'visibility_mi',
    'HourlySeaLevelPressure': 'sea_level_pressure_hpa',
}

LCD_SCHEMA = pa.schema(
    [
        ('station_id', pa.string()),
        ('iata_code', pa.string()),
        ('obs_time_utc', pa.timestamp('us', tz='UTC')),
        ('temp_f', pa.float32()),
        ('dew_point_f', pa.float32()),
        ('relative_humidity_pct', pa.float32()),
        ('wind_speed_kts', pa.float32()),
        ('wind_dir_deg', pa.float32()),
        ('precip_1h_in', pa.float32()),
        ('visibility_mi', pa.float32()),
        ('sky_conditions', pa.string()),
        ('present_weather', pa.string()),
        ('sea_level_pressure_hpa', pa.float32()),
    ]
)


@dataclass
class NoaaIngestResult:
    year: int
    month: int
    station_count: int
    row_count: int
    target_uri: str
    manifest_uri: str
    skipped: bool


# ---------------------------------------------------------------------------
# Station mapping
# ---------------------------------------------------------------------------


def build_station_map(iata_codes: set[str]) -> dict[str, str]:
    """
    Return {iata_code: lcd_station_id} for every airport in iata_codes.

    Downloads the ISD history file once and derives station IDs from ICAO codes.
    Logs a warning for any airport without a matched active station.
    """
    log.info('Downloading ISD station history')
    resp = httpx.get(ISD_HISTORY_URL, timeout=60, follow_redirects=True)
    resp.raise_for_status()

    df = pd.read_csv(io.BytesIO(resp.content), dtype=str, na_values=['', '99999'])
    df.columns = df.columns.str.strip()

    # Station ID used in the LCD bulk download path: zero-padded USAF + WBAN.
    df['station_id'] = df['USAF'].str.zfill(6) + df['WBAN'].str.zfill(5)

    # Derive IATA from ICAO. Continental US: strip leading K.
    # Non-continental airports use the explicit mapping above.
    def icao_to_iata(icao: str) -> str | None:
        if pd.isna(icao):
            return None
        icao = icao.strip()
        if icao in _NONCONTINENTAL_ICAO_TO_IATA:
            return _NONCONTINENTAL_ICAO_TO_IATA[icao]
        if icao.startswith('K') and len(icao) == 4:
            return icao[1:]
        return None

    df['iata_code'] = df['ICAO'].apply(icao_to_iata)

    # Keep only US stations that are active after 2020 and match our airport list.
    # ISD END dates are YYYYMMDD strings; lexicographic comparison works here.
    active = df[
        (df['CTRY'].fillna('') == 'US')
        & df['iata_code'].isin(iata_codes)
        & (df['END'].fillna('0') > '20200101')
    ].drop_duplicates('iata_code')

    # mapping = active.set_index('iata_code')['station_id'].to_dict()
    series = active.set_index('iata_code')['station_id']
    mapping: dict[str, str] = {str(k): str(v) for k, v in series.items()}
    # mapping: dict[str, str] = dict(series.astype(str).items())

    missing = iata_codes - set(mapping.keys())
    if missing:
        log.warning('No LCD station found for %d airports: %s', len(missing), sorted(missing))
    log.info('Station map: %d / %d airports matched', len(mapping), len(iata_codes))
    return mapping


# --------------------------------------------------------------------------#
# Download + parse                                                          #
# --------------------------------------------------------------------------#


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _download_lcd_year(station_id: str, year: int) -> bytes:
    """
    Download the full-year LCD CSV for one station. NCEI only publishes annual
    files, so we fetch the whole year and filter to the target month in the
    caller. Streaming avoids loading 10–50 MB per file into memory at once.
    """
    url = f'{LCD_BASE_URL}/{year}/{station_id}.csv'
    log.info('LCD download: station=%s year=%d', station_id, year)
    with httpx.stream(
        'GET',
        url,
        timeout=httpx.Timeout(30.0, read=300.0),
        follow_redirects=True,
    ) as r:
        r.raise_for_status()
        buf = io.BytesIO()
        for chunk in r.iter_bytes(chunk_size=1 << 20):
            buf.write(chunk)
        return buf.getvalue()


def _strip_quality_flag(series: pd.Series) -> pd.Series:
    """
    LCD appends a single-character quality flag to some numeric fields
    (e.g. "75s", "1014.2*"). Extract only the leading numeric portion.
    """
    return series.astype(str).str.extract(r'^(-?\d+\.?\d*)', expand=False)


def _parse_lcd_csv(
    csv_bytes: bytes,
    station_id: str,
    iata_code: str,
    year: int,
    month: int,
) -> pd.DataFrame:
    """
    Parse one LCD annual CSV and return only FM-15 hourly obs for the target month.
    Returns an empty DataFrame if no matching rows exist.
    """
    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        usecols=lambda c: c in _LCD_KEEP,
        dtype=str,
        low_memory=False,
    )

    # FM-15 = routine hourly METAR. Drop special obs and daily/monthly summaries.
    df = df[df['REPORT_TYPE'].str.strip() == 'FM-15'].copy()

    # DATE is already UTC in LCD files.
    df['DATE'] = pd.to_datetime(df['DATE'], utc=True, errors='coerce')
    df = df.dropna(subset=['DATE'])
    df = df[(df['DATE'].dt.year == year) & (df['DATE'].dt.month == month)]

    if df.empty:
        return df

    # Trace precipitation: "T" means < 0.005 in. Coerce to 0.001 to keep numeric
    # while preserving the signal that some precipitation occurred.
    if 'HourlyPrecipitation' in df.columns:
        df['HourlyPrecipitation'] = df['HourlyPrecipitation'].replace('T', '0.001')

    for raw_col, target_col in _NUMERIC_RENAME.items():
        if raw_col in df.columns:
            df[target_col] = pd.to_numeric(
                _strip_quality_flag(df[raw_col]), errors='coerce'
            ).astype('float32')

    df['station_id'] = station_id
    df['iata_code'] = iata_code
    df = df.rename(
        columns={
            'DATE': 'obs_time_utc',
            'HourlySkyConditions': 'sky_conditions',
            'HourlyPresentWeatherType': 'present_weather',
        }
    )

    keep = [
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
        'present_weather',
        'sea_level_pressure_hpa',
    ]
    return df[[c for c in keep if c in df.columns]]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def ingest_noaa_month(
    year: int,
    month: int,
    station_map: dict[str, str],
    store: ObjectStore,
    bucket: str = 'raw',
    prefix: str = 'noaa',
) -> NoaaIngestResult:
    """
    Download LCD weather for all stations in station_map, filter to the target
    month, and write a single Parquet file partitioned by year/month.

    station_map is {iata_code: lcd_station_id} — build it once with
    build_station_map() and reuse across months.
    """
    target_key = f'{prefix}/year={year}/month={month:02d}/weather.parquet'
    manifest_key = f'{prefix}/_manifests/{year}-{month:02d}.json'
    target_uri = f's3://{bucket}/{target_key}'
    manifest_uri = f's3://{bucket}/{manifest_key}'

    frames: list[pd.DataFrame] = []
    for iata_code, station_id in station_map.items():
        try:
            csv_bytes = _download_lcd_year(station_id, year)
            df = _parse_lcd_csv(csv_bytes, station_id, iata_code, year, month)
            if not df.empty:
                frames.append(df)
        except httpx.HTTPStatusError as exc:
            # 404 = NCEI has no file for this station+year. One missing station
            # shouldn't abort the whole month — log and continue.
            log.warning('LCD 404 for %s (%s) year=%d: %s', iata_code, station_id, year, exc)

    if not frames:
        raise RuntimeError(f'No LCD data retrieved for {year}-{month:02d}')

    combined = pd.concat(frames, ignore_index=True)
    table = pa.Table.from_pandas(combined, preserve_index=False).cast(LCD_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression='zstd', compression_level=3)
    store.put_bytes(bucket, target_key, buf.getvalue())

    manifest = {
        'year': year,
        'month': month,
        'station_count': len(frames),
        'row_count': len(combined),
        'target_uri': target_uri,
        'ingested_at_utc': datetime.now(timezone.utc).isoformat(),
    }
    store.put_bytes(bucket, manifest_key, json.dumps(manifest, indent=2).encode())

    return NoaaIngestResult(
        year=year,
        month=month,
        station_count=len(frames),
        row_count=len(combined),
        target_uri=target_uri,
        manifest_uri=manifest_uri,
        skipped=False,
    )
