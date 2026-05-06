# Data Ingestion

## Overview

The ingestion layer pulls data from three external sources into the raw S3 bucket as Parquet files. All assets live in [`dagster_project/assets/raw.py`](../dagster_project/assets/raw.py) (Dagster group `raw`); the actual download and parsing logic lives in [`src/bmo/ingestion/`](../src/bmo/ingestion/).

Asset dependency graph:

```text
raw_faa_airports ──► station_map ──► raw_noaa_weather  (monthly partitioned)
raw_openflights_routes
raw_bts_flights                                         (monthly partitioned)
```

---

## Data Sources

### BTS On-Time Performance (Flights)

#### Source & URL

`https://transtats.bts.gov/PREZIP/`

Files follow the naming pattern:

```text
On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{YYYY}_{M}.zip
```

Note: the month has **no leading zero** in the filename (e.g. `_2025_1.zip`, not `_2025_01.zip`). BTS publishes with a ~2-month lag, so in May 2026 the latest available month is typically February or March 2026. Data goes back to 1987.

#### Download Cadence

Monthly, triggered by `bts_new_month_sensor` (see [Ingestion Sensor](#ingestion-sensor)). The asset also has a freshness policy: warn after 32 days, fail after 35 days without a new materialization.

#### Raw Schema

The raw CSV has ~110 columns. The ingestion layer retains 35 (defined in `KEEP_COLUMNS` in [`src/bmo/ingestion/bts.py`](../src/bmo/ingestion/bts.py)) and renames them to snake_case:

| Raw column | Stored column | Type | Notes |
| --- | --- | --- | --- |
| `Year` | `year` | `int16` | |
| `Month` | `month` | `int8` | |
| `DayofMonth` | `day_of_month` | `int8` | |
| `DayOfWeek` | `day_of_week` | `int8` | |
| `FlightDate` | `flight_date` | `date32` | |
| `Reporting_Airline` | `reporting_airline` | `string` | IATA carrier code |
| `Tail_Number` | `tail_number` | `string` | |
| `Flight_Number_Reporting_Airline` | `flight_number` | `int32` | |
| `Origin` | `origin` | `string` | IATA airport code |
| `OriginCityName` | `origin_city` | `string` | |
| `OriginState` | `origin_state` | `string` | |
| `Dest` | `dest` | `string` | IATA airport code |
| `DestCityName` | `dest_city` | `string` | |
| `DestState` | `dest_state` | `string` | |
| `CRSDepTime` | `crs_dep_time_hhmm` | `int16` | Local airport time; UTC conversion done in staging |
| `DepTime` | `dep_time_hhmm` | `int16` | NULL on cancelled flights |
| `DepDelay` | `dep_delay_min` | `float32` | Signed: negative = early |
| `DepDelayMinutes` | `dep_delay_min_nonneg` | `float32` | Same delay, floored at 0 |
| `DepDel15` | `dep_del15` | `bool` | |
| `CRSArrTime` | `crs_arr_time_hhmm` | `int16` | |
| `ArrTime` | `arr_time_hhmm` | `int16` | |
| `ArrDelay` | `arr_delay_min` | `float32` | |
| `ArrDelayMinutes` | `arr_delay_min_nonneg` | `float32` | |
| `ArrDel15` | `arr_del15` | `bool` | |
| `Cancelled` | `cancelled` | `bool` | |
| `CancellationCode` | `cancellation_code` | `string` | |
| `Diverted` | `diverted` | `bool` | |
| `CRSElapsedTime` | `crs_elapsed_min` | `float32` | |
| `ActualElapsedTime` | `actual_elapsed_min` | `float32` | |
| `AirTime` | `air_time_min` | `float32` | |
| `Distance` | `distance_mi` | `float32` | |
| `CarrierDelay` | `carrier_delay_min` | `float32` | |
| `WeatherDelay` | `weather_delay_min` | `float32` | |
| `NASDelay` | `nas_delay_min` | `float32` | |
| `SecurityDelay` | `security_delay_min` | `float32` | |
| `LateAircraftDelay` | `late_aircraft_delay_min` | `float32` | |

#### Parsing Logic

Implemented in `ingest_month()` ([`src/bmo/ingestion/bts.py`](../src/bmo/ingestion/bts.py)):

1. Download the ZIP (up to 5 retries with exponential backoff).
2. Check SHA-256 against the stored manifest; skip if unchanged (idempotent).
3. Extract the single CSV from the ZIP.
4. Read with PyArrow CSV, filtering to `KEEP_COLUMNS` and treating `''`/`NA` as null.
5. Rename columns to snake_case via `RAW_TO_TARGET`.
6. Coerce boolean columns (`cancelled`, `diverted`, `dep_del15`, `arr_del15`) from BTS float strings (`"1.00"` / `"0.00"`) to native `bool`.
7. Cast to `PARQUET_SCHEMA` with `safe=False` (allows float64→float32 narrowing).

Notable gotchas:
- Times (`CRSDepTime`, `DepTime`, etc.) are `HHMM` integers in **local airport time**. UTC conversion happens in the staging layer.
- Cancelled flights have null `DepTime`/`ArrTime` but valid `CRSDepTime`/`CRSArrTime` — kept as-is.
- Historical CSVs sometimes have trailing commas producing a spurious unnamed column; `include_columns` drops it.

#### Output Format & Location

| Artifact | S3 key |
| --- | --- |
| Parquet data | `raw/bts/year={YYYY}/month={MM}/data.parquet` |
| Ingestion manifest | `raw/bts/_manifests/{YYYY}-{MM}.json` |

Compression: zstd level 3. The manifest records `source_url`, `source_sha256`, `row_count`, `ingested_at_utc`, and `parquet_schema_fingerprint`.

---

### NOAA Weather (LCD)

#### Source & URL

NOAA Local Climatological Data (LCD), served as annual CSV files:

```
https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/{station_id}.csv
```

LCD provides FM-15 hourly ASOS observations from on-airport weather stations. LCD is used instead of GHCN Daily because GHCN provides end-of-day aggregates — joining those to flight departure times is temporal leakage.

#### Station Selection

Station IDs are derived from the NOAA ISD history file (`https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv`), which maps ICAO codes to USAF/WBAN station IDs. The mapping is built once by the `station_map` Dagster asset and stored at `raw/noaa/_station_map.json`.

ICAO → IATA resolution:
- Continental US: strip the leading `K` (e.g. `KORD` → `ORD`).
- Alaska, Hawaii, territories: explicit lookup table in [`src/bmo/ingestion/noaa.py`](../src/bmo/ingestion/noaa.py) (`_NONCONTINENTAL_ICAO_TO_IATA`).

Only stations with `CTRY == "US"` and an active end-date after 2020-01-01 are included.

#### Raw Schema

Only FM-15 (routine hourly METAR) report type rows are kept from the ~120-column CSV. Stored columns:

| Column | Type | Notes |
| --- | --- | --- |
| `station_id` | `string` | USAF+WBAN (e.g. `72530094846`) |
| `iata_code` | `string` | Airport IATA code |
| `obs_time_utc` | `timestamp[us, UTC]` | LCD timestamps are already UTC |
| `temp_f` | `float32` | Dry-bulb temperature |
| `dew_point_f` | `float32` | |
| `relative_humidity_pct` | `float32` | |
| `wind_speed_kts` | `float32` | |
| `wind_dir_deg` | `float32` | |
| `precip_1h_in` | `float32` | Trace ("T") coerced to 0.001 |
| `visibility_mi` | `float32` | |
| `sky_conditions` | `string` | Raw METAR cloud layer string |
| `present_weather` | `string` | Raw METAR weather type string |
| `sea_level_pressure_hpa` | `float32` | |

#### Parsing Logic

Implemented in `ingest_noaa_month()` ([`src/bmo/ingestion/noaa.py`](../src/bmo/ingestion/noaa.py)):

1. For each airport in the station map, fetch the full annual LCD CSV (`_fetch_lcd_year`). Annual files are cached in S3 at `raw/noaa/_annual/{year}/{station_id}.csv` to avoid re-downloading during backfills.
2. Filter to `REPORT_TYPE == "FM-15"` (routine hourly METAR).
3. Filter to the target year and month.
4. Coerce trace precipitation: `"T"` → `0.001`.
5. Strip quality-flag characters from numeric fields (e.g. `"75s"` → `75`, `"1014.2*"` → `1014.2`).
6. Concatenate all stations, cast to `LCD_SCHEMA`, write Parquet.

404s and timeouts per station are logged as warnings and skipped; the month fails only if *no* stations return data.

#### Output Format & Location

| Artifact | S3 key |
| --- | --- |
| Parquet data | `raw/noaa/year={YYYY}/month={MM}/data.parquet` |
| Ingestion manifest | `raw/noaa/_manifests/{YYYY}-{MM}.json` |
| Annual CSV cache | `raw/noaa/_annual/{YYYY}/{station_id}.csv` |

Compression: zstd level 3.

---

### FAA Airport Reference (OurAirports / 5010)

#### Source & URL

`https://ourairports.com/data/airports.csv`

The official FAA 5010 database uses a fixed-width format that is difficult to work with. OurAirports publishes the same data as a clean CSV derived from official FAA/ICAO sources. Timezone data (missing from OurAirports since mid-2023) is supplemented from the `airportsdata` Python package.

#### Fields Used

Filtered to US `medium_airport` and `large_airport` rows with a valid IATA code:

| Column | Type |
| --- | --- |
| `iata_code` | `string` |
| `icao_code` | `string` |
| `name` | `string` |
| `type` | `string` (`medium_airport` / `large_airport`) |
| `latitude_deg` | `float64` |
| `longitude_deg` | `float64` |
| `elevation_ft` | `float32` |
| `municipality` | `string` |
| `iso_region` | `string` (e.g. `US-CA`) |
| `tz_database_timezone` | `string` (e.g. `America/Chicago`) |

#### Output Format & Location

Static (non-partitioned). Written once and refreshed when the asset is re-materialized.

| Artifact | S3 key |
| --- | --- |
| Parquet data | `raw/faa/airports.parquet` |

Compression: zstd (default level).

---

### OpenFlights Route Graph

#### Source & URL

`https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat`

A headerless CSV of nonstop airline routes. Null values are encoded as `\N` (MySQL dump convention).

#### Fields Used

| Column | Type | Notes |
| --- | --- | --- |
| `airline_iata` | `string` | |
| `origin` | `string` | IATA airport code |
| `dest` | `string` | IATA airport code |
| `codeshare` | `bool` | `"Y"` → `True` |
| `stops` | `int8` | Almost always 0 |
| `equipment` | `string` | Space-separated IATA aircraft codes |

Rows without valid 3-character IATA codes for origin or destination are dropped.

#### Output Format & Location

Static (non-partitioned).

| Artifact | S3 key |
| --- | --- |
| Parquet data | `raw/openflights/routes.parquet` |

Compression: zstd (default level).

---

## Ingestion Sensor

### `bts_new_month_sensor` — How It Works

Defined in [`dagster_project/sensors/bts_new_month.py`](../dagster_project/sensors/bts_new_month.py).

On each evaluation the sensor:

1. Scrapes `https://transtats.bts.gov/PREZIP/` for links matching the BTS filename regex.
2. Converts each matched filename to a Dagster partition key (`{YYYY}-{MM}-01`).
3. Compares the available set against the cursor (a JSON list of previously seen keys).
4. Yields one `RunRequest` per new partition key, using the partition key as `run_key` for Dagster-level deduplication.
5. Updates the cursor to the full available set.

### Trigger Conditions

- Interval: every 6 hours (`minimum_interval_seconds=21600`).
- A `SkipReason` is returned when no new months are detected; the latest available month is logged.

### Backfilling Historical Partitions

Use the Dagster UI partition backfill or the CLI:

```bash
dagster asset backfill --select raw_bts_flights --partition-range 2024-01-01...2024-12-01
dagster asset backfill --select raw_noaa_weather --partition-range 2024-01-01...2024-12-01
```

`ingest_month()` is idempotent: it downloads the upstream ZIP, computes its SHA-256, and skips the write if the manifest already records a matching hash and the Parquet file exists. Pass `--force` to the CLI entry point (`python -m bmo.ingestion.bts`) to override.

---

## Raw Data Storage

### S3 Path Convention

| Asset | S3 path |
| --- | --- |
| BTS flights (partitioned) | `raw/bts/year={YYYY}/month={MM}/data.parquet` |
| BTS manifest | `raw/bts/_manifests/{YYYY}-{MM}.json` |
| NOAA weather (partitioned) | `raw/noaa/year={YYYY}/month={MM}/data.parquet` |
| NOAA manifest | `raw/noaa/_manifests/{YYYY}-{MM}.json` |
| NOAA annual cache | `raw/noaa/_annual/{YYYY}/{station_id}.csv` |
| NOAA station map | `raw/noaa/_station_map.json` |
| FAA airports | `raw/faa/airports.parquet` |
| OpenFlights routes | `raw/openflights/routes.parquet` |

The bucket name defaults to `raw` (`S3_BUCKET_RAW` env var).

### Parquet Compression Settings

| Source | Compression |
| --- | --- |
| BTS flights | zstd, level 3 |
| NOAA weather | zstd, level 3 |
| FAA airports | zstd (default) |
| OpenFlights routes | zstd (default) |

### Retention Policy

No automated retention policy is currently enforced on the raw bucket. The NOAA annual CSV cache (`noaa/_annual/`) can be pruned once all months in a given year have been ingested — each cache entry is only needed to avoid re-downloading during backfills.

---

## Adding a New Data Source

1. **Implement the ingestion function** in a new module under [`src/bmo/ingestion/`](../src/bmo/ingestion/). Return a dataclass with at minimum `row_count`, `target_uri`, and `skipped`.

2. **Add path helpers** to [`src/bmo/common/paths.py`](../src/bmo/common/paths.py) following the `BtsPaths` / `NoaaPaths` pattern. Define `raw_key()` and `manifest_key()` so paths are computed consistently.

3. **Add a Dagster asset** in [`dagster_project/assets/raw.py`](../dagster_project/assets/raw.py). Use `partitions_def=MONTHLY_PARTITIONS` if the source is time-series data; leave it unpartitioned for static dimensions.

4. **Write a manifest** from the ingestion function (url, sha256, row_count, ingested_at_utc). This enables idempotent re-runs and gives Dagster metadata to display in the UI.

5. **Add a sensor or schedule** if the source publishes updates on a regular cadence. Follow the `bts_new_month_sensor` pattern: track seen partitions via cursor, yield `RunRequest` with `run_key` set to the partition key.
