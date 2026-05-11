# Staging & Validation

## Overview

The staging layer transforms raw ingested data into clean, UTC-normalized, validated Iceberg tables. Four assets are produced:

| Asset | Source | Partitioned |
| --- | --- | --- |
| `dim_airport` | FAA airport data + NOAA station map | No |
| `dim_route` | OpenFlights routes + `dim_airport` coords | No |
| `staged_flights` | BTS on-time performance (monthly) | Yes — `flight_date` |
| `staged_weather` | NOAA ASOS/LCD surface observations (monthly) | Yes — `obs_time_utc` |

Every staging asset writes directly to [Apache Iceberg](https://iceberg.apache.org/docs/latest/) tables via [PyIceberg](https://py.iceberg.apache.org/) and returns `MaterializeResult`. Rejected rows are quarantined to a separate S3 bucket (`s3://rejected/`) instead of causing hard failures, keeping bad data visible for debugging.

---

## PyArrow Schema Validation

Schemas are declared as [`pa.Schema`](https://arrow.apache.org/docs/python/api/datatypes.html#pyarrow.Schema) constants and validation is done with [`pyarrow.compute`](https://arrow.apache.org/docs/python/api/compute.html) predicates. There is no row-by-row Python loop for schema enforcement — PyArrow operates in bulk on columnar buffers.

### Flight Records

`STAGED_FLIGHTS_SCHEMA` ([src/bmo/staging/contracts.py](../src/bmo/staging/contracts.py)) is the authoritative column list for `staged_flights`. It adds four UTC timestamp columns that do not exist in the raw BTS file:

```
scheduled_departure_utc   pa.timestamp('us', tz='UTC')
actual_departure_utc      pa.timestamp('us', tz='UTC')   # null if cancelled
scheduled_arrival_utc     pa.timestamp('us', tz='UTC')
actual_arrival_utc        pa.timestamp('us', tz='UTC')   # null if cancelled/diverted
```

After UTC timestamps are computed, `stage_flights` trims the DataFrame to exactly the columns in `STAGED_FLIGHTS_SCHEMA`, inserts `None` for any optional column that was absent in the raw file, and casts to the schema with `safe=False` (numeric narrowing is intentional — e.g. BTS floats down-cast to `float32`).

#### Invalid-Row Guards

`validate_flights` applies four rules in priority order (first match wins):

| Rule | Rejection reason |
| --- | --- |
| `origin` or `dest` is not exactly 3 characters | `invalid_iata_code` |
| `distance_mi` is ≤ 0 or > 6,000 miles | `implausible_distance` |
| `scheduled_departure_utc` is null (timezone lookup failed) | `missing_scheduled_departure_utc` |
| Flight not cancelled but `actual_departure_utc` is null | `missing_actual_departure_for_operated_flight` |

The `reject_mask` is a boolean PyArrow array. Each rule is evaluated independently with `pc.and_` / `pc.or_`, then OR'd into the running mask so a row is only tagged once.

#### UTC Timestamp Conversion

BTS encodes all times as HHMM integers in the airport's **local** timezone — not UTC. The conversion pipeline in [src/bmo/staging/timezone.py](../src/bmo/staging/timezone.py) and [src/bmo/staging/flights.py](../src/bmo/staging/flights.py):

1. Look up the origin/dest IANA timezone from `dim_airport.tz_database_timezone`
2. Call `local_hhmm_to_utc(flight_date, hhmm, tz_name, day_offset)` per row
3. Construct a tz-aware `datetime` using [`zoneinfo.ZoneInfo`](https://docs.python.org/3/library/zoneinfo.html) and call `.astimezone(UTC)`

`zoneinfo` is used (not `pytz`) because it handles DST gaps/folds correctly via PEP 615 semantics — `pytz.localize()` can silently produce wrong results for ambiguous local times.

### Weather Observations (FM-15)

NOAA ASOS data follows the [METAR/FM-15](https://www.noaa.gov/jetstream/synoptic/metar) surface observation format. `stage_weather` applies two domain guards using `pyarrow.compute`:

| Rule | Rationale |
| --- | --- |
| `-100 °F ≤ temp_f ≤ 150 °F` | Instruments occasionally report sentinel error values (e.g. 9999) |
| `wind_speed_kts ≥ 0` | Negative wind speed is physically impossible |

Null observations pass the guards — sensors occasionally fail and a null is preferable to a fabricated imputation at this layer. Null tolerance for critical weather columns is 5% before the downstream asset check flags an error (vs. 1% for flights).

### Airport & Route Dimensions

`dim_airport` is built from the FAA airport database enriched with a `lcd_station_id` from a precomputed NOAA station-to-IATA mapping (`noaa/_station_map.json`). The `tz_database_timezone` column is the critical output — it feeds every UTC conversion in `staged_flights`.

`dim_route` joins OpenFlights route data with `dim_airport` coordinates to compute great-circle distances. Both dimension tables are **unpartitioned** and written with a full overwrite — they are small (< 10K rows) and infrequently updated.

#### Haversine Distance Computation

`_haversine_mi` ([src/bmo/staging/dimensions.py](../src/bmo/staging/dimensions.py)) computes as-the-crow-flies distance in statute miles using the [Haversine formula](https://en.wikipedia.org/wiki/Haversine_formula):

```
a = sin²(Δlat/2) + cos(lat1)·cos(lat2)·sin²(Δlon/2)
c = 2·atan2(√a, √(1−a))
d = R·c          # R = 3958.8 miles (mean Earth radius)
```

Routes without coordinates for either endpoint are dropped (`dropna(subset=['distance_mi'])`). BTS on-time records already include `distance_mi` directly from the source, so the route table distance is used for cross-validation rather than as the primary value.

---

## Schema Contracts

### How `contracts.py` Works

[src/bmo/staging/contracts.py](../src/bmo/staging/contracts.py) owns two things:

1. **Structural contract** — `STAGED_FLIGHTS_SCHEMA`, a `pa.Schema` that is the single source of truth for column names and types in the Iceberg table.
2. **Value-level contract** — `validate_flights()`, which splits a PyArrow table into `(valid, rejected)`. Rejected rows get an extra `rejection_reason` string column appended before being written to the quarantine bucket.

This separation means structural validation happens at cast time (`table.cast(STAGED_FLIGHTS_SCHEMA, safe=False)`) and value validation happens at check time (`validate_flights(staged_table)`). A cast failure is a hard error; a validation failure quarantines rows rather than stopping the pipeline.

### Null-Rate Checks

Null thresholds differ by table and column criticality:

| Table | Column(s) | Threshold | Rationale |
| --- | --- | --- | --- |
| `staged_flights` | `scheduled_departure_utc`, `origin`, `dest`, `flight_date` | 1% | Required for join keys and feature engineering |
| `staged_weather` | `station_id`, `iata_code`, `obs_time_utc` | 5% | Weather sensors have higher natural failure rate |
| `dim_airport` | `iata_code` | 0% | Primary key — any null is a data error |
| `dim_airport` | `tz_database_timezone` | 5% | Every null causes a `staged_flights` timezone miss |
| `dim_route` | `distance_mi` | 1% | Missing distance drops the route from training features |

### Asset Checks in Dagster

Checks are defined in [dagster_project/asset_checks/schema_checks.py](../dagster_project/asset_checks/schema_checks.py) using the [`@asset_check`](https://docs.dagster.io/guides/build/asset-checks/) decorator. Each check reads directly from the Iceberg table for the relevant partition rather than re-running staging logic:

```python
@asset_check(asset='staged_flights', partitions_def=MONTHLY_PARTITIONS)
def check_staged_flights_nulls(context) -> AssetCheckResult:
    ...
    table = iceberg_table.scan(row_filter=..., selected_fields=(...)).to_arrow()
```

This means checks pass through the Iceberg scan pushdown and only read the columns they need.

### Blocking vs. Warning Checks

Dagster checks have two severity levels ([`AssetCheckSeverity`](https://docs.dagster.io/api/python-api/asset-checks#dagster.AssetCheckSeverity)):

| Severity | Behavior | Used for |
| --- | --- | --- |
| `ERROR` | Blocks downstream assets from materializing | Null-rate failures, missing critical columns, empty tables |
| `WARN` | Logs a warning; pipeline continues | Schema evolution detection (added/removed columns vs. `STAGED_FLIGHTS_SCHEMA`) |

The schema evolution check (`check_staged_flights_schema_evolution`) always returns `passed=True` but uses `WARN` severity when columns diverge. This keeps the pipeline running when BTS adds or removes a column upstream, while surfacing the drift for human review. Iceberg handles the underlying schema change gracefully via its native [schema evolution](https://iceberg.apache.org/docs/latest/evolution/#schema-evolution) support.

---

## Iceberg Table Format

### Why Iceberg

[Apache Iceberg](https://iceberg.apache.org/docs/latest/) is used for all staging tables instead of plain Parquet for three reasons:

1. **Idempotent monthly overwrites** — `table.overwrite(data, overwrite_filter=row_filter)` replaces exactly one month's data atomically. Re-running a month's staging job is safe.
2. **Time travel** — every overwrite produces a new [snapshot](https://iceberg.apache.org/docs/latest/concepts/#snapshots). The `snapshot_id` is recorded in Dagster asset metadata so any materialization can be reproduced by scanning a specific snapshot.
3. **Schema evolution** — adding or removing columns does not require rewriting existing data files. The catalog tracks the current schema; old files are read with null-fill for new columns.

### Catalog Setup (SqlCatalog → Postgres)

`make_catalog()` ([src/bmo/common/iceberg.py](../src/bmo/common/iceberg.py)) returns a [`SqlCatalog`](https://py.iceberg.apache.org/api/#pyiceberg.catalog.sql.SqlCatalog) backed by PostgreSQL:

```python
SqlCatalog(
    'bmo',
    uri='postgresql+psycopg2://...',       # from settings.iceberg_catalog_uri
    's3.endpoint': settings.s3_endpoint_url,
    's3.path-style-access': 'true',        # required for MinIO
    ...
)
```

The catalog stores table metadata (schema, partition spec, snapshot history, manifest files) in Postgres. Data files (Parquet) live in MinIO under `s3://staging/iceberg/<table_name>/`. MinIO requires path-style S3 access (`s3.path-style-access = true`); virtual-hosted-style requests fail against non-AWS endpoints.

### Partition Strategy

Both fact tables are partitioned by month using Iceberg's built-in [`MonthTransform`](https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning):

| Table | Partition column | Transform |
| --- | --- | --- |
| `staged_flights` | `flight_date` (date32) | `MonthTransform` |
| `staged_weather` | `obs_time_utc` (timestamp UTC) | `MonthTransform` |

`MonthTransform` derives the partition value from the column at write time — queries that filter on the full date or timestamp column automatically benefit from partition pruning without requiring a separate partition column in the schema. See [Iceberg hidden partitioning](https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning).

Dimension tables (`dim_airport`, `dim_route`) are unpartitioned (`PartitionSpec()`) — they are small and fully overwritten on each run.

### Schema Evolution

`get_or_create_table` creates a new Iceberg table only if it does not exist; otherwise it loads the existing one. The existing schema in the catalog is authoritative — new staging runs do not attempt to alter it. If BTS adds a column, the asset check (`check_staged_flights_schema_evolution`) fires a `WARN`. A human then decides whether to migrate the schema using PyIceberg's [`update_schema()`](https://py.iceberg.apache.org/api/#pyiceberg.table.Table.update_schema).

### Partition Overwrite Semantics (Idempotency)

`overwrite_month_flights` and `overwrite_month_weather` use `Table.overwrite(data, overwrite_filter=row_filter)` to replace exactly one calendar month atomically:

```python
row_filter = And(
    GreaterThanOrEqual('flight_date', '2024-03-01'),
    LessThan('flight_date', '2024-04-01'),
)
table.overwrite(arrow_data, overwrite_filter=row_filter)
```

PyIceberg translates the filter into a [delete predicate](https://py.iceberg.apache.org/api/#pyiceberg.table.Table.overwrite) that removes existing data files touching that month before appending the new files in a single atomic commit. Year-wrap is handled explicitly: month 12 uses `date(year + 1, 1, 1)` as the exclusive upper bound.

---

## Staging Asset Dependencies

All staging assets (`dim_airport`, `dim_route`, `staged_flights`, `staged_weather`) return `MaterializeResult` and write their output directly to S3 via `ObjectStore`. Dagster's file IO manager never stores a value for these assets.

### `deps` vs `ins`

Because no asset value is stored by the IO manager, upstream assets must be declared using `deps` or `AssetDep` — never `ins` / `AssetIn`. Using `ins` tells Dagster to load the upstream asset's value from IO manager storage at execution time, which raises a `FileNotFoundError` since nothing is written there.

For unpartitioned dependencies (e.g. `dim_airport`), use a plain string in `deps`:

```python
deps=['dim_airport']
```

For partitioned dependencies where you need to express a partition mapping, use `AssetDep` with `partition_mapping=`:

```python
from dagster import AssetDep, TimeWindowPartitionMapping

deps=[
    AssetDep(
        'raw_bts_flights',
        partition_mapping=TimeWindowPartitionMapping(start_offset=0, end_offset=0),
    )
]
```

This preserves the partition dependency wiring in the asset graph without triggering an IO manager load. See [Dagster partition mappings](https://docs.dagster.io/guides/build/partitions-and-backfills/partition-dependencies).

---

## Timezone Handling

### UTC Conversion Utilities

[src/bmo/staging/timezone.py](../src/bmo/staging/timezone.py) provides two public functions:

**`local_hhmm_to_utc(flight_date, hhmm, tz_name, day_offset=0)`**

Converts a BTS HHMM integer (e.g. `1430` = 14:30) in the airport's local timezone to a UTC-aware `datetime`. The conversion:

1. Constructs a naive `datetime` by adding `hhmm_to_td(hhmm)` and `timedelta(days=day_offset)` to `flight_date`
2. Attaches the IANA timezone via `replace(tzinfo=ZoneInfo(tz_name))` — this is the PEP 615-compliant approach
3. Calls `.astimezone(UTC)` to get the UTC-aware result

**`arrival_day_offset(dep_hhmm, arr_hhmm) → int`**

Returns `1` if the flight arrives the next calendar day, `0` otherwise. The heuristic: if `arr_hhmm` is more than 60 minutes earlier (in clock time) than `dep_hhmm`, the flight crosses midnight. The 60-minute gap is chosen because the shortest nonstop US domestic flights are ~45 minutes, so any apparent clock regression of more than 60 minutes must be an overnight crossing.

### Edge Cases (DST, Midnight Crossings)

**`2400` — BTS midnight encoding**

BTS uses `2400` to represent exactly midnight at the start of the *next* calendar day. `hhmm_to_td(2400)` returns `timedelta(hours=24)`, which when added to the flight date advances to the correct date. Any value outside `[0, 2400]` raises `ValueError`.

**DST transitions**

`zoneinfo.ZoneInfo` uses the [IANA timezone database](https://www.iana.org/time-zones) and correctly resolves DST gaps (spring-forward) and folds (fall-back). Using `replace(tzinfo=tz)` on a naive datetime that falls in a DST gap produces a datetime in the "wall clock" interpretation — consistent with how BTS records times. `pytz.localize()` is explicitly avoided here due to its ambiguous behavior around fold times.

**Overnight arrivals**

Arrival timestamps use `arrival_day_offset` to determine whether `day_offset=1` should be passed to `local_hhmm_to_utc`. This is computed from the *scheduled* departure and arrival HHMM pair (not actual) so that cancelled flights with no actual times still get correct scheduled timestamps.

---

## What Happens When Validation Fails

Failures take two forms:

**Soft failures (quarantine)** — rows that fail `validate_flights` or the weather domain guards are written to `s3://rejected/{source}/year={Y}/month={MM}/` as Parquet with an appended `rejection_reason` string column. The staging asset still returns `MaterializeResult` with `rejected_count` in metadata. This keeps bad data visible and auditable without halting the pipeline.

**Hard failures (asset check ERROR)** — after materialization, Dagster runs the `@asset_check` functions against the Iceberg table. A null rate exceeding the threshold (or a missing column) returns `AssetCheckSeverity.ERROR`, which marks the asset check as failed and blocks downstream assets from materializing until the check passes.

**Warning signals** — schema drift (added/removed columns vs. `STAGED_FLIGHTS_SCHEMA`) returns `AssetCheckSeverity.WARN`. The pipeline continues, but the Dagster UI surfaces the drift. Iceberg handles the underlying column change; the human decision is whether to update `STAGED_FLIGHTS_SCHEMA` and downstream feature engineering.
