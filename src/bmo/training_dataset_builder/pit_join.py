"""
Implements the PIT join using DuckDB's ASOF JOIN, one feature view at a time, then assembles the results.

The ASOF JOIN is a specialized time-series join: for each row in the left table, find the "most recent" matching row in the right table that satisfies the join condition. right table must be sorted by the ordering column.

Design rationale: could use Feast's get_historical_features() for this,
but doing the join explicitly in DuckDB has three advantages:
  1. The SQL is auditable — can inspect exactly what query ran.
  2. It works without a running Feast registry (useful in CI).
  3. The ASOF JOIN semantics are testable in isolation.

Use the Feast registry to discover feature view configurations
(entity keys, TTLs, feature column names) — Feast as config, DuckDB as compute.

DuckDB ASOF JOIN docs: https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

import duckdb
import pandas as pd

from bmo.common.config import settings


@dataclass(frozen=True)
class FeatureViewConfig:
    """
    Configuration for one feature view's PIT join.

    This is the join spec: which Parquet to read, which column is the entity
    key, what the TTL is, and which columns are features vs. metadata.

    Frozen (immutable) so it's safe to cache and pass between threads.
    """

    name: str
    parquet_path: str
    entity_col: str
    label_entity_col: str
    ttl: timedelta
    feature_cols: list[str] = field(default_factory=list)

    @property
    def ttl_seconds(self) -> int:
        return int(self.ttl.total_seconds())


def _configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    """
    Configure DuckDB's httpfs extension with S3/MinIO credentials.

    DuckDB can read Parquet files directly from S3 URIs using the httpfs
    extension. Configure it here using the same env vars as the rest of
    the project. The training dataset builder works against
    MinIO in dev and against Cloudflare R2 in production.

    Docs: https://duckdb.org/docs/guides/network_and_cloud/s3_import.html
    """
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute(f"""
        SET s3_region='{settings.s3_region}';
        SET s3_access_key='{settings.s3_access_key_id}';
        SET s3_secret_access_key='{settings.s3_secret_access_key}';
        set s3_endpoint='{settings.s3_endpoint}';
        set s3_use_ssl=false;
        SET s3_url_style='path';
    """)


class PITJoiner:
    """
    Executes point-in-time joins for multiple feature views against a label DataFrame.

    Usage:
        joiner = PITJoiner(feature_views=[origin_cfg, dest_cfg, carrier_cfg], as_of=cutoff)
        dataset_df = joiner.join(label_df)

    The returned DataFrame has one row per row in label_df. Feature columns
    that couldn't be joined (no matching entity, or TTL exceeded) are NULL.
    NULLs are acceptable — the model training step handles imputation.
    """

    def __init__(
        self,
        feature_views: list[FeatureViewConfig],
        as_of: pd.Timestamp | None = None,
        use_s3: bool = True,
    ) -> None:
        self.feature_views = feature_views
        self.as_of = as_of
        self.use_s3 = use_s3

    def join(self, label_df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute PIT joins for all configured feature views and return a wide DataFrame.

        The join proceeds in steps:
          1. Register label_df as a DuckDB table ('labels').
          2. For each feature view, execute a single ASOF JOIN CTE.
          3. Collect the per-view feature DataFrames.
          4. Merge all results back onto label_df via flight_id.

        Step 4 is a pandas merge, not a SQL join. This keeps the SQL simple
        (each CTE is self-contained) and makes it easy to inspect intermediate
        results during debugging.
        """
        con = duckdb.connect()
        if self.use_s3:
            _configure_duckdb_s3(con)

        con.register('labels', label_df)

        result_df = label_df.copy()

        for fv in self.feature_views:
            fv_df = self._join_one_view(con, fv)
            feature_cols_present = [c for c in fv.feature_cols if c in fv_df.columns]
            # left merge: rows in label_df that have no matching entity get NULLs
            result_df = result_df.merge(
                fv_df[['flight_id'] + feature_cols_present + [f'{fv.name}_feature_ts']],
                on='flight_id',
                how='left',
            )

        con.close()
        return result_df

    def _join_one_view(self, con: duckdb.DuckDBPyConnection, fv: FeatureViewConfig) -> pd.DataFrame:
        """
        Execute the ASOF JOIN for a single feature view.

        The SQL has three layers:
          1. 'features' CTE: load the Parquet, apply the as_of filter,
             and sort by event_ts (required for ASOF JOIN correctness).
          2. 'pit' CTE: execute the ASOF JOIN. For each label row, this
             finds the latest feature snapshot where
               feature.event_ts <= label.event_timestamp.
          3. Outer SELECT: apply TTL filter by nulling out features where
               (event_timestamp - feature_ts) > ttl_seconds.

        Why null-out instead of filter-out?
          Filtering drops the row entirely, which shrinks the training set.
          Null-out keeps the row but marks the feature as missing, which lets
          the model learn from partial information and the pipeline stay aware
          that TTL was exceeded (useful for monitoring).

        Docs: https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins
        """
        as_of_filter = ''
        if self.as_of is not None:
            as_of_str = pd.Timestamp(self.as_of).isoformat()
            as_of_filter = f"WHERE event_ts <= TIMESTAMPTZ '{as_of_str}'"

        feature_select = ', '.join(f'pit.{col}' for col in fv.feature_cols)

        # Build TTL-masked feature select:
        # If age (seconds between feature snapshot and event) > TTL, return NULL.
        # epoch() returns a float of seconds since Unix epoch; subtracting two
        # epoch() calls gives the difference in seconds.
        ttl_masked_select = ', '.join(
            f'CASE WHEN age_seconds > {fv.ttl_seconds} THEN NULL ELSE pit.{col} END AS {col}'
            for col in fv.feature_cols
        )
        # pit query returns the most recent even if its older than TTL

        # features CTE columns: entity_key, event_ts, avg_dep_delay_7d, ..., etc.
        # PIT CTE columns: flight_id, event_timestamp, age_seconds, avg_dep_delay_7d, etc.

        sql = f"""
        WITH features AS (
            -- Load feature Parquet and apply as_of cutoff
            -- ORDER BY required for DuckDB ASOF JOIN
            -- table sorted by the ordering column (event_ts)
            SELECT
                {fv.entity_col} AS __entity_key,
                event_ts,
                {', '.join(fv.feature_cols)}
            FROM read_parquet('{fv.parquet_path}')
            {as_of_filter}
            ORDER BY __entity_key, event_ts
        ),
        pit AS (
            -- ASOF JOIN: for each label row, find the LATEST feature snapshot
            -- where feature.event_ts <= label.event_timestamp.
            -- NULLs appear when no feature snapshot exists for that entity.
            SELECT
                labels.flight_id,
                labels.event_timestamp,
                features.event_ts as __feature_ts,
                epoch(labels.event_timestamp::TIMESTAMP)
                    - epoch(features.event_ts::TIMESTAMP) AS age_seconds,
                {feature_select}
            FROM
            ASOF LEFT JOIN features
                ON labels.{fv.label_entity_col} = features.__entity_key
                AND labels.event_timestamp >= features.event_ts
        )
        SELECT
            flight_id,
            __feature_ts AS {fv.name}__feature_ts,
            {ttl_masked_select}
        FROM pit
        """

        result: pd.DataFrame = con.execute(sql).df()
        return result


def default_feature_view_configs(feast_s3_base: str) -> list[FeatureViewConfig]:
    """
    Return the canonical feature view configurations matching feature_repo/feature_views.py.

    Centralizing this here means:
      - The TTLs in one place mirror the Feast registry.
      - If you update a TTL in feature_views.py, update it here too.
      - Tests use this function, so they test the real TTLs.

    An improvement for later: read these directly from the Feast registry
    via store.get_feature_view(name).ttl to eliminate the duplication.
    See: https://docs.feast.dev/reference/feast-sdk/feast.feature_store
    """
    return [
        FeatureViewConfig(
            name='origin_airport_features',
            parquet_path=f'{feast_s3_base}/origin_airport/data/parquet',
            entity_col='origin',
            label_entity_col='origin',
            ttl=timedelta(hours=26),
            feature_cols=[
                'origin_flight_count_1h',
                'origin_avg_dep_delay_1h',
                'origin_pct_delayed_1h',
                'origin_avg_dep_delay_24h',
                'origin_pct_cancelled_24h',
                'origin_acg_dep_delay_7h',
                'origin_pct_delayed_7d',
                'origin_congestion_score_1h',
            ],
        ),
        FeatureViewConfig(
            name='dest_airport_features',
            parquet_path=f'{feast_s3_base}/dest_airport/data.parquet',
            entity_col='dest',
            label_entity_col='dest',
            ttl=timedelta(hours=26),
            feature_cols=[
                'dest_avg_arr_delay_1h',
                'dest_pct_delayed_1h',
                'dest_avg_arr_delay_24h',
                'dest_pct_diverted_24h',
            ],
        ),
        FeatureViewConfig(
            name='carrier_features',
            parquet_path=f'{feast_s3_base}/carrier/data.parquet',
            entity_col='carrier',
            label_entity_col='carrier',
            ttl=timedelta(days=8),
            feature_cols=[
                'carrier_on_time_pct_7d',
                'carrier_cancellation_rate_7d',
                'carrier_avg_delay_7d',
                'carrier_flight_count_7d',
            ],
        ),
        FeatureViewConfig(
            name='route_features',
            parquet_path=f'{feast_s3_base}/route/data.parquet',
            entity_col='route_key',
            label_entity_col='route_key',
            ttl=timedelta(days=8),
            feature_cols=[
                'route_avg_dep_delay_7d',
                'route_avg_arr_delay_7d',
                'route_pct_delayed_7d',
                'route_cancellation_rate_7d',
                'route_avg_elapsed_7d',
                'route_distance_mi',
            ],
        ),
        FeatureViewConfig(
            name='aircraft_features',
            parquet_path=f'{feast_s3_base}/aircraft/data.parquet',
            entity_col='tail_number',
            label_entity_col='tail_number',
            ttl=timedelta(hours=12),
            feature_cols=[
                'cascading_delay_min',
                'turnaround_min',
            ],
        ),
    ]
