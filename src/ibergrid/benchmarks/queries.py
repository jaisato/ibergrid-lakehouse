"""Benchmark 1 query definitions for DuckDB 2.0 and Polars 2.1 (ADR 0009).

Defines four representative Iberian electricity analytics queries evaluated identically across
three storage layouts (unpartitioned Parquet, partitioned Iceberg, compacted Iceberg) and two
engines (DuckDB and Polars), with cross-engine answer verification and exact Iceberg file/byte
pruning telemetry via PyIceberg's scan planner.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import duckdb
import polars as pl
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.table import Table

from ibergrid.settings import StorageSettings, base_url

StorageLayout = Literal["unpartitioned_parquet", "partitioned_iceberg", "compacted_iceberg"]
EngineName = Literal["duckdb", "polars"]

OCT_2025_START = datetime(2025, 10, 1, 0, 0, 0, tzinfo=UTC)
OCT_2025_END = datetime(2025, 11, 1, 0, 0, 0, tzinfo=UTC)


@dataclass(frozen=True)
class BenchmarkQuerySpec:
    """Declarative specification of one analytical benchmark query."""

    query_id: str
    title: str
    description: str
    source_table: str
    sql_template: str
    polars_builder: Callable[[pl.LazyFrame], pl.LazyFrame]
    iceberg_filter: str | BooleanExpression


def _q1_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.filter(
            (pl.col("dataset") == "demand_realtime")
            & (pl.col("ts_utc") >= pl.lit(OCT_2025_START))
            & (pl.col("ts_utc") < pl.lit(OCT_2025_END))
        )
        .group_by("series_id")
        .agg(
            pl.len().cast(pl.Int64).alias("points"),
            pl.col("value").mean().round(2).alias("avg_mw"),
            pl.col("value").min().round(2).alias("min_mw"),
            pl.col("value").max().round(2).alias("max_mw"),
        )
        .sort("series_id")
    )


def _q2_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.with_columns(pl.col("delivery_date").dt.year().cast(pl.Int32).alias("year"))
        .group_by("year")
        .agg(
            pl.len().cast(pl.Int64).alias("hours"),
            pl.col("price_es_eur_mwh").mean().round(4).alias("avg_price_es"),
            pl.col("price_pt_eur_mwh").mean().round(4).alias("avg_price_pt"),
            pl.col("spread_es_minus_pt_eur_mwh").abs().mean().round(4).alias("mean_abs_spread"),
            pl.col("is_decoupled").cast(pl.Int64).sum().alias("decoupled_hours"),
            pl.col("is_zero_or_negative_es").cast(pl.Int64).sum().alias("zero_or_neg_hours"),
        )
        .sort("year")
    )


def _q3_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.filter(
            (pl.col("ts_utc") >= pl.lit(OCT_2025_START))
            & (pl.col("ts_utc") < pl.lit(OCT_2025_END))
            & pl.col("demand_real_mw").is_not_null()
        )
        .sort("ts_utc")
        .with_columns(
            pl.col("demand_real_mw")
            .rolling_mean(window_size=12, min_samples=1)
            .alias("rolling_1h_mw")
        )
        .group_by("madrid_date")
        .agg(
            pl.len().cast(pl.Int64).alias("points"),
            pl.col("demand_real_mw").max().round(2).alias("peak_mw"),
            pl.col("demand_real_mw").mean().round(2).alias("avg_mw"),
            pl.col("rolling_1h_mw").max().round(2).alias("peak_rolling_1h_mw"),
        )
        .sort("madrid_date")
    )


def _q4_polars(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.filter(pl.col("electric_system").is_in(["peninsular", "canarias"]))
        .group_by("electric_system", "system_scope", "ts_utc_verified", "madrid_date")
        .agg(
            pl.len().cast(pl.Int64).alias("hours"),
            pl.col("demand_mw").sum().round(2).alias("daily_mwh"),
            pl.col("demand_mw").max().round(2).alias("peak_mw"),
        )
        .sort(["electric_system", "madrid_date"])
    )


BENCHMARK_QUERIES: tuple[BenchmarkQuerySpec, ...] = (
    BenchmarkQuerySpec(
        query_id="Q1",
        title="Single-month point lookup (2025-10 OMIE v2 cut-over)",
        description=(
            "Filters bronze.redata_observations (1.69M rows) to 1 dataset and 1 calendar month "
            "(2025-10), exercising double Iceberg partition pruning (dataset + month(ts_utc))."
        ),
        source_table="bronze.redata_observations",
        sql_template="""
            SELECT
                series_id,
                CAST(count(*) AS BIGINT) AS points,
                round(avg(value), 2) AS avg_mw,
                round(min(value), 2) AS min_mw,
                round(max(value), 2) AS max_mw
            FROM {scan_expr}
            WHERE dataset = 'demand_realtime'
              AND ts_utc >= TIMESTAMPTZ '2025-10-01 00:00:00+00'
              AND ts_utc < TIMESTAMPTZ '2025-11-01 00:00:00+00'
            GROUP BY series_id
            ORDER BY series_id
        """,
        polars_builder=_q1_polars,
        iceberg_filter=(
            "dataset == 'demand_realtime' "
            "and ts_utc >= '2025-10-01T00:00:00+00:00' "
            "and ts_utc < '2025-11-01T00:00:00+00:00'"
        ),
    ),
    BenchmarkQuerySpec(
        query_id="Q2",
        title="Full 3.9-year annual MIBEL aggregation",
        description=(
            "Full multi-year analytical aggregation over gold.mibel_hourly_market (2023-2026) "
            "computing annual average ES/PT prices, mean absolute spread, decoupled hours and "
            "zero/negative-price hours."
        ),
        source_table="gold.mibel_hourly_market",
        sql_template="""
            SELECT
                CAST(extract(year FROM delivery_date) AS INTEGER) AS year,
                CAST(count(*) AS BIGINT) AS hours,
                round(avg(price_es_eur_mwh), 4) AS avg_price_es,
                round(avg(price_pt_eur_mwh), 4) AS avg_price_pt,
                round(avg(abs(spread_es_minus_pt_eur_mwh)), 4) AS mean_abs_spread,
                CAST(sum(CASE WHEN is_decoupled THEN 1 ELSE 0 END) AS BIGINT) AS decoupled_hours,
                CAST(
                    sum(CASE WHEN is_zero_or_negative_es THEN 1 ELSE 0 END) AS BIGINT
                ) AS zero_or_neg_hours
            FROM {scan_expr}
            GROUP BY 1
            ORDER BY 1
        """,
        polars_builder=_q2_polars,
        iceberg_filter=AlwaysTrue(),
    ),
    BenchmarkQuerySpec(
        query_id="Q3",
        title="5-minute national demand rolling 1h peak (2025-10)",
        description=(
            "Partition-pruned 1-month scan over silver.demand_national_5min (~409k rows) with "
            "a 12-point (1-hour) rolling mean window function and daily peak aggregation."
        ),
        source_table="silver.demand_national_5min",
        sql_template="""
            WITH windowed AS (
                SELECT
                    madrid_date,
                    demand_real_mw,
                    avg(demand_real_mw) OVER (
                        ORDER BY ts_utc
                        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                    ) AS rolling_1h_mw
                FROM {scan_expr}
                WHERE ts_utc >= TIMESTAMPTZ '2025-10-01 00:00:00+00'
                  AND ts_utc < TIMESTAMPTZ '2025-11-01 00:00:00+00'
                  AND demand_real_mw IS NOT NULL
            )
            SELECT
                madrid_date,
                CAST(count(*) AS BIGINT) AS points,
                round(max(demand_real_mw), 2) AS peak_mw,
                round(avg(demand_real_mw), 2) AS avg_mw,
                round(max(rolling_1h_mw), 2) AS peak_rolling_1h_mw
            FROM windowed
            GROUP BY madrid_date
            ORDER BY madrid_date
        """,
        polars_builder=_q3_polars,
        iceberg_filter=(
            "ts_utc >= '2025-10-01T00:00:00+00:00' and ts_utc < '2025-11-01T00:00:00+00:00'"
        ),
    ),
    BenchmarkQuerySpec(
        query_id="Q4",
        title="Peninsular vs Canarias isolated daily demand scan",
        description=(
            "Multi-year filtered aggregation over silver.demand_system_hourly (170k rows) keeping "
            "Peninsular and Canarias strictly isolated with ts_utc_verified provenance."
        ),
        source_table="silver.demand_system_hourly",
        sql_template="""
            SELECT
                electric_system,
                system_scope,
                ts_utc_verified,
                madrid_date,
                CAST(count(*) AS BIGINT) AS hours,
                round(sum(demand_mw), 2) AS daily_mwh,
                round(max(demand_mw), 2) AS peak_mw
            FROM {scan_expr}
            WHERE electric_system IN ('peninsular', 'canarias')
            GROUP BY electric_system, system_scope, ts_utc_verified, madrid_date
            ORDER BY electric_system, madrid_date
        """,
        polars_builder=_q4_polars,
        iceberg_filter="electric_system in ('peninsular', 'canarias')",
    ),
)


def polars_s3_storage_options(storage: StorageSettings) -> dict[str, str]:
    """Object-store options for Polars ``scan_parquet`` / ``scan_iceberg`` against S3/RustFS/R2."""
    endpoint = base_url(storage.endpoint_url)
    return {
        "aws_endpoint_url": endpoint,
        "aws_access_key_id": storage.access_key.get_secret_value(),
        "aws_secret_access_key": storage.secret_key.get_secret_value(),
        "aws_region": storage.region,
        "aws_virtual_hosted_style_request": "false",
        "aws_allow_http": "true" if endpoint.startswith("http://") else "false",
    }


def plan_iceberg_scan(table: Table, query: BenchmarkQuerySpec) -> tuple[int, int]:
    """Return ``(files_planned, bytes_planned)`` after Iceberg partition & manifest pruning."""
    tasks = list(table.scan(row_filter=query.iceberg_filter).plan_files())
    files_planned = len(tasks)
    bytes_planned = sum(int(task.file.file_size_in_bytes) for task in tasks)
    return files_planned, bytes_planned


def run_duckdb_query(
    con: duckdb.DuckDBPyConnection,
    query: BenchmarkQuerySpec,
    *,
    source: str,
    is_iceberg: bool,
) -> list[tuple[Any, ...]]:
    """Execute ``query`` in DuckDB over an Iceberg table or a Parquet file."""
    scan_expr = "iceberg_scan($source)" if is_iceberg else "read_parquet($source)"
    sql = query.sql_template.format(scan_expr=scan_expr)
    return con.execute(sql, {"source": source}).fetchall()


def run_polars_parquet_query(
    query: BenchmarkQuerySpec,
    parquet_source: str,
    *,
    storage_options: dict[str, str] | None = None,
) -> list[tuple[Any, ...]]:
    """Execute ``query`` in Polars over a Parquet file (local path or ``s3://`` URI)."""
    lf = pl.scan_parquet(parquet_source, storage_options=storage_options)
    return query.polars_builder(lf).collect().rows()


def run_polars_iceberg_query(
    query: BenchmarkQuerySpec,
    table: Table,
    *,
    storage_options: dict[str, str] | None = None,
) -> list[tuple[Any, ...]]:
    """Execute ``query`` in Polars over an Iceberg table using native ``pl.scan_iceberg``."""
    lf = pl.scan_iceberg(
        table,
        storage_options=storage_options,
        use_metadata_statistics=True,
        use_pyiceberg_filter=True,
    )
    return query.polars_builder(lf).collect().rows()


def results_match(
    left: list[tuple[Any, ...]],
    right: list[tuple[Any, ...]],
    *,
    float_tol: float = 0.02,
) -> bool:
    """Verify two engine result sets match row-by-row within ``float_tol`` for float columns."""
    if len(left) != len(right):
        return False
    for row_a, row_b in zip(left, right, strict=True):
        if len(row_a) != len(row_b):
            return False
        for val_a, val_b in zip(row_a, row_b, strict=True):
            if isinstance(val_a, float) and isinstance(val_b, float):
                if abs(val_a - val_b) > float_tol:
                    return False
            elif val_a != val_b:
                return False
    return True
