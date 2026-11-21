"""Benchmark 2: zero-scan Iceberg metadata checks vs full SQL data scans (ADR 0009)."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from ibergrid.benchmarks.queries import results_match
from ibergrid.benchmarks.stats import LatencySummary, time_callable
from ibergrid.quality.metadata import (
    TABLE_METADATA_RULES,
    TableMetadataSummary,
    inspect_table_metadata,
)

# Column pairs (time/key column, numeric metric column) verified by both metadata stats and full
# SQL scans across Bronze, Silver and Gold tables.
METADATA_BENCHMARK_TARGETS: tuple[tuple[str, str, str], ...] = (
    ("bronze.redata_observations", "ts_utc", "value"),
    ("bronze.omie_day_ahead_prices", "delivery_date", "price_es_eur_mwh"),
    ("bronze.redata_demand_stream", "ts_utc", "value"),
    ("silver.demand_national_5min", "ts_utc", "demand_real_mw"),
    ("silver.demand_system_hourly", "ts_utc", "demand_mw"),
    ("silver.mibel_prices_period", "delivery_date", "price_es_eur_mwh"),
    ("silver.mibel_prices_hourly", "hour_utc", "price_es_eur_mwh"),
    ("silver.generation_daily", "generation_date", "generation_mwh"),
    ("gold.mibel_hourly_market", "hour_utc", "price_es_eur_mwh"),
    ("gold.mibel_daily_summary", "delivery_date", "price_es_mean_eur_mwh"),
    ("gold.system_demand_daily", "demand_date", "demand_total_mwh"),
    ("gold.optimal_consumption_windows", "calendar_month", "price_es_mean_eur_mwh"),
)


@dataclass(frozen=True)
class MetadataVsScanRow:
    """Paired timing and byte-scan comparison for one Iceberg table."""

    table: str
    total_records: int
    data_files: int
    total_data_bytes: int
    metadata_bytes_scanned: int
    metadata_timing: LatencySummary
    full_scan_timing: LatencySummary
    speedup_p50: float
    bytes_saved_pct: float
    counts_match: bool


def benchmark_metadata_vs_full_scan(
    con: duckdb.DuckDBPyConnection,
    *,
    logical_table: str,
    duck_table: str,
    time_col: str,
    metric_col: str,
    warmup: int = 2,
    iterations: int = 10,
) -> MetadataVsScanRow | None:
    """Compare zero-scan Iceberg metadata audit against an equivalent full SQL scan.

    Returns ``None`` when ``duck_table`` has no committed snapshot yet.
    """
    initial = inspect_table_metadata(con, duck_table)
    if initial.snapshot_id is None or initial.total_records == 0:
        return None

    def _run_metadata() -> TableMetadataSummary:
        return inspect_table_metadata(con, duck_table)

    # Full SQL equivalent: computes row count, null counts and min/max across the primary metric
    # and all columns audited by TABLE_METADATA_RULES for this table (apples-to-apples with
    # inspect_table_metadata, which computes manifest statistics for every column).
    # Column identifiers come exclusively from static registry/rule definitions.
    rule = TABLE_METADATA_RULES.get(logical_table)
    extra_cols: list[str] = []
    if rule is not None:
        for col in (*rule.non_null_columns, *rule.numeric_bounds.keys()):
            if col not in {metric_col, time_col} and col not in extra_cols:
                extra_cols.append(col)

    extra_selects = "".join(
        f", CAST(count(*) FILTER (WHERE {col} IS NULL) AS BIGINT), min({col}), max({col})"
        for col in extra_cols
    )
    full_sql = (
        f"SELECT CAST(count(*) AS BIGINT), "  # noqa: S608 - static schema columns
        f"CAST(count(*) FILTER (WHERE {metric_col} IS NULL) AS BIGINT), "
        f"min({metric_col}), max({metric_col}), min({time_col}), max({time_col})"
        f"{extra_selects} "
        f"FROM iceberg_scan($table)"
    )

    def _run_full_scan() -> tuple[int, int, float | None, float | None]:
        row = con.execute(full_sql, {"table": duck_table}).fetchone()
        assert row is not None  # noqa: S101 - aggregate query always returns 1 row
        return (
            int(row[0]),
            int(row[1]),
            None if row[2] is None else float(row[2]),
            None if row[3] is None else float(row[3]),
        )

    meta_summary, meta_timing = time_callable(_run_metadata, warmup=warmup, iterations=iterations)
    scan_stats, scan_timing = time_callable(_run_full_scan, warmup=warmup, iterations=iterations)

    metric_stat = meta_summary.columns.get(metric_col)
    meta_tuple = (
        meta_summary.total_records,
        metric_stat.null_value_count if metric_stat is not None else -1,
        metric_stat.min_numeric if metric_stat is not None else None,
        metric_stat.max_numeric if metric_stat is not None else None,
    )
    counts_match = results_match([meta_tuple], [scan_stats], float_tol=1e-4)

    speedup = round(scan_timing.p50_ms / meta_timing.p50_ms, 2) if meta_timing.p50_ms > 0.0 else 0.0
    bytes_saved_pct = 100.0 if meta_summary.total_data_bytes > 0 else 0.0
    return MetadataVsScanRow(
        table=logical_table,
        total_records=meta_summary.total_records,
        data_files=meta_summary.data_files,
        total_data_bytes=meta_summary.total_data_bytes,
        metadata_bytes_scanned=meta_summary.data_bytes_scanned,
        metadata_timing=meta_timing,
        full_scan_timing=scan_timing,
        speedup_p50=speedup,
        bytes_saved_pct=bytes_saved_pct,
        counts_match=counts_match,
    )
