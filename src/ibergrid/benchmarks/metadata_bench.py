"""Benchmark 2: zero-scan Iceberg metadata checks vs full SQL data scans (ADR 0009)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import duckdb

from ibergrid.benchmarks.stats import LatencySummary, time_callable
from ibergrid.quality.metadata import (
    TABLE_METADATA_RULES,
    ColumnStat,
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
    audited_columns: int
    metadata_timing: LatencySummary
    full_scan_timing: LatencySummary
    speedup_p50: float
    bytes_saved_pct: float
    counts_match: bool


def _parse_utc_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        # DuckDB iceberg_column_stats formats UTC timestamps like "2023-01-01 00:00:00+00".
        if len(raw) >= 3 and raw[-3] in {"+", "-"} and raw[-2:].isdigit():
            raw = raw + ":00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    return None


def _column_stat_matches(
    stat: ColumnStat | None,
    *,
    sql_nulls: int,
    sql_min: Any,
    sql_max: Any,
    float_tol: float = 1e-4,
) -> bool:
    """Verify one column's Iceberg manifest stats against full SQL null count and min/max."""
    if stat is None or stat.null_value_count != sql_nulls:
        return False
    if sql_min is None and sql_max is None:
        return stat.lower_bound is None and stat.upper_bound is None

    # Numeric columns (including INT/DOUBLE/BIGINT): compare parsed float bounds.
    if isinstance(sql_min, (int, float)) and not isinstance(sql_min, bool):
        if stat.min_numeric is None or stat.max_numeric is None:
            return False
        return (
            abs(stat.min_numeric - float(sql_min)) <= float_tol
            and abs(stat.max_numeric - float(sql_max)) <= float_tol
        )

    if isinstance(sql_min, bool):
        if stat.lower_bound is None or stat.upper_bound is None:
            return False
        return (
            stat.lower_bound.lower() == str(sql_min).lower()
            and stat.upper_bound.lower() == str(sql_max).lower()
        )

    if isinstance(sql_min, datetime):
        meta_min = _parse_utc_timestamp(stat.lower_bound)
        meta_max = _parse_utc_timestamp(stat.upper_bound)
        ref_min = _parse_utc_timestamp(sql_min)
        ref_max = _parse_utc_timestamp(sql_max)
        return (
            meta_min is not None
            and meta_max is not None
            and meta_min == ref_min
            and meta_max == ref_max
        )

    if isinstance(sql_min, date):
        return stat.lower_bound == sql_min.isoformat() and stat.upper_bound == sql_max.isoformat()

    # Strings: Iceberg truncates string bounds (default truncate(16)) such that
    # lower_bound <= actual_min and upper_bound >= actual_max (exact when untruncated).
    if isinstance(sql_min, str) and isinstance(sql_max, str):
        if stat.lower_bound is None or stat.upper_bound is None:
            return False
        return stat.lower_bound <= sql_min and stat.upper_bound >= sql_max

    return stat.lower_bound == str(sql_min) and stat.upper_bound == str(sql_max)


def _audited_columns_for(logical_table: str, time_col: str, metric_col: str) -> tuple[str, ...]:
    cols: list[str] = [metric_col, time_col]
    rule = TABLE_METADATA_RULES.get(logical_table)
    if rule is not None:
        for col in (*rule.non_null_columns, *rule.numeric_bounds.keys()):
            if col not in cols:
                cols.append(col)
    return tuple(cols)


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

    Verifies total row count plus null counts and min/max bounds for EVERY audited column
    (primary metric, time column and every column in ``TABLE_METADATA_RULES``). Returns ``None``
    when ``duck_table`` has no committed snapshot yet.
    """
    initial = inspect_table_metadata(con, duck_table)
    if initial.snapshot_id is None or initial.total_records == 0:
        return None

    audited_cols = _audited_columns_for(logical_table, time_col, metric_col)

    def _run_metadata() -> TableMetadataSummary:
        return inspect_table_metadata(con, duck_table)

    # Full SQL equivalent: computes row count plus null count and min/max for every audited column.
    # Column identifiers come exclusively from static registry/rule definitions.
    col_selects = ", ".join(
        f"CAST(count(*) FILTER (WHERE {col} IS NULL) AS BIGINT), min({col}), max({col})"
        for col in audited_cols
    )
    full_sql = (
        f"SELECT CAST(count(*) AS BIGINT), {col_selects} "  # noqa: S608 - static schema columns
        f"FROM iceberg_scan($table)"
    )

    def _run_full_scan() -> tuple[int, tuple[tuple[int, Any, Any], ...]]:
        row = con.execute(full_sql, {"table": duck_table}).fetchone()
        assert row is not None  # noqa: S101 - aggregate query always returns 1 row
        per_col = tuple(
            (int(row[1 + 3 * idx]), row[2 + 3 * idx], row[3 + 3 * idx])
            for idx in range(len(audited_cols))
        )
        return int(row[0]), per_col

    meta_summary, meta_timing = time_callable(_run_metadata, warmup=warmup, iterations=iterations)
    (sql_total_rows, sql_col_stats), scan_timing = time_callable(
        _run_full_scan, warmup=warmup, iterations=iterations
    )

    counts_match = meta_summary.total_records == sql_total_rows and all(
        _column_stat_matches(
            meta_summary.columns.get(col),
            sql_nulls=nulls,
            sql_min=min_val,
            sql_max=max_val,
        )
        for col, (nulls, min_val, max_val) in zip(audited_cols, sql_col_stats, strict=True)
    )

    speedup = round(scan_timing.p50_ms / meta_timing.p50_ms, 2) if meta_timing.p50_ms > 0.0 else 0.0
    bytes_saved_pct = 100.0 if meta_summary.total_data_bytes > 0 else 0.0
    return MetadataVsScanRow(
        table=logical_table,
        total_records=meta_summary.total_records,
        data_files=meta_summary.data_files,
        total_data_bytes=meta_summary.total_data_bytes,
        metadata_bytes_scanned=meta_summary.data_bytes_scanned,
        audited_columns=len(audited_cols),
        metadata_timing=meta_timing,
        full_scan_timing=scan_timing,
        speedup_p50=speedup,
        bytes_saved_pct=bytes_saved_pct,
        counts_match=counts_match,
    )
