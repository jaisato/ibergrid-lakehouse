"""Zero-scan Iceberg metadata quality checks (ADR 0008).

Reads ONLY Iceberg metadata and Avro manifests through DuckDB's ``iceberg_snapshots``,
``iceberg_files`` and ``iceberg_column_stats`` table functions — zero Parquet data files are opened
(``data_bytes_scanned == 0``). Supports both ``main`` and specific WAP audit branch snapshots
(``snapshot_id``), and works identically against Lakekeeper catalog names (``lake.ns.table``) and
local ``metadata.json`` paths.
"""

from __future__ import annotations

import math
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import duckdb

NUMERIC_COLUMN_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "INT",
        "BIGINT",
        "HUGEINT",
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DECIMAL",
    }
)


@dataclass(frozen=True)
class ColumnStat:
    """Aggregated manifest-level statistics for one column across all live data files."""

    column_name: str
    column_type: str
    value_count: int
    null_value_count: int
    nan_value_count: int
    lower_bound: str | None
    upper_bound: str | None
    min_numeric: float | None = None
    max_numeric: float | None = None


@dataclass(frozen=True)
class TableMetadataSummary:
    """Manifest-only summary of a table snapshot; ``data_bytes_scanned`` is always 0."""

    table: str
    snapshot_id: int | None
    snapshot_committed_utc: datetime | None
    operation: str | None
    data_files: int
    delete_files: int
    total_records: int
    total_data_bytes: int
    data_bytes_scanned: int
    partitions: int
    columns: dict[str, ColumnStat]
    elapsed_ms: float


@dataclass(frozen=True)
class MetadataRule:
    """Declarative checks evaluated purely from Iceberg manifest statistics."""

    min_rows: int = 0
    non_null_columns: tuple[str, ...] = ()
    numeric_bounds: Mapping[str, tuple[float | None, float | None]] = field(default_factory=dict)
    max_delete_files: int | None = None
    max_staleness_hours: float | None = None


@dataclass(frozen=True)
class MetadataCheckViolation:
    """One failed zero-scan metadata rule."""

    table: str
    rule_name: str
    reason: str


@dataclass(frozen=True)
class MetadataAuditResult:
    """Outcome of auditing a table snapshot via metadata alone."""

    summary: TableMetadataSummary
    passed: bool
    violations: tuple[MetadataCheckViolation, ...]
    elapsed_ms: float


# Default domain metadata rules for registered tables (ADR 0003, 0004, 0007, 0008).
TABLE_METADATA_RULES: dict[str, MetadataRule] = {
    "bronze.redata_observations": MetadataRule(
        min_rows=0,
        non_null_columns=("dataset", "electric_system", "series_id", "ts_utc", "value"),
    ),
    "bronze.omie_day_ahead_prices": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "delivery_date",
            "period",
            "interval_start_utc",
            "price_es_eur_mwh",
            "price_pt_eur_mwh",
        ),
        numeric_bounds={
            "price_es_eur_mwh": (-500.0, 4000.0),
            "price_pt_eur_mwh": (-500.0, 4000.0),
            "period": (1.0, 100.0),
        },
    ),
    "bronze.redata_demand_stream": MetadataRule(
        min_rows=0,
        non_null_columns=("series_id", "electric_system", "ts_utc", "value", "kafka_offset"),
        numeric_bounds={"value": (0.0001, None)},
    ),
    "silver.demand_national_5min": MetadataRule(
        min_rows=0,
        non_null_columns=("ts_utc", "madrid_date", "source_lane"),
        numeric_bounds={"demand_real_mw": (0.0001, None)},
    ),
    "silver.demand_system_hourly": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "ts_utc",
            "madrid_date",
            "electric_system",
            "system_scope",
            "ts_utc_verified",
            "demand_mw",
        ),
        numeric_bounds={"demand_mw": (0.0001, None)},
    ),
    "silver.mibel_prices_period": MetadataRule(
        min_rows=0,
        non_null_columns=("delivery_date", "period", "price_es_eur_mwh", "price_pt_eur_mwh"),
        numeric_bounds={
            "price_es_eur_mwh": (-500.0, 4000.0),
            "price_pt_eur_mwh": (-500.0, 4000.0),
        },
    ),
    "silver.mibel_prices_hourly": MetadataRule(
        min_rows=0,
        non_null_columns=("hour_utc", "delivery_date", "price_es_eur_mwh", "price_pt_eur_mwh"),
        numeric_bounds={
            "price_es_eur_mwh": (-500.0, 4000.0),
            "price_pt_eur_mwh": (-500.0, 4000.0),
        },
    ),
    "silver.generation_daily": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "generation_date",
            "electric_system",
            "series_id",
            "technology_slug",
            "generation_mwh",
        ),
    ),
    "gold.mibel_hourly_market": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "hour_utc",
            "delivery_date",
            "price_es_eur_mwh",
            "price_pt_eur_mwh",
            "source_attribution",
        ),
        numeric_bounds={
            "price_es_eur_mwh": (-500.0, 4000.0),
            "price_pt_eur_mwh": (-500.0, 4000.0),
            "peninsular_demand_mw": (0.0001, None),
        },
    ),
    "gold.mibel_daily_summary": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "delivery_date",
            "hours_in_day",
            "price_es_mean_eur_mwh",
            "source_attribution",
        ),
        numeric_bounds={
            "hours_in_day": (23.0, 25.0),
            "price_es_mean_eur_mwh": (-500.0, 4000.0),
            "price_pt_mean_eur_mwh": (-500.0, 4000.0),
        },
    ),
    "gold.system_demand_daily": MetadataRule(
        min_rows=0,
        non_null_columns=(
            "demand_date",
            "electric_system",
            "system_scope",
            "ts_utc_verified",
            "demand_total_mwh",
        ),
        numeric_bounds={"demand_total_mwh": (0.0001, None), "demand_min_mw": (0.0001, None)},
    ),
    "gold.optimal_consumption_windows": MetadataRule(
        min_rows=0,
        non_null_columns=("calendar_month", "day_type", "madrid_hour", "window_rank"),
        numeric_bounds={"calendar_month": (1.0, 12.0), "madrid_hour": (0.0, 23.0)},
    ),
}


def inspect_table_metadata(
    con: duckdb.DuckDBPyConnection,
    table: str,
    *,
    snapshot_id: int | None = None,
) -> TableMetadataSummary:
    """Read snapshot, file and column statistics from Iceberg metadata alone (0 data bytes read)."""
    started = time.perf_counter()
    resolved_id, operation, committed = _resolve_snapshot(con, table, snapshot_id)
    if resolved_id is None:
        return TableMetadataSummary(
            table=table,
            snapshot_id=None,
            snapshot_committed_utc=None,
            operation=None,
            data_files=0,
            delete_files=0,
            total_records=0,
            total_data_bytes=0,
            data_bytes_scanned=0,
            partitions=0,
            columns={},
            elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
        )

    params: dict[str, Any] = {"table": table, "snapshot_id": resolved_id}
    files_rows = con.execute(
        "SELECT content, count(*), coalesce(sum(record_count), 0), "
        "coalesce(sum(file_size_in_bytes), 0) "
        "FROM iceberg_files($table, snapshot_from_id => $snapshot_id) "
        "GROUP BY content",
        params,
    ).fetchall()
    data_files = 0
    delete_files = 0
    total_records = 0
    total_data_bytes = 0
    for content, count, records, size_bytes in files_rows:
        if int(content) == 0:
            data_files += int(count)
            total_records += int(records)
            total_data_bytes += int(size_bytes)
        else:
            delete_files += int(count)

    stats_rows = con.execute(
        "SELECT column_name, column_type, partition, value_count, null_value_count, "
        "nan_value_count, lower_bound, upper_bound "
        "FROM iceberg_column_stats($table, snapshot_from_id => $snapshot_id) "
        "WHERE content = 'DATA'",
        params,
    ).fetchall()

    partitions: set[str] = set()
    by_column: dict[str, list[tuple[str, int, int, int, str | None, str | None]]] = {}
    for col_name, col_type, part, val_cnt, null_cnt, nan_cnt, lower, upper in stats_rows:
        if part is not None:
            partitions.add(str(part))
        by_column.setdefault(str(col_name), []).append(
            (
                str(col_type),
                int(val_cnt or 0),
                int(null_cnt or 0),
                int(nan_cnt or 0),
                None if lower is None else str(lower),
                None if upper is None else str(upper),
            )
        )

    columns = {name: _aggregate_column(name, rows) for name, rows in by_column.items()}
    return TableMetadataSummary(
        table=table,
        snapshot_id=resolved_id,
        snapshot_committed_utc=committed,
        operation=operation,
        data_files=data_files,
        delete_files=delete_files,
        total_records=total_records,
        total_data_bytes=total_data_bytes,
        data_bytes_scanned=0,
        partitions=len(partitions),
        columns=columns,
        elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
    )


def audit_table_metadata(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rule: MetadataRule,
    *,
    snapshot_id: int | None = None,
    now: datetime | None = None,
) -> MetadataAuditResult:
    """Validate ``rule`` purely against Iceberg manifest metadata (zero Parquet data scans)."""
    started = time.perf_counter()
    summary = inspect_table_metadata(con, table, snapshot_id=snapshot_id)
    violations: list[MetadataCheckViolation] = []

    if summary.total_records < rule.min_rows:
        violations.append(
            MetadataCheckViolation(
                table=table,
                rule_name="metadata.min_rows",
                reason=f"record_count {summary.total_records} < min_rows {rule.min_rows}",
            )
        )

    if rule.max_delete_files is not None and summary.delete_files > rule.max_delete_files:
        violations.append(
            MetadataCheckViolation(
                table=table,
                rule_name="metadata.max_delete_files",
                reason=f"delete_files {summary.delete_files} > {rule.max_delete_files}",
            )
        )

    if rule.max_staleness_hours is not None:
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if summary.snapshot_committed_utc is None:
            violations.append(
                MetadataCheckViolation(
                    table=table,
                    rule_name="metadata.freshness",
                    reason="table has no snapshot",
                )
            )
        else:
            age_hours = (current - summary.snapshot_committed_utc).total_seconds() / 3600.0
            if age_hours > rule.max_staleness_hours:
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name="metadata.freshness",
                        reason=(
                            f"latest snapshot is {age_hours:.2f}h old "
                            f"(limit {rule.max_staleness_hours:.2f}h)"
                        ),
                    )
                )

    if summary.total_records > 0:
        for column in rule.non_null_columns:
            stat = summary.columns.get(column)
            if stat is None:
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name=f"metadata.non_null.{column}",
                        reason=f"column {column!r} missing from Iceberg manifest statistics",
                    )
                )
                continue
            if stat.null_value_count > 0:
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name=f"metadata.non_null.{column}",
                        reason=f"manifest reports {stat.null_value_count} null(s) in {column!r}",
                    )
                )
            if stat.nan_value_count > 0:
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name=f"metadata.non_nan.{column}",
                        reason=f"manifest reports {stat.nan_value_count} NaN(s) in {column!r}",
                    )
                )

        for column, (min_allowed, max_allowed) in rule.numeric_bounds.items():
            stat = summary.columns.get(column)
            if stat is None:
                continue
            if (
                min_allowed is not None
                and stat.min_numeric is not None
                and stat.min_numeric < min_allowed
            ):
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name=f"metadata.bounds.{column}",
                        reason=(
                            f"manifest lower_bound {stat.min_numeric} < min {min_allowed} "
                            f"on {column!r}"
                        ),
                    )
                )
            if (
                max_allowed is not None
                and stat.max_numeric is not None
                and stat.max_numeric > max_allowed
            ):
                violations.append(
                    MetadataCheckViolation(
                        table=table,
                        rule_name=f"metadata.bounds.{column}",
                        reason=(
                            f"manifest upper_bound {stat.max_numeric} > max {max_allowed} "
                            f"on {column!r}"
                        ),
                    )
                )

    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    return MetadataAuditResult(
        summary=summary,
        passed=not violations,
        violations=tuple(violations),
        elapsed_ms=elapsed_ms,
    )


def _resolve_snapshot(
    con: duckdb.DuckDBPyConnection,
    table: str,
    snapshot_id: int | None,
) -> tuple[int | None, str | None, datetime | None]:
    if snapshot_id is not None:
        rows = con.execute(
            "SELECT snapshot_id, operation, timestamp_ms FROM iceberg_snapshots($table) "
            "WHERE snapshot_id = $snapshot_id",
            {"table": table, "snapshot_id": snapshot_id},
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT s.snapshot_id, s.operation, s.timestamp_ms "
            "FROM iceberg_snapshots($table) AS s "
            "JOIN iceberg_refs($table) AS r ON s.snapshot_id = r.snapshot_id "
            "WHERE r.name = 'main'",
            {"table": table},
        ).fetchall()
    if not rows:
        return None, None, None
    snap_id, operation, committed = rows[0]
    committed_dt: datetime | None = None
    if isinstance(committed, datetime):
        committed_dt = committed if committed.tzinfo is not None else committed.replace(tzinfo=UTC)
    return int(snap_id), str(operation), committed_dt


def _aggregate_column(
    name: str,
    entries: list[tuple[str, int, int, int, str | None, str | None]],
) -> ColumnStat:
    col_type = entries[0][0]
    base_type = col_type.split("(")[0].strip().upper()
    value_count = sum(item[1] for item in entries)
    null_count = sum(item[2] for item in entries)
    nan_count = sum(item[3] for item in entries)
    lowers = [item[4] for item in entries if item[4] is not None]
    uppers = [item[5] for item in entries if item[5] is not None]

    min_numeric: float | None = None
    max_numeric: float | None = None
    lower_bound: str | None = None
    upper_bound: str | None = None

    if base_type in NUMERIC_COLUMN_TYPES:
        lower_nums = [v for v in (_parse_float(raw) for raw in lowers) if v is not None]
        upper_nums = [v for v in (_parse_float(raw) for raw in uppers) if v is not None]
        if lower_nums:
            min_numeric = min(lower_nums)
            lower_bound = str(min_numeric)
        if upper_nums:
            max_numeric = max(upper_nums)
            upper_bound = str(max_numeric)
    else:
        if lowers:
            lower_bound = min(lowers)
        if uppers:
            upper_bound = max(uppers)

    return ColumnStat(
        column_name=name,
        column_type=col_type,
        value_count=value_count,
        null_value_count=null_count,
        nan_value_count=nan_count,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        min_numeric=min_numeric,
        max_numeric=max_numeric,
    )


def _parse_float(raw: str) -> float | None:
    try:
        value = float(raw)
    except ValueError:
        return None
    return None if math.isnan(value) else value
