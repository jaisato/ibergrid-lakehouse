"""Declarative Soda v4 quality gates over DuckDB + Iceberg WAP audit branches (ADR 0008).

Evaluates declarative YAML contracts in ``contracts/soda/*.yml`` against ephemeral DuckDB views
pinned to an Iceberg audit branch snapshot (or ``main``) BEFORE ``wap.publish``. When any gate
fails, violations and bounded sample payloads are written to ``quarantine.rejected_records`` via
WAP and the audit branch is discarded so ``main`` never sees corrupt data.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import structlog
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from ibergrid.lake import duck, wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.quality.metadata import (
    TABLE_METADATA_RULES,
    MetadataAuditResult,
    MetadataRule,
    audit_table_metadata,
)
from ibergrid.quality.quarantine import QuarantineRecord, write_quarantine
from ibergrid.settings import Settings

log = structlog.get_logger(__name__)

# Registered tables that carry a declarative Soda v4 YAML contract under contracts/soda/.
CONTRACT_MAP: dict[str, str] = {
    "bronze.omie_day_ahead_prices": "bronze_omie_day_ahead_prices.yml",
    "bronze.redata_observations": "bronze_redata_observations.yml",
    "silver.mibel_prices_hourly": "silver_mibel_prices_hourly.yml",
    "silver.demand_system_hourly": "silver_demand_system_hourly.yml",
    "gold.mibel_hourly_market": "gold_mibel_hourly_market.yml",
}

MAX_SAMPLES_PER_VIOLATION = 20


class QualityGateError(RuntimeError):
    """Raised when a WAP audit branch fails metadata or Soda v4 quality gates."""

    def __init__(self, report: GateReport) -> None:
        self.report = report
        summary = "; ".join(f"{v.rule_name}: {v.reason}" for v in report.violations[:5])
        more = len(report.violations) - 5
        suffix = f" (+{more} more)" if more > 0 else ""
        super().__init__(f"quality gate failed on {report.table}: {summary}{suffix}")


@dataclass(frozen=True)
class QualityViolation:
    """One failed check (zero-scan metadata or Soda v4 contract)."""

    table: str
    rule_name: str
    severity: str
    reason: str
    failed_count: int = 1
    samples: tuple[tuple[str | None, str], ...] = ()


@dataclass(frozen=True)
class GateReport:
    """Complete result of auditing one table or WAP audit branch."""

    table: str
    contract_path: str | None
    snapshot_id: int | None
    audit_branch: str | None
    passed: bool
    checks_passed: int
    checks_failed: int
    violations: tuple[QualityViolation, ...]
    metadata_result: MetadataAuditResult | None = None
    decoupled_periods_verified: int | None = None
    quarantine_snapshot_id: int | None = None
    elapsed_ms: float = 0.0


def find_contracts_dir(settings: Settings | None = None) -> Path:
    """Locate ``contracts/soda`` from settings, baked container path, or repo checkout."""
    configured = settings.quality.contracts_dir if settings is not None else None
    if configured:
        path = Path(configured)
        if path.is_dir():
            return path
        raise RuntimeError(f"configured contracts directory does not exist: {path}")

    baked = Path("/opt/ibergrid/contracts/soda")
    if baked.is_dir():
        return baked

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts" / "soda"
        if candidate.is_dir():
            return candidate
    raise RuntimeError(
        "could not locate contracts/soda; set IBERGRID_QUALITY_CONTRACTS_DIR or run from a checkout"
    )


def _load_soda() -> tuple[Any, Any]:
    """Import soda-core and soda-duckdb lazily so the lean app image stays free of them."""
    try:
        from soda_core.contracts.api.verify_api import verify_contract_locally
        from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl
    except ImportError as exc:
        raise RuntimeError(
            "soda-core and soda-duckdb are required for quality gates; install the 'quality' "
            "dependency group or run from the orchestration image"
        ) from exc
    return verify_contract_locally, DuckDBDataSourceImpl


def run_soda_contract(
    con: duckdb.DuckDBPyConnection,
    contract_path: Path,
    *,
    table_name: str,
    view_name: str,
    relation_sql: str,
    snapshot_id: int | None = None,
    audit_branch: str | None = None,
) -> GateReport:
    """Run one Soda v4 YAML contract against ``relation_sql`` exposed as ``audit.<view_name>``."""
    started = time.perf_counter()
    verify_contract_locally, duckdb_ds_cls = _load_soda()
    con.execute("SET GLOBAL TimeZone = 'UTC'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute(f"CREATE OR REPLACE VIEW audit.{view_name} AS {relation_sql}")

    ds = duckdb_ds_cls.from_existing_cursor(con, "ibergrid")
    session = verify_contract_locally(data_sources=ds, contract_file_path=str(contract_path))

    violations: list[QualityViolation] = []
    checks_passed = 0
    checks_failed = 0

    for result in session.results:
        for check_result in result.check_results:
            outcome = str(getattr(check_result.outcome, "value", check_result.outcome)).upper()
            if outcome == "PASSED":
                checks_passed += 1
                continue
            checks_failed += 1
            rule_name = _format_rule_name(check_result)
            diag = dict(getattr(check_result, "diagnostic_metric_values", {}) or {})
            failed_count = _extract_failed_count(check_result, diag)
            samples = _collect_samples(con, view_name, check_result)
            reason = _format_reason(check_result, diag, failed_count)
            violations.append(
                QualityViolation(
                    table=table_name,
                    rule_name=rule_name,
                    severity="error",
                    reason=reason,
                    failed_count=failed_count,
                    samples=samples,
                )
            )
        if result.has_errors and not violations:
            checks_failed += 1
            violations.append(
                QualityViolation(
                    table=table_name,
                    rule_name="soda.execution_error",
                    severity="error",
                    reason=result.get_errors_str() or "Soda contract execution error",
                )
            )

    decoupled_verified: int | None = None
    if table_name == "silver.mibel_prices_hourly":
        decoupled_verified = _count_verified_decoupled_hours(con, view_name)

    elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
    return GateReport(
        table=table_name,
        contract_path=str(contract_path),
        snapshot_id=snapshot_id,
        audit_branch=audit_branch,
        passed=checks_failed == 0 and not session.has_errors,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        violations=tuple(violations),
        decoupled_periods_verified=decoupled_verified,
        elapsed_ms=elapsed_ms,
    )


def audit_branch_and_quarantine(
    catalog: Catalog,
    table: Table,
    audit: wap.AuditBranch,
    *,
    table_name: str,
    con: duckdb.DuckDBPyConnection,
    table_ref: str,
    contracts_dir: Path,
    run_id: str,
    metadata_rule: MetadataRule | None = None,
    quarantine_namespace: str | None = None,
    discard_on_failure: bool = True,
    raise_on_failure: bool = True,
) -> GateReport:
    """Audit a WAP branch with zero-scan metadata checks + Soda v4 contract before ``wap.publish``.

    On failure:
    1. Writes every violation (plus bounded sample keys/payloads) to ``quarantine.rejected_records``
       via WAP.
    2. Discards the audit branch (when ``discard_on_failure=True``) so ``main`` is never touched.
    3. Raises ``QualityGateError`` (when ``raise_on_failure=True``) or returns the failed report.
    """
    started = time.perf_counter()
    snapshot_id = wap.head(table, audit)
    all_violations: list[QualityViolation] = []
    checks_passed = 0
    checks_failed = 0

    rule = metadata_rule if metadata_rule is not None else TABLE_METADATA_RULES.get(table_name)
    meta_result: MetadataAuditResult | None = None
    if rule is not None:
        meta_result = audit_table_metadata(con, table_ref, rule, snapshot_id=snapshot_id)
        if meta_result.passed:
            checks_passed += 1
        else:
            for mv in meta_result.violations:
                checks_failed += 1
                all_violations.append(
                    QualityViolation(
                        table=table_name,
                        rule_name=mv.rule_name,
                        severity="error",
                        reason=mv.reason,
                    )
                )

    contract_file = CONTRACT_MAP.get(table_name)
    contract_path = (contracts_dir / contract_file) if contract_file else None
    decoupled_verified: int | None = None

    if contract_path is not None and contract_path.is_file() and snapshot_id is not None:
        view_name = contract_path.stem
        quoted_ref = _quote(table_ref)
        relation_sql = (
            f"SELECT * FROM iceberg_scan({quoted_ref}, "  # noqa: S608 (quoted table ref + int id)
            f"snapshot_from_id => {int(snapshot_id)})"
        )
        soda_report = run_soda_contract(
            con,
            contract_path,
            table_name=table_name,
            view_name=view_name,
            relation_sql=relation_sql,
            snapshot_id=snapshot_id,
            audit_branch=audit.name,
        )
        checks_passed += soda_report.checks_passed
        checks_failed += soda_report.checks_failed
        all_violations.extend(soda_report.violations)
        decoupled_verified = soda_report.decoupled_periods_verified

    passed = checks_failed == 0
    quarantine_snap: int | None = None
    if not passed:
        q_records = _violations_to_quarantine(
            all_violations,
            run_id=run_id,
            source_table=table_name,
            audit_branch=audit.name,
            snapshot_id=snapshot_id,
        )
        quarantine_snap = write_quarantine(
            catalog,
            q_records,
            run_id_prefix=run_id,
            target_namespace=quarantine_namespace,
        )
        if discard_on_failure:
            wap.discard(table, audit)

    report = GateReport(
        table=table_name,
        contract_path=None if contract_path is None else str(contract_path),
        snapshot_id=snapshot_id,
        audit_branch=audit.name,
        passed=passed,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        violations=tuple(all_violations),
        metadata_result=meta_result,
        decoupled_periods_verified=decoupled_verified,
        quarantine_snapshot_id=quarantine_snap,
        elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
    )
    log.info(
        "quality.gate.completed",
        table=table_name,
        audit_branch=audit.name,
        snapshot_id=snapshot_id,
        passed=passed,
        checks_passed=checks_passed,
        checks_failed=checks_failed,
        decoupled_periods_verified=decoupled_verified,
        quarantine_snapshot_id=quarantine_snap,
        elapsed_ms=report.elapsed_ms,
    )
    if not passed and raise_on_failure:
        raise QualityGateError(report)
    return report


def check_tables(
    settings: Settings,
    tables: Sequence[str] = (),
    *,
    metadata_only: bool = False,
    quarantine: bool = False,
    run_id: str = "quality-check",
    con: duckdb.DuckDBPyConnection | None = None,
    catalog: Catalog | None = None,
) -> list[GateReport]:
    """Run zero-scan metadata checks and (unless ``metadata_only``) Soda contracts on ``main``."""
    target_names = list(tables) if tables else sorted(CONTRACT_MAP)
    owns_con = con is None
    conn = con if con is not None else duck.connect(settings)
    lake_catalog = (
        catalog if catalog is not None else (rest_catalog(settings) if quarantine else None)
    )
    contracts_dir = None if metadata_only else find_contracts_dir(settings)
    reports: list[GateReport] = []

    try:
        conn.execute("SET GLOBAL TimeZone = 'UTC'")
        conn.execute("SET TimeZone = 'UTC'")
        for name in target_names:
            started = time.perf_counter()
            table_ref = duck.table_name(name)
            rule = TABLE_METADATA_RULES.get(name, MetadataRule())
            meta_result = audit_table_metadata(conn, table_ref, rule)
            violations: list[QualityViolation] = [
                QualityViolation(
                    table=name,
                    rule_name=mv.rule_name,
                    severity="error",
                    reason=mv.reason,
                )
                for mv in meta_result.violations
            ]
            checks_passed = 1 if meta_result.passed else 0
            checks_failed = len(violations)
            contract_path: Path | None = None
            decoupled_verified: int | None = None

            if (
                not metadata_only
                and contracts_dir is not None
                and name in CONTRACT_MAP
                and meta_result.summary.snapshot_id is not None
                and meta_result.summary.total_records > 0
            ):
                contract_path = contracts_dir / CONTRACT_MAP[name]
                quoted_ref = _quote(table_ref)
                snap_id = meta_result.summary.snapshot_id
                soda_report = run_soda_contract(
                    conn,
                    contract_path,
                    table_name=name,
                    view_name=contract_path.stem,
                    relation_sql=(
                        f"SELECT * FROM iceberg_scan({quoted_ref}, "  # noqa: S608 (quoted + int)
                        f"snapshot_from_id => {int(snap_id)})"
                    ),
                    snapshot_id=snap_id,
                    audit_branch="main",
                )
                checks_passed += soda_report.checks_passed
                checks_failed += soda_report.checks_failed
                violations.extend(soda_report.violations)
                decoupled_verified = soda_report.decoupled_periods_verified

            quarantine_snap: int | None = None
            if violations and quarantine and lake_catalog is not None:
                try:
                    q_records = _violations_to_quarantine(
                        violations,
                        run_id=run_id,
                        source_table=name,
                        audit_branch="main",
                        snapshot_id=meta_result.summary.snapshot_id,
                    )
                    quarantine_snap = write_quarantine(
                        lake_catalog, q_records, run_id_prefix=run_id
                    )
                except (NoSuchTableError, RuntimeError) as exc:
                    log.warning("quality.quarantine.skipped", table=name, error=str(exc))

            reports.append(
                GateReport(
                    table=name,
                    contract_path=None if contract_path is None else str(contract_path),
                    snapshot_id=meta_result.summary.snapshot_id,
                    audit_branch="main",
                    passed=checks_failed == 0,
                    checks_passed=checks_passed,
                    checks_failed=checks_failed,
                    violations=tuple(violations),
                    metadata_result=meta_result,
                    decoupled_periods_verified=decoupled_verified,
                    quarantine_snapshot_id=quarantine_snap,
                    elapsed_ms=round((time.perf_counter() - started) * 1000, 3),
                )
            )
    finally:
        if owns_con:
            conn.close()
    return reports


def _format_rule_name(check_result: Any) -> str:
    check = check_result.check
    name = str(getattr(check, "name", "") or "check")
    check_type = str(getattr(check, "type", "") or "")
    column = getattr(check, "column_name", None) or getattr(check, "column", None)
    if column:
        return f"soda.{check_type}.{column}"
    slug = name.lower().replace(" ", "_")
    return f"soda.{check_type}.{slug}" if check_type and check_type not in slug else f"soda.{slug}"


def _extract_failed_count(check_result: Any, diag: Mapping[str, Any]) -> int:
    for key in (
        "failed_rows_count",
        "duplicate_count",
        "invalid_count",
        "missing_count",
        "schema_events_count",
    ):
        value = diag.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return int(value)
    threshold = getattr(check_result, "threshold_value", None)
    if isinstance(threshold, (int, float)) and threshold > 0:
        return int(threshold)
    return 1


def _format_reason(check_result: Any, diag: Mapping[str, Any], failed_count: int) -> str:
    check = check_result.check
    name = str(getattr(check, "name", "check"))
    column = getattr(check, "column_name", None) or getattr(check, "column", None)
    col_part = f" on column {column!r}" if column else ""
    metrics = json.dumps(
        {k: v for k, v in diag.items() if v is not None}, sort_keys=True, default=str
    )
    return f"{name}{col_part} failed ({failed_count} failing; metrics={metrics})"


def _parse_check_definition(check: Any) -> dict[str, Any]:
    raw_def = getattr(check, "definition", None)
    if not isinstance(raw_def, str) or not raw_def.strip():
        return {}
    import yaml  # transitive dep of soda-core; imported lazily inside Soda-only path

    parsed = yaml.safe_load(raw_def)
    if isinstance(parsed, dict):
        checks = parsed.get("checks")
        if isinstance(checks, list) and checks and isinstance(checks[0], dict):
            return dict(checks[0])
        return dict(parsed)
    return {}


def _collect_samples(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    check_result: Any,
) -> tuple[tuple[str | None, str], ...]:
    """Capture bounded sample payloads for failing SQL/duplicate checks for quarantine forensics."""
    check = check_result.check
    check_type = str(getattr(check, "type", "") or "")
    definition = _parse_check_definition(check)
    query: str | None = None
    if check_type == "failed_rows":
        raw_query = definition.get("query") or getattr(check, "query", None)
        if isinstance(raw_query, str) and raw_query.strip():
            inner = raw_query.strip().rstrip(";")
            query = (
                f"SELECT * FROM ({inner}) AS _failed "  # noqa: S608 (trusted repo Soda contract)
                f"LIMIT {MAX_SAMPLES_PER_VIOLATION}"
            )
    elif check_type == "duplicate":
        cols = definition.get("columns") or getattr(check, "columns", None)
        if cols:
            col_list = ", ".join(str(c) for c in cols)
            query = (
                f"SELECT {col_list}, count(*) AS duplicate_occurrences "  # noqa: S608 (contract cols)
                f"FROM audit.{view_name} GROUP BY {col_list} HAVING count(*) > 1 "
                f"LIMIT {MAX_SAMPLES_PER_VIOLATION}"
            )
    if query is None:
        return ()
    try:
        rel = con.sql(query)
        col_names = list(rel.columns)
        rows = rel.fetchall()
    except duckdb.Error:
        return ()

    samples: list[tuple[str | None, str]] = []
    for row in rows:
        payload = {col: _json_value(val) for col, val in zip(col_names, row, strict=True)}
        key = _extract_record_key(payload)
        samples.append((key, json.dumps(payload, sort_keys=True, ensure_ascii=False)))
    return tuple(samples)


def _count_verified_decoupled_hours(con: duckdb.DuckDBPyConnection, view_name: str) -> int:
    try:
        [(count,)] = con.execute(
            f"SELECT count(*) FROM audit.{view_name} "  # noqa: S608 (trusted contract stem)
            "WHERE is_decoupled IS TRUE "
            "AND redata_spot_eur_mwh IS NOT NULL "
            "AND abs(price_es_eur_mwh - redata_spot_eur_mwh) <= 0.02"
        ).fetchall()
    except duckdb.Error:
        return 0
    return int(count)


def _violations_to_quarantine(
    violations: Sequence[QualityViolation],
    *,
    run_id: str,
    source_table: str,
    audit_branch: str | None,
    snapshot_id: int | None,
) -> list[QuarantineRecord]:
    records: list[QuarantineRecord] = []
    for violation in violations:
        if violation.samples:
            for key, payload in violation.samples:
                records.append(
                    QuarantineRecord(
                        run_id=run_id,
                        source_table=source_table,
                        rule_name=violation.rule_name,
                        severity=violation.severity,
                        reason=violation.reason,
                        record_key=key,
                        payload_json=payload,
                        audit_branch=audit_branch,
                        snapshot_id=snapshot_id,
                    )
                )
        else:
            records.append(
                QuarantineRecord(
                    run_id=run_id,
                    source_table=source_table,
                    rule_name=violation.rule_name,
                    severity=violation.severity,
                    reason=violation.reason,
                    audit_branch=audit_branch,
                    snapshot_id=snapshot_id,
                )
            )
    return records


def _extract_record_key(payload: Mapping[str, Any]) -> str | None:
    for candidates in (
        ("delivery_date", "period"),
        ("hour_utc",),
        ("electric_system", "ts_utc"),
        ("dataset", "electric_system", "series_id", "ts_utc"),
        ("delivery_date",),
    ):
        if all(key in payload and payload[key] is not None for key in candidates):
            return "|".join(str(payload[key]) for key in candidates)
    return None


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
