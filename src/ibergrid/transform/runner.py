"""Run dbt Core over Bronze Iceberg tables and publish tested Silver/Gold tables via WAP (ADR 0007).

DuckDB 2.0.1 cannot write Iceberg branches or guard ``main`` (ADR 0002), so ``run_medallion`` splits
transformation from publication:

1. ``dbt build`` executes seeds, staging views, Silver/Gold models and every schema and singular
   test inside an ephemeral staging DuckDB file, reading snapshot-pinned Bronze views configured by
   ``ibergrid.transform.plugin``.
2. If any model or test fails, ``TransformError`` is raised immediately and no Iceberg table on
   ``main`` is modified.
3. Only after every model and test passes, each selected Silver and Gold table is exported from the
   staging DuckDB to Arrow, aligned strictly to its ``TableSpec`` schema and published to Lakekeeper
   through ``ibergrid.lake.wap`` (``start`` -> ``write`` -> branch count audit -> ``publish``),
   fenced against each target table's ``main`` snapshot captured before ``dbt build``. On a
   ``PublishConflict`` the audit branch is discarded and the conflict propagates so callers
   (CLI or Dagster retries) rebuild from current Bronze snapshots instead of publishing stale rows.
"""

import json
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import structlog
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.table import Table
from pyiceberg.table.refs import MAIN_BRANCH

from ibergrid.lake import registry, wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.tables import TableSpec
from ibergrid.lineage import LineageAssertion, LineageEmitter, build_dataset
from ibergrid.settings import Settings

log = structlog.get_logger(__name__)

MEDALLION_BY_NAME: dict[str, TableSpec] = {spec.name: spec for spec in registry.MEDALLION_TABLES}
MEDALLION_BY_QUALIFIED: dict[str, TableSpec] = {
    f"{spec.namespace}.{spec.name}": spec for spec in registry.MEDALLION_TABLES
}
LAYERS: dict[str, tuple[TableSpec, ...]] = {
    "silver": registry.SILVER_TABLES,
    "gold": registry.GOLD_TABLES,
}


class TransformError(RuntimeError):
    """dbt build failed, a required table or project is missing, or WAP audit rejected a table."""


@dataclass(frozen=True)
class TableReport:
    """Measurements of one Silver or Gold table published through WAP."""

    namespace: str
    name: str
    rows: int
    snapshot_id: int | None
    run_id: str
    seconds: float

    @property
    def qualified(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True)
class TransformReport:
    """Summary of a full or subsetted medallion transform run."""

    tables: tuple[TableReport, ...]
    bronze_snapshots: dict[str, int | None]
    dbt_models_built: int
    dbt_tests_passed: int
    dbt_seconds: float
    total_seconds: float


def resolve_selection(select: Sequence[str] = ()) -> tuple[TableSpec, ...]:
    """Resolve CLI/Dagster selectors (layer names, table names or ``ns.table``) in publish order."""
    if not select:
        return registry.MEDALLION_TABLES
    chosen: set[str] = set()
    for raw in select:
        token = raw.strip()
        if token in LAYERS:
            chosen.update(spec.name for spec in LAYERS[token])
        elif token in MEDALLION_BY_NAME:
            chosen.add(token)
        elif token in MEDALLION_BY_QUALIFIED:
            chosen.add(MEDALLION_BY_QUALIFIED[token].name)
        else:
            known = sorted([*LAYERS, *MEDALLION_BY_NAME])
            raise ValueError(
                f"unknown transform target {raw!r}; expected one of: {', '.join(known)}"
            )
    return tuple(spec for spec in registry.MEDALLION_TABLES if spec.name in chosen)


def find_dbt_project(settings: Settings) -> Path:
    """Locate ``dbt/dbt_project.yml`` from ``IBERGRID_TRANSFORM_DBT_PROJECT_DIR`` or checkout."""
    configured = settings.transform.dbt_project_dir
    if configured is not None:
        candidate = Path(configured)
        if (candidate / "dbt_project.yml").is_file():
            return candidate
        raise TransformError(
            f"IBERGRID_TRANSFORM_DBT_PROJECT_DIR has no dbt_project.yml: {candidate}"
        )
    # Repository checkout: src/ibergrid/transform/runner.py -> parents[3] / "dbt".
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "dbt"
        if (candidate / "dbt_project.yml").is_file():
            return candidate
    raise TransformError(
        "could not locate dbt/dbt_project.yml; set IBERGRID_TRANSFORM_DBT_PROJECT_DIR"
    )


def run_medallion(
    settings: Settings,
    *,
    select: Sequence[str] = (),
    run_id_prefix: str | None = None,
    bronze_namespace: str | None = None,
    target_namespace: str | None = None,
    catalog: Catalog | None = None,
    on_table: Callable[[TableReport], None] | None = None,
) -> TransformReport:
    """Run ``dbt build`` in a staging DuckDB and publish the tested tables to Iceberg via WAP."""
    started = time.monotonic()
    targets = resolve_selection(select)
    dbt_runner_cls, close_dbt_connections, plugin_module = _load_dbt()
    project_dir = find_dbt_project(settings)

    prefix = run_id_prefix or f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:4]}"
    source_ns = bronze_namespace or "bronze"
    context = plugin_module.TransformContext(settings=settings, bronze_namespace=source_ns)

    # Capture each target table's main head before dbt build starts: if another transform process
    # publishes newer output during the build or write, we refuse to overwrite it with stale rows.
    lake_catalog = catalog if catalog is not None else rest_catalog(settings)
    expected_bases = _capture_target_heads(lake_catalog, targets, target_namespace=target_namespace)

    emitter = LineageEmitter.from_settings(settings)
    lineage_job = "medallion_transform"
    emitter.emit("START", job_name=lineage_job, run_id=prefix)

    try:
        with tempfile.TemporaryDirectory(prefix="ibergrid-dbt-") as tmp:
            work = Path(tmp)
            staging_db = work / "staging.duckdb"
            target_path = work / "target"
            log_path = work / "logs"

            dbt_started = time.monotonic()
            models_built, tests_passed = _run_dbt_build(
                dbt_runner_cls,
                close_dbt_connections,
                plugin_module,
                context,
                project_dir=project_dir,
                staging_db=staging_db,
                target_path=target_path,
                log_path=log_path,
                targets=targets,
                full_project=not select,
                threads=settings.transform.threads,
            )
            dbt_seconds = round(time.monotonic() - dbt_started, 3)

            reports = _publish_all(
                staging_db,
                lake_catalog,
                targets,
                expected_bases=expected_bases,
                target_namespace=target_namespace,
                run_id_prefix=prefix,
                bronze_snapshots=dict(context.bronze_snapshots),
                dbt_tests_passed=tests_passed,
                on_table=on_table,
            )
    except Exception as exc:
        emitter.emit("FAIL", job_name=lineage_job, run_id=prefix, error_message=str(exc))
        raise

    spec_by_name = {spec.name: spec for spec in targets}
    input_datasets = [
        build_dataset(
            spec,
            warehouse_uri=emitter.warehouse_uri,
            snapshot_id=context.bronze_snapshots.get(spec.name),
            namespace_override=source_ns,
        )
        for spec in registry.BRONZE_TABLES
    ]
    output_datasets = [
        build_dataset(
            spec_by_name[report.name],
            warehouse_uri=emitter.warehouse_uri,
            snapshot_id=report.snapshot_id,
            row_count=report.rows,
            assertions=(LineageAssertion(assertion="dbt_tests_passed", success=True),),
            namespace_override=report.namespace,
        )
        for report in reports
    ]
    emitter.emit(
        "COMPLETE",
        job_name=lineage_job,
        run_id=prefix,
        inputs=input_datasets,
        outputs=output_datasets,
    )

    total_seconds = round(time.monotonic() - started, 3)
    log.info(
        "transform.medallion.completed",
        tables=len(reports),
        dbt_models=models_built,
        dbt_tests=tests_passed,
        dbt_seconds=dbt_seconds,
        total_seconds=total_seconds,
        bronze_snapshots=context.bronze_snapshots,
    )
    return TransformReport(
        tables=tuple(reports),
        bronze_snapshots=dict(context.bronze_snapshots),
        dbt_models_built=models_built,
        dbt_tests_passed=tests_passed,
        dbt_seconds=dbt_seconds,
        total_seconds=total_seconds,
    )


def _load_dbt() -> tuple[Any, Callable[[], None], Any]:
    """Import dbt-core and dbt-duckdb lazily so the lean app image stays free of them."""
    try:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
        from dbt.cli.main import dbtRunner

        from ibergrid.transform import plugin as plugin_module
    except ImportError as exc:
        raise TransformError(
            "dbt-core and dbt-duckdb are required for transforms; install the 'transform' "
            "dependency group or run from the orchestration image"
        ) from exc
    return dbtRunner, DuckDBConnectionManager.close_all_connections, plugin_module


def _run_dbt_build(
    dbt_runner_cls: Any,
    close_dbt_connections: Callable[[], None],
    plugin_module: Any,
    context: Any,
    *,
    project_dir: Path,
    staging_db: Path,
    target_path: Path,
    log_path: Path,
    targets: tuple[TableSpec, ...],
    full_project: bool,
    threads: int,
) -> tuple[int, int]:
    """Execute ``dbt build`` inside ``use_context(context)`` and return ``(models, tests)``."""
    args = [
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target-path",
        str(target_path),
        "--log-path",
        str(log_path),
        "--no-send-anonymous-usage-stats",
        "--no-write-json",
        "--quiet",
    ]
    if not full_project:
        # Always include upstream seeds/staging/silver (`+model`) so a fresh staging DuckDB has
        # every prerequisite relation built and tested before the selected targets. Cautious
        # indirect selection ensures multi-parent tests only run when all their parents are built.
        args.extend(
            ["--indirect-selection=cautious", "--select", *[f"+{spec.name}" for spec in targets]]
        )

    env_overrides = {
        "IBERGRID_DBT_DUCKDB_PATH": str(staging_db),
        "IBERGRID_TRANSFORM_THREADS": str(threads),
    }
    previous = {key: os.environ.get(key) for key in env_overrides}
    with plugin_module.use_context(context):
        os.environ.update(env_overrides)
        try:
            result = dbt_runner_cls().invoke(args)
        finally:
            close_dbt_connections()
            for key, old in previous.items():
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old

    if not result.success:
        detail = _describe_dbt_failure(result)
        log.error("transform.dbt.failed", detail=detail)
        raise TransformError(f"dbt build failed: {detail}")

    nodes = list(getattr(result.result, "results", ()) or ())
    models_built = sum(
        1
        for node in nodes
        if str(getattr(node.node, "resource_type", "")) == "model"
        and str(getattr(node, "status", "")) == "success"
    )
    tests_passed = sum(
        1
        for node in nodes
        if str(getattr(node.node, "resource_type", "")) == "test"
        and str(getattr(node, "status", "")) == "pass"
    )
    return models_built, tests_passed


def _describe_dbt_failure(result: Any) -> str:
    """Concise, actionable summary of why ``dbt build`` failed (failing tests, models or exc)."""
    problems: list[str] = []
    for item in getattr(result.result, "results", ()) or ():
        status = str(getattr(item, "status", ""))
        if status in {"error", "fail"}:
            name = getattr(item.node, "name", "unknown")
            message = (getattr(item, "message", "") or "").strip()
            problems.append(f"{name} ({status}: {message})" if message else f"{name} ({status})")
    if problems:
        shown = "; ".join(problems[:5])
        more = len(problems) - 5
        return shown + (f" (+{more} more)" if more > 0 else "")
    exc = getattr(result, "exception", None)
    return str(exc) if exc is not None else "unknown dbt error"


def _capture_target_heads(
    catalog: Catalog,
    targets: tuple[TableSpec, ...],
    *,
    target_namespace: str | None,
) -> dict[tuple[str, str], int | None]:
    """Load each target table before ``dbt build`` and record its current ``main`` snapshot id."""
    heads: dict[tuple[str, str], int | None] = {}
    for spec in targets:
        identifier = (target_namespace or spec.namespace, spec.name)
        try:
            table: Table = catalog.load_table(identifier)
        except NoSuchTableError as exc:
            raise TransformError(
                f"{'.'.join(identifier)} does not exist in catalog; run `ibergrid lake init` first"
            ) from exc
        ref = table.metadata.refs.get(MAIN_BRANCH)
        heads[identifier] = None if ref is None else ref.snapshot_id
    return heads


def _publish_all(
    staging_db: Path,
    catalog: Catalog,
    targets: tuple[TableSpec, ...],
    *,
    expected_bases: dict[tuple[str, str], int | None],
    target_namespace: str | None,
    run_id_prefix: str,
    bronze_snapshots: dict[str, int | None],
    dbt_tests_passed: int,
    on_table: Callable[[TableReport], None] | None,
) -> list[TableReport]:
    """Read each built table from the staging DuckDB and publish it via WAP."""
    reports: list[TableReport] = []
    con = duckdb.connect(str(staging_db), read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        for spec in targets:
            table_started = time.monotonic()
            raw_arrow = con.sql(
                f"SELECT * FROM {spec.namespace}.{spec.name}"  # noqa: S608 (trusted TableSpec)
            ).to_arrow_table()
            aligned = _align_to_schema(raw_arrow, spec)
            identifier = (target_namespace or spec.namespace, spec.name)
            snapshot_id, run_id = _publish_table(
                catalog,
                identifier,
                aligned,
                expected_base=expected_bases[identifier],
                run_id_prefix=run_id_prefix,
                bronze_snapshots=bronze_snapshots,
                dbt_tests_passed=dbt_tests_passed,
            )
            report = TableReport(
                namespace=identifier[0],
                name=identifier[1],
                rows=aligned.num_rows,
                snapshot_id=snapshot_id,
                run_id=run_id,
                seconds=round(time.monotonic() - table_started, 3),
            )
            log.info(
                "transform.table.published",
                table=report.qualified,
                rows=report.rows,
                snapshot_id=report.snapshot_id,
                run_id=report.run_id,
                seconds=report.seconds,
            )
            reports.append(report)
            if on_table is not None:
                on_table(report)
    finally:
        con.close()
    return reports


def _align_to_schema(table: pa.Table, spec: TableSpec) -> pa.Table:
    """Align column order, Arrow types and non-null flags strictly to ``spec.schema.as_arrow()``."""
    target = spec.schema.as_arrow()
    missing = [name for name in target.names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in target.names]
    if missing or extra:
        raise TransformError(
            f"{spec.namespace}.{spec.name} columns do not match registry schema "
            f"(missing={missing}, extra={extra})"
        )
    arrays: list[pa.ChunkedArray] = []
    for field in target:
        column = table[field.name]
        if column.type != field.type:
            column = pc.cast(column, field.type)
        if not field.nullable and column.null_count:
            raise TransformError(
                f"{spec.namespace}.{spec.name}.{field.name} has {column.null_count} null(s) "
                "in a required column"
            )
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=target)


def _publish_table(
    catalog: Catalog,
    identifier: tuple[str, str],
    rows: pa.Table,
    *,
    expected_base: int | None,
    run_id_prefix: str,
    bronze_snapshots: dict[str, int | None],
    dbt_tests_passed: int,
) -> tuple[int | None, str]:
    """Full-refresh overwrite of one Silver/Gold table through WAP, fenced on ``expected_base``.

    Never retries with stale rows on ``PublishConflict``: if another writer moved ``main`` during
    the dbt build or WAP write, the audit branch is discarded and ``PublishConflict`` propagates
    so the caller rebuilds from current Bronze snapshots.
    """
    try:
        table: Table = catalog.load_table(identifier)
    except NoSuchTableError as exc:
        raise TransformError(
            f"{'.'.join(identifier)} does not exist in catalog; run `ibergrid lake init` first"
        ) from exc

    properties = {
        "ibergrid.transform.layer": identifier[0],
        "ibergrid.transform.table": identifier[1],
        "ibergrid.transform.rows": str(rows.num_rows),
        "ibergrid.transform.dbt-tests-passed": str(dbt_tests_passed),
        "ibergrid.transform.bronze-snapshots": json.dumps(bronze_snapshots, sort_keys=True),
    }
    slug = f"transform-{identifier[1].replace('_', '-')}"
    run_id = f"{run_id_prefix}-{slug}-{uuid.uuid4().hex[:4]}"
    audit = wap.start(table, run_id)
    try:
        if audit.base_snapshot_id != expected_base:
            raise wap.PublishConflict(
                f"{'.'.join(identifier)}: main moved from {expected_base} to "
                f"{audit.base_snapshot_id} during dbt build; rebuild from current Bronze snapshots"
            )
        # Skip overwrite on a brand-new empty table with 0 rows: an empty delete on an empty
        # branch leaves nothing to audit or publish, and DuckDB cannot read an empty root.
        if rows.num_rows == 0 and audit.base_snapshot_id is None:
            wap.discard(table, audit)
            return None, run_id
        wap.write(
            table,
            audit,
            rows,
            overwrite_filter=None if audit.base_snapshot_id is None else AlwaysTrue(),
            snapshot_properties=properties,
        )
        audited = wap.head(table, audit)
        actual_rows = (
            0
            if audited is None
            else table.scan(
                snapshot_id=audited,
                selected_fields=(table.schema().fields[0].name,),
            )
            .to_arrow()
            .num_rows
        )
        if actual_rows != rows.num_rows:
            raise TransformError(
                f"{'.'.join(identifier)} audit mismatch: branch has {actual_rows} "
                f"row(s), expected {rows.num_rows}"
            )
        published = wap.publish(table, audit, audited)
        return published, run_id
    except Exception:
        wap.discard(table, audit)
        raise
