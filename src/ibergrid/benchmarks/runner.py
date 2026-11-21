"""End-to-end benchmark orchestrator (ADR 0009).

Non-destructive by construction: live ``bronze.*``, ``silver.*``, ``gold.*`` and
``quarantine.*`` tables are read-only. Temporary unpartitioned Parquet dumps and compacted
Iceberg clones live under an isolated S3 prefix and Lakekeeper namespace that are always deleted
in ``finally:``.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.fs as pafs
import structlog
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table

from ibergrid.benchmarks.cost import build_cost_comparison
from ibergrid.benchmarks.metadata_bench import (
    METADATA_BENCHMARK_TARGETS,
    MetadataVsScanRow,
    benchmark_metadata_vs_full_scan,
)
from ibergrid.benchmarks.queries import (
    BENCHMARK_QUERIES,
    BenchmarkQuerySpec,
    plan_iceberg_scan,
    polars_s3_storage_options,
    results_match,
    run_duckdb_query,
    run_polars_iceberg_query,
    run_polars_parquet_query,
)
from ibergrid.benchmarks.report import (
    DEFAULT_CHAOS_PROOFS,
    BenchmarkSuiteResult,
    QueryBenchmarkRow,
    TableInventoryRow,
    detect_environment,
)
from ibergrid.benchmarks.stats import time_callable
from ibergrid.lake import duck, registry
from ibergrid.lake.catalog import rest_catalog, s3_properties
from ibergrid.lake.maintenance import MaintenanceConflict, rewrite_data_files
from ibergrid.lake.tables import TableSpec, ensure_table
from ibergrid.quality.metadata import inspect_table_metadata
from ibergrid.settings import Settings, StorageSettings

os.environ.setdefault("POLARS_ICEBERG_PLANNER", "pyiceberg")
log = structlog.get_logger(__name__)

SPEC_BY_NAME: dict[str, TableSpec] = {
    f"{spec.namespace}.{spec.name}": spec for spec in registry.TABLES
}


def measure_s3_bucket_footprint(storage: StorageSettings) -> tuple[int, int]:
    """Return ``(object_count, total_bytes)`` across the configured object-storage bucket."""
    io = PyArrowFileIO(s3_properties(storage))
    fs = io.fs_by_scheme("s3", storage.bucket)
    try:
        infos = fs.get_file_info(pafs.FileSelector(storage.bucket, recursive=True))
    except FileNotFoundError:
        return 0, 0
    files = [info for info in infos if info.type == pafs.FileType.File]
    return len(files), sum(int(info.size or 0) for info in files)


def collect_table_inventory(
    con: duckdb.DuckDBPyConnection,
    specs: Sequence[TableSpec] = registry.TABLES,
    *,
    duck_name_fn: Any = None,
) -> tuple[TableInventoryRow, ...]:
    """Collect zero-scan Iceberg inventory rows across all registered tables."""
    resolve = duck_name_fn or duck.table_name
    rows: list[TableInventoryRow] = []
    for spec in specs:
        qualified = f"{spec.namespace}.{spec.name}"
        try:
            summary = inspect_table_metadata(con, resolve(qualified))
        except duckdb.Error:
            continue
        rows.append(
            TableInventoryRow(
                table=qualified,
                total_records=summary.total_records,
                data_files=summary.data_files,
                partitions=summary.partitions,
                total_data_bytes=summary.total_data_bytes,
            )
        )
    return tuple(rows)


def measure_cold_starts() -> dict[str, float]:
    """Measure fresh-process import + trivial query latency (ms) for DuckDB and Polars."""
    probes = {
        "duckdb_import_and_query_ms": (
            "import duckdb; assert duckdb.connect().execute('SELECT 1 + 1').fetchone() == (2,)"
        ),
        "polars_import_and_query_ms": (
            "import polars as pl; "
            "assert pl.DataFrame({'a': [1, 2]}).select(pl.col('a').sum()).item() == 3"
        ),
    }
    measured: dict[str, float] = {}
    for key, code in probes.items():
        # Run 3 cold subprocesses and take the median so OS page cache is warm but Python/engine
        # startup is genuinely cold every time.
        samples: list[float] = []
        for _ in range(3):
            started = time.perf_counter()
            subprocess.run(  # noqa: S603 - fixed interpreter + static probe snippet
                [sys.executable, "-c", code],
                check=True,
                capture_output=True,
            )
            samples.append((time.perf_counter() - started) * 1000.0)
        samples.sort()
        measured[key] = round(samples[1], 2)
    return measured


def measure_isolated_query_rss_mib(
    settings: Settings,
    *,
    query_id: str,
    engine: str,
    is_iceberg: bool,
    source: str,
) -> float:
    """Measure uncontaminated peak RSS (MiB) of one query in a clean child Python process.

    Running in a fresh subprocess avoids contamination from parent-process setup steps (such as
    building compacted clones) because OS ``ru_maxrss`` is a monotonic process high-water mark.
    Credentials are passed via environment variables, never on the command line.
    """
    probe = "\n".join(
        [
            "import os",
            "from ibergrid.settings import Settings",
            "from ibergrid.lake import duck",
            "from ibergrid.lake.catalog import rest_catalog",
            "from ibergrid.benchmarks.queries import (",
            "    BENCHMARK_QUERIES, polars_s3_storage_options, run_duckdb_query,",
            "    run_polars_iceberg_query, run_polars_parquet_query,",
            ")",
            "from ibergrid.benchmarks.stats import current_peak_rss_mib",
            "q = next(x for x in BENCHMARK_QUERIES if x.query_id == os.environ['BENCH_QID'])",
            "s = Settings()",
            "eng = os.environ['BENCH_ENGINE']",
            "ice = os.environ['BENCH_IS_ICEBERG'] == '1'",
            "src = os.environ['BENCH_SOURCE']",
            "if eng == 'duckdb':",
            "    con = duck.connect(s)",
            "    try:",
            "        run_duckdb_query(con, q, source=src, is_iceberg=ice)",
            "    finally:",
            "        con.close()",
            "elif ice:",
            "    ns, _, tbl = src.partition('.')",
            "    t = rest_catalog(s).load_table((ns, tbl))",
            "    run_polars_iceberg_query(",
            "        q, t, storage_options=polars_s3_storage_options(s.storage)",
            "    )",
            "else:",
            "    run_polars_parquet_query(",
            "        q, src, storage_options=polars_s3_storage_options(s.storage)",
            "    )",
            "print(current_peak_rss_mib())",
        ]
    )
    env = {
        **os.environ,
        "POLARS_ICEBERG_PLANNER": "pyiceberg",
        "IBERGRID_S3_ENDPOINT_URL": str(settings.storage.endpoint_url),
        "IBERGRID_S3_REGION": settings.storage.region,
        "IBERGRID_S3_BUCKET": settings.storage.bucket,
        "IBERGRID_S3_ACCESS_KEY": settings.storage.access_key.get_secret_value(),
        "IBERGRID_S3_SECRET_KEY": settings.storage.secret_key.get_secret_value(),
        "IBERGRID_CATALOG_URL": str(settings.catalog.url),
        "IBERGRID_CATALOG_WAREHOUSE": settings.catalog.warehouse,
        "BENCH_QID": query_id,
        "BENCH_ENGINE": engine,
        "BENCH_IS_ICEBERG": "1" if is_iceberg else "0",
        "BENCH_SOURCE": source,
    }
    completed = subprocess.run(  # noqa: S603 - fixed interpreter + static probe snippet
        [sys.executable, "-c", probe],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"isolated RSS probe failed for {query_id}/{engine}: {completed.stderr.strip()}"
        )
    return round(float(completed.stdout.strip().splitlines()[-1]), 2)


def export_unpartitioned_parquet(
    con: duckdb.DuckDBPyConnection,
    source_duck_table: str,
    target_uri: str,
) -> None:
    """Export an Iceberg table as a single ZSTD-compressed unpartitioned Parquet file."""
    quoted = "'" + target_uri.replace("'", "''") + "'"
    con.execute(
        f"COPY (SELECT * FROM iceberg_scan($source)) TO {quoted} "  # noqa: S608 - escaped URI
        "(FORMAT PARQUET, COMPRESSION ZSTD)",
        {"source": source_duck_table},
    )


def build_compacted_clone(
    catalog: Catalog,
    con: duckdb.DuckDBPyConnection,
    *,
    source_table: Table,
    source_spec: TableSpec,
    bench_namespace: str,
    clone_duck_name: str | None = None,
) -> Table:
    """Create a throwaway Iceberg table with identical spec and single-file-per-partition layout."""
    clone_spec = TableSpec(
        namespace=bench_namespace,
        name=f"{source_spec.namespace}_{source_spec.name}",
        schema=source_spec.schema,
        partition_spec=source_spec.partition_spec,
        properties=source_spec.properties,
    )
    ensure_table(catalog, clone_spec)
    clone_table = catalog.load_table(clone_spec.identifier)
    arrow_data = source_table.scan().to_arrow()
    if arrow_data.num_rows > 0:
        clone_table.append(arrow_data)
        if clone_duck_name is not None:
            # Single-append write already produces 1 data file per partition; compaction is
            # best-effort if DuckDB reports nothing to rewrite.
            with contextlib.suppress(MaintenanceConflict, duckdb.Error):
                rewrite_data_files(con, clone_duck_name)
            clone_table.refresh()
    return clone_table


def benchmark_query_across_layouts(
    con: duckdb.DuckDBPyConnection,
    query: BenchmarkQuerySpec,
    *,
    parquet_uri: str,
    parquet_bytes: int,
    live_table: Table,
    live_duck_source: str,
    compacted_table: Table,
    compacted_duck_source: str,
    storage_options: dict[str, str] | None = None,
    warmup: int = 2,
    iterations: int = 10,
    rss_resolver: Callable[[str, str, bool, str], float] | None = None,
    live_logical_name: str | None = None,
    compacted_logical_name: str | None = None,
) -> list[QueryBenchmarkRow]:
    """Run ``query`` on DuckDB and Polars across all three storage layouts and verify answers."""
    rows: list[QueryBenchmarkRow] = []

    def _with_rss(timing: Any, *, engine: str, is_iceberg: bool, source: str) -> Any:
        if rss_resolver is None:
            return timing
        return replace(
            timing, peak_rss_mib=rss_resolver(query.query_id, engine, is_iceberg, source)
        )

    live_meta = inspect_table_metadata(con, live_duck_source)
    live_planned_files, live_planned_bytes = plan_iceberg_scan(live_table, query)

    comp_meta = inspect_table_metadata(con, compacted_duck_source)
    comp_planned_files, comp_planned_bytes = plan_iceberg_scan(compacted_table, query)

    # 1. Unpartitioned Parquet
    d_parq_res, d_parq_time = time_callable(
        lambda: run_duckdb_query(con, query, source=parquet_uri, is_iceberg=False),
        warmup=warmup,
        iterations=iterations,
    )
    p_parq_res, p_parq_time = time_callable(
        lambda: run_polars_parquet_query(query, parquet_uri, storage_options=storage_options),
        warmup=warmup,
        iterations=iterations,
    )
    parq_match = results_match(d_parq_res, p_parq_res)

    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="unpartitioned_parquet",
            engine="duckdb",
            files_scanned=1,
            total_table_files=1,
            bytes_scanned=parquet_bytes,
            total_table_bytes=parquet_bytes,
            row_count=len(d_parq_res),
            answer_verified=parq_match,
            timing=_with_rss(d_parq_time, engine="duckdb", is_iceberg=False, source=parquet_uri),
        )
    )
    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="unpartitioned_parquet",
            engine="polars",
            files_scanned=1,
            total_table_files=1,
            bytes_scanned=parquet_bytes,
            total_table_bytes=parquet_bytes,
            row_count=len(p_parq_res),
            answer_verified=parq_match,
            timing=_with_rss(p_parq_time, engine="polars", is_iceberg=False, source=parquet_uri),
        )
    )

    # 2. Live Partitioned Iceberg
    d_live_res, d_live_time = time_callable(
        lambda: run_duckdb_query(con, query, source=live_duck_source, is_iceberg=True),
        warmup=warmup,
        iterations=iterations,
    )
    p_live_res, p_live_time = time_callable(
        lambda: run_polars_iceberg_query(query, live_table, storage_options=storage_options),
        warmup=warmup,
        iterations=iterations,
    )
    live_match = results_match(d_live_res, p_live_res) and results_match(d_live_res, d_parq_res)
    live_polars_src = live_logical_name or query.source_table

    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="partitioned_iceberg",
            engine="duckdb",
            files_scanned=live_planned_files,
            total_table_files=live_meta.data_files,
            bytes_scanned=live_planned_bytes,
            total_table_bytes=live_meta.total_data_bytes,
            row_count=len(d_live_res),
            answer_verified=live_match,
            timing=_with_rss(
                d_live_time, engine="duckdb", is_iceberg=True, source=live_duck_source
            ),
        )
    )
    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="partitioned_iceberg",
            engine="polars",
            files_scanned=live_planned_files,
            total_table_files=live_meta.data_files,
            bytes_scanned=live_planned_bytes,
            total_table_bytes=live_meta.total_data_bytes,
            row_count=len(p_live_res),
            answer_verified=live_match,
            timing=_with_rss(p_live_time, engine="polars", is_iceberg=True, source=live_polars_src),
        )
    )

    # 3. Compacted Iceberg
    d_comp_res, d_comp_time = time_callable(
        lambda: run_duckdb_query(con, query, source=compacted_duck_source, is_iceberg=True),
        warmup=warmup,
        iterations=iterations,
    )
    p_comp_res, p_comp_time = time_callable(
        lambda: run_polars_iceberg_query(query, compacted_table, storage_options=storage_options),
        warmup=warmup,
        iterations=iterations,
    )
    comp_match = results_match(d_comp_res, p_comp_res) and results_match(d_comp_res, d_parq_res)
    comp_polars_src = compacted_logical_name or compacted_duck_source

    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="compacted_iceberg",
            engine="duckdb",
            files_scanned=comp_planned_files,
            total_table_files=comp_meta.data_files,
            bytes_scanned=comp_planned_bytes,
            total_table_bytes=comp_meta.total_data_bytes,
            row_count=len(d_comp_res),
            answer_verified=comp_match,
            timing=_with_rss(
                d_comp_time, engine="duckdb", is_iceberg=True, source=compacted_duck_source
            ),
        )
    )
    rows.append(
        QueryBenchmarkRow(
            query_id=query.query_id,
            title=query.title,
            source_table=query.source_table,
            layout="compacted_iceberg",
            engine="polars",
            files_scanned=comp_planned_files,
            total_table_files=comp_meta.data_files,
            bytes_scanned=comp_planned_bytes,
            total_table_bytes=comp_meta.total_data_bytes,
            row_count=len(p_comp_res),
            answer_verified=comp_match,
            timing=_with_rss(p_comp_time, engine="polars", is_iceberg=True, source=comp_polars_src),
        )
    )

    return rows


def run_live_benchmark_suite(
    settings: Settings,
    *,
    warmup: int = 2,
    iterations: int = 10,
    on_progress: Any = None,
) -> BenchmarkSuiteResult:
    """Run the complete benchmark suite against the live Lakekeeper + RustFS/R2 stack.

    Strictly non-destructive: all live tables are read-only; throwaway S3 Parquet files and the
    compacted Iceberg namespace are always cleaned up in ``finally:``.
    """
    progress = on_progress or (lambda _msg: None)
    env = detect_environment()
    progress(
        f"Environment: Python {env.python_version}, DuckDB {env.duckdb_version}, "
        f"Polars {env.polars_version}"
    )

    total_s3_objects, total_s3_bytes = measure_s3_bucket_footprint(settings.storage)
    progress(f"Bucket footprint: {total_s3_objects} objects, {total_s3_bytes:,} bytes")

    cold_starts = measure_cold_starts()
    progress(f"Cold starts: {cold_starts}")

    con = duck.connect(settings)
    catalog = rest_catalog(settings)
    storage_options = polars_s3_storage_options(settings.storage)
    io = PyArrowFileIO(s3_properties(settings.storage))
    s3_fs = io.fs_by_scheme("s3", settings.storage.bucket)

    run_tag = uuid.uuid4().hex[:10]
    bench_namespace = f"bench_{run_tag}"
    parquet_prefix = f"{settings.storage.bucket}/bench-tmp-{run_tag}"
    parquet_uri_prefix = f"s3://{parquet_prefix}"

    try:
        inventory = collect_table_inventory(con)
        progress(f"Inventory: {len(inventory)} tables")

        catalog.create_namespace(bench_namespace)

        # Prepare unpartitioned Parquet and compacted Iceberg copies once per unique source table.
        unique_tables = list(dict.fromkeys(q.source_table for q in BENCHMARK_QUERIES))
        parquet_info: dict[str, tuple[str, int]] = {}
        live_tables: dict[str, Table] = {}
        compacted_tables: dict[str, tuple[Table, str, str]] = {}

        for qualified in unique_tables:
            ns, _, tbl_name = qualified.partition(".")
            source_duck = duck.table_name(qualified)
            source_table = catalog.load_table((ns, tbl_name))
            live_tables[qualified] = source_table

            file_name = f"{ns}_{tbl_name}.parquet"
            target_uri = f"{parquet_uri_prefix}/{file_name}"
            progress(f"Exporting unpartitioned Parquet for {qualified} -> {target_uri}")
            export_unpartitioned_parquet(con, source_duck, target_uri)
            info = s3_fs.get_file_info(f"{parquet_prefix}/{file_name}")
            parquet_info[qualified] = (target_uri, int(info.size or 0))

            progress(f"Building compacted Iceberg clone for {qualified} in {bench_namespace}")
            clone_logical = f"{bench_namespace}.{ns}_{tbl_name}"
            comp_table = build_compacted_clone(
                catalog,
                con,
                source_table=source_table,
                source_spec=SPEC_BY_NAME[qualified],
                bench_namespace=bench_namespace,
                clone_duck_name=None,
            )
            clone_duck = duck.table_name(clone_logical)
            try:
                rewrite_data_files(con, clone_duck)
                comp_table.refresh()
            except (MaintenanceConflict, duckdb.Error):
                pass
            compacted_tables[qualified] = (comp_table, clone_duck, clone_logical)

        def _rss_probe(qid: str, engine: str, is_iceberg: bool, source: str) -> float:
            return measure_isolated_query_rss_mib(
                settings,
                query_id=qid,
                engine=engine,
                is_iceberg=is_iceberg,
                source=source,
            )

        # Benchmark 1: Storage layout & multi-engine latency
        query_rows: list[QueryBenchmarkRow] = []
        for query in BENCHMARK_QUERIES:
            progress(f"Running Benchmark 1 {query.query_id}: {query.title}")
            parq_uri, parq_bytes = parquet_info[query.source_table]
            comp_table, comp_duck, comp_logical = compacted_tables[query.source_table]
            rows = benchmark_query_across_layouts(
                con,
                query,
                parquet_uri=parq_uri,
                parquet_bytes=parq_bytes,
                live_table=live_tables[query.source_table],
                live_duck_source=duck.table_name(query.source_table),
                compacted_table=comp_table,
                compacted_duck_source=comp_duck,
                storage_options=storage_options,
                warmup=warmup,
                iterations=iterations,
                rss_resolver=_rss_probe,
                live_logical_name=query.source_table,
                compacted_logical_name=comp_logical,
            )
            query_rows.extend(rows)

        # Benchmark 2: Zero-scan Iceberg metadata checks vs Full SQL scan
        meta_rows: list[MetadataVsScanRow] = []
        for logical_table, time_col, metric_col in METADATA_BENCHMARK_TARGETS:
            progress(f"Running Benchmark 2 metadata vs full scan: {logical_table}")
            try:
                row = benchmark_metadata_vs_full_scan(
                    con,
                    logical_table=logical_table,
                    duck_table=duck.table_name(logical_table),
                    time_col=time_col,
                    metric_col=metric_col,
                    warmup=warmup,
                    iterations=iterations,
                )
            except duckdb.Error:
                continue
            if row is not None:
                meta_rows.append(row)

        r2_tiers, infra_scenarios = build_cost_comparison(total_s3_bytes)

        return BenchmarkSuiteResult(
            environment=env,
            cold_starts_ms=cold_starts,
            total_s3_objects=total_s3_objects,
            total_s3_bytes=total_s3_bytes,
            table_inventory=inventory,
            query_benchmarks=tuple(query_rows),
            metadata_benchmarks=tuple(meta_rows),
            chaos_proofs=DEFAULT_CHAOS_PROOFS,
            r2_tiers=r2_tiers,
            infra_scenarios=infra_scenarios,
        )
    finally:
        # Always purge throwaway namespace and S3 Parquet dumps; never touch live namespaces.
        try:
            for identifier in catalog.list_tables(bench_namespace):
                catalog.purge_table(identifier)
            catalog.drop_namespace(bench_namespace)
        except Exception as exc:
            log.warning(
                "benchmark.cleanup.namespace_failed", namespace=bench_namespace, error=str(exc)
            )
        try:
            s3_fs.delete_dir(parquet_prefix)
        except Exception as exc:
            log.warning("benchmark.cleanup.parquet_failed", prefix=parquet_prefix, error=str(exc))
        con.close()


def load_host_settings(
    repo_root: Path,
    *,
    catalog_url: str = "http://127.0.0.1:28181",
    s3_url: str = "http://127.0.0.1:29000",
) -> Settings:
    """Load credentials from ``<repo_root>/.env`` with host-published compose endpoints."""
    from pydantic import HttpUrl, SecretStr

    from ibergrid.settings import CatalogSettings, StorageSettings

    env_path = repo_root / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f"missing {env_path}; run `make env` or restore .env first")
    env = dict(
        line.split("=", 1)
        for line in env_path.read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.startswith("#")
    )
    return Settings(
        storage=StorageSettings(
            endpoint_url=HttpUrl(s3_url),
            access_key=SecretStr(env["IBERGRID_S3_ACCESS_KEY"]),
            secret_key=SecretStr(env["IBERGRID_S3_SECRET_KEY"]),
        ),
        catalog=CatalogSettings(url=HttpUrl(catalog_url)),
    )
