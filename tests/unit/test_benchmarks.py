"""Unit tests for the multi-engine benchmark suite and EU cost model (ADR 0009)."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from pydantic import HttpUrl, SecretStr
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from ibergrid.benchmarks import (
    BENCHMARK_QUERIES,
    DEFAULT_CHAOS_PROOFS,
    BenchmarkSuiteResult,
    benchmark_metadata_vs_full_scan,
    benchmark_query_across_layouts,
    build_compacted_clone,
    build_cost_comparison,
    collect_table_inventory,
    current_peak_rss_mib,
    detect_environment,
    estimate_r2_monthly_cost,
    export_unpartitioned_parquet,
    format_bytes,
    load_host_settings,
    measure_cold_starts,
    measure_s3_bucket_footprint,
    percentile,
    plan_iceberg_scan,
    polars_s3_storage_options,
    project_ibergrid_monthly_ops,
    render_markdown_report,
    results_match,
    run_duckdb_query,
    run_live_benchmark_suite,
    run_polars_iceberg_query,
    run_polars_parquet_query,
    summarize_timings,
    time_callable,
    to_json_dict,
    write_json_report,
)
from ibergrid.benchmarks import runner as bench_runner
from ibergrid.lake import duck, registry
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import CatalogSettings, Settings, StorageSettings

NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
ATTRIBUTION = "Source: OMIE (omie.es) & Red Eléctrica (REData)"


def _duck_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.load_extension("iceberg")
    con.execute("SET GLOBAL TimeZone = 'UTC'")
    con.execute("SET TimeZone = 'UTC'")
    return con


def _local_catalog(tmp_path: Path) -> SqlCatalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "bench_unit",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=warehouse.as_uri(),
    )


def _seed_redata_table(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, registry.REDATA_OBSERVATIONS)
    tbl = catalog.load_table(registry.REDATA_OBSERVATIONS.identifier)
    schema = registry.REDATA_OBSERVATIONS.schema.as_arrow()
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}

    # Two months (2025-09 and 2025-10) x two datasets so partition pruning is exercised.
    points = [
        ("demand_realtime", "1293", datetime(2025, 9, 15, 10, 0, tzinfo=UTC), 28000.0),
        ("demand_realtime", "1293", datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 25000.0),
        ("demand_realtime", "1293", datetime(2025, 10, 1, 0, 5, tzinfo=UTC), 25500.0),
        ("demand_realtime", "544", datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 25100.0),
        ("demand_hourly", "10299", datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 24000.0),
    ]
    for dataset, series_id, ts_utc, val in points:
        cols["dataset"].append(dataset)
        cols["widget"].append("demanda/demanda-tiempo-real")
        cols["electric_system"].append("national")
        cols["series_id"].append(series_id)
        cols["series_type"].append("Real")
        cols["series_title"].append("Real demand")
        cols["granularity"].append("5min")
        cols["ts_utc"].append(ts_utc)
        cols["ts_local"].append(ts_utc.isoformat())
        cols["value"].append(val)
        cols["percentage"].append(None)
        cols["series_last_update"].append(NOW)
        cols["fetched_at"].append(NOW)
        cols["run_id"].append("seed")
        cols["landing_path"].append("landing/seed.json")
        cols["payload_sha256"].append("a" * 64)

    # Write in two appends so the 2025-10 partition has multiple files before compaction.
    full = pa.table(cols, schema=schema)
    tbl.append(full.slice(0, 3))
    tbl.append(full.slice(3))
    return tbl


def _seed_gold_mibel_table(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, registry.GOLD_MIBEL_HOURLY_MARKET)
    tbl = catalog.load_table(registry.GOLD_MIBEL_HOURLY_MARKET.identifier)
    schema = registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow()
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}

    rows = [
        (datetime(2024, 5, 1, 12, 0, tzinfo=UTC), date(2024, 5, 1), 14, 0.0, 0.0, False, True),
        (datetime(2025, 4, 28, 7, 0, tzinfo=UTC), date(2025, 4, 28), 9, 47.94, 40.70, True, False),
        (datetime(2025, 10, 1, 10, 0, tzinfo=UTC), date(2025, 10, 1), 12, -1.5, -1.5, False, True),
    ]
    for ts_utc, dday, mhour, es, pt, dec, zneg in rows:
        cols["hour_utc"].append(ts_utc)
        cols["delivery_date"].append(dday)
        cols["madrid_hour"].append(mhour)
        cols["day_type"].append("weekday")
        cols["price_es_eur_mwh"].append(es)
        cols["price_pt_eur_mwh"].append(pt)
        cols["spread_es_minus_pt_eur_mwh"].append(round(es - pt, 4))
        cols["is_decoupled"].append(dec)
        cols["is_zero_or_negative_es"].append(zneg)
        cols["pvpc_eur_mwh"].append(80.0)
        cols["peninsular_demand_mw"].append(27000.0)
        cols["demand_ramp_1h_mw"].append(100.0)
        cols["price_ramp_1h_eur_mwh"].append(2.0)
        cols["intraday_price_rank"].append(1)
        cols["is_evening_ramp_window"].append(False)
        cols["peninsular_renewable_share_pct"].append(62.5)
        cols["peninsular_solar_share_pct"].append(25.0)
        cols["peninsular_wind_share_pct"].append(22.0)
        cols["source_attribution"].append(ATTRIBUTION)
        cols["updated_at_utc"].append(NOW)

    tbl.append(pa.table(cols, schema=schema))
    return tbl


def _seed_silver_demand_5min(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, registry.SILVER_DEMAND_NATIONAL_5MIN)
    tbl = catalog.load_table(registry.SILVER_DEMAND_NATIONAL_5MIN.identifier)
    schema = registry.SILVER_DEMAND_NATIONAL_5MIN.schema.as_arrow()
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}

    base_oct = datetime(2025, 10, 1, 0, 0, tzinfo=UTC)
    entries: list[tuple[datetime, date, float | None]] = [
        (datetime(2025, 9, 15, 0, 0, tzinfo=UTC), date(2025, 9, 15), 22000.0),
    ]
    for idx in range(15):
        entries.append(
            (base_oct + timedelta(minutes=5 * idx), date(2025, 10, 1), 24000.0 + idx * 10)
        )
    entries.append((base_oct + timedelta(minutes=80), date(2025, 10, 1), None))

    for ts_utc, mdate, real_mw in entries:
        cols["ts_utc"].append(ts_utc)
        cols["madrid_date"].append(mdate)
        cols["demand_real_mw"].append(real_mw)
        cols["demand_forecast_mw"].append(24500.0)
        cols["demand_scheduled_mw"].append(24400.0)
        cols["demand_total_scheduled_mw"].append(None)
        cols["source_lane"].append("batch")
        cols["series_last_update_utc"].append(NOW)

    tbl.append(pa.table(cols, schema=schema))
    return tbl


def _seed_silver_demand_system(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, registry.SILVER_DEMAND_SYSTEM_HOURLY)
    tbl = catalog.load_table(registry.SILVER_DEMAND_SYSTEM_HOURLY.identifier)
    schema = registry.SILVER_DEMAND_SYSTEM_HOURLY.schema.as_arrow()
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}

    rows = [
        ("peninsular", "peninsular", True, datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 25000.0),
        ("peninsular", "peninsular", True, datetime(2025, 10, 1, 1, 0, tzinfo=UTC), 26000.0),
        ("canarias", "non_peninsular", False, datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 900.0),
        ("baleares", "non_peninsular", True, datetime(2025, 10, 1, 0, 0, tzinfo=UTC), 600.0),
    ]
    for sys_name, scope, verified, ts_utc, mw in rows:
        cols["ts_utc"].append(ts_utc)
        cols["madrid_date"].append(date(2025, 10, 1))
        cols["madrid_hour"].append(ts_utc.hour)
        cols["electric_system"].append(sys_name)
        cols["system_scope"].append(scope)
        cols["ts_utc_verified"].append(verified)
        cols["demand_mw"].append(mw)
        cols["series_last_update_utc"].append(NOW)

    tbl.append(pa.table(cols, schema=schema))
    return tbl


# --- stats.py ------------------------------------------------------------------------------------


def test_percentile_and_timing_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    assert percentile([42.0], 50.0) == 42.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 50.0) == 25.0
    assert percentile([10.0, 20.0, 30.0], 50.0) == 20.0
    assert percentile([10.0, 20.0, 30.0, 40.0], 100.0) == 40.0

    with pytest.raises(ValueError, match="at least one"):
        percentile([], 50.0)
    with pytest.raises(ValueError, match="in \\[0, 100\\]"):
        percentile([1.0], 101.0)
    with pytest.raises(ValueError, match="at least one sample"):
        summarize_timings([], warmup=1)
    with pytest.raises(ValueError, match="warmup"):
        summarize_timings([1.0], warmup=-1)

    summary = summarize_timings([10.0, 20.0, 30.0], warmup=2, peak_rss_mib=128.456)
    assert summary.p50_ms == 20.0
    assert summary.min_ms == 10.0
    assert summary.max_ms == 30.0
    assert summary.peak_rss_mib == 128.46

    calls = 0

    def _work() -> int:
        nonlocal calls
        calls += 1
        return calls

    result, timed = time_callable(_work, warmup=1, iterations=3)
    assert result == 4
    assert calls == 4
    assert timed.warmup == 1
    assert timed.iterations == 3
    assert timed.peak_rss_mib > 0.0

    with pytest.raises(ValueError, match="warmup"):
        time_callable(_work, warmup=-1, iterations=1)
    with pytest.raises(ValueError, match="iterations"):
        time_callable(_work, warmup=0, iterations=0)

    monkeypatch.setattr(sys, "platform", "darwin")
    assert current_peak_rss_mib() >= 0.0

    monkeypatch.setattr(sys, "platform", "freebsd")
    assert current_peak_rss_mib() >= 0.0

    monkeypatch.setattr(sys, "platform", "linux")
    orig_read_text = Path.read_text

    def _fail_proc(self: Path, *args: Any, **kwargs: Any) -> str:
        if str(self) == "/proc/self/status":
            raise OSError("proc unavailable")
        return orig_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _fail_proc)
    assert current_peak_rss_mib() >= 0.0


# --- cost.py -------------------------------------------------------------------------------------


def test_r2_cost_model_and_eu_scenarios() -> None:
    free = estimate_r2_monthly_cost(
        label="small",
        storage_bytes=240_000_000,
        monthly_class_a_ops=200_000,
        monthly_class_b_ops=500_000,
        monthly_egress_gb=50.0,
    )
    assert free.total_usd == 0.0
    assert free.total_eur == 0.0
    assert free.egress_usd == 0.0

    large = estimate_r2_monthly_cost(
        label="1tb",
        storage_bytes=1024 * 1024**3,
        monthly_class_a_ops=2_000_000,
        monthly_class_b_ops=15_000_000,
        monthly_egress_gb=2000.0,
    )
    # (1024 - 10) * 0.015 = 15.21; 1M Class A = 4.50; 5M Class B = 1.80 -> 21.51 USD
    assert large.storage_usd == 15.21
    assert large.class_a_usd == 4.5
    assert large.class_b_usd == 1.8
    assert large.egress_usd == 0.0
    assert large.total_usd == 21.51
    assert large.total_eur == round(21.51 * 0.92, 2)

    with pytest.raises(ValueError, match="storage_bytes"):
        estimate_r2_monthly_cost(
            label="bad", storage_bytes=-1, monthly_class_a_ops=0, monthly_class_b_ops=0
        )
    with pytest.raises(ValueError, match="operation counts"):
        estimate_r2_monthly_cost(
            label="bad", storage_bytes=0, monthly_class_a_ops=-1, monthly_class_b_ops=0
        )
    with pytest.raises(ValueError, match="monthly_egress_gb"):
        estimate_r2_monthly_cost(
            label="bad",
            storage_bytes=0,
            monthly_class_a_ops=0,
            monthly_class_b_ops=0,
            monthly_egress_gb=-1.0,
        )

    class_a, class_b = project_ibergrid_monthly_ops()
    assert 0 < class_a < 1_000_000
    assert 0 < class_b < 10_000_000

    r2_tiers, scenarios = build_cost_comparison(240_381_948)
    assert len(r2_tiers) == 4
    assert r2_tiers[0].total_eur == 0.0
    assert len(scenarios) == 3
    assert scenarios[0].total_eur_month < 5.0
    assert scenarios[1].total_eur_month < 10.0
    assert scenarios[2].total_eur_month > 300.0


# --- queries.py, metadata_bench.py, runner.py, report.py ----------------------------------------


def test_polars_storage_options_and_results_match() -> None:
    http_opts = polars_s3_storage_options(
        StorageSettings(
            endpoint_url=HttpUrl("http://127.0.0.1:29000"),
            access_key=SecretStr("ak"),
            secret_key=SecretStr("sk"),
        )
    )
    assert http_opts["aws_allow_http"] == "true"
    assert http_opts["aws_virtual_hosted_style_request"] == "false"

    https_opts = polars_s3_storage_options(
        StorageSettings(
            endpoint_url=HttpUrl("https://account.r2.cloudflarestorage.com"),
            access_key=SecretStr("ak"),
            secret_key=SecretStr("sk"),
        )
    )
    assert https_opts["aws_allow_http"] == "false"

    assert results_match([(1, 10.001, "a")], [(1, 10.005, "a")], float_tol=0.01)
    assert not results_match([(1, 10.0)], [(1, 10.5)], float_tol=0.01)
    assert not results_match([(1, "a")], [(1, "b")])
    assert not results_match([(1,)], [(1,), (2,)])
    assert not results_match([(1, 2)], [(1,)])


def test_all_four_benchmark_queries_and_layouts_local_iceberg(tmp_path: Path) -> None:
    catalog = _local_catalog(tmp_path)
    tables = {
        "bronze.redata_observations": _seed_redata_table(catalog),
        "gold.mibel_hourly_market": _seed_gold_mibel_table(catalog),
        "silver.demand_national_5min": _seed_silver_demand_5min(catalog),
        "silver.demand_system_hourly": _seed_silver_demand_system(catalog),
    }

    con = _duck_con()
    try:
        catalog.create_namespace("bench_comp")
        all_rows = []
        for query in BENCHMARK_QUERIES:
            live_tbl = tables[query.source_table]
            live_meta_path = live_tbl.metadata_location

            # Verify Iceberg pruning works on Q1 and Q3 (only 2025-10 files planned).
            planned_files, planned_bytes = plan_iceberg_scan(live_tbl, query)
            assert planned_files >= 1
            assert planned_bytes > 0
            if query.query_id in {"Q1", "Q3"}:
                total_files = len(list(live_tbl.scan().plan_files()))
                assert planned_files < total_files

            parquet_file = tmp_path / f"{query.query_id}.parquet"
            export_unpartitioned_parquet(con, live_meta_path, str(parquet_file))
            assert parquet_file.exists()

            comp_tbl = build_compacted_clone(
                catalog,
                con,
                source_table=live_tbl,
                source_spec=bench_runner.SPEC_BY_NAME[query.source_table],
                bench_namespace="bench_comp",
                clone_duck_name=None,
            )

            # Direct helper check + full 3-layout x 2-engine cross-verification.
            d_direct = run_duckdb_query(con, query, source=live_meta_path, is_iceberg=True)
            p_direct = run_polars_iceberg_query(query, live_tbl)
            p_parq = run_polars_parquet_query(query, str(parquet_file))
            assert results_match(d_direct, p_direct)
            assert results_match(d_direct, p_parq)

            bench_rows = benchmark_query_across_layouts(
                con,
                query,
                parquet_uri=str(parquet_file),
                parquet_bytes=parquet_file.stat().st_size,
                live_table=live_tbl,
                live_duck_source=live_meta_path,
                compacted_table=comp_tbl,
                compacted_duck_source=comp_tbl.metadata_location,
                storage_options=None,
                warmup=0,
                iterations=2,
            )
            assert len(bench_rows) == 6
            assert all(r.answer_verified for r in bench_rows)
            all_rows.extend(bench_rows)

        # Benchmark 2: zero-scan metadata vs full SQL scan (populated + empty table).
        ensure_table(catalog, registry.QUARANTINE_REJECTED_RECORDS)
        empty_tbl = catalog.load_table(registry.QUARANTINE_REJECTED_RECORDS.identifier)
        assert (
            benchmark_metadata_vs_full_scan(
                con,
                logical_table="quarantine.rejected_records",
                duck_table=empty_tbl.metadata_location,
                time_col="rejected_at_utc",
                metric_col="snapshot_id",
                warmup=0,
                iterations=1,
            )
            is None
        )

        meta_row = benchmark_metadata_vs_full_scan(
            con,
            logical_table="gold.mibel_hourly_market",
            duck_table=tables["gold.mibel_hourly_market"].metadata_location,
            time_col="hour_utc",
            metric_col="price_es_eur_mwh",
            warmup=0,
            iterations=2,
        )
        assert meta_row is not None
        assert meta_row.metadata_bytes_scanned == 0
        assert meta_row.bytes_saved_pct == 100.0
        assert meta_row.counts_match is True
        assert meta_row.total_records == 3

        # Inventory collection (skips missing tables cleanly).
        meta_by_name = {name: tbl.metadata_location for name, tbl in tables.items()}
        inventory = collect_table_inventory(
            con,
            specs=(registry.REDATA_OBSERVATIONS, registry.OMIE_DAY_AHEAD_PRICES),
            duck_name_fn=lambda q: meta_by_name.get(q, str(tmp_path / "does-not-exist.json")),
        )
        assert len(inventory) == 1
        assert inventory[0].table == "bronze.redata_observations"
        assert inventory[0].total_records == 5

        # Report JSON + Markdown rendering.
        r2_tiers, scenarios = build_cost_comparison(240_381_948)
        suite = BenchmarkSuiteResult(
            environment=detect_environment(),
            cold_starts_ms={
                "duckdb_import_and_query_ms": 110.0,
                "polars_import_and_query_ms": 125.0,
            },
            total_s3_objects=4844,
            total_s3_bytes=240_381_948,
            table_inventory=inventory,
            query_benchmarks=tuple(all_rows),
            metadata_benchmarks=(meta_row,),
            chaos_proofs=DEFAULT_CHAOS_PROOFS,
            r2_tiers=r2_tiers,
            infra_scenarios=scenarios,
        )
        payload = to_json_dict(suite)
        assert payload["total_s3_objects"] == 4844
        out_json = tmp_path / "results" / "latest.json"
        write_json_report(suite, out_json)
        assert json.loads(out_json.read_text(encoding="utf-8"))["total_s3_bytes"] == 240_381_948

        md = render_markdown_report(suite)
        assert "# IberGrid Lakehouse — Benchmarks and EU Cost Analysis" in md
        assert "## 2. Benchmark 1" in md
        assert "## 3. Benchmark 2" in md
        assert "## 4. Benchmark 3" in md
        assert "## 5. Real European Cloud Monthly Cost Breakdown" in md
        assert "**0 B**" in md
    finally:
        con.close()


def test_format_bytes_and_environment_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    assert format_bytes(512) == "512 B"
    assert format_bytes(2048) == "2.0 KiB"
    assert format_bytes(5 * 1024**2) == "5.00 MiB"
    assert format_bytes(3 * 1024**3) == "3.00 GiB"

    def _boom(_name: str) -> int:
        raise OSError("no sysconf")

    monkeypatch.setattr("os.sysconf", _boom)
    env = detect_environment()
    assert env.ram_gib == 0.0
    assert env.cpu_count >= 1


def test_cold_starts_and_host_settings(tmp_path: Path) -> None:
    starts = measure_cold_starts()
    assert starts["duckdb_import_and_query_ms"] > 0.0
    assert starts["polars_import_and_query_ms"] > 0.0

    with pytest.raises(FileNotFoundError, match="\\.env"):
        load_host_settings(tmp_path)

    (tmp_path / ".env").write_text(
        "# comment\nIBERGRID_S3_ACCESS_KEY=my-ak\nIBERGRID_S3_SECRET_KEY=my-sk\n",
        encoding="utf-8",
    )
    loaded = load_host_settings(tmp_path)
    assert loaded.storage.access_key.get_secret_value() == "my-ak"
    assert loaded.storage.secret_key.get_secret_value() == "my-sk"
    assert str(loaded.catalog.url).startswith("http://127.0.0.1:28181")


def test_measure_s3_bucket_footprint_and_live_suite_mocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = StorageSettings(
        endpoint_url=HttpUrl("http://127.0.0.1:29000"),
        access_key=SecretStr("ak"),
        secret_key=SecretStr("sk"),
    )
    settings = Settings(
        storage=storage,
        catalog=CatalogSettings(url=HttpUrl("http://127.0.0.1:28181")),
    )

    class _FakeFS:
        def __init__(self, entries: list[Any] | None = None, *, missing: bool = False) -> None:
            self._entries = entries or []
            self._missing = missing
            self.deleted_dirs: list[str] = []

        def get_file_info(self, target: Any) -> Any:
            if self._missing:
                raise FileNotFoundError("empty bucket")
            if isinstance(target, pafs.FileSelector):
                return self._entries
            return SimpleNamespace(size=1234)

        def delete_dir(self, prefix: str) -> None:
            self.deleted_dirs.append(prefix)

    fake_entries = [
        SimpleNamespace(type=pafs.FileType.Directory, size=0),
        SimpleNamespace(type=pafs.FileType.File, size=100),
        SimpleNamespace(type=pafs.FileType.File, size=250),
    ]
    fake_io = SimpleNamespace(fs_by_scheme=lambda _s, _b: _FakeFS(fake_entries))
    monkeypatch.setattr(bench_runner, "PyArrowFileIO", lambda _props: fake_io)
    assert measure_s3_bucket_footprint(storage) == (2, 350)

    missing_io = SimpleNamespace(fs_by_scheme=lambda _s, _b: _FakeFS(missing=True))
    monkeypatch.setattr(bench_runner, "PyArrowFileIO", lambda _props: missing_io)
    assert measure_s3_bucket_footprint(storage) == (0, 0)

    # Full run_live_benchmark_suite wired to local SqlCatalog + local Parquet files so cleanup
    # paths and orchestration are tested without Docker.
    catalog = _local_catalog(tmp_path)
    seeded = {
        "bronze.redata_observations": _seed_redata_table(catalog),
        "gold.mibel_hourly_market": _seed_gold_mibel_table(catalog),
        "silver.demand_national_5min": _seed_silver_demand_5min(catalog),
        "silver.demand_system_hourly": _seed_silver_demand_system(catalog),
    }
    con = _duck_con()
    local_dir = tmp_path / "parq"
    local_dir.mkdir()
    created_files: dict[str, Path] = {}

    def _fake_duck_table_name(qualified: str) -> str:
        if qualified in seeded:
            return seeded[qualified].metadata_location
        ns, _, name = qualified.partition(".")
        if ns.startswith("bench_"):
            return catalog.load_table((ns, name)).metadata_location
        return str(tmp_path / "missing.json")

    def _fake_export(connection: duckdb.DuckDBPyConnection, src: str, target_uri: str) -> None:
        fname = Path(target_uri).name
        dest = local_dir / fname
        created_files[target_uri] = dest
        quoted = "'" + str(dest).replace("'", "''") + "'"
        connection.execute(
            f"COPY (SELECT * FROM iceberg_scan($source)) TO {quoted} (FORMAT PARQUET)",
            {"source": src},
        )

    orig_run_duck = run_duckdb_query
    orig_run_pol_parq = run_polars_parquet_query

    def _mapped_duck(
        c: duckdb.DuckDBPyConnection, q: Any, *, source: str, is_iceberg: bool
    ) -> list[tuple[Any, ...]]:
        mapped = (
            str(created_files[source]) if (not is_iceberg and source in created_files) else source
        )
        return orig_run_duck(c, q, source=mapped, is_iceberg=is_iceberg)

    def _mapped_pol_parq(
        q: Any, parquet_source: str, *, storage_options: dict[str, str] | None = None
    ) -> list[tuple[Any, ...]]:
        mapped = str(created_files.get(parquet_source, parquet_source))
        return orig_run_pol_parq(q, mapped, storage_options=None)

    def _mapped_pol_iceberg(
        q: Any, tbl: Table, *, storage_options: dict[str, str] | None = None
    ) -> list[tuple[Any, ...]]:
        return run_polars_iceberg_query(q, tbl, storage_options=None)

    fake_fs = _FakeFS(fake_entries)
    monkeypatch.setattr(
        bench_runner,
        "PyArrowFileIO",
        lambda _p: SimpleNamespace(fs_by_scheme=lambda _s, _b: fake_fs),
    )
    monkeypatch.setattr(
        bench_runner,
        "measure_cold_starts",
        lambda: {"duckdb_import_and_query_ms": 100.0, "polars_import_and_query_ms": 110.0},
    )
    monkeypatch.setattr(
        bench_runner,
        "measure_isolated_query_rss_mib",
        lambda _s, *, query_id, engine, is_iceberg, source: 142.5 if engine == "duckdb" else 168.0,
    )
    monkeypatch.setattr(duck, "connect", lambda _s: con)
    monkeypatch.setattr(duck, "table_name", _fake_duck_table_name)
    monkeypatch.setattr(bench_runner, "rest_catalog", lambda _s: catalog)
    monkeypatch.setattr(bench_runner, "export_unpartitioned_parquet", _fake_export)
    monkeypatch.setattr(bench_runner, "run_duckdb_query", _mapped_duck)
    monkeypatch.setattr(bench_runner, "run_polars_parquet_query", _mapped_pol_parq)
    monkeypatch.setattr(bench_runner, "run_polars_iceberg_query", _mapped_pol_iceberg)

    def _noop_rewrite(_c: Any, _t: str) -> Any:
        raise duckdb.Error("not a catalog table in local SqlCatalog")

    monkeypatch.setattr(bench_runner, "rewrite_data_files", _noop_rewrite)

    progress_lines: list[str] = []
    suite = run_live_benchmark_suite(
        settings, warmup=0, iterations=1, on_progress=progress_lines.append
    )
    assert suite.total_s3_objects == 2
    assert suite.total_s3_bytes == 350
    assert len(suite.query_benchmarks) == 24
    assert all(r.answer_verified for r in suite.query_benchmarks)
    assert any(r.timing.peak_rss_mib == 142.5 for r in suite.query_benchmarks)
    assert any(r.timing.peak_rss_mib == 168.0 for r in suite.query_benchmarks)
    assert len(suite.metadata_benchmarks) == 4
    assert all(r.counts_match for r in suite.metadata_benchmarks)
    assert len(fake_fs.deleted_dirs) == 1
    assert not [ns for ns in catalog.list_namespaces() if ns[0].startswith("bench_")]
    assert len(progress_lines) > 5


def test_measure_isolated_query_rss_mib_and_compaction_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = StorageSettings(
        endpoint_url=HttpUrl("http://127.0.0.1:29000"),
        access_key=SecretStr("ak"),
        secret_key=SecretStr("sk"),
    )
    settings = Settings(
        storage=storage,
        catalog=CatalogSettings(url=HttpUrl("http://127.0.0.1:28181")),
    )
    captured_env: dict[str, str] = {}

    def _fake_run(
        cmd: list[str], *, check: bool, capture_output: bool, text: bool, env: dict[str, str]
    ) -> Any:
        assert check is False
        assert capture_output is True
        assert text is True
        assert cmd[0] == sys.executable
        captured_env.update(env)
        return SimpleNamespace(returncode=0, stdout="153.25\n", stderr="")

    monkeypatch.setattr(subprocess, "run", _fake_run)
    rss = bench_runner.measure_isolated_query_rss_mib(
        settings,
        query_id="Q1",
        engine="duckdb",
        is_iceberg=True,
        source="lake.bronze.redata_observations",
    )
    assert rss == 153.25
    assert captured_env["BENCH_QID"] == "Q1"
    assert captured_env["BENCH_ENGINE"] == "duckdb"
    assert captured_env["BENCH_IS_ICEBERG"] == "1"

    # Also cover build_compacted_clone's optional DuckDB compaction fallback path.
    catalog = _local_catalog(tmp_path)
    src_tbl = _seed_gold_mibel_table(catalog)
    catalog.create_namespace("bench_fb")
    con = _duck_con()
    try:
        monkeypatch.setattr(
            bench_runner,
            "rewrite_data_files",
            lambda _c, _t: (_ for _ in ()).throw(duckdb.Error("simulated")),
        )
        clone = build_compacted_clone(
            catalog,
            con,
            source_table=src_tbl,
            source_spec=bench_runner.SPEC_BY_NAME["gold.mibel_hourly_market"],
            bench_namespace="bench_fb",
            clone_duck_name="lake.bench_fb.gold_mibel_hourly_market",
        )
        assert clone.scan().to_arrow().num_rows == 3
    finally:
        con.close()
