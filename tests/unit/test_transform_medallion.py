"""End-to-end dbt medallion transforms on a real SQL Iceberg catalog and FakeIcebergRest.

Exercises the full ``dbt build`` + PyIceberg WAP publication pipeline over the real recorded
REData and OMIE fixtures (DST transitions, v1->v2 15-minute cut-over, 2025-04-28 blackout
decoupling, peninsular vs Canarias isolation, generation total reconciliation and batch+stream
reconciliation) without Docker or live APIs.
"""

import json
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyarrow import fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table
from typer.testing import CliRunner

from ibergrid.cli import app
from ibergrid.lake import registry, wap
from ibergrid.lake.tables import TableSpec, ensure_table
from ibergrid.settings import Settings
from ibergrid.sources.omie import ingest as omie_ingest
from ibergrid.sources.omie.client import OmieClient
from ibergrid.sources.redata import ingest as redata_ingest
from ibergrid.sources.redata.client import RedataClient
from ibergrid.transform import TransformError, plugin, resolve_selection
from ibergrid.transform import runner as transform_runner
from ibergrid.transform.runner import (
    _align_to_schema,
    _describe_dbt_failure,
    _load_dbt,
    _publish_table,
    find_dbt_project,
    run_medallion,
)

from .conftest import FakeIcebergRest, FakeOmie, FakeRedata, LocalServer, Responder, fixture

Serve = Callable[[Responder], LocalServer]
cli_runner = CliRunner()


def _no_sleep(_seconds: float) -> None:
    pass


def _stream_rows(entries: list[tuple[str, datetime, float, int, int]]) -> pa.Table:
    """Build a PyArrow table matching ``bronze.redata_demand_stream`` for reconciliation tests."""
    now = datetime(2026, 11, 19, 10, 0, tzinfo=UTC)
    schema = registry.REDATA_DEMAND_STREAM.schema.as_arrow()
    return pa.table(
        {
            "series_id": [series for series, *_ in entries],
            "series_title": ["Real" for _ in entries],
            "electric_system": ["national" for _ in entries],
            "ts_utc": pa.array([ts for _, ts, *_ in entries], pa.timestamp("us", "UTC")),
            "ts_local": [ts.isoformat() for _, ts, *_ in entries],
            "value": pa.array([value for _, _, value, *_ in entries], pa.float64()),
            "series_last_update": pa.array([now for _ in entries], pa.timestamp("us", "UTC")),
            "content_sha256": [f"sha-{i}" for i in range(len(entries))],
            "producer_instance": ["test-host" for _ in entries],
            "producer_version": ["0.1.0" for _ in entries],
            "emitted_at": pa.array([now for _ in entries], pa.timestamp("us", "UTC")),
            "kafka_topic": ["redata.demand.realtime.v1" for _ in entries],
            "kafka_partition": pa.array([0 for _ in entries], pa.int32()),
            "kafka_offset": pa.array([offset for *_, offset in entries], pa.int64()),
            "kafka_timestamp": pa.array([now for _ in entries], pa.timestamp("us", "UTC")),
            "ingested_at": pa.array([now for _ in entries], pa.timestamp("us", "UTC")),
            "run_id": ["stream-run" for _ in entries],
            "kafka_topic_id": ["topic-1" for _ in entries],
            "stream_epoch": pa.array([epoch for _, _, _, epoch, _ in entries], pa.int32()),
        },
        schema=schema,
    )


@pytest.fixture
def medallion_env(
    catalog: SqlCatalog,
    tmp_path: Path,
    serve: Serve,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> tuple[SqlCatalog, Settings, FakeRedata, FakeOmie]:
    """Bronze+Silver+Gold tables on ``SqlCatalog`` served through ``FakeIcebergRest``."""
    for spec in registry.TABLES:
        ensure_table(catalog, spec)
    rest = serve(FakeIcebergRest(catalog))
    fake_redata = FakeRedata()
    fake_omie = FakeOmie()
    redata_srv = serve(fake_redata)
    omie_srv = serve(fake_omie)

    monkeypatch.setenv("IBERGRID_CATALOG_URL", rest.url)
    monkeypatch.setenv("IBERGRID_REDATA_BASE_URL", redata_srv.url)
    monkeypatch.setenv("IBERGRID_OMIE_BASE_URL", omie_srv.url)

    landing = fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())
    for module in (redata_ingest, omie_ingest):
        monkeypatch.setattr(module, "rest_catalog", lambda _settings: catalog)
        monkeypatch.setattr(module, "s3_filesystem", lambda *_args, **_kw: landing)
    monkeypatch.setattr(transform_runner, "rest_catalog", lambda _settings: catalog)
    monkeypatch.setattr(omie_ingest, "madrid_today", lambda: date(2026, 11, 21))
    for client_cls in (RedataClient, OmieClient):
        monkeypatch.setattr(
            client_cls,
            "from_settings",
            classmethod(
                lambda cls, s: cls(
                    str(s.base_url).rstrip("/"), timeout=5, max_rps=1e9, sleep=_no_sleep
                )
            ),
        )
    return catalog, Settings(), fake_redata, fake_omie


def _populate_real_fixtures(
    catalog: SqlCatalog, settings: Settings, fake_redata: FakeRedata
) -> None:
    """Ingest a rich cross-section of real recorded OMIE + REData fixtures plus stream events."""
    # OMIE: DST spring/fall in v1 & v2, blackout decoupling, v1->v2 cut-over, and 2026-11-18.
    for day in (
        date(2024, 3, 31),
        date(2024, 10, 27),
        date(2025, 4, 28),
        date(2025, 9, 30),
        date(2025, 10, 1),
        date(2025, 10, 26),
        date(2026, 3, 29),
        date(2026, 11, 18),
    ):
        omie_ingest.ingest(settings, day, day)

    # REData market prices matching blackout, v2 cut-over, DST spring and 2026-11-18.
    for day in (date(2025, 4, 28), date(2025, 10, 1), date(2026, 3, 29), date(2026, 11, 18)):
        redata_ingest.ingest(settings, "market_prices", day, day)

    # REData hourly demand: peninsular on DST + 2026-11-18, plus canarias on 2026-11-18.
    redata_ingest.ingest(
        settings, "demand_hourly", date(2026, 3, 29), date(2026, 3, 29), ("peninsular",)
    )
    redata_ingest.ingest(
        settings,
        "demand_hourly",
        date(2026, 11, 18),
        date(2026, 11, 18),
        ("peninsular", "canarias"),
    )

    # REData generation_daily on 2026-11-18: national, canarias, and peninsular (using national
    # fixture structure so peninsular renewable shares flow into Gold mibel_hourly_market).
    fake_redata.payloads["generation_daily-peninsular-2026-11-18_2026-11-18.json"] = fixture(
        "generation_daily-national-2026-11-18_2026-11-18.json"
    )
    redata_ingest.ingest(
        settings,
        "generation_daily",
        date(2026, 11, 18),
        date(2026, 11, 18),
        ("national", "peninsular", "canarias"),
    )

    # REData realtime demand (closed batch day 2026-11-18 with 288 points).
    redata_ingest.ingest(settings, "demand_realtime", date(2026, 11, 18), date(2026, 11, 18))

    # Stream table:
    # 1. Competing stale value on closed day 2026-11-18 -> batch MUST win!
    # 2. Open day 2026-11-19 with 3 revisions across epochs 0 and 1 -> (epoch 1, offset 2) wins!
    stream_table = catalog.load_table(registry.REDATA_DEMAND_STREAM.identifier)
    stream_table.append(
        _stream_rows(
            [
                ("2037", datetime(2026, 11, 18, 0, 0, tzinfo=UTC), 99999.0, 1, 50),
                ("2037", datetime(2026, 11, 19, 8, 0, tzinfo=UTC), 25000.0, 0, 100),
                ("2037", datetime(2026, 11, 19, 8, 0, tzinfo=UTC), 26000.0, 1, 1),
                ("2037", datetime(2026, 11, 19, 8, 0, tzinfo=UTC), 26500.0, 1, 2),
                ("2052", datetime(2026, 11, 19, 8, 0, tzinfo=UTC), 26400.0, 1, 3),
            ]
        )
    )


def _rows(catalog: SqlCatalog, spec: TableSpec) -> list[dict[str, Any]]:
    table: Table = catalog.load_table(spec.identifier)
    return list(table.scan().to_arrow().to_pylist())


def test_full_medallion_run_publishes_all_silver_and_gold_tables_over_real_fixtures(
    medallion_env: tuple[SqlCatalog, Settings, FakeRedata, FakeOmie],
) -> None:
    catalog, settings, fake_redata, _ = medallion_env
    _populate_real_fixtures(catalog, settings, fake_redata)

    seen_tables: list[str] = []
    report = run_medallion(
        settings, run_id_prefix="test-medallion", on_table=lambda r: seen_tables.append(r.qualified)
    )

    assert seen_tables == [f"{s.namespace}.{s.name}" for s in registry.MEDALLION_TABLES]
    assert report.dbt_models_built == 12  # 3 staging + 5 silver + 4 gold
    assert report.dbt_tests_passed > 50
    assert all(snap is not None for snap in report.bronze_snapshots.values())

    # No leftover audit branches anywhere and snapshot summaries record bronze snapshot provenance.
    for spec in registry.MEDALLION_TABLES:
        table = catalog.load_table(spec.identifier)
        assert wap.audit_branches(table) == []
        current = table.current_snapshot()
        assert current is not None
        assert current.summary is not None
        bronze_json = current.summary["ibergrid.transform.bronze-snapshots"]
        assert bronze_json is not None
        assert json.loads(bronze_json) == report.bronze_snapshots

    # 1. DST hour counts & v1->v2 cut-over + blackout decoupling in silver.mibel_prices_hourly.
    hourly_prices = _rows(catalog, registry.SILVER_MIBEL_PRICES_HOURLY)
    by_day: dict[date, list[dict[str, Any]]] = {}
    for row in hourly_prices:
        by_day.setdefault(row["delivery_date"], []).append(row)
    assert {d: len(r) for d, r in by_day.items()} == {
        date(2024, 3, 31): 23,
        date(2024, 10, 27): 25,
        date(2025, 4, 28): 24,
        date(2025, 9, 30): 24,
        date(2025, 10, 1): 24,
        date(2025, 10, 26): 25,
        date(2026, 3, 29): 23,
        date(2026, 11, 18): 24,
    }
    assert {r["periods_in_hour"] for r in by_day[date(2025, 9, 30)]} == {1}
    assert {r["periods_in_hour"] for r in by_day[date(2025, 10, 1)]} == {4}
    assert any(r["is_decoupled"] for r in by_day[date(2025, 4, 28)])
    assert any(r["is_zero_or_negative_es"] for r in by_day[date(2025, 4, 28)])
    assert any(r["is_decoupled"] for r in by_day[date(2025, 10, 1)])
    # Spot reconciliation on all matched hours.
    matched = [r for r in hourly_prices if r["omie_redata_diff_eur_mwh"] is not None]
    assert len(matched) == 24 + 24 + 23 + 24
    assert max(abs(r["omie_redata_diff_eur_mwh"]) for r in matched) <= 0.01

    # 2. Strict peninsular vs Canarias isolation in Silver and Gold.
    sys_hourly = _rows(catalog, registry.SILVER_DEMAND_SYSTEM_HOURLY)
    canarias_rows = [r for r in sys_hourly if r["electric_system"] == "canarias"]
    pen_rows = [r for r in sys_hourly if r["electric_system"] == "peninsular"]
    assert len(canarias_rows) == 24
    assert all(
        r["system_scope"] == "non_peninsular" and not r["ts_utc_verified"] for r in canarias_rows
    )
    assert all(r["system_scope"] == "peninsular" and r["ts_utc_verified"] for r in pen_rows)

    daily_summary = {
        r["delivery_date"]: r for r in _rows(catalog, registry.GOLD_MIBEL_DAILY_SUMMARY)
    }
    nov18 = daily_summary[date(2026, 11, 18)]
    pen_nov18_mwh = round(
        sum(r["demand_mw"] for r in pen_rows if r["madrid_date"] == date(2026, 11, 18)), 4
    )
    can_nov18_mwh = round(sum(r["demand_mw"] for r in canarias_rows), 4)
    assert nov18["peninsular_demand_mwh"] == pytest.approx(pen_nov18_mwh, abs=1e-3)
    assert nov18["non_peninsular_demand_mwh"] == pytest.approx(can_nov18_mwh, abs=1e-3)
    assert nov18["peninsular_renewable_share_pct"] is not None
    assert "OMIE" in nov18["source_attribution"]
    assert "REData" in nov18["source_attribution"]

    # 3. Generation daily excludes Total (10296) and reconciles cleanly.
    gen_rows = _rows(catalog, registry.SILVER_GENERATION_DAILY)
    assert all(r["series_id"] != "10296" for r in gen_rows)
    pen_gen = [r for r in gen_rows if r["electric_system"] == "peninsular"]
    assert sum(r["generation_mwh"] for r in pen_gen) == pytest.approx(
        pen_gen[0]["reported_total_mwh"], abs=0.05
    )

    # 4. Batch + stream reconciliation (closed day -> batch wins; open day -> highest epoch/offset).
    nat_5min = _rows(catalog, registry.SILVER_DEMAND_NATIONAL_5MIN)
    closed_pt = next(r for r in nat_5min if r["ts_utc"] == datetime(2026, 11, 18, 0, 0, tzinfo=UTC))
    assert closed_pt["source_lane"] == "batch"
    assert closed_pt["demand_real_mw"] != 99999.0  # stale stream row did NOT override closed batch
    open_pt = next(r for r in nat_5min if r["ts_utc"] == datetime(2026, 11, 19, 8, 0, tzinfo=UTC))
    assert open_pt["source_lane"] == "stream"
    assert open_pt["demand_real_mw"] == 26500.0  # epoch 1, offset 2 won over (0, 100) and (1, 1)
    assert open_pt["demand_forecast_mw"] == 26400.0

    # 5. Optimal consumption windows & system demand daily populated.
    windows = _rows(catalog, registry.GOLD_OPTIMAL_CONSUMPTION_WINDOWS)
    assert len(windows) > 0
    assert any(w["is_optimal_4h_window"] and w["window_rank"] == 1 for w in windows)
    sys_daily = _rows(catalog, registry.GOLD_SYSTEM_DEMAND_DAILY)
    assert {(r["demand_date"], r["electric_system"]) for r in sys_daily} == {
        (date(2026, 3, 29), "peninsular"),
        (date(2026, 11, 18), "peninsular"),
        (date(2026, 11, 18), "canarias"),
    }

    # 6. Idempotent rerun produces identical row counts.
    second = run_medallion(settings)
    assert [(t.qualified, t.rows) for t in second.tables] == [
        (t.qualified, t.rows) for t in report.tables
    ]


def test_subset_selection_builds_upstreams_and_publishes_only_requested_tables(
    medallion_env: tuple[SqlCatalog, Settings, FakeRedata, FakeOmie],
) -> None:
    catalog, settings, _, _ = medallion_env
    omie_ingest.ingest(settings, date(2025, 9, 30), date(2025, 10, 1))

    report = run_medallion(settings, select=("gold.mibel_daily_summary",))

    assert [t.qualified for t in report.tables] == ["gold.mibel_daily_summary"]
    assert report.tables[0].rows == 2
    gold_summary = catalog.load_table(registry.GOLD_MIBEL_DAILY_SUMMARY.identifier)
    assert gold_summary.current_snapshot() is not None
    # Upstream silver tables were built in staging DuckDB but NOT published when not selected.
    silver_hourly = catalog.load_table(registry.SILVER_MIBEL_PRICES_HOURLY.identifier)
    assert silver_hourly.current_snapshot() is None


def test_empty_bronze_tables_succeed_without_creating_empty_snapshots(
    medallion_env: tuple[SqlCatalog, Settings, FakeRedata, FakeOmie],
) -> None:
    catalog, settings, _, _ = medallion_env

    report = run_medallion(settings)

    assert all(snap is None for snap in report.bronze_snapshots.values())
    assert all(t.rows == 0 and t.snapshot_id is None for t in report.tables)
    for spec in registry.MEDALLION_TABLES:
        table = catalog.load_table(spec.identifier)
        assert table.current_snapshot() is None
        assert wap.audit_branches(table) == []


def test_failing_dbt_test_blocks_all_iceberg_publishes(
    medallion_env: tuple[SqlCatalog, Settings, FakeRedata, FakeOmie],
) -> None:
    """An unmapped generation series_id fails dbt tests and leaves main untouched."""
    catalog, settings, fake_redata, _ = medallion_env
    omie_ingest.ingest(settings, date(2026, 11, 18), date(2026, 11, 18))
    first = run_medallion(settings, select=("mibel_prices_hourly",))
    initial_snapshot = first.tables[0].snapshot_id

    # Inject an unmapped generation_daily series_id directly into Bronze.
    bad_gen = json.loads(fixture("generation_daily-canarias-2026-11-18_2026-11-18.json"))
    bad_gen["included"][0]["id"] = "99999"
    fake_redata.payloads["generation_daily-canarias-2026-11-18_2026-11-18.json"] = json.dumps(
        bad_gen
    ).encode()
    redata_ingest.ingest(
        settings, "generation_daily", date(2026, 11, 18), date(2026, 11, 18), ("canarias",)
    )

    with pytest.raises(TransformError, match="assert_generation_technologies_mapped"):
        run_medallion(settings)

    # Previous snapshot on mibel_prices_hourly untouched, generation_daily never published.
    prices = catalog.load_table(registry.SILVER_MIBEL_PRICES_HOURLY.identifier)
    assert prices.metadata.current_snapshot_id == initial_snapshot
    gen = catalog.load_table(registry.SILVER_GENERATION_DAILY.identifier)
    assert gen.current_snapshot() is None


def test_cli_transform_run_and_error_handling(
    medallion_env: tuple[SqlCatalog, Settings, FakeRedata, FakeOmie],
) -> None:
    _, settings, _, _ = medallion_env
    omie_ingest.ingest(settings, date(2025, 10, 1), date(2025, 10, 1))

    ok = cli_runner.invoke(app, ["transform", "run", "--select", "silver"])
    assert ok.exit_code == 0, ok.output
    assert "transform: silver.mibel_prices_hourly: 24 rows, snapshot " in ok.stdout
    assert "transform: 5 table(s) published," in ok.stdout

    bad = cli_runner.invoke(app, ["transform", "run", "--select", "nonexistent"])
    assert bad.exit_code == 1
    assert "unknown transform target 'nonexistent'" in bad.stderr


def test_resolve_selection_and_find_dbt_project_errors(
    tmp_path: Path, credentials: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert resolve_selection() == registry.MEDALLION_TABLES
    assert resolve_selection(["silver"]) == registry.SILVER_TABLES
    assert resolve_selection(["gold"]) == registry.GOLD_TABLES
    assert resolve_selection(["mibel_prices_hourly", "gold.mibel_daily_summary"]) == (
        registry.SILVER_MIBEL_PRICES_HOURLY,
        registry.GOLD_MIBEL_DAILY_SUMMARY,
    )
    with pytest.raises(ValueError, match="unknown transform target"):
        resolve_selection(["bogus"])

    monkeypatch.setenv("IBERGRID_TRANSFORM_DBT_PROJECT_DIR", str(tmp_path))
    with pytest.raises(TransformError, match=r"has no dbt_project\.yml"):
        find_dbt_project(Settings())

    (tmp_path / "dbt_project.yml").write_text("name: test\n")
    assert find_dbt_project(Settings()) == tmp_path


def test_find_dbt_project_missing_without_env(
    tmp_path: Path, credentials: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(transform_runner, "__file__", str(tmp_path / "a" / "b" / "runner.py"))
    with pytest.raises(TransformError, match=r"could not locate dbt/dbt_project\.yml"):
        find_dbt_project(Settings())


def test_plugin_identifier_validation_and_missing_bronze_table(
    catalog: SqlCatalog, serve: Serve, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    with pytest.raises(ValueError, match="invalid SQL identifier"):
        plugin._validate_identifier("bronze; DROP TABLE x")

    # Standalone fallback when no active context is bound.
    assert plugin.current_context().bronze_namespace == "bronze"

    # Catalog has NO Bronze tables created -> dbt build fails cleanly naming lake init.
    rest = serve(FakeIcebergRest(catalog))
    monkeypatch.setenv("IBERGRID_CATALOG_URL", rest.url)
    monkeypatch.setattr(transform_runner, "rest_catalog", lambda _s: catalog)
    with pytest.raises(TransformError, match="lake init"):
        run_medallion(Settings())


def test_align_to_schema_and_publish_table_edge_cases(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = registry.GOLD_MIBEL_DAILY_SUMMARY
    # Missing/extra columns rejected.
    with pytest.raises(TransformError, match="columns do not match registry schema"):
        _align_to_schema(pa.table({"extra_col": [1]}), spec)

    # Null in required column rejected.
    empty = spec.schema.as_arrow().empty_table()
    bad_cols = {field.name: pa.array([None], type=field.type) for field in spec.schema.as_arrow()}
    with pytest.raises(TransformError, match="null\\(s\\) in a required column"):
        _align_to_schema(pa.table(bad_cols), spec)

    # Missing target table in catalog fails cleanly.
    with pytest.raises(TransformError, match="run `ibergrid lake init` first"):
        _publish_table(
            catalog,
            spec.identifier,
            empty,
            run_id_prefix="r1",
            bronze_snapshots={},
            dbt_tests_passed=1,
        )

    # PublishConflict retries and succeeds on second attempt; exhausts on 3rd.
    ensure_table(catalog, spec)
    one_row = pa.table(
        {
            field.name: (
                pa.array([date(2026, 11, 18)], field.type)
                if pa.types.is_date(field.type)
                else pa.array([1], field.type)
                if pa.types.is_integer(field.type)
                else pa.array([10.0], field.type)
                if pa.types.is_floating(field.type)
                else pa.array([datetime(2026, 11, 18, tzinfo=UTC)], field.type)
                if pa.types.is_timestamp(field.type)
                else pa.array(["src"], field.type)
            )
            for field in spec.schema.as_arrow()
        },
        schema=spec.schema.as_arrow(),
    )
    real_publish = wap.publish
    calls = 0

    def flaky_publish(*args: Any, **kwargs: Any) -> int | None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise wap.PublishConflict("main moved")
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(wap, "publish", flaky_publish)
    snap, _ = _publish_table(
        catalog,
        spec.identifier,
        one_row,
        run_id_prefix="retry",
        bronze_snapshots={},
        dbt_tests_passed=1,
    )
    assert snap is not None
    assert calls == 2
    assert wap.audit_branches(catalog.load_table(spec.identifier)) == []

    # Audit row count mismatch discards branch and raises TransformError.
    monkeypatch.setattr(wap, "publish", real_publish)
    monkeypatch.setattr(wap, "head", lambda _t, _a: None)
    with pytest.raises(TransformError, match="audit mismatch"):
        _publish_table(
            catalog,
            spec.identifier,
            one_row,
            run_id_prefix="mismatch",
            bronze_snapshots={},
            dbt_tests_passed=1,
        )
    assert wap.audit_branches(catalog.load_table(spec.identifier)) == []

    # Persistent conflict exhausts MAX_PUBLISH_ATTEMPTS and propagates PublishConflict.
    monkeypatch.undo()

    def always_conflict(*_a: Any, **_kw: Any) -> int | None:
        raise wap.PublishConflict("conflict")

    monkeypatch.setattr(wap, "publish", always_conflict)
    with pytest.raises(wap.PublishConflict):
        _publish_table(
            catalog,
            spec.identifier,
            one_row,
            run_id_prefix="exhaust",
            bronze_snapshots={},
            dbt_tests_passed=1,
        )
    assert wap.audit_branches(catalog.load_table(spec.identifier)) == []


def test_load_dbt_missing_package_and_failure_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "dbt.cli.main", None)
    with pytest.raises(TransformError, match="dbt-core and dbt-duckdb are required"):
        _load_dbt()

    class FakeNode:
        def __init__(self, name: str) -> None:
            self.name = name

    class FakeItem:
        def __init__(self, name: str, status: str, message: str = "") -> None:
            self.node = FakeNode(name)
            self.status = status
            self.message = message

    class FakeResult:
        def __init__(self, items: list[FakeItem], exc: Exception | None = None) -> None:
            self.result = type("R", (), {"results": items})()
            self.exception = exc

    many = [FakeItem(f"t{i}", "fail", f"m{i}") for i in range(6)]
    summary = _describe_dbt_failure(FakeResult(many))
    assert "t0 (fail: m0)" in summary
    assert "(+1 more)" in summary
    assert _describe_dbt_failure(FakeResult([], RuntimeError("boom"))) == "boom"
    assert _describe_dbt_failure(FakeResult([])) == "unknown dbt error"
