"""Zero-scan Iceberg metadata checks, Soda v4 contracts and WAP quarantine (ADR 0008)."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from ibergrid.lake import duck, registry, wap
from ibergrid.lake.tables import ensure_table
from ibergrid.quality import (
    CONTRACT_MAP,
    MetadataRule,
    QualityGateError,
    QuarantineRecord,
    audit_branch_and_quarantine,
    audit_table_metadata,
    check_tables,
    find_contracts_dir,
    inspect_table_metadata,
    run_soda_contract,
    write_quarantine,
)
from ibergrid.quality import gates as quality_gates
from ibergrid.settings import Settings

MADRID = ZoneInfo("Europe/Madrid")
NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
ATTRIBUTION = "Source: OMIE (omie.es) & Red Eléctrica (REData) — non-commercial portfolio project"


def _madrid_hours(day: date) -> list[datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=MADRID).astimezone(UTC)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=MADRID).astimezone(UTC)
    out: list[datetime] = []
    cur = start
    while cur < end:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


def _duck_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.load_extension("iceberg")
    con.execute("SET GLOBAL TimeZone = 'UTC'")
    con.execute("SET TimeZone = 'UTC'")
    return con


def _omie_rows(days: list[date]) -> pa.Table:
    cols: dict[str, list[Any]] = {
        f.name: [] for f in registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow()
    }
    for day in days:
        for idx, start_utc in enumerate(_madrid_hours(day), start=1):
            es = 47.94 if (day == date(2025, 4, 28) and idx == 8) else 50.0 + idx
            pt = 40.70 if (day == date(2025, 4, 28) and idx == 8) else es
            cols["delivery_date"].append(day)
            cols["period"].append(idx)
            cols["resolution_minutes"].append(60)
            cols["interval_start_utc"].append(start_utc)
            cols["interval_end_utc"].append(start_utc + timedelta(hours=1))
            cols["price_es_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(pt)
            cols["contract_version"].append(1)
            cols["source_file"].append(f"marginalpdbc_{day:%Y%m%d}.1")
            cols["file_version"].append(1)
            cols["content_sha256"].append("a" * 64)
            cols["fetched_at"].append(NOW)
            cols["run_id"].append("seed")
            cols["landing_path"].append(f"landing/omie/{day:%Y%m%d}.1")
    return pa.table(cols, schema=registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow())


def _mibel_hourly_rows(days: list[date]) -> pa.Table:
    cols: dict[str, list[Any]] = {
        f.name: [] for f in registry.SILVER_MIBEL_PRICES_HOURLY.schema.as_arrow()
    }
    for day in days:
        for idx, start_utc in enumerate(_madrid_hours(day), start=1):
            local = start_utc.astimezone(MADRID)
            es = 47.94 if (day == date(2025, 4, 28) and idx == 8) else 50.0 + idx
            pt = 40.70 if (day == date(2025, 4, 28) and idx == 8) else es
            cols["hour_utc"].append(start_utc)
            cols["delivery_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["contract_version"].append(1)
            cols["periods_in_hour"].append(1)
            cols["price_es_eur_mwh"].append(es)
            cols["price_es_min_eur_mwh"].append(es)
            cols["price_es_max_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(pt)
            cols["spread_es_minus_pt_eur_mwh"].append(round(es - pt, 4))
            cols["is_decoupled"].append(abs(es - pt) > 0.01)
            cols["is_zero_or_negative_es"].append(es <= 0.0)
            cols["pvpc_eur_mwh"].append(es + 20.0)
            cols["redata_spot_eur_mwh"].append(es)
            cols["omie_redata_diff_eur_mwh"].append(0.0)
    return pa.table(cols, schema=registry.SILVER_MIBEL_PRICES_HOURLY.schema.as_arrow())


def _demand_system_rows(day: date) -> pa.Table:
    cols: dict[str, list[Any]] = {
        f.name: [] for f in registry.SILVER_DEMAND_SYSTEM_HOURLY.schema.as_arrow()
    }
    for sys_name, scope, verified, base_mw in (
        ("peninsular", "peninsular", True, 24000.0),
        ("canarias", "non_peninsular", False, 1000.0),
    ):
        for start_utc in _madrid_hours(day):
            local = start_utc.astimezone(MADRID)
            cols["ts_utc"].append(start_utc)
            cols["madrid_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["electric_system"].append(sys_name)
            cols["system_scope"].append(scope)
            cols["ts_utc_verified"].append(verified)
            cols["demand_mw"].append(base_mw + local.hour * 10.0)
            cols["series_last_update_utc"].append(NOW)
    return pa.table(cols, schema=registry.SILVER_DEMAND_SYSTEM_HOURLY.schema.as_arrow())


def _gold_market_rows(days: list[date]) -> pa.Table:
    cols: dict[str, list[Any]] = {
        f.name: [] for f in registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow()
    }
    for day in days:
        day_type = "weekend" if day.weekday() >= 5 else "weekday"
        for idx, start_utc in enumerate(_madrid_hours(day), start=1):
            local = start_utc.astimezone(MADRID)
            es = 50.0 + idx
            cols["hour_utc"].append(start_utc)
            cols["delivery_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["day_type"].append(day_type)
            cols["price_es_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(es)
            cols["spread_es_minus_pt_eur_mwh"].append(0.0)
            cols["is_decoupled"].append(False)
            cols["is_zero_or_negative_es"].append(False)
            cols["pvpc_eur_mwh"].append(es + 20.0)
            cols["peninsular_demand_mw"].append(25000.0)
            cols["demand_ramp_1h_mw"].append(0.0)
            cols["price_ramp_1h_eur_mwh"].append(0.0)
            cols["intraday_price_rank"].append(idx)
            cols["is_evening_ramp_window"].append(18 <= local.hour <= 21)
            cols["peninsular_renewable_share_pct"].append(55.0)
            cols["peninsular_solar_share_pct"].append(20.0)
            cols["peninsular_wind_share_pct"].append(25.0)
            cols["source_attribution"].append(ATTRIBUTION)
            cols["updated_at_utc"].append(NOW)
    return pa.table(cols, schema=registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow())


# --- Zero-scan metadata checks -------------------------------------------------------------------


def test_zero_scan_metadata_handles_empty_multifile_numeric_bounds_and_audit_snapshots(
    catalog: SqlCatalog,
) -> None:
    catalog.create_namespace("q")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "price", DoubleType(), required=False),
        NestedField(3, "sys", StringType(), required=True),
    )
    table = catalog.create_table(("q", "prices"), schema)
    con = _duck_con()
    try:
        # Empty table: 0 files, 0 bytes scanned, freshness rule fails cleanly.
        empty_summary = inspect_table_metadata(con, table.metadata_location)
        assert empty_summary.snapshot_id is None
        assert empty_summary.total_records == 0
        assert empty_summary.data_bytes_scanned == 0

        empty_audit = audit_table_metadata(
            con,
            table.metadata_location,
            MetadataRule(min_rows=1, max_staleness_hours=24.0),
        )
        assert not empty_audit.passed
        assert {v.rule_name for v in empty_audit.violations} == {
            "metadata.min_rows",
            "metadata.freshness",
        }

        # Append two separate files with values 9.5 and 100.0 (proves numeric, not lexical min/max).
        arrow_schema = schema.as_arrow()
        table.append(
            pa.table(
                {
                    "id": pa.array([1], pa.int64()),
                    "price": pa.array([9.5], pa.float64()),
                    "sys": pa.array(["peninsular"], pa.string()),
                },
                schema=arrow_schema,
            )
        )
        table.append(
            pa.table(
                {
                    "id": pa.array([2], pa.int64()),
                    "price": pa.array([100.0], pa.float64()),
                    "sys": pa.array(["canarias"], pa.string()),
                },
                schema=arrow_schema,
            )
        )
        summary = inspect_table_metadata(con, table.metadata_location)
        assert summary.data_files == 2
        assert summary.total_records == 2
        assert summary.data_bytes_scanned == 0
        assert summary.columns["price"].min_numeric == 9.5
        assert summary.columns["price"].max_numeric == 100.0
        assert summary.columns["sys"].lower_bound == "canarias"
        assert summary.columns["sys"].upper_bound == "peninsular"

        ok = audit_table_metadata(
            con,
            table.metadata_location,
            MetadataRule(
                min_rows=2,
                non_null_columns=("id", "price", "sys"),
                numeric_bounds={"price": (-500.0, 4000.0)},
                max_delete_files=0,
                max_staleness_hours=24.0,
            ),
        )
        assert ok.passed

        # Stage a bad row (null + out-of-range price) on an audit branch; inspect via snapshot_id.
        audit = wap.start(table, "bad-meta")
        bad_snap = wap.write(
            table,
            audit,
            pa.table(
                {
                    "id": pa.array([3, 4], pa.int64()),
                    "price": pa.array([None, 9999.0], pa.float64()),
                    "sys": pa.array(["peninsular", "peninsular"], pa.string()),
                },
                schema=arrow_schema,
            ),
        )
        bad_audit = audit_table_metadata(
            con,
            table.metadata_location,
            MetadataRule(
                non_null_columns=("price", "missing_col"),
                numeric_bounds={"price": (20.0, 4000.0)},
                max_staleness_hours=0.0,
            ),
            snapshot_id=bad_snap,
            now=datetime.now(UTC) + timedelta(hours=2),
        )
        assert not bad_audit.passed
        assert bad_audit.summary.data_bytes_scanned == 0
        rules = {v.rule_name for v in bad_audit.violations}
        assert "metadata.non_null.price" in rules
        assert "metadata.non_null.missing_col" in rules
        assert "metadata.bounds.price" in rules
        assert "metadata.freshness" in rules
        wap.discard(table, audit)

        with pytest.raises(ValueError, match="timezone-aware"):
            audit_table_metadata(
                con,
                table.metadata_location,
                MetadataRule(max_staleness_hours=1.0),
                now=datetime.now(),
            )
    finally:
        con.close()


# --- Quarantine writer ---------------------------------------------------------------------------


def test_write_quarantine_appends_via_wap_and_retries_conflict(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert write_quarantine(catalog, []) is None
    with pytest.raises(RuntimeError, match="lake init"):
        write_quarantine(
            catalog,
            [QuarantineRecord("r1", "bronze.x", "rule", "error", "bad")],
        )

    ensure_table(catalog, registry.QUARANTINE_REJECTED_RECORDS)
    real_publish = wap.publish
    attempts = 0

    def fail_once(*args: Any, **kwargs: Any) -> int | None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise wap.PublishConflict("race")
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(wap, "publish", fail_once)
    snap = write_quarantine(
        catalog,
        [
            QuarantineRecord(
                run_id="run-1",
                source_table="bronze.omie_day_ahead_prices",
                rule_name="soda.duplicate",
                severity="error",
                reason="duplicate key",
                record_key="2025-04-28|8",
                payload_json='{"delivery_date":"2025-04-28","period":8}',
                audit_branch="audit-run-1",
                snapshot_id=123,
            )
        ],
    )
    assert snap is not None
    assert attempts == 2
    q_table = catalog.load_table(registry.QUARANTINE_REJECTED_RECORDS.identifier)
    rows = q_table.scan().to_arrow().to_pylist()
    assert len(rows) == 1
    assert rows[0]["record_key"] == "2025-04-28|8"
    assert wap.audit_branches(q_table) == []


# --- Soda v4 contracts & WAP audit-and-quarantine ------------------------------------------------


def test_contracts_dir_discovery_and_missing_soda_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    contracts = find_contracts_dir(Settings())
    for filename in CONTRACT_MAP.values():
        assert (contracts / filename).is_file()

    monkeypatch.setenv("IBERGRID_QUALITY_CONTRACTS_DIR", str(tmp_path / "nope"))
    with pytest.raises(RuntimeError, match="does not exist"):
        find_contracts_dir(Settings())

    monkeypatch.setitem(sys.modules, "soda_core.contracts.api.verify_api", None)
    with pytest.raises(RuntimeError, match="soda-core and soda-duckdb are required"):
        quality_gates._load_soda()


def test_wap_audit_blocks_corrupt_batches_and_quarantines_all_four_chaos_classes(
    catalog: SqlCatalog, credentials: None
) -> None:
    for spec in (
        registry.OMIE_DAY_AHEAD_PRICES,
        registry.SILVER_MIBEL_PRICES_HOURLY,
        registry.SILVER_DEMAND_SYSTEM_HOURLY,
        registry.GOLD_MIBEL_HOURLY_MARKET,
        registry.QUARANTINE_REJECTED_RECORDS,
    ):
        ensure_table(catalog, spec)

    contracts_dir = find_contracts_dir(Settings())
    days = [date(2025, 3, 30), date(2025, 4, 28), date(2025, 10, 26)]
    con = _duck_con()

    def seed(table: Table, data: pa.Table, run_id: str) -> int:
        a = wap.start(table, run_id)
        h = wap.write(table, a, data)
        pub = wap.publish(table, a, h)
        assert pub is not None
        return pub

    try:
        t_omie = catalog.load_table(registry.OMIE_DAY_AHEAD_PRICES.identifier)
        t_hourly = catalog.load_table(registry.SILVER_MIBEL_PRICES_HOURLY.identifier)
        t_demand = catalog.load_table(registry.SILVER_DEMAND_SYSTEM_HOURLY.identifier)
        t_gold = catalog.load_table(registry.GOLD_MIBEL_HOURLY_MARKET.identifier)

        omie_data = _omie_rows(days)
        hourly_data = _mibel_hourly_rows(days)
        demand_data = _demand_system_rows(date(2025, 4, 28))
        gold_data = _gold_market_rows(days)

        snap_omie = seed(t_omie, omie_data, "seed-omie")
        snap_hourly = seed(t_hourly, hourly_data, "seed-hourly")
        snap_demand = seed(t_demand, demand_data, "seed-demand")
        snap_gold = seed(t_gold, gold_data, "seed-gold")

        # Clean silver.mibel_prices_hourly passes and reports 1 verified decoupled blackout hour.
        clean_audit = wap.start(t_hourly, "clean-hourly")
        clean_report = audit_branch_and_quarantine(
            catalog,
            t_hourly,
            clean_audit,
            table_name="silver.mibel_prices_hourly",
            con=con,
            table_ref=t_hourly.metadata_location,
            contracts_dir=contracts_dir,
            run_id="clean-hourly",
        )
        assert clean_report.passed
        assert clean_report.decoupled_periods_verified == 1
        wap.discard(t_hourly, clean_audit)

        # 1. Duplicate key in bronze.omie_day_ahead_prices -> blocked & quarantined.
        a1 = wap.start(t_omie, "dup-omie")
        wap.write(t_omie, a1, omie_data.slice(0, 1))
        with pytest.raises(QualityGateError, match="duplicate"):
            audit_branch_and_quarantine(
                catalog,
                t_omie,
                a1,
                table_name="bronze.omie_day_ahead_prices",
                con=con,
                table_ref=t_omie.metadata_location,
                contracts_dir=contracts_dir,
                run_id="dup-omie",
            )
        t_omie.refresh()
        assert t_omie.metadata.current_snapshot_id == snap_omie
        assert wap.audit_branches(t_omie) == []

        # 2. OMIE ES/PT swap on 2025-04-28 decoupled hour -> caught by cross-source check!
        es_i = hourly_data.schema.get_field_index("price_es_eur_mwh")
        pt_i = hourly_data.schema.get_field_index("price_pt_eur_mwh")
        swapped = hourly_data.set_column(
            es_i, hourly_data.schema.field(es_i), hourly_data.column("price_pt_eur_mwh")
        ).set_column(pt_i, hourly_data.schema.field(pt_i), hourly_data.column("price_es_eur_mwh"))
        a2 = wap.start(t_hourly, "swap-es-pt")
        wap.write(t_hourly, a2, swapped, overwrite_filter=AlwaysTrue())
        with pytest.raises(QualityGateError, match="cross_source_omie_redata_spot_reconciliation"):
            audit_branch_and_quarantine(
                catalog,
                t_hourly,
                a2,
                table_name="silver.mibel_prices_hourly",
                con=con,
                table_ref=t_hourly.metadata_location,
                contracts_dir=contracts_dir,
                run_id="swap-es-pt",
            )
        t_hourly.refresh()
        assert t_hourly.metadata.current_snapshot_id == snap_hourly
        assert wap.audit_branches(t_hourly) == []

        # 3. Canarias mixed into peninsular & marked verified -> blocked by BOTH rules!
        sc_i = demand_data.schema.get_field_index("system_scope")
        vr_i = demand_data.schema.get_field_index("ts_utc_verified")
        mixed = demand_data.set_column(
            sc_i,
            demand_data.schema.field(sc_i),
            pa.array(["peninsular"] * demand_data.num_rows, demand_data.schema.field(sc_i).type),
        ).set_column(
            vr_i,
            demand_data.schema.field(vr_i),
            pa.array([True] * demand_data.num_rows, demand_data.schema.field(vr_i).type),
        )
        a3 = wap.start(t_demand, "mix-canarias")
        wap.write(t_demand, a3, mixed, overwrite_filter=AlwaysTrue())
        with pytest.raises(QualityGateError) as caught:
            audit_branch_and_quarantine(
                catalog,
                t_demand,
                a3,
                table_name="silver.demand_system_hourly",
                con=con,
                table_ref=t_demand.metadata_location,
                contracts_dir=contracts_dir,
                run_id="mix-canarias",
            )
        triggered = {v.rule_name for v in caught.value.report.violations}
        assert any("peninsular_non_peninsular_isolation" in r for r in triggered)
        assert any("canarias_unverified_timestamp_flag" in r for r in triggered)
        t_demand.refresh()
        assert t_demand.metadata.current_snapshot_id == snap_demand
        assert wap.audit_branches(t_demand) == []

        # 4. DST missing hour (22h on 23h day 2025-03-30) in gold.mibel_hourly_market -> blocked!
        a4 = wap.start(t_gold, "dst-missing")
        wap.write(t_gold, a4, gold_data.slice(1), overwrite_filter=AlwaysTrue())
        with pytest.raises(QualityGateError, match="dst_hourly_continuity"):
            audit_branch_and_quarantine(
                catalog,
                t_gold,
                a4,
                table_name="gold.mibel_hourly_market",
                con=con,
                table_ref=t_gold.metadata_location,
                contracts_dir=contracts_dir,
                run_id="dst-missing",
            )
        t_gold.refresh()
        assert t_gold.metadata.current_snapshot_id == snap_gold
        assert wap.audit_branches(t_gold) == []

        # All 4 chaos rejections landed in quarantine.rejected_records!
        q_table = catalog.load_table(registry.QUARANTINE_REJECTED_RECORDS.identifier)
        q_rows = q_table.scan().to_arrow().to_pylist()
        assert {r["run_id"] for r in q_rows} == {
            "dup-omie",
            "swap-es-pt",
            "mix-canarias",
            "dst-missing",
        }
        assert any(r["payload_json"] is not None for r in q_rows)

        # Also exercise bronze.redata_observations Soda contract directly via run_soda_contract.
        t_redata = ensure_table(catalog, registry.REDATA_OBSERVATIONS)
        assert t_redata.created
        redata_tbl = catalog.load_table(registry.REDATA_OBSERVATIONS.identifier)
        redata_rows = pa.table(
            {
                "dataset": ["demand_hourly"],
                "widget": ["demanda/evolucion"],
                "electric_system": ["peninsular"],
                "series_id": ["1293"],
                "series_type": ["Demanda"],
                "series_title": ["Demanda"],
                "granularity": ["hour"],
                "ts_utc": pa.array(
                    [datetime(2025, 4, 28, 7, tzinfo=UTC)], pa.timestamp("us", "UTC")
                ),
                "ts_local": ["2025-04-28T09:00:00+02:00"],
                "value": pa.array([25000.0], pa.float64()),
                "percentage": pa.array([None], pa.float64()),
                "series_last_update": pa.array([NOW], pa.timestamp("us", "UTC")),
                "fetched_at": pa.array([NOW], pa.timestamp("us", "UTC")),
                "run_id": ["seed"],
                "landing_path": ["landing/redata/x.json"],
                "payload_sha256": ["b" * 64],
            },
            schema=registry.REDATA_OBSERVATIONS.schema.as_arrow(),
        )
        redata_tbl.append(redata_rows)
        redata_report = run_soda_contract(
            con,
            contracts_dir / CONTRACT_MAP["bronze.redata_observations"],
            table_name="bronze.redata_observations",
            view_name="bronze_redata_observations",
            relation_sql=f"SELECT * FROM iceberg_scan('{redata_tbl.metadata_location}')",
        )
        assert redata_report.passed
    finally:
        con.close()


def test_check_tables_metadata_only_and_full_mode(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    ensure_table(catalog, registry.OMIE_DAY_AHEAD_PRICES)
    table = catalog.load_table(registry.OMIE_DAY_AHEAD_PRICES.identifier)
    table.append(_omie_rows([date(2025, 4, 28)]))
    con = _duck_con()
    monkeypatch.setattr(duck, "table_name", lambda _name: table.metadata_location)
    try:
        meta_reports = check_tables(
            Settings(),
            tables=("bronze.omie_day_ahead_prices",),
            metadata_only=True,
            con=con,
            catalog=catalog,
        )
        assert len(meta_reports) == 1
        assert meta_reports[0].passed
        assert meta_reports[0].contract_path is None
        assert meta_reports[0].metadata_result is not None
        assert meta_reports[0].metadata_result.summary.data_bytes_scanned == 0

        full_reports = check_tables(
            Settings(),
            tables=("bronze.omie_day_ahead_prices",),
            metadata_only=False,
            con=con,
            catalog=catalog,
        )
        assert full_reports[0].passed
        assert full_reports[0].checks_passed >= 10
    finally:
        con.close()
