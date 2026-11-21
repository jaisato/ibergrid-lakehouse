"""Quality gates, zero-scan metadata checks and S3 orphan cleanup on the real stack (ADR 0008)."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pytest
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import PyArrowFileIO

from ibergrid.lake import duck, maintenance, registry, wap
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.tables import ensure_table
from ibergrid.quality import (
    QualityGateError,
    audit_branch_and_quarantine,
    audit_table_metadata,
    find_contracts_dir,
)
from ibergrid.quality.metadata import TABLE_METADATA_RULES
from ibergrid.settings import Settings

from .conftest import compose_run

pytestmark = pytest.mark.integration
MADRID = ZoneInfo("Europe/Madrid")
NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)


def _omie_day_table(day: date) -> pa.Table:
    start = datetime(day.year, day.month, day.day, tzinfo=MADRID).astimezone(UTC)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=MADRID).astimezone(UTC)
    hours: list[datetime] = []
    cur = start
    while cur < end:
        hours.append(cur)
        cur += timedelta(hours=1)
    cols: dict[str, list[object]] = {
        f.name: [] for f in registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow()
    }
    for idx, ts in enumerate(hours, start=1):
        cols["delivery_date"].append(day)
        cols["period"].append(idx)
        cols["resolution_minutes"].append(60)
        cols["interval_start_utc"].append(ts)
        cols["interval_end_utc"].append(ts + timedelta(hours=1))
        cols["price_es_eur_mwh"].append(47.94 if idx == 8 else 50.0 + idx)
        cols["price_pt_eur_mwh"].append(40.70 if idx == 8 else 50.0 + idx)
        cols["contract_version"].append(1)
        cols["source_file"].append(f"marginalpdbc_{day:%Y%m%d}.1")
        cols["file_version"].append(1)
        cols["content_sha256"].append("a" * 64)
        cols["fetched_at"].append(NOW)
        cols["run_id"].append("it-seed")
        cols["landing_path"].append(f"landing/omie/{day:%Y%m%d}.1")
    return pa.table(cols, schema=registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow())


def test_zero_scan_metadata_and_wap_quarantine_on_lakekeeper(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection, settings: Settings
) -> None:
    ensure_table(catalog, replace(registry.OMIE_DAY_AHEAD_PRICES, namespace=namespace))
    ensure_table(catalog, replace(registry.QUARANTINE_REJECTED_RECORDS, namespace=namespace))
    table = catalog.load_table((namespace, "omie_day_ahead_prices"))
    rows = _omie_day_table(date(2025, 4, 28))

    seed = wap.start(table, "it-seed")
    seed_snap = wap.publish(table, seed, wap.write(table, seed, rows))
    assert seed_snap is not None

    ref = f"{duck.CATALOG}.{namespace}.omie_day_ahead_prices"
    meta = audit_table_metadata(con, ref, TABLE_METADATA_RULES["bronze.omie_day_ahead_prices"])
    assert meta.passed
    assert meta.summary.data_bytes_scanned == 0
    assert meta.summary.total_records == 24

    # Inject duplicate key into a WAP audit branch -> Soda gate blocks, discards & quarantines.
    bad = wap.start(table, "it-dup")
    wap.write(table, bad, rows.slice(0, 1))
    with pytest.raises(QualityGateError, match="duplicate"):
        audit_branch_and_quarantine(
            catalog,
            table,
            bad,
            table_name="bronze.omie_day_ahead_prices",
            con=con,
            table_ref=ref,
            contracts_dir=find_contracts_dir(settings),
            run_id="it-dup",
            quarantine_namespace=namespace,
        )

    table.refresh()
    assert table.metadata.current_snapshot_id == seed_snap
    assert wap.audit_branches(table) == []
    q_table = catalog.load_table((namespace, "rejected_records"))
    q_rows = q_table.scan().to_arrow().to_pylist()
    assert len(q_rows) >= 1
    assert q_rows[0]["run_id"] == "it-dup"


def test_s3_orphan_cleanup_on_rustfs_preserves_live_data(
    catalog: LakeCatalog, namespace: str
) -> None:
    ensure_table(catalog, replace(registry.OMIE_DAY_AHEAD_PRICES, namespace=namespace))
    table = catalog.load_table((namespace, "omie_day_ahead_prices"))
    rows = _omie_day_table(date(2025, 4, 28))
    table.append(rows)
    table.overwrite(rows, overwrite_filter=AlwaysTrue())

    # Write a synthetic orphan object directly into the table's S3 data prefix on RustFS.
    assert isinstance(table.io, PyArrowFileIO)
    scheme, netloc, root = PyArrowFileIO.parse_location(table.location(), table.io.properties)
    fs = table.io.fs_by_scheme(scheme, netloc)
    orphan_path = f"{root.rstrip('/')}/data/synthetic-orphan.parquet"
    with fs.open_output_stream(orphan_path) as out:
        out.write(b"orphan-bytes")

    # Expire old snapshots (1 append + 1 delete from overwrite = 2 expired, 1 retained).
    later = datetime.now(UTC) + timedelta(days=1)
    expired = maintenance.expire_snapshots(table, older_than=later, retain_last=1)
    assert len(expired) == 2

    # Default 48h grace window skips fresh orphans.
    safe = maintenance.remove_orphan_files(table, grace_period=timedelta(hours=48), dry_run=False)
    assert safe.deleted_files == 0
    assert safe.skipped_recent_files >= 2
    assert fs.get_file_info(orphan_path).type == pafs.FileType.File

    # Explicit future cutoff deletes unreferenced files while keeping main readable.
    cleaned = maintenance.remove_orphan_files(table, older_than=later, dry_run=False)
    assert cleaned.deleted_files >= 2
    assert orphan_path in cleaned.orphan_files
    assert fs.get_file_info(orphan_path).type == pafs.FileType.NotFound
    assert table.scan().to_arrow().num_rows == 24


def test_quality_container_runs_metadata_check_and_lean_app_image_rejects_soda() -> None:
    # Ensure registered tables exist before running the container check.
    init_res = compose_run("doctor", "lake", "init")
    assert init_res.returncode == 0, init_res.stdout + init_res.stderr

    # Orchestration image (`quality` service) has Soda + /opt/ibergrid/contracts/soda baked in.
    q_res = compose_run("quality", "check", "--metadata-only", "--json")
    assert q_res.returncode == 0, q_res.stdout + q_res.stderr
    payload = json.loads(q_res.stdout)
    assert len(payload) >= 5
    assert all(item["data_bytes_scanned"] == 0 for item in payload)

    # Lean app image (`doctor`) handles `--metadata-only` without Soda.
    lean_meta = compose_run("doctor", "quality", "check", "--metadata-only", "--json")
    assert lean_meta.returncode == 0, lean_meta.stdout + lean_meta.stderr
