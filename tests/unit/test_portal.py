"""Unit tests for static portal bundle exporter (local disk & S3/R2, ADR 0010)."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import fs
from pydantic import HttpUrl
from pyiceberg.catalog.sql import SqlCatalog

from ibergrid.lake import duck, registry
from ibergrid.lake.tables import ensure_table
from ibergrid.portal.export import (
    export_portal_bundle,
    find_benchmarks_json,
    find_portal_assets_dir,
)
from ibergrid.settings import ApiSettings, CatalogSettings, Settings

from .conftest import FakeIcebergRest, LocalServer, Responder
from .test_api import _seed_medallion

Serve = Callable[[Responder], LocalServer]


def test_export_portal_bundle_writes_local_and_s3_with_assets(
    serve: Serve, catalog: SqlCatalog, tmp_path: Path, credentials: None
) -> None:
    _seed_medallion(catalog)
    ensure_table(catalog, registry.QUARANTINE_REJECTED_RECORDS)
    catalog.load_table(registry.QUARANTINE_REJECTED_RECORDS.identifier).append(
        pa.table(
            {
                "rejected_at_utc": pa.array(
                    [datetime(2026, 11, 21, 12, 0, tzinfo=UTC)], pa.timestamp("us", "UTC")
                ),
                "run_id": ["chaos-1"],
                "source_table": ["gold.mibel_hourly_market"],
                "rule_name": ["soda.price_bounds"],
                "severity": ["error"],
                "reason": ["price exceeded 4000 EUR/MWh"],
                "record_key": ["k1"],
                "payload_json": ['{"price_es_eur_mwh": 9999}'],
                "audit_branch": ["audit-chaos-1"],
                "snapshot_id": pa.array([12345], pa.int64()),
            },
            schema=registry.QUARANTINE_REJECTED_RECORDS.schema.as_arrow(),
        )
    )

    rest_url = serve(FakeIcebergRest(catalog)).url
    bench_file = tmp_path / "latest.json"
    bench_file.write_text(
        json.dumps({"generated_at_utc": "2026-11-21T20:00:00Z"}), encoding="utf-8"
    )

    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    (assets_dir / "index.html").write_text("<html>portal</html>", encoding="utf-8")
    (assets_dir / "styles.css").write_text("body { color: #fff; }", encoding="utf-8")
    (assets_dir / "app.js").write_text("console.log('ok');", encoding="utf-8")

    settings = Settings(
        catalog=CatalogSettings(url=HttpUrl(rest_url)),
        api=ApiSettings(portal_dir=str(assets_dir), benchmarks_path=str(bench_file)),
    )
    out_dir = tmp_path / "out-data"
    s3_root = tmp_path / "s3-mock"
    s3_root.mkdir()
    local_s3 = fs.SubTreeFileSystem(str(s3_root), fs.LocalFileSystem())

    report = export_portal_bundle(
        settings,
        output_dir=out_dir,
        s3_prefix="portal/v1/",
        include_assets=True,
        connection_factory=duck.connect,
        filesystem=local_s3,
    )

    assert report.daily_market_rows == 1
    assert report.optimal_window_rows == 2
    assert report.systems_monthly_rows == 2
    assert report.blackout_hourly_rows == 2
    assert len(report.local_files) == 8
    # 8 data files + 3 static portal assets (index.html, styles.css, app.js).
    assert len(report.s3_uris) == 11

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["coverage_start"] == "2025-04-28"
    assert summary["annual_mibel"][0]["year"] == 2025
    assert summary["annual_mibel"][0]["zero_or_negative_es_hours"] == 8
    assert summary["systems_overview"][0]["electric_system"] == "peninsular"
    assert summary["systems_overview"][1]["electric_system"] == "canarias"
    assert summary["systems_overview"][1]["ts_utc_verified"] is False
    assert summary["quarantine_summary"]["total_rejected_records"] == 1
    assert summary["metadata_inventory"]["total_data_bytes_scanned"] == 0

    with (out_dir / "daily_market.csv").open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["delivery_date"] == "2025-04-28"

    # Verify files landed under the S3 bucket & prefix.
    bucket_dir = s3_root / settings.storage.bucket / "portal" / "v1"
    assert (bucket_dir / "index.html").read_text(encoding="utf-8") == "<html>portal</html>"
    assert (bucket_dir / "data" / "summary.json").is_file()
    assert (bucket_dir / "data" / "benchmarks.json").is_file()

    # Export to S3 only without static assets.
    report_no_assets = export_portal_bundle(
        settings,
        s3_prefix="portal-data-only",
        include_assets=False,
        connection_factory=duck.connect,
        filesystem=local_s3,
    )
    assert report_no_assets.local_files == ()
    assert len(report_no_assets.s3_uris) == 8


def test_export_portal_bundle_validation_and_discovery(
    serve: Serve, catalog: SqlCatalog, tmp_path: Path, credentials: None
) -> None:
    rest_url = serve(FakeIcebergRest(catalog)).url
    settings = Settings(catalog=CatalogSettings(url=HttpUrl(rest_url)))

    with pytest.raises(ValueError, match="at least one"):
        export_portal_bundle(settings)
    with pytest.raises(ValueError, match="must not be empty"):
        export_portal_bundle(settings, s3_prefix="  /  ")
    with pytest.raises(ValueError, match="\\.\\."):
        export_portal_bundle(settings, s3_prefix="portal/../escape")

    # Empty catalog without quarantine table still succeeds with empty CSV/JSON files.
    out_empty = tmp_path / "empty"
    report = export_portal_bundle(
        settings,
        output_dir=out_empty,
        benchmarks_path=tmp_path / "missing.json",
        connection_factory=duck.connect,
    )
    assert report.daily_market_rows == 0
    assert (out_empty / "daily_market.csv").read_text(encoding="utf-8") == ""
    summary = json.loads((out_empty / "summary.json").read_text(encoding="utf-8"))
    assert summary["quarantine_summary"]["total_rejected_records"] == 0

    # Discovery helpers (configured valid/invalid + repository auto-discovery).
    assert find_benchmarks_json(Settings()) is not None
    assert (
        find_benchmarks_json(Settings(api=ApiSettings(benchmarks_path=str(tmp_path / "nope"))))
        is None
    )
    assert find_portal_assets_dir(Settings()) is not None
    assert (
        find_portal_assets_dir(Settings(api=ApiSettings(portal_dir=str(tmp_path / "nope")))) is None
    )
