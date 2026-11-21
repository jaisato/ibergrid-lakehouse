"""Integration tests for the live FastAPI + DuckDB API, static portal and R2/S3 export."""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

from ibergrid.portal.export import export_portal_bundle
from ibergrid.settings import Settings
from ibergrid.storage import s3_filesystem

from .conftest import compose_run

pytestmark = pytest.mark.integration

API_URL = os.environ.get("IBERGRID_TEST_API_URL", "http://127.0.0.1:28000")


def test_live_api_endpoints_headers_and_portal() -> None:
    with httpx.Client(base_url=API_URL, timeout=30.0) as client:
        health = client.get("/health")
        assert health.status_code == 200, health.text
        h_json = health.json()
        assert h_json["status"] == "ok"
        assert h_json["catalog_reachable"] is True

        # Root redirects to mounted static analytical portal.
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 307
        assert root.headers["location"] == "/portal/"

        portal_html = client.get("/portal/")
        assert portal_html.status_code == 200
        assert "IberGrid Lakehouse" in portal_html.text
        assert 'integrity="sha384-' in portal_html.text

        portal_summary = client.get("/portal/data/summary.json")
        assert portal_summary.status_code == 200
        assert "annual_mibel" in portal_summary.json()

        # Zero-scan metadata endpoint always scans 0 Parquet data bytes.
        meta = client.get("/v1/metadata/tables")
        assert meta.status_code == 200, meta.text
        meta_json = meta.json()
        assert meta_json["total_data_bytes_scanned"] == 0
        assert "X-RateLimit-Limit" in meta.headers
        assert "X-RateLimit-Remaining" in meta.headers
        assert meta.headers["X-Content-Type-Options"] == "nosniff"
        assert meta.headers["X-Frame-Options"] == "DENY"

        # All analytical endpoints answer 200 with attribution envelopes (empty or populated).
        for path in (
            "/v1/market/hourly?start=2025-04-27&end=2025-04-29",
            "/v1/market/daily?start=2025-04-27&end=2025-04-29",
            "/v1/systems/daily?system=canarias&start=2025-04-27&end=2025-04-29",
            "/v1/windows/optimal?month=5&day_type=weekday",
            "/v1/demand/realtime?hours=24",
        ):
            resp = client.get(path)
            assert resp.status_code == 200, f"{path}: {resp.text}"
            body = resp.json()
            assert "source_attribution" in body
            assert "non_commercial_notice" in body
            assert isinstance(body["items"], list)

        # Canarias items, when present, must keep ts_utc_verified=False.
        canarias = client.get(
            "/v1/systems/daily?system=canarias&start=2025-04-27&end=2025-04-29"
        ).json()
        for row in canarias["items"]:
            assert row["ts_utc_verified"] is False
            assert row["system_scope"] == "non_peninsular"

        # Inverted date range is rejected with 422.
        assert client.get("/v1/market/hourly?start=2025-05-01&end=2025-04-01").status_code == 422


def test_lean_app_image_starts_without_fastapi() -> None:
    """The lean `ibergrid-app` image must remain importable without the optional `api` group."""
    ver = compose_run("doctor", "version")
    assert ver.returncode == 0, ver.stderr
    assert ver.stdout.startswith("ibergrid ")

    portal_help = compose_run("doctor", "portal", "export", "--help")
    assert portal_help.returncode == 0, portal_help.stderr


def test_portal_export_round_trips_to_s3(settings: Settings) -> None:
    """Uploading the static portal + data bundle to S3/R2 works and cleans up cleanly."""
    prefix = f"it-portal-{uuid.uuid4().hex[:10]}"
    s3 = s3_filesystem(settings.storage, timeout=30.0)
    try:
        report = export_portal_bundle(
            settings,
            s3_prefix=prefix,
            include_assets=True,
            filesystem=s3,
        )
        assert len(report.s3_uris) >= 10
        assert any(uri.endswith(f"/{prefix}/index.html") for uri in report.s3_uris)
        assert any(uri.endswith(f"/{prefix}/data/summary.json") for uri in report.s3_uris)
    finally:
        # Remove the throwaway test prefix so RustFS/R2 stays clean.
        s3.delete_dir(f"{settings.storage.bucket}/{prefix}")
