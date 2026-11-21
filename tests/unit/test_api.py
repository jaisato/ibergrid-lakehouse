"""Unit tests for the FastAPI + DuckDB real-time serving layer (ADR 0010)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from pydantic import HttpUrl
from pyiceberg.catalog.sql import SqlCatalog

from ibergrid.api.app import create_app, find_portal_dir
from ibergrid.api.rate_limit import SlidingWindowRateLimiter
from ibergrid.api.schemas import ATTRIBUTION_MIBEL_AND_REE, ATTRIBUTION_REE_ONLY
from ibergrid.api.store import MAX_CACHE_ENTRIES, LakeQueryStore, _opt_float, _to_utc_datetime
from ibergrid.lake import duck, registry
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import ApiSettings, CatalogSettings, Settings

from .conftest import FakeIcebergRest, FakeLakekeeper, LocalServer, Responder

Serve = Callable[[Responder], LocalServer]


def _seed_medallion(catalog: SqlCatalog) -> None:
    """Create and populate the Silver/Gold tables queried by the API."""
    for spec in (
        registry.GOLD_MIBEL_HOURLY_MARKET,
        registry.GOLD_MIBEL_DAILY_SUMMARY,
        registry.GOLD_SYSTEM_DEMAND_DAILY,
        registry.GOLD_OPTIMAL_CONSUMPTION_WINDOWS,
        registry.SILVER_DEMAND_NATIONAL_5MIN,
    ):
        ensure_table(catalog, spec)

    now = datetime(2026, 11, 21, 18, 0, tzinfo=UTC)

    catalog.load_table(registry.GOLD_MIBEL_HOURLY_MARKET.identifier).append(
        pa.table(
            {
                "hour_utc": pa.array(
                    [
                        datetime(2025, 4, 28, 11, 0, tzinfo=UTC),
                        datetime(2025, 4, 28, 12, 0, tzinfo=UTC),
                    ],
                    pa.timestamp("us", "UTC"),
                ),
                "delivery_date": pa.array([date(2025, 4, 28), date(2025, 4, 28)], pa.date32()),
                "madrid_hour": pa.array([13, 14], pa.int32()),
                "day_type": ["weekday", "weekday"],
                "price_es_eur_mwh": [-1.5, 0.0],
                "price_pt_eur_mwh": [-1.5, 12.0],
                "spread_es_minus_pt_eur_mwh": [0.0, -12.0],
                "is_decoupled": [False, True],
                "is_zero_or_negative_es": [True, True],
                "pvpc_eur_mwh": [45.0, None],
                "peninsular_demand_mw": [25000.0, 12000.0],
                "demand_ramp_1h_mw": [-100.0, -13000.0],
                "price_ramp_1h_eur_mwh": [-2.0, 1.5],
                "intraday_price_rank": pa.array([1, 2], pa.int32()),
                "is_evening_ramp_window": [False, False],
                "peninsular_renewable_share_pct": [65.3, 65.3],
                "peninsular_solar_share_pct": [22.1, 22.1],
                "peninsular_wind_share_pct": [16.2, 16.2],
                "source_attribution": [ATTRIBUTION_MIBEL_AND_REE, ATTRIBUTION_MIBEL_AND_REE],
                "updated_at_utc": pa.array([now, now], pa.timestamp("us", "UTC")),
            },
            schema=registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow(),
        )
    )

    catalog.load_table(registry.GOLD_MIBEL_DAILY_SUMMARY.identifier).append(
        pa.table(
            {
                "delivery_date": pa.array([date(2025, 4, 28)], pa.date32()),
                "hours_in_day": pa.array([24], pa.int32()),
                "price_es_mean_eur_mwh": [18.5],
                "price_es_min_eur_mwh": [-3.0],
                "price_es_max_eur_mwh": [80.8],
                "price_es_peak_eur_mwh": [2.78],
                "price_es_offpeak_eur_mwh": [34.22],
                "price_pt_mean_eur_mwh": [18.26],
                "spread_mean_eur_mwh": [0.24],
                "spread_abs_mean_eur_mwh": [0.37],
                "decoupled_hours": pa.array([3], pa.int32()),
                "zero_or_negative_es_hours": pa.array([8], pa.int32()),
                "peninsular_demand_mwh": [364239.0],
                "peninsular_peak_demand_mw": [27356.2],
                "evening_ramp_max_mw": [2279.7],
                "non_peninsular_demand_mwh": [39058.5],
                "peninsular_renewable_share_pct": [65.31],
                "peninsular_solar_share_pct": [22.06],
                "peninsular_wind_share_pct": [16.16],
                "source_attribution": [ATTRIBUTION_MIBEL_AND_REE],
                "updated_at_utc": pa.array([now], pa.timestamp("us", "UTC")),
            },
            schema=registry.GOLD_MIBEL_DAILY_SUMMARY.schema.as_arrow(),
        )
    )

    catalog.load_table(registry.GOLD_SYSTEM_DEMAND_DAILY.identifier).append(
        pa.table(
            {
                "demand_date": pa.array([date(2025, 4, 28), date(2025, 4, 28)], pa.date32()),
                "electric_system": ["peninsular", "canarias"],
                "system_scope": ["peninsular", "non_peninsular"],
                "ts_utc_verified": [True, False],
                "hours_in_day": pa.array([24, 24], pa.int32()),
                "demand_total_mwh": [364239.0, 24500.0],
                "demand_mean_mw": [15176.6, 1020.8],
                "demand_min_mw": [8000.0, 750.0],
                "demand_peak_mw": [27356.2, 1450.0],
                "generation_total_mwh": [380000.0, 24600.0],
                "renewable_generation_mwh": [248000.0, 5000.0],
                "renewable_share_pct": [65.3, 20.3],
                "solar_share_pct": [22.1, 4.8],
                "wind_share_pct": [16.2, 15.2],
                "source_attribution": [ATTRIBUTION_REE_ONLY, ATTRIBUTION_REE_ONLY],
                "updated_at_utc": pa.array([now, now], pa.timestamp("us", "UTC")),
            },
            schema=registry.GOLD_SYSTEM_DEMAND_DAILY.schema.as_arrow(),
        )
    )

    catalog.load_table(registry.GOLD_OPTIMAL_CONSUMPTION_WINDOWS.identifier).append(
        pa.table(
            {
                "calendar_month": pa.array([5, 5], pa.int32()),
                "day_type": ["weekday", "weekday"],
                "madrid_hour": pa.array([14, 21], pa.int32()),
                "sample_hours": pa.array([88, 88], pa.int32()),
                "price_es_mean_eur_mwh": [12.81, 94.91],
                "price_es_p10_eur_mwh": [-1.0, 60.0],
                "price_es_p50_eur_mwh": [5.0, 92.0],
                "price_es_p90_eur_mwh": [40.0, 130.0],
                "zero_or_negative_es_pct": [48.3, 0.0],
                "decoupled_pct": [3.5, 1.2],
                "peninsular_demand_mean_mw": [26500.0, 31200.0],
                "peninsular_renewable_share_mean_pct": [68.0, 54.0],
                "window_rank": pa.array([1, 24], pa.int32()),
                "is_optimal_4h_window": [True, False],
                "source_attribution": [ATTRIBUTION_MIBEL_AND_REE, ATTRIBUTION_MIBEL_AND_REE],
                "updated_at_utc": pa.array([now, now], pa.timestamp("us", "UTC")),
            },
            schema=registry.GOLD_OPTIMAL_CONSUMPTION_WINDOWS.schema.as_arrow(),
        )
    )

    catalog.load_table(registry.SILVER_DEMAND_NATIONAL_5MIN.identifier).append(
        pa.table(
            {
                "ts_utc": pa.array(
                    [
                        datetime(2026, 11, 20, 23, 55, tzinfo=UTC),
                        datetime(2026, 11, 21, 0, 0, tzinfo=UTC),
                    ],
                    pa.timestamp("us", "UTC"),
                ),
                "madrid_date": pa.array([date(2026, 11, 21), date(2026, 11, 21)], pa.date32()),
                "demand_real_mw": [24100.0, 24250.0],
                "demand_forecast_mw": [24050.0, 24200.0],
                "demand_scheduled_mw": [23900.0, 24000.0],
                "demand_total_scheduled_mw": [24000.0, 24150.0],
                "source_lane": ["batch", "stream"],
                "series_last_update_utc": pa.array([now, now], pa.timestamp("us", "UTC")),
            },
            schema=registry.SILVER_DEMAND_NATIONAL_5MIN.schema.as_arrow(),
        )
    )


def test_rate_limiter_enforces_sliding_window_and_eviction() -> None:
    now = [100.0]
    limiter = SlidingWindowRateLimiter(
        limit=2, window_seconds=10.0, clock=lambda: now[0], max_clients=2
    )
    assert limiter.limit == 2

    d1 = limiter.check("1.1.1.1")
    assert (d1.allowed, d1.remaining, d1.retry_after_seconds) == (True, 1, 0)
    d2 = limiter.check("1.1.1.1")
    assert (d2.allowed, d2.remaining, d2.retry_after_seconds) == (True, 0, 0)
    d3 = limiter.check("1.1.1.1")
    assert d3.allowed is False
    assert d3.remaining == 0
    assert d3.retry_after_seconds == 10

    # Active client eviction when max_clients is reached.
    assert limiter.check("2.2.2.2").allowed is True
    assert limiter.check("3.3.3.3").allowed is True

    # Stale client eviction after the window advances.
    now[0] = 115.0
    assert limiter.check("4.4.4.4").allowed is True
    assert limiter.check("1.1.1.1").allowed is True

    with pytest.raises(ValueError, match="limit"):
        SlidingWindowRateLimiter(limit=0)
    with pytest.raises(ValueError, match="window_seconds"):
        SlidingWindowRateLimiter(limit=1, window_seconds=0)
    with pytest.raises(ValueError, match="max_clients"):
        SlidingWindowRateLimiter(limit=1, max_clients=0)


def test_store_helpers_and_portal_dir_discovery(tmp_path: Path, credentials: None) -> None:
    naive = datetime(2025, 4, 28, 12, 0)
    assert _to_utc_datetime(naive).tzinfo is UTC
    assert _opt_float(None) is None
    assert _opt_float(3) == 3.0
    with pytest.raises(TypeError, match="expected datetime"):
        _to_utc_datetime("2025-04-28")

    valid_portal = tmp_path / "portal"
    valid_portal.mkdir()
    (valid_portal / "index.html").write_text("<html>ok</html>")
    assert find_portal_dir(Settings(api=ApiSettings(portal_dir=str(valid_portal)))) == valid_portal
    assert find_portal_dir(Settings(api=ApiSettings(portal_dir=str(tmp_path / "missing")))) is None
    # Auto-discovery finds the repository's portal/ directory.
    discovered = find_portal_dir(Settings())
    assert discovered is not None
    assert (discovered / "index.html").is_file()


def test_api_endpoints_populated_and_guarded(
    serve: Serve, catalog: SqlCatalog, tmp_path: Path, credentials: None
) -> None:
    _seed_medallion(catalog)
    rest_url = serve(FakeIcebergRest(catalog)).url
    portal_dir = tmp_path / "portal"
    portal_dir.mkdir()
    (portal_dir / "index.html").write_text("<!doctype html><title>IberGrid</title>")

    now = [1000.0]
    settings = Settings(
        catalog=CatalogSettings(url=HttpUrl(rest_url)),
        api=ApiSettings(
            rate_limit_rpm=50,
            cors_origins="https://portal.example.com",
            cache_ttl_seconds=30.0,
            max_hourly_range_days=7,
            max_daily_range_days=30,
            portal_dir=str(portal_dir),
        ),
    )
    store = LakeQueryStore(
        settings,
        connection_factory=duck.connect,
        clock=lambda: now[0],
        catalog_probe=lambda _s: True,
    )
    app = create_app(settings, store=store, clock=lambda: now[0])

    with TestClient(app) as client:
        # Root redirects to /portal/ and static HTML is served.
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 307
        assert root.headers["location"] == "/portal/"
        portal = client.get("/portal/")
        assert portal.status_code == 200
        assert "IberGrid" in portal.text

        # Health endpoint.
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"
        assert health.json()["catalog_reachable"] is True
        assert health.json()["tables_registered"] == len(registry.TABLES)

        # Hourly market endpoint + security + rate-limit + CORS headers.
        hourly = client.get(
            "/v1/market/hourly",
            params={"start": "2025-04-28", "end": "2025-04-28"},
            headers={"Origin": "https://portal.example.com"},
        )
        assert hourly.status_code == 200
        body = hourly.json()
        assert body["source_attribution"] == ATTRIBUTION_MIBEL_AND_REE
        assert body["count"] == 2
        assert body["items"][1]["is_decoupled"] is True
        assert body["items"][0]["hour_utc"].endswith("Z")
        assert hourly.headers["x-ratelimit-limit"] == "50"
        assert hourly.headers["x-content-type-options"] == "nosniff"
        assert hourly.headers["x-frame-options"] == "DENY"
        assert hourly.headers["access-control-allow-origin"] == "https://portal.example.com"

        # Date range guards return 422.
        assert client.get("/v1/market/hourly?start=2025-04-29&end=2025-04-28").status_code == 422
        assert client.get("/v1/market/hourly?start=2025-04-01&end=2025-04-20").status_code == 422
        assert client.get("/v1/market/daily?start=2025-01-01&end=2025-06-01").status_code == 422

        # Daily market endpoint.
        daily = client.get("/v1/market/daily?start=2025-04-27&end=2025-04-29")
        assert daily.status_code == 200
        assert daily.json()["count"] == 1
        assert daily.json()["items"][0]["peninsular_demand_mwh"] == 364239.0

        # Systems endpoint preserves Canarias ts_utc_verified=False and rejects unknown systems.
        sys_all = client.get("/v1/systems/daily?start=2025-04-28&end=2025-04-28")
        assert sys_all.status_code == 200
        assert sys_all.json()["count"] == 2
        can = client.get("/v1/systems/daily?system=canarias&start=2025-04-28&end=2025-04-28")
        assert can.status_code == 200
        assert can.json()["items"][0]["ts_utc_verified"] is False
        assert can.json()["items"][0]["system_scope"] == "non_peninsular"
        assert client.get("/v1/systems/daily?system=mallorca").status_code == 422

        # Optimal consumption windows endpoint.
        win = client.get("/v1/windows/optimal?month=5&day_type=weekday")
        assert win.status_code == 200
        assert win.json()["count"] == 2
        assert win.json()["items"][0]["is_optimal_4h_window"] is True
        assert client.get("/v1/windows/optimal?month=13").status_code == 422

        # Realtime 5-min demand provenance (batch + stream).
        rt = client.get("/v1/demand/realtime?hours=24")
        assert rt.status_code == 200
        rt_json = rt.json()
        assert (rt_json["batch_points"], rt_json["stream_points"], rt_json["count"]) == (1, 1, 2)
        assert client.get("/v1/demand/realtime?hours=0").status_code == 422

        # Zero-scan metadata inventory endpoint.
        meta = client.get("/v1/metadata/tables")
        assert meta.status_code == 200
        meta_json = meta.json()
        assert meta_json["total_data_bytes_scanned"] == 0
        assert meta_json["total_tables"] == 5
        assert meta_json["total_records"] == 9

        # Cache hit and TTL expiration reload.
        assert client.get("/v1/metadata/tables").status_code == 200
        now[0] += 60.0
        assert client.get("/v1/metadata/tables").status_code == 200

    store.close()


def test_api_empty_tables_and_uninitialized_catalog(
    serve: Serve, catalog: SqlCatalog, credentials: None
) -> None:
    """Endpoints answer 200 with empty lists on fresh/unpopulated catalogs (including CI)."""
    rest_url = serve(FakeIcebergRest(catalog)).url
    settings = Settings(
        catalog=CatalogSettings(url=HttpUrl(rest_url)),
        api=ApiSettings(cache_ttl_seconds=0.0, portal_dir="/nonexistent-portal-dir"),
    )
    store = LakeQueryStore(settings, connection_factory=duck.connect, catalog_probe=lambda _s: True)
    app = create_app(settings, store=store)

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        for path in (
            "/v1/market/hourly?start=2025-04-28&end=2025-04-28",
            "/v1/market/daily?start=2025-04-28&end=2025-04-28",
            "/v1/systems/daily?start=2025-04-28&end=2025-04-28",
            "/v1/windows/optimal",
            "/v1/demand/realtime?hours=24",
        ):
            resp = client.get(path)
            assert resp.status_code == 200, path
            assert resp.json()["count"] == 0
            assert resp.json()["items"] == []

        meta = client.get("/v1/metadata/tables")
        assert meta.status_code == 200
        assert meta.json()["total_tables"] == 0

    store.close()


def test_rate_limit_429_and_proxy_header_trust(
    serve: Serve, catalog: SqlCatalog, credentials: None
) -> None:
    rest_url = serve(FakeIcebergRest(catalog)).url
    settings = Settings(
        catalog=CatalogSettings(url=HttpUrl(rest_url)),
        api=ApiSettings(
            rate_limit_rpm=2,
            trust_proxy_headers=True,
            cors_origins="",
            portal_dir="/nonexistent",
        ),
    )
    store = LakeQueryStore(settings, connection_factory=duck.connect, catalog_probe=lambda _s: True)
    app = create_app(settings, store=store)

    with TestClient(app) as client:
        # Rightmost X-Forwarded-For hop identifies the real client behind Caddy.
        headers_a = {"X-Forwarded-For": "10.0.0.1, 203.0.113.10"}
        headers_b = {"X-Forwarded-For": "10.0.0.1, 203.0.113.20"}
        assert client.get("/v1/windows/optimal", headers=headers_a).status_code == 200
        assert client.get("/v1/windows/optimal", headers=headers_a).status_code == 200
        blocked = client.get("/v1/windows/optimal", headers=headers_a)
        assert blocked.status_code == 429
        assert int(blocked.headers["retry-after"]) >= 1
        assert blocked.headers["x-ratelimit-remaining"] == "0"
        assert blocked.headers["x-frame-options"] == "DENY"
        # Different client IP still has full budget.
        assert client.get("/v1/windows/optimal", headers=headers_b).status_code == 200

    store.close()


def test_health_degraded_and_duckdb_503_recovery(
    serve: Serve, credentials: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Default check_catalog probe path via FakeLakekeeper (unhealthy once, then healthy).
    fake_lk = FakeLakekeeper(unhealthy_answers=1)
    lk_url = serve(fake_lk).url
    settings = Settings(
        catalog=CatalogSettings(url=HttpUrl(lk_url)),
        api=ApiSettings(cache_ttl_seconds=60.0, portal_dir="/nonexistent"),
    )

    def _in_mem_connect(_s: Settings) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(":memory:")

    monkeypatch.setattr(duck, "connect", _in_mem_connect)
    app = create_app(settings)
    with TestClient(app) as client:
        first = client.get("/health")
        assert first.status_code == 503
        assert first.json()["status"] == "degraded"

        second = client.get("/health")
        assert second.status_code == 200
        assert second.json()["status"] == "ok"

    # Simulated DuckDB outage triggers HTTP 503 across every /v1/* endpoint and resets connection.
    def _boom_connect(_s: Settings) -> duckdb.DuckDBPyConnection:
        raise duckdb.IOException("simulated catalog outage")

    broken_store = LakeQueryStore(settings, connection_factory=_boom_connect)
    broken_app = create_app(settings, store=broken_store)
    with TestClient(broken_app) as client:
        assert client.get("/health").status_code == 503
        for path in (
            "/v1/market/hourly?start=2025-04-28&end=2025-04-28",
            "/v1/market/daily?start=2025-04-28&end=2025-04-28",
            "/v1/systems/daily?start=2025-04-28&end=2025-04-28",
            "/v1/windows/optimal",
            "/v1/demand/realtime?hours=24",
            "/v1/metadata/tables",
        ):
            resp = client.get(path)
            assert resp.status_code == 503, path

    # Verify cache clear when MAX_CACHE_ENTRIES is reached and non-404 DuckDB error re-raises.
    ok_store = LakeQueryStore(settings, connection_factory=_in_mem_connect)
    for idx in range(MAX_CACHE_ENTRIES + 2):
        assert ok_store._run_cached(("k", idx), lambda i=idx: i) == idx  # type: ignore[misc]
    assert len(ok_store._cache) <= MAX_CACHE_ENTRIES

    def _raise_io() -> Any:
        raise duckdb.IOException("transient socket reset")

    with pytest.raises(duckdb.IOException):
        ok_store._run_cached(("fail",), _raise_io)
    ok_store.close()
