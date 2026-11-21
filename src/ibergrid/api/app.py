"""FastAPI application for IberGrid Lakehouse real-time serving & portal host (ADR 0010)."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Annotated

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from ibergrid.api.rate_limit import SlidingWindowRateLimiter
from ibergrid.api.schemas import (
    DayType,
    ElectricSystem,
    HealthResponse,
    MarketDailyResponse,
    MarketHourlyResponse,
    MetadataTablesResponse,
    OptimalWindowsResponse,
    RealtimeDemandResponse,
    SystemDailyResponse,
)
from ibergrid.api.store import LakeQueryStore
from ibergrid.settings import Settings


def find_portal_dir(settings: Settings) -> Path | None:
    """Locate static portal assets (configured path or ``portal/`` next to ``pyproject.toml``)."""
    if settings.api.portal_dir is not None:
        configured = Path(settings.api.portal_dir)
        return configured if configured.is_dir() else None
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "portal"
        if (candidate / "index.html").is_file():
            return candidate
    return None


def _client_ip(request: Request, *, trust_proxy_headers: bool) -> str:
    if trust_proxy_headers:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Rightmost non-empty hop appended by our trusted Caddy reverse proxy.
            hops = [part.strip() for part in forwarded.split(",") if part.strip()]
            if hops:
                return hops[-1]
    return request.client.host if request.client is not None else "unknown"


def _validate_date_range(start: date, end: date, *, max_days: int) -> None:
    if start > end:
        raise HTTPException(
            status_code=422, detail=f"start ({start}) must be on or before end ({end})"
        )
    span_days = (end - start).days
    if span_days > max_days:
        raise HTTPException(
            status_code=422,
            detail=f"date range of {span_days} days exceeds maximum allowed ({max_days} days)",
        )


def create_app(
    settings: Settings | None = None,
    *,
    store: LakeQueryStore | None = None,
    clock: Callable[[], float] | None = None,
) -> FastAPI:
    """Construct the configured FastAPI application with rate limiting, CORS and static portal."""
    resolved_settings = settings or Settings()
    query_store = store or LakeQueryStore(resolved_settings, clock=clock)
    owns_store = store is None
    limiter = SlidingWindowRateLimiter(
        limit=resolved_settings.api.rate_limit_rpm,
        window_seconds=60.0,
        clock=clock,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_store:
                query_store.close()

    app = FastAPI(
        title="IberGrid Lakehouse API",
        description=(
            "Open multi-engine Iberian electricity lakehouse serving layer over Apache Iceberg "
            "(OMIE MIBEL day-ahead prices & Red Eléctrica REData demand/generation)."
        ),
        version=pkg_version("ibergrid"),
        lifespan=lifespan,
    )

    origins = resolved_settings.api.cors_origin_list()
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "HEAD", "OPTIONS"],
            allow_headers=["Accept", "Content-Type"],
        )

    @app.middleware("http")
    async def security_and_rate_limit(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        rate_headers: dict[str, str] = {}
        if request.url.path.startswith("/v1/"):
            ip = _client_ip(request, trust_proxy_headers=resolved_settings.api.trust_proxy_headers)
            decision = limiter.check(ip)
            rate_headers = {
                "X-RateLimit-Limit": str(decision.limit),
                "X-RateLimit-Remaining": str(decision.remaining),
            }
            if not decision.allowed:
                rate_headers["Retry-After"] = str(decision.retry_after_seconds)
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded; retry after the window resets."},
                    headers={
                        **rate_headers,
                        "X-Content-Type-Options": "nosniff",
                        "X-Frame-Options": "DENY",
                        "Referrer-Policy": "strict-origin-when-cross-origin",
                    },
                )

        response = await call_next(request)
        for key, value in rate_headers.items():
            response.headers[key] = value
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        return response

    @app.get("/health", response_model=HealthResponse, tags=["ops"])
    def health(response: Response) -> HealthResponse:
        status = query_store.health()
        if status.status != "ok":
            response.status_code = 503
        return status

    @app.get("/v1/market/hourly", response_model=MarketHourlyResponse, tags=["market"])
    def market_hourly(
        start: Annotated[
            date | None, Query(description="First delivery date (YYYY-MM-DD, Madrid).")
        ] = None,
        end: Annotated[
            date | None, Query(description="Last delivery date (YYYY-MM-DD, Madrid).")
        ] = None,
    ) -> MarketHourlyResponse:
        today = datetime.now(UTC).date()
        resolved_end = end or today
        resolved_start = start or (resolved_end - timedelta(days=7))
        _validate_date_range(
            resolved_start,
            resolved_end,
            max_days=resolved_settings.api.max_hourly_range_days,
        )
        try:
            return query_store.market_hourly(resolved_start, resolved_end)
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    @app.get("/v1/market/daily", response_model=MarketDailyResponse, tags=["market"])
    def market_daily(
        start: Annotated[
            date | None, Query(description="First delivery date (YYYY-MM-DD, Madrid).")
        ] = None,
        end: Annotated[
            date | None, Query(description="Last delivery date (YYYY-MM-DD, Madrid).")
        ] = None,
    ) -> MarketDailyResponse:
        today = datetime.now(UTC).date()
        resolved_end = end or today
        resolved_start = start or date(2023, 1, 1)
        _validate_date_range(
            resolved_start,
            resolved_end,
            max_days=resolved_settings.api.max_daily_range_days,
        )
        try:
            return query_store.market_daily(resolved_start, resolved_end)
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    @app.get("/v1/systems/daily", response_model=SystemDailyResponse, tags=["systems"])
    def systems_daily(
        system: Annotated[
            ElectricSystem | None,
            Query(
                description=(
                    "Spanish electric system (peninsular, baleares, canarias, ceuta, melilla). "
                    "Omit for all systems (kept strictly separated by system_scope)."
                )
            ),
        ] = None,
        start: Annotated[date | None, Query(description="First demand date (YYYY-MM-DD).")] = None,
        end: Annotated[date | None, Query(description="Last demand date (YYYY-MM-DD).")] = None,
    ) -> SystemDailyResponse:
        today = datetime.now(UTC).date()
        resolved_end = end or today
        resolved_start = start or (resolved_end - timedelta(days=30))
        _validate_date_range(
            resolved_start,
            resolved_end,
            max_days=resolved_settings.api.max_daily_range_days,
        )
        try:
            return query_store.systems_daily(system, resolved_start, resolved_end)
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    @app.get("/v1/windows/optimal", response_model=OptimalWindowsResponse, tags=["market"])
    def optimal_windows(
        month: Annotated[
            int | None, Query(ge=1, le=12, description="Calendar month 1..12.")
        ] = None,
        day_type: Annotated[DayType | None, Query(description="weekday or weekend.")] = None,
    ) -> OptimalWindowsResponse:
        try:
            return query_store.optimal_windows(month, day_type)
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    @app.get("/v1/demand/realtime", response_model=RealtimeDemandResponse, tags=["demand"])
    def realtime_demand(
        hours: Annotated[
            int, Query(ge=1, le=168, description="Trailing hours relative to latest data (1..168).")
        ] = 24,
    ) -> RealtimeDemandResponse:
        try:
            return query_store.realtime_demand(hours)
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    @app.get("/v1/metadata/tables", response_model=MetadataTablesResponse, tags=["metadata"])
    def metadata_tables() -> MetadataTablesResponse:
        try:
            return query_store.metadata_tables()
        except duckdb.Error as exc:
            raise HTTPException(status_code=503, detail=f"Lake query unavailable: {exc}") from exc

    portal_path = find_portal_dir(resolved_settings)
    if portal_path is not None:
        app.mount("/portal", StaticFiles(directory=str(portal_path), html=True), name="portal")

        @app.get("/", include_in_schema=False)
        def root_redirect() -> RedirectResponse:
            return RedirectResponse(url="/portal/", status_code=307)

    return app
