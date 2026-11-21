"""Thread-safe DuckDB + Iceberg query store with short TTL caching (ADR 0010)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb
import structlog

from ibergrid.api.schemas import (
    ATTRIBUTION_MIBEL_AND_REE,
    ATTRIBUTION_REE_ONLY,
    DayType,
    ElectricSystem,
    HealthResponse,
    MarketDailyPoint,
    MarketDailyResponse,
    MarketHourlyPoint,
    MarketHourlyResponse,
    MetadataTablesResponse,
    OptimalWindowPoint,
    OptimalWindowsResponse,
    RealtimeDemandPoint,
    RealtimeDemandResponse,
    SystemDailyPoint,
    SystemDailyResponse,
    TableMetadataItem,
)
from ibergrid.doctor import check_catalog
from ibergrid.lake import duck, registry
from ibergrid.quality.metadata import inspect_table_metadata
from ibergrid.settings import Settings

log = structlog.get_logger(__name__)
MAX_CACHE_ENTRIES = 256


def _to_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    raise TypeError(f"expected datetime, got {type(value).__name__}")


def _iso_utc(dt: datetime | None = None) -> str:
    target = dt if dt is not None else datetime.now(UTC)
    return _to_utc_datetime(target).strftime("%Y-%m-%dT%H:%M:%SZ")


def _opt_float(value: Any) -> float | None:
    return None if value is None else float(value)


class LakeQueryStore:
    """Serialized read-only DuckDB query store over Lakekeeper Iceberg tables."""

    def __init__(
        self,
        settings: Settings,
        *,
        connection_factory: Callable[[Settings], duckdb.DuckDBPyConnection] | None = None,
        table_resolver: Callable[[str], str] | None = None,
        clock: Callable[[], float] | None = None,
        catalog_probe: Callable[[Settings], bool] | None = None,
    ) -> None:
        self._settings = settings
        self._connect_fn = connection_factory or duck.connect
        self._owns_default_connection = connection_factory is None
        self._resolve_table = table_resolver or duck.table_name
        self._clock = clock or time.monotonic
        self._catalog_probe = catalog_probe
        self._cache_ttl = settings.api.cache_ttl_seconds
        self._lock = threading.Lock()
        self._con: duckdb.DuckDBPyConnection | None = None
        self._cache: dict[tuple[Any, ...], tuple[float, Any]] = {}

    def close(self) -> None:
        """Close any cached DuckDB connection cleanly."""
        with self._lock:
            if self._con is not None:
                self._con.close()
                self._con = None
            self._cache.clear()

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            con = self._connect_fn(self._settings)
            con.execute("SET GLOBAL TimeZone = 'UTC'")
            con.execute("SET TimeZone = 'UTC'")
            self._con = con
        return self._con

    def _run_cached[T](self, cache_key: tuple[Any, ...], loader: Callable[[], T]) -> T:
        now = self._clock()
        with self._lock:
            if self._cache_ttl > 0:
                entry = self._cache.get(cache_key)
                if entry is not None and entry[0] > now:
                    return entry[1]  # type: ignore[no-any-return]
            try:
                value = loader()
            except duckdb.Error:
                # Reset connection on catalog/network errors so the next call reconnects cleanly.
                if self._con is not None:
                    self._con.close()
                    self._con = None
                raise
            if self._cache_ttl > 0:
                if len(self._cache) >= MAX_CACHE_ENTRIES:
                    self._cache.clear()
                self._cache[cache_key] = (now + self._cache_ttl, value)
            return value

    def _table_has_snapshot(self, con: duckdb.DuckDBPyConnection, logical_table: str) -> bool:
        try:
            summary = inspect_table_metadata(con, self._resolve_table(logical_table))
        except duckdb.CatalogException:
            return False
        except duckdb.Error as exc:
            message = str(exc).lower()
            if (
                "does not exist" in message
                or "nosuchtable" in message
                or "404" in message
                or "no version-hint could be found" in message
            ):
                return False
            raise
        return summary.snapshot_id is not None and summary.total_records > 0

    def health(self) -> HealthResponse:
        """Verify DuckDB and Lakekeeper catalog connectivity (works even before lake init)."""
        checked_at = _iso_utc()
        try:
            with self._lock:
                con = self._connection()
                con.execute("SELECT 1").fetchone()
            if self._catalog_probe is not None:
                if not self._catalog_probe(self._settings):
                    raise RuntimeError("catalog health probe returned unhealthy")
            elif self._owns_default_connection:
                probe = check_catalog(self._settings.catalog, timeout=3.0)
                if not probe.ok:
                    raise RuntimeError(probe.detail)
            return HealthResponse(
                status="ok",
                catalog_reachable=True,
                tables_registered=len(registry.TABLES),
                checked_at_utc=checked_at,
            )
        except Exception as exc:
            log.warning("api.health.degraded", error=str(exc))
            with self._lock:
                if self._con is not None:
                    self._con.close()
                    self._con = None
            return HealthResponse(
                status="degraded",
                catalog_reachable=False,
                tables_registered=len(registry.TABLES),
                checked_at_utc=checked_at,
            )

    def market_hourly(self, start: date, end: date) -> MarketHourlyResponse:
        """Query peninsular hourly MIBEL market rows from ``gold.mibel_hourly_market``."""

        def _load() -> MarketHourlyResponse:
            con = self._connection()
            if not self._table_has_snapshot(con, "gold.mibel_hourly_market"):
                return MarketHourlyResponse(
                    source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                    updated_at_utc=_iso_utc(),
                    start=start,
                    end=end,
                    count=0,
                    items=[],
                )
            rows = con.execute(
                """
                SELECT
                    hour_utc,
                    delivery_date,
                    madrid_hour,
                    day_type,
                    price_es_eur_mwh,
                    price_pt_eur_mwh,
                    spread_es_minus_pt_eur_mwh,
                    is_decoupled,
                    is_zero_or_negative_es,
                    pvpc_eur_mwh,
                    peninsular_demand_mw,
                    demand_ramp_1h_mw,
                    price_ramp_1h_eur_mwh,
                    intraday_price_rank,
                    is_evening_ramp_window,
                    peninsular_renewable_share_pct,
                    peninsular_solar_share_pct,
                    peninsular_wind_share_pct,
                    updated_at_utc
                FROM iceberg_scan($table)
                WHERE delivery_date >= $start AND delivery_date <= $end
                ORDER BY hour_utc ASC
                """,
                {
                    "table": self._resolve_table("gold.mibel_hourly_market"),
                    "start": start,
                    "end": end,
                },
            ).fetchall()
            latest_upd = max((_to_utc_datetime(r[18]) for r in rows), default=None)
            items = [
                MarketHourlyPoint(
                    hour_utc=_to_utc_datetime(r[0]),
                    delivery_date=r[1],
                    madrid_hour=int(r[2]),
                    day_type=str(r[3]),
                    price_es_eur_mwh=float(r[4]),
                    price_pt_eur_mwh=float(r[5]),
                    spread_es_minus_pt_eur_mwh=float(r[6]),
                    is_decoupled=bool(r[7]),
                    is_zero_or_negative_es=bool(r[8]),
                    pvpc_eur_mwh=_opt_float(r[9]),
                    peninsular_demand_mw=_opt_float(r[10]),
                    demand_ramp_1h_mw=_opt_float(r[11]),
                    price_ramp_1h_eur_mwh=_opt_float(r[12]),
                    intraday_price_rank=int(r[13]),
                    is_evening_ramp_window=bool(r[14]),
                    peninsular_renewable_share_pct=_opt_float(r[15]),
                    peninsular_solar_share_pct=_opt_float(r[16]),
                    peninsular_wind_share_pct=_opt_float(r[17]),
                )
                for r in rows
            ]
            return MarketHourlyResponse(
                source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                updated_at_utc=_iso_utc(latest_upd),
                start=start,
                end=end,
                count=len(items),
                items=items,
            )

        return self._run_cached(("market_hourly", start, end), _load)

    def market_daily(self, start: date, end: date) -> MarketDailyResponse:
        """Query daily MIBEL summary rows from ``gold.mibel_daily_summary``."""

        def _load() -> MarketDailyResponse:
            con = self._connection()
            if not self._table_has_snapshot(con, "gold.mibel_daily_summary"):
                return MarketDailyResponse(
                    source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                    updated_at_utc=_iso_utc(),
                    start=start,
                    end=end,
                    count=0,
                    items=[],
                )
            rows = con.execute(
                """
                SELECT
                    delivery_date,
                    hours_in_day,
                    price_es_mean_eur_mwh,
                    price_es_min_eur_mwh,
                    price_es_max_eur_mwh,
                    price_es_peak_eur_mwh,
                    price_es_offpeak_eur_mwh,
                    price_pt_mean_eur_mwh,
                    spread_mean_eur_mwh,
                    spread_abs_mean_eur_mwh,
                    decoupled_hours,
                    zero_or_negative_es_hours,
                    peninsular_demand_mwh,
                    peninsular_peak_demand_mw,
                    evening_ramp_max_mw,
                    non_peninsular_demand_mwh,
                    peninsular_renewable_share_pct,
                    peninsular_solar_share_pct,
                    peninsular_wind_share_pct,
                    updated_at_utc
                FROM iceberg_scan($table)
                WHERE delivery_date >= $start AND delivery_date <= $end
                ORDER BY delivery_date ASC
                """,
                {
                    "table": self._resolve_table("gold.mibel_daily_summary"),
                    "start": start,
                    "end": end,
                },
            ).fetchall()
            latest_upd = max((_to_utc_datetime(r[19]) for r in rows), default=None)
            items = [
                MarketDailyPoint(
                    delivery_date=r[0],
                    hours_in_day=int(r[1]),
                    price_es_mean_eur_mwh=float(r[2]),
                    price_es_min_eur_mwh=float(r[3]),
                    price_es_max_eur_mwh=float(r[4]),
                    price_es_peak_eur_mwh=_opt_float(r[5]),
                    price_es_offpeak_eur_mwh=_opt_float(r[6]),
                    price_pt_mean_eur_mwh=float(r[7]),
                    spread_mean_eur_mwh=float(r[8]),
                    spread_abs_mean_eur_mwh=float(r[9]),
                    decoupled_hours=int(r[10]),
                    zero_or_negative_es_hours=int(r[11]),
                    peninsular_demand_mwh=_opt_float(r[12]),
                    peninsular_peak_demand_mw=_opt_float(r[13]),
                    evening_ramp_max_mw=_opt_float(r[14]),
                    non_peninsular_demand_mwh=_opt_float(r[15]),
                    peninsular_renewable_share_pct=_opt_float(r[16]),
                    peninsular_solar_share_pct=_opt_float(r[17]),
                    peninsular_wind_share_pct=_opt_float(r[18]),
                )
                for r in rows
            ]
            return MarketDailyResponse(
                source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                updated_at_utc=_iso_utc(latest_upd),
                start=start,
                end=end,
                count=len(items),
                items=items,
            )

        return self._run_cached(("market_daily", start, end), _load)

    def systems_daily(
        self, system: ElectricSystem | None, start: date, end: date
    ) -> SystemDailyResponse:
        """Query per-system daily demand and renewables from ``gold.system_demand_daily``."""

        def _load() -> SystemDailyResponse:
            con = self._connection()
            if not self._table_has_snapshot(con, "gold.system_demand_daily"):
                return SystemDailyResponse(
                    source_attribution=ATTRIBUTION_REE_ONLY,
                    updated_at_utc=_iso_utc(),
                    system=system,
                    start=start,
                    end=end,
                    count=0,
                    items=[],
                )
            rows = con.execute(
                """
                SELECT
                    demand_date,
                    electric_system,
                    system_scope,
                    ts_utc_verified,
                    hours_in_day,
                    demand_total_mwh,
                    demand_mean_mw,
                    demand_min_mw,
                    demand_peak_mw,
                    generation_total_mwh,
                    renewable_generation_mwh,
                    renewable_share_pct,
                    solar_share_pct,
                    wind_share_pct,
                    updated_at_utc
                FROM iceberg_scan($table)
                WHERE demand_date >= $start
                  AND demand_date <= $end
                  AND ($system IS NULL OR electric_system = $system)
                ORDER BY demand_date ASC, electric_system ASC
                """,
                {
                    "table": self._resolve_table("gold.system_demand_daily"),
                    "start": start,
                    "end": end,
                    "system": system,
                },
            ).fetchall()
            latest_upd = max((_to_utc_datetime(r[14]) for r in rows), default=None)
            items = [
                SystemDailyPoint(
                    demand_date=r[0],
                    electric_system=str(r[1]),
                    system_scope=str(r[2]),
                    ts_utc_verified=bool(r[3]),
                    hours_in_day=int(r[4]),
                    demand_total_mwh=float(r[5]),
                    demand_mean_mw=float(r[6]),
                    demand_min_mw=float(r[7]),
                    demand_peak_mw=float(r[8]),
                    generation_total_mwh=_opt_float(r[9]),
                    renewable_generation_mwh=_opt_float(r[10]),
                    renewable_share_pct=_opt_float(r[11]),
                    solar_share_pct=_opt_float(r[12]),
                    wind_share_pct=_opt_float(r[13]),
                )
                for r in rows
            ]
            return SystemDailyResponse(
                source_attribution=ATTRIBUTION_REE_ONLY,
                updated_at_utc=_iso_utc(latest_upd),
                system=system,
                start=start,
                end=end,
                count=len(items),
                items=items,
            )

        return self._run_cached(("systems_daily", system, start, end), _load)

    def optimal_windows(
        self, month: int | None, day_type: DayType | None
    ) -> OptimalWindowsResponse:
        """Query intraday load-shifting profiles from ``gold.optimal_consumption_windows``."""

        def _load() -> OptimalWindowsResponse:
            con = self._connection()
            if not self._table_has_snapshot(con, "gold.optimal_consumption_windows"):
                return OptimalWindowsResponse(
                    source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                    updated_at_utc=_iso_utc(),
                    month=month,
                    day_type=day_type,
                    count=0,
                    items=[],
                )
            rows = con.execute(
                """
                SELECT
                    calendar_month,
                    day_type,
                    madrid_hour,
                    sample_hours,
                    price_es_mean_eur_mwh,
                    price_es_p10_eur_mwh,
                    price_es_p50_eur_mwh,
                    price_es_p90_eur_mwh,
                    zero_or_negative_es_pct,
                    decoupled_pct,
                    peninsular_demand_mean_mw,
                    peninsular_renewable_share_mean_pct,
                    window_rank,
                    is_optimal_4h_window,
                    updated_at_utc
                FROM iceberg_scan($table)
                WHERE ($month IS NULL OR calendar_month = $month)
                  AND ($day_type IS NULL OR day_type = $day_type)
                ORDER BY calendar_month ASC, day_type ASC, madrid_hour ASC
                """,
                {
                    "table": self._resolve_table("gold.optimal_consumption_windows"),
                    "month": month,
                    "day_type": day_type,
                },
            ).fetchall()
            latest_upd = max((_to_utc_datetime(r[14]) for r in rows), default=None)
            items = [
                OptimalWindowPoint(
                    calendar_month=int(r[0]),
                    day_type=str(r[1]),
                    madrid_hour=int(r[2]),
                    sample_hours=int(r[3]),
                    price_es_mean_eur_mwh=float(r[4]),
                    price_es_p10_eur_mwh=float(r[5]),
                    price_es_p50_eur_mwh=float(r[6]),
                    price_es_p90_eur_mwh=float(r[7]),
                    zero_or_negative_es_pct=float(r[8]),
                    decoupled_pct=float(r[9]),
                    peninsular_demand_mean_mw=_opt_float(r[10]),
                    peninsular_renewable_share_mean_pct=_opt_float(r[11]),
                    window_rank=int(r[12]),
                    is_optimal_4h_window=bool(r[13]),
                )
                for r in rows
            ]
            return OptimalWindowsResponse(
                source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                updated_at_utc=_iso_utc(latest_upd),
                month=month,
                day_type=day_type,
                count=len(items),
                items=items,
            )

        return self._run_cached(("optimal_windows", month, day_type), _load)

    def realtime_demand(self, hours: int) -> RealtimeDemandResponse:
        """Query latest ``hours`` of reconciled 5-min national demand (batch + stream lanes)."""

        def _load() -> RealtimeDemandResponse:
            con = self._connection()
            if not self._table_has_snapshot(con, "silver.demand_national_5min"):
                return RealtimeDemandResponse(
                    source_attribution=ATTRIBUTION_REE_ONLY,
                    updated_at_utc=_iso_utc(),
                    hours=hours,
                    batch_points=0,
                    stream_points=0,
                    count=0,
                    items=[],
                )
            table_ref = self._resolve_table("silver.demand_national_5min")
            row = con.execute(
                "SELECT max(ts_utc) FROM iceberg_scan($table)", {"table": table_ref}
            ).fetchone()
            if row is None or row[0] is None:
                return RealtimeDemandResponse(
                    source_attribution=ATTRIBUTION_REE_ONLY,
                    updated_at_utc=_iso_utc(),
                    hours=hours,
                    batch_points=0,
                    stream_points=0,
                    count=0,
                    items=[],
                )
            latest_ts = _to_utc_datetime(row[0])
            cutoff = latest_ts - timedelta(hours=hours)
            rows = con.execute(
                """
                SELECT
                    ts_utc,
                    madrid_date,
                    demand_real_mw,
                    demand_forecast_mw,
                    demand_scheduled_mw,
                    demand_total_scheduled_mw,
                    source_lane
                FROM iceberg_scan($table)
                WHERE ts_utc > $cutoff AND ts_utc <= $latest
                ORDER BY ts_utc ASC
                """,
                {"table": table_ref, "cutoff": cutoff, "latest": latest_ts},
            ).fetchall()
            items = [
                RealtimeDemandPoint(
                    ts_utc=_to_utc_datetime(r[0]),
                    madrid_date=r[1],
                    demand_real_mw=_opt_float(r[2]),
                    demand_forecast_mw=_opt_float(r[3]),
                    demand_scheduled_mw=_opt_float(r[4]),
                    demand_total_scheduled_mw=_opt_float(r[5]),
                    source_lane=str(r[6]),
                )
                for r in rows
            ]
            batch_cnt = sum(1 for item in items if item.source_lane == "batch")
            stream_cnt = sum(1 for item in items if item.source_lane == "stream")
            return RealtimeDemandResponse(
                source_attribution=ATTRIBUTION_REE_ONLY,
                updated_at_utc=_iso_utc(latest_ts),
                hours=hours,
                batch_points=batch_cnt,
                stream_points=stream_cnt,
                count=len(items),
                items=items,
            )

        return self._run_cached(("realtime_demand", hours), _load)

    def metadata_tables(self) -> MetadataTablesResponse:
        """Zero-scan Iceberg metadata inventory across all registered tables (0 data bytes read)."""

        def _load() -> MetadataTablesResponse:
            con = self._connection()
            items: list[TableMetadataItem] = []
            for spec in registry.TABLES:
                logical = f"{spec.namespace}.{spec.name}"
                try:
                    summary = inspect_table_metadata(con, self._resolve_table(logical))
                except duckdb.Error:
                    continue
                items.append(
                    TableMetadataItem(
                        table=logical,
                        snapshot_id=summary.snapshot_id,
                        snapshot_committed_utc=summary.snapshot_committed_utc,
                        operation=summary.operation,
                        data_files=summary.data_files,
                        delete_files=summary.delete_files,
                        total_records=summary.total_records,
                        total_data_bytes=summary.total_data_bytes,
                        data_bytes_scanned=summary.data_bytes_scanned,
                        partitions=summary.partitions,
                    )
                )
            latest_commit = max(
                (i.snapshot_committed_utc for i in items if i.snapshot_committed_utc is not None),
                default=None,
            )
            return MetadataTablesResponse(
                source_attribution=ATTRIBUTION_MIBEL_AND_REE,
                updated_at_utc=_iso_utc(latest_commit),
                total_tables=len(items),
                total_records=sum(i.total_records for i in items),
                total_data_bytes=sum(i.total_data_bytes for i in items),
                total_data_bytes_scanned=sum(i.data_bytes_scanned for i in items),
                items=items,
            )

        return self._run_cached(("metadata_tables",), _load)
