"""Pydantic v2 response schemas for the IberGrid real-time serving API (ADR 0010)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel

ATTRIBUTION_MIBEL_AND_REE = "Source: OMIE (omie.es) & Red Eléctrica, REData"
ATTRIBUTION_REE_ONLY = "Source: Red Eléctrica, REData"
NON_COMMERCIAL_NOTICE = (
    "Non-commercial open-data portfolio project; not affiliated with or endorsed by REE or OMIE."
)

ElectricSystem = Literal["peninsular", "baleares", "canarias", "ceuta", "melilla"]
DayType = Literal["weekday", "weekend"]


class ApiEnvelope(BaseModel):
    """Common attribution and provenance envelope returned on every /v1/* response."""

    source_attribution: str
    updated_at_utc: str
    non_commercial_notice: str = NON_COMMERCIAL_NOTICE


class HealthResponse(BaseModel):
    """Readiness probe for the API service and underlying Lakekeeper + DuckDB catalog."""

    status: Literal["ok", "degraded"]
    catalog_reachable: bool
    tables_registered: int
    checked_at_utc: str


class MarketHourlyPoint(BaseModel):
    """One peninsular hourly MIBEL record from ``gold.mibel_hourly_market``."""

    hour_utc: datetime
    delivery_date: date
    madrid_hour: int
    day_type: str
    price_es_eur_mwh: float
    price_pt_eur_mwh: float
    spread_es_minus_pt_eur_mwh: float
    is_decoupled: bool
    is_zero_or_negative_es: bool
    pvpc_eur_mwh: float | None = None
    peninsular_demand_mw: float | None = None
    demand_ramp_1h_mw: float | None = None
    price_ramp_1h_eur_mwh: float | None = None
    intraday_price_rank: int
    is_evening_ramp_window: bool
    peninsular_renewable_share_pct: float | None = None
    peninsular_solar_share_pct: float | None = None
    peninsular_wind_share_pct: float | None = None


class MarketHourlyResponse(ApiEnvelope):
    start: date
    end: date
    count: int
    items: list[MarketHourlyPoint]


class MarketDailyPoint(BaseModel):
    """One daily MIBEL executive summary row from ``gold.mibel_daily_summary``."""

    delivery_date: date
    hours_in_day: int
    price_es_mean_eur_mwh: float
    price_es_min_eur_mwh: float
    price_es_max_eur_mwh: float
    price_es_peak_eur_mwh: float | None = None
    price_es_offpeak_eur_mwh: float | None = None
    price_pt_mean_eur_mwh: float
    spread_mean_eur_mwh: float
    spread_abs_mean_eur_mwh: float
    decoupled_hours: int
    zero_or_negative_es_hours: int
    peninsular_demand_mwh: float | None = None
    peninsular_peak_demand_mw: float | None = None
    evening_ramp_max_mw: float | None = None
    non_peninsular_demand_mwh: float | None = None
    peninsular_renewable_share_pct: float | None = None
    peninsular_solar_share_pct: float | None = None
    peninsular_wind_share_pct: float | None = None


class MarketDailyResponse(ApiEnvelope):
    start: date
    end: date
    count: int
    items: list[MarketDailyPoint]


class SystemDailyPoint(BaseModel):
    """One daily electric-system row from ``gold.system_demand_daily``.

    Preserves ``system_scope`` (peninsular vs non_peninsular) and ``ts_utc_verified``
    (``False`` for Canarias pending clock verification per ADR 0003).
    """

    demand_date: date
    electric_system: str
    system_scope: str
    ts_utc_verified: bool
    hours_in_day: int
    demand_total_mwh: float
    demand_mean_mw: float
    demand_min_mw: float
    demand_peak_mw: float
    generation_total_mwh: float | None = None
    renewable_generation_mwh: float | None = None
    renewable_share_pct: float | None = None
    solar_share_pct: float | None = None
    wind_share_pct: float | None = None


class SystemDailyResponse(ApiEnvelope):
    system: ElectricSystem | None = None
    start: date
    end: date
    count: int
    items: list[SystemDailyPoint]


class OptimalWindowPoint(BaseModel):
    """One calendar-month x day-type x Madrid-hour row from ``gold.optimal_consumption_windows``."""

    calendar_month: int
    day_type: str
    madrid_hour: int
    sample_hours: int
    price_es_mean_eur_mwh: float
    price_es_p10_eur_mwh: float
    price_es_p50_eur_mwh: float
    price_es_p90_eur_mwh: float
    zero_or_negative_es_pct: float
    decoupled_pct: float
    peninsular_demand_mean_mw: float | None = None
    peninsular_renewable_share_mean_pct: float | None = None
    window_rank: int
    is_optimal_4h_window: bool


class OptimalWindowsResponse(ApiEnvelope):
    month: int | None = None
    day_type: DayType | None = None
    count: int
    items: list[OptimalWindowPoint]


class RealtimeDemandPoint(BaseModel):
    """One reconciled 5-minute national demand point from ``silver.demand_national_5min``."""

    ts_utc: datetime
    madrid_date: date
    demand_real_mw: float | None = None
    demand_forecast_mw: float | None = None
    demand_scheduled_mw: float | None = None
    demand_total_scheduled_mw: float | None = None
    source_lane: str


class RealtimeDemandResponse(ApiEnvelope):
    hours: int
    batch_points: int
    stream_points: int
    count: int
    items: list[RealtimeDemandPoint]


class TableMetadataItem(BaseModel):
    """Zero-scan Iceberg manifest summary for one registered table (0 Parquet bytes scanned)."""

    table: str
    snapshot_id: int | None = None
    snapshot_committed_utc: datetime | None = None
    operation: str | None = None
    data_files: int
    delete_files: int
    total_records: int
    total_data_bytes: int
    data_bytes_scanned: int
    partitions: int


class MetadataTablesResponse(ApiEnvelope):
    total_tables: int
    total_records: int
    total_data_bytes: int
    total_data_bytes_scanned: int
    items: list[TableMetadataItem]
