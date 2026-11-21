"""What ibergrid pulls from REData, one entry per widget, as measured on 2026-11-21 (ADR 0003).

Every request asks for whole local days (``YYYY-MM-DDT00:00`` to ``T23:59``) inside one calendar
month (sub-daily widgets) or one calendar year (daily): the largest windows the API accepted.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

# REData's days are Spanish peninsular days for every system: Canarias is delivered in
# peninsular time too (+01:00 / +02:00 offsets).
MADRID = ZoneInfo("Europe/Madrid")

Granularity = Literal["5min", "15min", "hour", "day"]
STEP_SECONDS: dict[Granularity, int] = {"5min": 300, "15min": 900, "hour": 3600}


@dataclass(frozen=True)
class System:
    name: str
    geo_id: int | None = None  # None: the widget's own scope, asked without geo parameters


@dataclass(frozen=True)
class ValueRule:
    description: str
    check: Callable[[float], bool]


@dataclass(frozen=True)
class Dataset:
    name: str
    widget: str
    time_trunc: Literal["hour", "day"]
    # Nominal spacing; each series' own is measured per chunk (ADR 0003), this is only the fallback
    # for a series with fewer than two points.
    granularity: Granularity
    chunk: Literal["month", "year"]
    systems: tuple[System, ...]
    value_rule: ValueRule | None = None  # None: any finite value


POSITIVE = ValueRule("> 0", lambda value: value > 0)
# SDAC's harmonised clearing-price limits in EUR/MWh, applied to both REData price series.
HARMONISED_PRICE = ValueRule("within -500..4000 EUR/MWh", lambda value: -500 <= value <= 4000)

DATASETS: dict[str, Dataset] = {
    dataset.name: dataset
    for dataset in (
        # 5-minute points: Real, Forecasted, Scheduled and, in recent data only, Total scheduled.
        # Peninsular only: with geo parameters the widget answers 400 or 502.
        Dataset(
            "demand_realtime",
            "demanda/demanda-tiempo-real",
            "hour",
            "5min",
            "month",
            (System("peninsular"),),
            POSITIVE,
        ),
        # Hourly demand per electric system; without geo parameters it answers 400.
        Dataset(
            "demand_hourly",
            "demanda/evolucion",
            "hour",
            "hour",
            "month",
            (
                System("peninsular", 8741),
                System("canarias", 8742),
                System("baleares", 8743),
                System("ceuta", 8744),
                System("melilla", 8745),
            ),
            POSITIVE,
        ),
        # Daily only (time_trunc=hour answers 400). Net generation can be negative (auxiliary
        # consumption: coal at -120 MWh on some 2026 days), so only finiteness is checked.
        Dataset(
            "generation_daily",
            "generacion/estructura-generacion",
            "day",
            "day",
            "year",
            (
                System("national"),
                System("peninsular", 8741),
                System("canarias", 8742),
                System("baleares", 8743),
                System("ceuta", 8744),
                System("melilla", 8745),
            ),
        ),
        # PVPC (hourly) and the spot price (24 points a day until 2024-12-31, then 96). With geo
        # parameters the widget drops the spot price, so it is asked without them.
        Dataset(
            "market_prices",
            "mercados/precios-mercados-tiempo-real",
            "hour",
            "hour",
            "month",
            (System("peninsular"),),
            HARMONISED_PRICE,
        ),
    )
}


@dataclass(frozen=True)
class Chunk:
    """One request and one Write-Audit-Publish run: a dataset, a system and whole local days."""

    dataset: Dataset
    system: System
    start: date  # first local day
    end: date  # last local day, inclusive

    @property
    def window_utc(self) -> tuple[datetime, datetime]:
        """[UTC of the first local midnight, UTC of the midnight after the last day)."""
        return utc_midnight(self.start), utc_midnight(self.end + timedelta(days=1))

    @property
    def days(self) -> list[date]:
        return [self.start + timedelta(days=n) for n in range((self.end - self.start).days + 1)]

    @property
    def slug(self) -> str:
        return f"{self.dataset.name}-{self.system.name}-{self.start:%Y%m%d}-{self.end:%Y%m%d}"

    @property
    def landing_key(self) -> str:
        """Deterministic, outside the warehouse prefix: a rerun overwrites the same object."""
        return (
            f"landing/redata/{self.dataset.name}/{self.system.name}/"
            f"{self.start:%Y}/{self.start:%m}/{self.start}_{self.end}.json"
        )

    def params(self) -> dict[str, str]:
        params = {
            "start_date": f"{self.start}T00:00",
            "end_date": f"{self.end}T23:59",
            "time_trunc": self.dataset.time_trunc,
        }
        if self.system.geo_id is not None:
            params |= {
                "geo_trunc": "electric_system",
                "geo_limit": self.system.name,
                "geo_ids": str(self.system.geo_id),
            }
        return params

    def row_filter(self) -> str:
        """Exactly the rows this chunk replaces, in PyIceberg's filter syntax (the names are this
        module's constants, so nothing needs escaping)."""
        start, end = self.window_utc
        return (
            f"dataset = '{self.dataset.name}' AND electric_system = '{self.system.name}' "
            f"AND ts_utc >= '{start.isoformat()}' AND ts_utc < '{end.isoformat()}'"
        )


def utc_midnight(day: date) -> datetime:
    return datetime.combine(day, time(), MADRID).astimezone(UTC)


def points_per_day(day: date, granularity: Granularity) -> int:
    """276, 288 or 300 five-minute points; 92, 96 or 100 quarter-hours; 23, 24 or 25 hours."""
    if granularity == "day":
        return 1
    seconds = (utc_midnight(day + timedelta(days=1)) - utc_midnight(day)).total_seconds()
    return int(seconds) // STEP_SECONDS[granularity]


def dataset_named(name: str) -> Dataset:
    if name not in DATASETS:
        raise ValueError(f"unknown dataset {name!r}; expected one of: {', '.join(DATASETS)}")
    return DATASETS[name]


def plan(dataset: Dataset, start: date, end: date, systems: tuple[str, ...] = ()) -> list[Chunk]:
    """The chunks for [start, end] (local days, inclusive): every selected system (all of them
    when none is named), each split at calendar month or year boundaries."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    known = {system.name: system for system in dataset.systems}
    unknown = [name for name in systems if name not in known]
    if unknown:
        raise ValueError(
            f"{dataset.name} has no system {', '.join(map(repr, unknown))}; "
            f"expected one of: {', '.join(known)}"
        )
    chunks = []
    for system in [known[name] for name in dict.fromkeys(systems)] or dataset.systems:
        first = start
        while first <= end:
            if dataset.chunk == "month":
                following = (first.replace(day=1) + timedelta(days=32)).replace(day=1)
            else:
                following = first.replace(year=first.year + 1, month=1, day=1)
            last = min(end, following - timedelta(days=1))
            chunks.append(Chunk(dataset, system, first, last))
            first = last + timedelta(days=1)
    return chunks
