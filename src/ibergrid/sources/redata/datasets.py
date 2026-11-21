"""What ibergrid pulls from REData, one entry per widget, as measured on 2026-11-21 (ADR 0003).

Every request asks for whole local days (``YYYY-MM-DDT00:00`` to ``T23:59``) inside one calendar
month (sub-daily widgets) or one calendar year (daily), the largest windows the API accepted; a
year chunk that would end inside a month stops at the month before, because a long daily request
ending mid-month is answered up to the month's end.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

# REData's days are Madrid days, and every system is stamped with peninsular offsets (+01:00 /
# +02:00). Whether Canarias' stamps are peninsular instants or Canarian wall-clock times is not
# verified (ADR 0003): until it is, joining Canarias to other systems on ts_utc is unsafe.
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
    # The answer's data.id (language-neutral, the same on every date probed); a different one means
    # REData changed what the widget returns, and the chunk is refused.
    answer_id: str
    time_trunc: Literal["hour", "day"]
    # Nominal spacing; each series' own is measured per chunk (ADR 0003), this is only the fallback
    # for a series with fewer than two points.
    granularity: Granularity
    chunk: Literal["month", "year"]
    systems: tuple[System, ...]
    # Counted, not enforced, in Bronze (quality gates are phase 4). None: any finite value.
    value_rule: ValueRule | None = None


POSITIVE = ValueRule("> 0", lambda value: value > 0)
# SDAC's harmonised clearing-price limits in EUR/MWh, applied to both REData price series.
HARMONISED_PRICE = ValueRule("within -500..4000 EUR/MWh", lambda value: -500 <= value <= 4000)
SYSTEMS = (
    System("peninsular", 8741),
    System("canarias", 8742),
    System("baleares", 8743),
    System("ceuta", 8744),
    System("melilla", 8745),
)

DATASETS: dict[str, Dataset] = {
    dataset.name: dataset
    for dataset in (
        # 5-minute points: Real, Forecasted, Scheduled and, in recent data only, Total scheduled.
        # National, as its data.type says (Real is above the five systems' sum); with geo
        # parameters the widget answers 400 or 502.
        Dataset(
            name="demand_realtime",
            widget="demanda/demanda-tiempo-real",
            answer_id="dem15",  # data.type "Demanda nacional en tiempo real"
            time_trunc="hour",
            granularity="5min",
            chunk="month",
            systems=(System("national"),),
            value_rule=POSITIVE,
        ),
        # Hourly demand per electric system; without geo parameters it answers 400.
        Dataset(
            name="demand_hourly",
            widget="demanda/evolucion",
            answer_id="dem1",  # "Evolución de la demanda"
            time_trunc="hour",
            granularity="hour",
            chunk="month",
            systems=SYSTEMS,
            value_rule=POSITIVE,
        ),
        # Daily only (time_trunc=hour answers 400). Net generation can be negative (auxiliary
        # consumption: coal at -120 MWh on some 2026 days), so no value rule.
        Dataset(
            name="generation_daily",
            widget="generacion/estructura-generacion",
            answer_id="gen1",  # "Generation structure by technology"
            time_trunc="day",
            granularity="day",
            chunk="year",
            systems=(System("national"), *SYSTEMS),
        ),
        # PVPC (hourly) and the spot price (24 points a day until 2024-12-31, then 96). With geo
        # parameters the widget drops the spot price, so it is asked without them.
        Dataset(
            name="market_prices",
            widget="mercados/precios-mercados-tiempo-real",
            answer_id="mer13",  # "Precios mercado peninsular en tiempo real"
            time_trunc="hour",
            granularity="hour",
            chunk="month",
            systems=(System("peninsular"),),
            value_rule=HARMONISED_PRICE,
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

    def landing_key(self, prefix: str, payload_sha256: str) -> str:
        """Outside the warehouse prefix, named by window and content: a rerun with the same answer
        overwrites the same object, a different answer gets its own."""
        return (
            f"{prefix}/{self.dataset.name}/{self.system.name}/{self.start:%Y}/{self.start:%m}/"
            f"{self.start}_{self.end}-{payload_sha256[:12]}.json"
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

    # PyIceberg filter syntax; the names are this module's constants, so nothing needs escaping.
    def scope_filter(self) -> str:
        """Every row of this dataset and system."""
        return f"dataset = '{self.dataset.name}' AND electric_system = '{self.system.name}'"

    def row_filter(self) -> str:
        """Exactly the rows this chunk replaces."""
        start, end = self.window_utc
        return (
            f"{self.scope_filter()} "
            f"AND ts_utc >= '{start.isoformat()}' AND ts_utc < '{end.isoformat()}'"
        )

    def outside_filter(self) -> str:
        """The dataset's and system's rows that this chunk must leave alone."""
        start, end = self.window_utc
        return (
            f"{self.scope_filter()} "
            f"AND (ts_utc < '{start.isoformat()}' OR ts_utc >= '{end.isoformat()}')"
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
    """The chunks for [start, end] (local days, inclusive), split at calendar month or year
    boundaries, window by window: every selected system's chunk of a window (all systems when none
    is named) before the next window, so a window is complete as soon as its last system is. A
    year chunk that would end inside a month ends at the month before, and the rest of that month
    is a chunk of its own: REData answers a long daily window ending mid-month up to the month's
    end (ADR 0003)."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    known = {system.name: system for system in dataset.systems}
    unknown = [name for name in systems if name not in known]
    if unknown:
        raise ValueError(
            f"{dataset.name} has no system {', '.join(map(repr, unknown))}; "
            f"expected one of: {', '.join(known)}"
        )
    windows = []
    first = start
    while first <= end:
        if dataset.chunk == "month":
            last = min(end, _month_end(first))
        else:
            last = min(end, first.replace(month=12, day=31))
            if last != _month_end(last) and last.replace(day=1) > first:
                last = last.replace(day=1) - timedelta(days=1)
        windows.append((first, last))
        first = last + timedelta(days=1)
    selected = [known[name] for name in dict.fromkeys(systems)] or dataset.systems
    return [Chunk(dataset, system, first, last) for first, last in windows for system in selected]


def _month_end(day: date) -> date:
    return (day.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
