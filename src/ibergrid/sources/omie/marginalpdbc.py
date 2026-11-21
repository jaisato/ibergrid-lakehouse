"""OMIE's ``marginalpdbc`` file, the day-ahead marginal prices of one delivery day (ADR 0004).

The file is Latin-1 text with CRLF line ends: the header ``MARGINALPDBC;``, one line per period,
``YYYY;MM;DD;period;price_PT;price_ES;`` (EUR/MWh, a dot for decimals), and the trailer ``*``.
Column 5 is Portugal and column 6 Spain: on 2025-04-28, with the zones decoupled, period 8 has
40.7 and 47.94, and REData's Spanish spot price for that hour is 47.94.

Two contract versions, both measured: v1, delivery days up to 2025-09-30, has hourly periods (23,
24 or 25 a day); v2, from 2025-10-01 (SDAC's 15-minute market time unit), quarter-hours (92, 96
or 100). The delivery date picks the version and the period count must match it, so a v1-shaped
file after the cut-over, a v2-shaped one before it, or any other count is a contract violation.
Anything that breaks the grammar refuses the whole file; prices outside the harmonised limits
are real data and only counted.
"""

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ibergrid.sources.omie.client import OmieError, file_name

# OMIE's delivery days are Spanish peninsular days (CET/CEST).
MADRID = ZoneInfo("Europe/Madrid")
CUTOVER = date(2025, 10, 1)  # the first delivery day of 15-minute periods
HEADER, TRAILER = "MARGINALPDBC;", "*"
_PRICE = r"(-?\d+(?:\.\d+)?)"
_LINE = re.compile(rf"(\d{{4}});(\d{{2}});(\d{{2}});(\d{{1,3}});{_PRICE};{_PRICE};")
# SDAC's harmonised clearing-price limits, EUR/MWh: counted per column, never refused.
PRICE_LIMITS = (-500.0, 4000.0)
PRICE_COLUMNS = {"es": "price_es_eur_mwh", "pt": "price_pt_eur_mwh"}
MAX_PROBLEMS_SHOWN = 5


@dataclass(frozen=True)
class Contract:
    version: int
    resolution: timedelta

    @property
    def minutes(self) -> int:
        return int(self.resolution.total_seconds()) // 60


V1, V2 = Contract(1, timedelta(hours=1)), Contract(2, timedelta(minutes=15))


class InvalidFile(OmieError):
    """The file breaks its contract: nothing of its chunk was landed or written."""

    def __init__(self, day: date, problems: list[str]) -> None:
        shown = "; ".join(problems[:MAX_PROBLEMS_SHOWN])
        more = len(problems) - MAX_PROBLEMS_SHOWN
        suffix = f" (and {more} more)" if more > 0 else ""
        super().__init__(f"{file_name(day)}: invalid OMIE file: {shown}{suffix}")
        self.problems = problems


@dataclass(frozen=True)
class Period:
    number: int
    start_utc: datetime
    end_utc: datetime
    price_es: float
    price_pt: float


@dataclass(frozen=True)
class DayFile:
    day: date
    contract: Contract
    periods: tuple[Period, ...]


def contract_for(day: date) -> Contract:
    return V2 if day >= CUTOVER else V1


def utc_midnight(day: date) -> datetime:
    return datetime.combine(day, time(), MADRID).astimezone(UTC)


def expected_periods(day: date, contract: Contract) -> int:
    """23, 24 or 25 hours; 92, 96 or 100 quarter-hours: the length of the local day."""
    return (utc_midnight(day + timedelta(days=1)) - utc_midnight(day)) // contract.resolution


def parse(payload: bytes, day: date) -> DayFile:
    """The periods of ``day``'s file, or ``InvalidFile`` listing every problem found."""
    lines = payload.decode("latin-1").split("\n")  # every byte is Latin-1: decoding cannot fail
    if lines[-1] == "":
        lines.pop()  # the final line end
    lines = [line.removesuffix("\r") for line in lines]
    if not lines or lines[0] != HEADER:
        first = lines[0][:40] if lines else ""
        raise InvalidFile(day, [f"line 1 is {first!r}, not the header {HEADER!r}"])
    problems = []
    if len(lines) < 2 or lines[-1] != TRAILER:
        problems.append(f"the last line is not the trailer {TRAILER!r}")
    else:
        lines.pop()
    rows = []
    for number, line in enumerate(lines[1:], start=2):
        match = _LINE.fullmatch(line)
        if match is None:
            problems.append(f"line {number}: {_mismatch(line)}")
            continue
        year, month, dom, period, price_pt, price_es = match.groups()
        if (int(year), int(month), int(dom)) != (day.year, day.month, day.day):
            problems.append(f"line {number}: delivery day {year}-{month}-{dom}, not {day}")
        rows.append((int(period), float(price_pt), float(price_es)))
    contract = contract_for(day)
    if not problems:
        problems = _check_periods(day, contract, [row[0] for row in rows])
    if problems:
        raise InvalidFile(day, problems)
    start = utc_midnight(day)
    return DayFile(
        day,
        contract,
        tuple(
            Period(
                n,
                start + (n - 1) * contract.resolution,
                start + n * contract.resolution,
                price_es=es,
                price_pt=pt,
            )
            for n, pt, es in rows
        ),
    )


def outside_limits(files: Iterable[DayFile]) -> dict[str, int]:
    """Per price column, the periods outside the harmonised limits (only columns with any)."""
    low, high = PRICE_LIMITS
    found: Counter[str] = Counter()
    for period in (p for f in files for p in f.periods):
        for zone, price in (("es", period.price_es), ("pt", period.price_pt)):
            if not low <= price <= high:
                found[PRICE_COLUMNS[zone]] += 1
    return dict(sorted(found.items()))


def to_rows(day_file: DayFile, **constant: Any) -> list[dict[str, Any]]:
    """Bronze rows of one file; ``constant`` holds the per-file and per-run columns."""
    return [
        {
            "delivery_date": day_file.day,
            "period": period.number,
            "resolution_minutes": day_file.contract.minutes,
            "interval_start_utc": period.start_utc,
            "interval_end_utc": period.end_utc,
            "price_es_eur_mwh": period.price_es,
            "price_pt_eur_mwh": period.price_pt,
            "contract_version": day_file.contract.version,
            "source_file": file_name(day_file.day),
            **constant,
        }
        for period in day_file.periods
    ]


def _check_periods(day: date, contract: Contract, periods: list[int]) -> list[str]:
    for position, period in enumerate(periods, start=1):
        if period != position:
            return [f"period {period} where {position} belongs: periods run 1, 2, 3 and so on"]
    expected = expected_periods(day, contract)
    if len(periods) == expected:
        return []
    other = V1 if contract is V2 else V2
    if len(periods) == expected_periods(day, other):
        return [
            f"{len(periods)} periods make a v{other.version} file ({other.minutes}-minute "
            f"periods), but {day} is a v{contract.version} day ({contract.minutes}-minute periods; "
            f"the cut-over is {CUTOVER})"
        ]
    return [
        f"{len(periods)} periods, expected {expected} ({contract.minutes}-minute periods, "
        f"contract v{contract.version})"
    ]


def _mismatch(line: str) -> str:
    shown = repr(line[:60])
    if not line.endswith(";"):
        return f"{shown} does not end with ';'"
    fields = line[:-1].split(";")
    if len(fields) != 6:
        return f"{len(fields)} fields, expected 6 ({shown})"
    return f"{shown} is not YYYY;MM;DD;period;price_PT;price_ES; with decimal prices"
