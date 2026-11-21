"""From a REData JSON:API answer to Bronze rows, validated before anything is written.

The answer lists one ``included[]`` item per series (``id``, ``type``, ``attributes.title``,
``attributes.last-update`` and ``attributes.values[]`` of ``{value, percentage, datetime}``).
Structural problems refuse the whole chunk: another widget scope (``data.type``), shape and
types, a number that is not finite, a datetime without an offset, a point outside the window or a
repeated key. Everything else is measured, not enforced, because Bronze keeps what the source
gave (quality gates are phase 4): completeness per local day, and the points that break the
dataset's value rule or have a percentage outside 0..1.
"""

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import pairwise
from typing import Any

import pyarrow as pa

from ibergrid.lake.registry import REDATA_OBSERVATIONS
from ibergrid.sources.redata.client import RedataError
from ibergrid.sources.redata.datasets import (
    MADRID,
    STEP_SECONDS,
    Chunk,
    Granularity,
    points_per_day,
)

ARROW_SCHEMA = REDATA_OBSERVATIONS.schema.as_arrow()
MAX_PROBLEMS_SHOWN = 5


class InvalidResponse(RedataError):
    """The answer is not what the dataset expects: nothing was landed or written."""

    def __init__(self, chunk: Chunk, problems: list[str]) -> None:
        shown = "; ".join(problems[:MAX_PROBLEMS_SHOWN])
        more = len(problems) - MAX_PROBLEMS_SHOWN
        suffix = f" (and {more} more)" if more > 0 else ""
        super().__init__(f"{chunk.slug}: invalid REData answer: {shown}{suffix}")
        self.problems = problems


@dataclass(frozen=True)
class Point:
    series_id: str
    series_type: str | None
    series_title: str | None
    series_last_update: datetime | None
    ts_utc: datetime
    ts_local: str
    value: float
    percentage: float | None


@dataclass(frozen=True)
class Completeness:
    """Expected and received points per local day, summed over the chunk's series."""

    days: tuple[tuple[date, int, int], ...]

    @property
    def expected(self) -> int:
        return sum(expected for _, expected, _ in self.days)

    @property
    def received(self) -> int:
        return sum(received for _, _, received in self.days)

    @property
    def incomplete(self) -> list[tuple[date, int, int]]:
        return [day for day in self.days if day[1] != day[2]]


def parse(payload: bytes, chunk: Chunk) -> list[Point]:
    """Every point of the answer, or ``InvalidResponse`` listing every problem found."""
    try:
        document = json.loads(payload)
    except ValueError as exc:
        raise InvalidResponse(chunk, [f"not JSON ({exc})"]) from exc
    included = document.get("included") if isinstance(document, dict) else None
    if not isinstance(included, list):
        raise InvalidResponse(chunk, ["no included[] list of series"])
    data = document.get("data")
    answer_type = data.get("type") if isinstance(data, dict) else None
    if answer_type != chunk.dataset.answer_type:
        raise InvalidResponse(
            chunk,
            [
                f"the answer is {answer_type!r}, not {chunk.dataset.answer_type!r}: "
                "REData changed what this widget returns"
            ],
        )
    problems: list[str] = []
    points = [point for item in included for point in _series(item, problems)]
    problems += _check(points, chunk)
    if not points and not problems:
        problems.append("no observations: the window would be replaced with nothing")
    if problems:
        raise InvalidResponse(chunk, problems)
    return points


def violations(points: list[Point], chunk: Chunk) -> dict[str, int]:
    """Per series, the points whose value breaks the dataset's rule or whose percentage is
    outside 0..1: recorded with the run, not refused."""
    rule = chunk.dataset.value_rule
    found = Counter(
        point.series_id
        for point in points
        if (rule is not None and not rule.check(point.value))
        or (point.percentage is not None and not 0 <= point.percentage <= 1)
    )
    return dict(sorted(found.items()))


def granularities(points: list[Point], chunk: Chunk) -> dict[str, Granularity]:
    """Each series' spacing as delivered (the spot price went from hourly to quarter-hourly on
    2025-01-01); a series with fewer than two points takes the dataset's nominal one."""
    stamps: dict[str, set[datetime]] = defaultdict(set)
    for point in points:
        stamps[point.series_id].add(point.ts_utc)
    found: dict[str, Granularity] = {}
    problems = []
    for series, times in stamps.items():
        ordered = sorted(times)
        if len(ordered) < 2:
            found[series] = chunk.dataset.granularity
            continue
        step = min(int((b - a).total_seconds()) for a, b in pairwise(ordered))
        grain = _granularity(step)
        if grain is None:
            problems.append(f"series {series}: points {step} s apart match no granularity")
        else:
            found[series] = grain
    if problems:
        raise InvalidResponse(chunk, problems)
    return found


def completeness(points: list[Point], grains: dict[str, Granularity], chunk: Chunk) -> Completeness:
    received = Counter(point.ts_utc.astimezone(MADRID).date() for point in points)
    return Completeness(
        tuple(
            (day, sum(points_per_day(day, grain) for grain in grains.values()), received[day])
            for day in chunk.days
        )
    )


def to_arrow(
    points: list[Point],
    grains: dict[str, Granularity],
    chunk: Chunk,
    *,
    fetched_at: datetime,
    run_id: str,
    landing_path: str,
    payload_sha256: str,
) -> pa.Table:
    constant = {
        "dataset": chunk.dataset.name,
        "widget": chunk.dataset.widget,
        "electric_system": chunk.system.name,
        "fetched_at": fetched_at,
        "run_id": run_id,
        "landing_path": landing_path,
        "payload_sha256": payload_sha256,
    }
    rows = [
        {
            **constant,
            "series_id": point.series_id,
            "series_type": point.series_type,
            "series_title": point.series_title,
            "granularity": grains[point.series_id],
            "ts_utc": point.ts_utc,
            "ts_local": point.ts_local,
            "value": point.value,
            "percentage": point.percentage,
            "series_last_update": point.series_last_update,
        }
        for point in points
    ]
    return pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)


def _series(item: Any, problems: list[str]) -> list[Point]:
    if not isinstance(item, dict) or not isinstance(item.get("attributes"), dict):
        problems.append("a series without attributes")
        return []
    series_id, attributes = item.get("id"), item["attributes"]
    if not isinstance(series_id, str) or not series_id:
        problems.append(f"a series without an id ({attributes.get('title')!r})")
        return []
    values = attributes.get("values")
    if not isinstance(values, list):
        problems.append(f"series {series_id}: no values[] list")
        return []
    raw_update = attributes.get("last-update")
    last_update = _timestamp(raw_update)
    if raw_update is not None and last_update is None:
        problems.append(f"series {series_id}: last-update {raw_update!r} has no usable offset")
    series_type, title = _text(item.get("type")), _text(attributes.get("title"))
    points = []
    for raw in values:
        stamp = _timestamp(raw.get("datetime")) if isinstance(raw, dict) else None
        if stamp is None:
            problems.append(f"series {series_id}: a point without a datetime with an offset")
            continue
        value, percentage = raw.get("value"), raw.get("percentage")
        if not _finite(value):
            problems.append(f"series {series_id} at {raw['datetime']}: value {value!r}")
            continue
        if percentage is not None and not _finite(percentage):
            problems.append(f"series {series_id} at {raw['datetime']}: percentage {percentage!r}")
            continue
        points.append(
            Point(
                series_id,
                series_type,
                title,
                last_update,
                stamp.astimezone(UTC),
                raw["datetime"],
                float(value),
                None if percentage is None else float(percentage),
            )
        )
    return points


def _check(points: list[Point], chunk: Chunk) -> list[str]:
    start, end = chunk.window_utc
    keys = Counter((point.series_id, point.ts_utc) for point in points)
    problems = [
        f"series {series} at {ts:%Y-%m-%dT%H:%MZ}: {count} points"
        for (series, ts), count in keys.items()
        if count > 1
    ]
    for point in points:
        where = f"series {point.series_id} at {point.ts_local}"
        if not start <= point.ts_utc < end:
            problems.append(f"{where}: outside the window {chunk.start}..{chunk.end}")
    return problems


def _granularity(step: int) -> Granularity | None:
    if step >= 23 * 3600:  # local midnights are 23, 24 or 25 hours apart
        return "day"
    for grain in ("hour", "15min", "5min"):
        if step % STEP_SECONDS[grain] == 0:
            return grain
    return None


def _timestamp(value: object) -> datetime | None:
    """The datetime of an ISO string with an offset, else None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _finite(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None
