"""Normalisation, validation and completeness on recorded REData answers."""

import json
from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
import pytest

from ibergrid.lake.registry import REDATA_OBSERVATIONS
from ibergrid.sources.redata.datasets import DATASETS, Chunk
from ibergrid.sources.redata.normalize import (
    InvalidResponse,
    completeness,
    content_sha256,
    granularities,
    parse,
    to_arrow,
    violations,
)

from .conftest import fixture


def chunk(dataset: str, system: str, start: str, end: str | None = None) -> Chunk:
    spec = DATASETS[dataset]
    [found] = [s for s in spec.systems if s.name == system]
    return Chunk(spec, found, date.fromisoformat(start), date.fromisoformat(end or start))


def recorded(c: Chunk) -> bytes:
    return fixture(f"{c.dataset.name}-{c.system.name}-{c.start}_{c.end}.json")


def document(c: Chunk) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(recorded(c))
    return parsed


def problems_of(c: Chunk, doc: object) -> list[str]:
    with pytest.raises(InvalidResponse) as failure:
        parse(json.dumps(doc).encode(), c)
    return failure.value.problems


@pytest.mark.parametrize(
    ("dataset", "system", "day", "series", "grains", "expected"),
    [
        ("demand_realtime", "national", "2026-11-18", 4, {"5min"}, 4 * 288),
        ("demand_realtime", "national", "2026-03-29", 1, {"5min"}, 276),
        ("demand_realtime", "national", "2026-10-25", 1, {"5min"}, 300),
        ("demand_hourly", "peninsular", "2026-11-18", 1, {"hour"}, 24),
        ("demand_hourly", "peninsular", "2026-03-29", 1, {"hour"}, 23),
        ("demand_hourly", "peninsular", "2026-10-25", 1, {"hour"}, 25),
        ("demand_hourly", "canarias", "2026-11-18", 1, {"hour"}, 24),
        ("generation_daily", "national", "2026-11-18", 15, {"day"}, 15),
        ("generation_daily", "canarias", "2026-11-18", 7, {"day"}, 7),
        ("market_prices", "peninsular", "2026-11-18", 2, {"15min", "hour"}, 96 + 24),
        ("market_prices", "peninsular", "2026-03-29", 2, {"15min", "hour"}, 92 + 23),
        ("market_prices", "peninsular", "2026-10-25", 2, {"15min", "hour"}, 100 + 25),
        ("market_prices", "peninsular", "2024-12-15", 2, {"hour"}, 24 + 24),
    ],
)
def test_recorded_days_are_complete(
    dataset: str, system: str, day: str, series: int, grains: set[str], expected: int
) -> None:
    c = chunk(dataset, system, day)

    points = parse(recorded(c), c)
    found = granularities(points, c)
    measured = completeness(points, found, c)

    assert len(found) == series
    assert set(found.values()) == grains
    assert measured.days == ((date.fromisoformat(day), expected, expected),)
    assert (measured.expected, measured.received, measured.incomplete) == (expected, expected, [])


def test_the_25_hour_day_keeps_both_two_oclocks_apart() -> None:
    c = chunk("demand_hourly", "peninsular", "2026-10-25")

    points = parse(recorded(c), c)

    twos = [(p.ts_local, p.ts_utc) for p in points if p.ts_local.startswith("2026-10-25T02")]
    assert twos == [
        ("2026-10-25T02:00:00.000+02:00", datetime(2026, 10, 25, 0, tzinfo=UTC)),
        ("2026-10-25T02:00:00.000+01:00", datetime(2026, 10, 25, 1, tzinfo=UTC)),
    ]


def test_total_scheduled_exists_only_in_recent_data() -> None:
    old, new = (
        chunk("demand_realtime", "national", "2023-06-15"),
        chunk("demand_realtime", "national", "2026-11-18"),
    )

    old_series = {p.series_title for p in parse(recorded(old), old)}
    new_series = {p.series_title for p in parse(recorded(new), new)}

    assert old_series == {"Real", "Forecasted"}  # the recorder keeps two of the three
    assert new_series == {"Real", "Forecasted", "Scheduled", "Total scheduled"}


def test_canarias_is_delivered_in_peninsular_time() -> None:
    c = chunk("demand_hourly", "canarias", "2026-11-18")

    first = parse(recorded(c), c)[0]

    assert first.ts_local == "2026-11-18T00:00:00.000+01:00"
    assert first.ts_utc == datetime(2026, 11, 17, 23, tzinfo=UTC)
    assert (first.series_id, first.series_type, first.series_title) == ("10297", "Demand", "Demand")
    assert first.series_last_update is not None
    assert first.series_last_update.tzinfo is not None


def test_spot_price_granularity_is_measured_per_series() -> None:
    before, after = (
        chunk("market_prices", "peninsular", "2024-12-15"),
        chunk("market_prices", "peninsular", "2026-11-18"),
    )

    assert granularities(parse(recorded(before), before), before) == {"1001": "hour", "600": "hour"}
    assert granularities(parse(recorded(after), after), after) == {"1001": "hour", "600": "15min"}


def test_missing_points_show_as_incomplete_days() -> None:
    c = chunk("demand_hourly", "canarias", "2026-11-18", "2026-11-19")  # one recorded day of two
    doc = document(chunk("demand_hourly", "canarias", "2026-11-18"))
    doc["included"][0]["attributes"]["values"] = doc["included"][0]["attributes"]["values"][:20]

    points = parse(json.dumps(doc).encode(), c)
    measured = completeness(points, granularities(points, c), c)

    assert measured.days == ((date(2026, 11, 18), 24, 20), (date(2026, 11, 19), 24, 0))
    assert measured.incomplete == list(measured.days)


def test_a_single_point_takes_the_nominal_granularity() -> None:
    c = chunk("demand_hourly", "canarias", "2026-11-18")
    doc = document(c)
    doc["included"][0]["attributes"]["values"] = doc["included"][0]["attributes"]["values"][:1]

    points = parse(json.dumps(doc).encode(), c)

    assert granularities(points, c) == {"10297": "hour"}


def test_a_spacing_that_matches_no_granularity_is_refused() -> None:
    c = chunk("demand_hourly", "canarias", "2026-11-18")
    doc = document(c)
    values = doc["included"][0]["attributes"]["values"]
    values[1]["datetime"] = "2026-11-18T00:07:00.000+01:00"

    with pytest.raises(InvalidResponse, match="points 420 s apart match no granularity"):
        granularities(parse(json.dumps(doc).encode(), c), c)


def test_rows_match_the_bronze_schema() -> None:
    c = chunk("market_prices", "peninsular", "2026-11-18")
    points = parse(recorded(c), c)
    fetched = datetime(2026, 11, 21, 4, tzinfo=UTC)

    table = to_arrow(
        points,
        granularities(points, c),
        c,
        fetched_at=fetched,
        run_id="r1",
        landing_path="s3://ibergrid/landing/x.json",
        payload_sha256="0" * 64,
    )

    assert table.schema == REDATA_OBSERVATIONS.schema.as_arrow()
    assert table.num_rows == 120
    row = table.filter(pa.compute.equal(table["series_id"], "600")).to_pylist()[0]
    assert row | {"series_last_update": None} == {
        "dataset": "market_prices",
        "widget": "mercados/precios-mercados-tiempo-real",
        "electric_system": "peninsular",
        "series_id": "600",
        "series_type": "Spot market price",
        "series_title": "Spot market price",
        "granularity": "15min",
        "ts_utc": datetime(2026, 11, 17, 23, tzinfo=UTC),
        "ts_local": "2026-11-18T00:00:00.000+01:00",
        "value": row["value"],
        "percentage": row["percentage"],
        "series_last_update": None,
        "fetched_at": fetched,
        "run_id": "r1",
        "landing_path": "s3://ibergrid/landing/x.json",
        "payload_sha256": "0" * 64,
    }
    assert isinstance(row["value"], float)
    assert 0 <= row["percentage"] <= 1


# --- validation: any problem refuses the whole chunk ------------------------------------------

HOURLY = chunk("demand_hourly", "canarias", "2026-11-18")
PRICES = chunk("market_prices", "peninsular", "2026-11-18")


def with_point(c: Chunk, **changes: Any) -> dict[str, Any]:
    doc = document(c)
    doc["included"][0]["attributes"]["values"][0] |= changes
    return doc


def scoped(included: str, answer: str = "dem1", title: str = "Evolución de la demanda") -> bytes:
    data = json.dumps({"id": answer, "type": title}, ensure_ascii=False)
    return f'{{"data": {data}, "included": {included}}}'.encode()


@pytest.mark.parametrize(
    ("payload", "problem"),
    [
        (b"<html>", "not JSON (Expecting value: line 1 column 1 (char 0))"),
        (b"[]", "no included[] list of series"),
        (b'{"data": {}}', "no included[] list of series"),
        (
            b'{"included": []}',
            "the answer is None (None), not 'dem1': REData changed what this widget returns",
        ),
        (
            scoped("[]", answer="dem15", title="Demanda nacional en tiempo real"),
            "the answer is 'dem15' ('Demanda nacional en tiempo real'), not 'dem1': "
            "REData changed what this widget returns",
        ),
        # The id decides, not the display string, which REData could translate.
        (
            scoped("[]", title="Demand evolution"),
            "no observations: the window would be replaced with nothing",
        ),
        (scoped("[]"), "no observations: the window would be replaced with nothing"),
        (scoped("[1]"), "a series without attributes"),
        (scoped('[{"attributes": {"title": "Demand"}}]'), "a series without an id ('Demand')"),
        (scoped('[{"id": "1", "attributes": {}}]'), "series 1: no values[] list"),
    ],
)
def test_malformed_answers(payload: bytes, problem: str) -> None:
    with pytest.raises(InvalidResponse) as failure:
        parse(payload, HOURLY)

    assert failure.value.problems == [problem]
    assert str(failure.value).startswith("demand_hourly-canarias-20261118-20261118: invalid REData")


@pytest.mark.parametrize(
    ("changes", "problem"),
    [
        ({"datetime": "2026-11-18T00:00:00"}, "a point without a datetime with an offset"),
        ({"datetime": "yesterday"}, "a point without a datetime with an offset"),
        ({"datetime": None}, "a point without a datetime with an offset"),
        ({"value": None}, "value None"),
        ({"value": "888.2"}, "value '888.2'"),
        ({"value": True}, "value True"),
        ({"value": float("nan")}, "value nan"),
        ({"percentage": "1"}, "percentage '1'"),
        ({"percentage": float("inf")}, "percentage inf"),
        (
            {"datetime": "2026-11-17T23:00:00.000+01:00"},
            "outside the window 2026-11-18..2026-11-18",
        ),
        ({"datetime": "2026-11-19T00:00:00.000+01:00"}, "outside the window"),
        ({"datetime": "2026-11-18T01:00:00.000+01:00"}, "2 points"),
    ],
)
def test_invalid_points(changes: dict[str, Any], problem: str) -> None:
    problems = problems_of(HOURLY, with_point(HOURLY, **changes))

    assert len(problems) == 1, problems
    assert problem in problems[0]


@pytest.mark.parametrize(
    ("c", "changes", "counted"),
    [
        (HOURLY, {"value": 0}, {"10297": 1}),
        (HOURLY, {"value": -3.5, "percentage": 1.5}, {"10297": 1}),  # one point, counted once
        (HOURLY, {"percentage": -0.1}, {"10297": 1}),
        (PRICES, {"value": 4000.5}, {"1001": 1}),
        (PRICES, {"value": -500}, {}),
        (chunk("generation_daily", "national", "2026-11-18"), {"value": -120.0}, {}),
    ],
)
def test_value_rule_breaches_are_counted_not_refused(
    c: Chunk, changes: dict[str, Any], counted: dict[str, int]
) -> None:
    points = parse(json.dumps(with_point(c, **changes)).encode(), c)

    assert violations(points, c) == counted
    assert points[0].percentage == changes.get("percentage", points[0].percentage)


def test_recorded_answers_break_no_rule() -> None:
    for c in (HOURLY, PRICES, chunk("demand_realtime", "national", "2026-11-18")):
        assert violations(parse(recorded(c), c), c) == {}


def test_a_long_daily_window_ending_mid_month_is_refused() -> None:
    """REData answers it up to the month's end (2025-10-31 here); plan() never asks for it."""
    leaking = chunk("generation_daily", "canarias", "2025-02-10", "2025-10-20")

    with pytest.raises(InvalidResponse) as failure:
        parse(recorded(leaking), leaking)

    assert len(failure.value.problems) == 11  # 2025-10-21 .. 2025-10-31
    assert "outside the window 2025-02-10..2025-10-20" in failure.value.problems[0]
    for start, end in (("2025-02-10", "2025-09-30"), ("2025-10-01", "2025-10-20")):
        exact = chunk("generation_daily", "canarias", start, end)
        points = parse(recorded(exact), exact)
        assert {p.ts_local[:10] for p in points} <= {str(day) for day in exact.days}


def test_a_null_percentage_is_fine() -> None:
    assert (
        parse(json.dumps(with_point(HOURLY, percentage=None)).encode(), HOURLY)[0].percentage
        is None
    )


def test_an_unusable_last_update_is_a_problem() -> None:
    doc = document(HOURLY)
    doc["included"][0]["attributes"]["last-update"] = "2026-11-18"

    assert problems_of(HOURLY, doc) == [
        "series 10297: last-update '2026-11-18' has no usable offset"
    ]


def test_every_problem_is_counted_and_the_message_stays_short() -> None:
    doc = document(HOURLY)
    for point in doc["included"][0]["attributes"]["values"]:
        point["value"] = "n/a"

    with pytest.raises(InvalidResponse) as failure:
        parse(json.dumps(doc).encode(), HOURLY)

    assert len(failure.value.problems) == 24
    assert str(failure.value).endswith("value 'n/a' (and 19 more)")


def test_daily_points_are_days_apart_also_across_daylight_saving() -> None:
    c = chunk("generation_daily", "canarias", "2026-03-29", "2026-03-30")  # 23 hours apart
    doc = document(chunk("generation_daily", "canarias", "2026-11-18"))
    for item in doc["included"]:
        [point] = item["attributes"]["values"]
        item["attributes"]["values"] = [
            point | {"datetime": "2026-03-29T00:00:00.000+01:00"},
            point | {"datetime": "2026-03-30T00:00:00.000+02:00"},
        ]

    points = parse(json.dumps(doc).encode(), c)

    assert set(granularities(points, c).values()) == {"day"}
    assert completeness(points, granularities(points, c), c).incomplete == []


def test_the_content_hash_ignores_only_redatas_cache_metadata() -> None:
    raw = recorded(HOURLY)
    hit = document(HOURLY)
    hit["data"]["meta"] = {"cache-control": {"cache": "HIT", "expireAt": "2026-12-21T04:44:58"}}
    changed = document(HOURLY)
    changed["included"][0]["attributes"]["values"][0]["value"] += 1

    assert document(HOURLY)["data"]["meta"] == {"cache-control": {"cache": "MISS"}}
    assert content_sha256(json.dumps(hit, indent=2).encode()) == content_sha256(raw)
    assert content_sha256(json.dumps(changed).encode()) != content_sha256(raw)
