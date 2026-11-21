from datetime import UTC, date, datetime

import pytest

from ibergrid.sources.redata.datasets import (
    DATASETS,
    Chunk,
    dataset_named,
    plan,
    points_per_day,
    utc_midnight,
)

REALTIME, HOURLY = DATASETS["demand_realtime"], DATASETS["demand_hourly"]
GENERATION = DATASETS["generation_daily"]


def windows(chunks: list[Chunk]) -> list[tuple[str, str, str]]:
    return [(c.system.name, str(c.start), str(c.end)) for c in chunks]


def test_sub_daily_datasets_split_at_calendar_months() -> None:
    chunks = plan(REALTIME, date(2026, 1, 15), date(2026, 3, 2))

    assert windows(chunks) == [
        ("peninsular", "2026-01-15", "2026-01-31"),
        ("peninsular", "2026-02-01", "2026-02-28"),
        ("peninsular", "2026-03-01", "2026-03-02"),
    ]


def test_daily_datasets_split_at_calendar_years() -> None:
    chunks = plan(GENERATION, date(2024, 6, 1), date(2026, 2, 1), ("canarias",))

    assert windows(chunks) == [
        ("canarias", "2024-06-01", "2024-12-31"),
        ("canarias", "2025-01-01", "2025-12-31"),
        ("canarias", "2026-01-01", "2026-02-01"),
    ]


def test_every_system_by_default_and_named_ones_once_in_order() -> None:
    day = date(2026, 11, 18)

    assert [c.system.name for c in plan(HOURLY, day, day)] == [
        "peninsular",
        "canarias",
        "baleares",
        "ceuta",
        "melilla",
    ]
    named = plan(HOURLY, day, day, ("melilla", "canarias", "melilla"))
    assert [c.system.name for c in named] == ["melilla", "canarias"]


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda: dataset_named("balance"), "unknown dataset 'balance'; expected one of: demand_"),
        (lambda: plan(HOURLY, date(2026, 1, 2), date(2026, 1, 1)), "end 2026-01-01 is before"),
        (lambda: plan(REALTIME, date(2026, 1, 1), date(2026, 1, 1), ("canarias",)), "no system"),
    ],
)
def test_bad_requests_are_refused(call: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        call()  # type: ignore[operator]


def test_windows_are_local_days_in_utc() -> None:
    winter = Chunk(HOURLY, HOURLY.systems[1], date(2026, 11, 18), date(2026, 11, 18))
    october = Chunk(REALTIME, REALTIME.systems[0], date(2026, 10, 1), date(2026, 10, 31))

    assert winter.window_utc == (
        datetime(2026, 11, 17, 23, tzinfo=UTC),
        datetime(2026, 11, 18, 23, tzinfo=UTC),
    )
    # Summer time at the start, winter time at the end.
    assert october.window_utc == (
        datetime(2026, 9, 30, 22, tzinfo=UTC),
        datetime(2026, 10, 31, 23, tzinfo=UTC),
    )
    assert utc_midnight(date(2026, 3, 29)) == datetime(2026, 3, 28, 23, tzinfo=UTC)


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 11, 18), {"5min": 288, "15min": 96, "hour": 24, "day": 1}),
        (date(2026, 3, 29), {"5min": 276, "15min": 92, "hour": 23, "day": 1}),
        (date(2026, 10, 25), {"5min": 300, "15min": 100, "hour": 25, "day": 1}),
    ],
)
def test_points_per_day_follow_daylight_saving(day: date, expected: dict[str, int]) -> None:
    assert {grain: points_per_day(day, grain) for grain in expected} == expected  # type: ignore[arg-type]


def test_request_parameters_landing_key_and_filter() -> None:
    canarias = Chunk(HOURLY, HOURLY.systems[1], date(2026, 11, 1), date(2026, 11, 30))
    national = Chunk(GENERATION, GENERATION.systems[0], date(2025, 1, 1), date(2025, 12, 31))

    assert canarias.params() == {
        "start_date": "2026-11-01T00:00",
        "end_date": "2026-11-30T23:59",
        "time_trunc": "hour",
        "geo_trunc": "electric_system",
        "geo_limit": "canarias",
        "geo_ids": "8742",
    }
    assert national.params() == {
        "start_date": "2025-01-01T00:00",
        "end_date": "2025-12-31T23:59",
        "time_trunc": "day",
    }
    assert canarias.landing_key == (
        "landing/redata/demand_hourly/canarias/2026/11/2026-11-01_2026-11-30.json"
    )
    assert canarias.slug == "demand_hourly-canarias-20261101-20261130"
    assert canarias.row_filter() == (
        "dataset = 'demand_hourly' AND electric_system = 'canarias' "
        "AND ts_utc >= '2026-10-31T23:00:00+00:00' AND ts_utc < '2026-11-30T23:00:00+00:00'"
    )


def test_value_rules() -> None:
    demand, prices = DATASETS["demand_hourly"].value_rule, DATASETS["market_prices"].value_rule
    assert demand is not None
    assert prices is not None
    assert (demand.check(0.1), demand.check(0)) == (True, False)
    assert [prices.check(v) for v in (-500, 4000, -500.01, 4000.01)] == [True, True, False, False]
    assert GENERATION.value_rule is None
