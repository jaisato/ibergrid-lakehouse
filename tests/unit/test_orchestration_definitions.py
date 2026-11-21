"""The Dagster definitions: they load, and their partitions, pools and schedules say what ADR 0005
says, DST days included."""

import importlib
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
import pytest

from ibergrid.orchestration import definitions
from ibergrid.orchestration.assets import (
    OMIE_PARTITIONS,
    REDATA_ASSETS,
    REDATA_PARTITIONS,
    omie_day_ahead_prices,
)
from ibergrid.sources.redata.datasets import DATASETS

MADRID = ZoneInfo("Europe/Madrid")
SPRING, FALL = date(2026, 3, 29), date(2026, 10, 25)  # 23 and 25 hours in Madrid


def at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=MADRID)


def requested(schedule_name: str, when: datetime) -> list[dg.RunRequest]:
    schedule = definitions.defs.get_schedule_def(schedule_name)
    with dg.build_schedule_context(scheduled_execution_time=when) as context:
        result = schedule.evaluate_tick(context)
    return list(result.run_requests or [])


def test_the_definitions_load_and_validate() -> None:
    dg.Definitions.validate_loadable(definitions.defs)

    keys = {
        key.to_user_string() for key in definitions.defs.resolve_asset_graph().get_all_asset_keys()
    }
    assert keys == {f"bronze/redata_observations/{name}" for name in DATASETS} | {
        "bronze/omie_day_ahead_prices"
    }
    for job in ("redata_daily", "omie_afternoon", "omie_evening", "sweep_audit_branches"):
        assert definitions.defs.resolve_job_def(job).name == job


def test_redata_assets_share_one_pool_and_omie_has_its_own() -> None:
    assert {asset.op.pool for asset in REDATA_ASSETS} == {"redata"}
    assert omie_day_ahead_prices.op.pool == "omie"


def test_a_backfill_is_one_run_per_range() -> None:
    for asset in [*REDATA_ASSETS, omie_day_ahead_prices]:
        assert asset.backfill_policy == dg.BackfillPolicy.single_run()


def test_partitions_start_in_2023_and_end_yesterday_for_redata_tomorrow_for_omie() -> None:
    now = at(date(2026, 11, 21), 9)

    assert REDATA_PARTITIONS.get_first_partition_key() == "2023-01-01"
    assert OMIE_PARTITIONS.get_first_partition_key() == "2023-01-01"
    assert REDATA_PARTITIONS.get_partition_keys(current_time=now)[-1] == "2026-11-20"
    assert OMIE_PARTITIONS.get_partition_keys(current_time=now)[-1] == "2026-11-22"


@pytest.mark.parametrize("dst", [SPRING, FALL])
def test_dst_days_are_one_partition_each(dst: date) -> None:
    keys = REDATA_PARTITIONS.get_partition_keys(current_time=at(dst + timedelta(days=3), 9))
    around = [key for key in keys if dst - timedelta(days=2) <= date.fromisoformat(key)]

    assert around == [str(dst + timedelta(days=n)) for n in range(-2, 3)]
    window = REDATA_PARTITIONS.time_window_for_partition_key(str(dst))
    hours = (window.end.timestamp() - window.start.timestamp()) / 3600
    assert hours == (23 if dst == SPRING else 25)


def test_no_schedule_fires_in_the_hour_dst_skips_or_repeats() -> None:
    for schedule in definitions.defs.schedules or []:
        resolved = definitions.defs.get_schedule_def(schedule.name)
        hour = str(resolved.cron_schedule).split()[1]
        assert resolved.execution_timezone == "Europe/Madrid"
        assert hour.isdigit(), resolved.name
        assert int(hour) != 2, resolved.name


@pytest.mark.parametrize("dst", [SPRING, FALL])
def test_redata_asks_for_yesterday_once_a_day_across_dst(dst: date) -> None:
    days = [dst + timedelta(days=n) for n in range(-2, 4)]

    keys = [
        request.partition_key
        for day in days
        for request in requested("redata_daily_schedule", at(day, 8))
    ]

    assert keys == [str(day - timedelta(days=1)) for day in days]  # the DST day itself once


@pytest.mark.parametrize("schedule", ["omie_afternoon_schedule", "omie_evening_schedule"])
@pytest.mark.parametrize("dst", [SPRING, FALL])
def test_omie_asks_for_tomorrow_once_a_day_across_dst(schedule: str, dst: date) -> None:
    days = [dst + timedelta(days=n) for n in range(-3, 3)]
    hour = 16 if schedule == "omie_afternoon_schedule" else 21

    keys = [request.partition_key for day in days for request in requested(schedule, at(day, hour))]

    assert keys == [str(day + timedelta(days=1)) for day in days]


def test_the_omie_jobs_reingest_seven_days_and_only_the_evening_one_requires_publication() -> None:
    def config(job: str) -> object:
        partitioned = definitions.defs.resolve_job_def(job).partitioned_config
        assert partitioned is not None
        run_config = partitioned.get_run_config_for_partition_key("2026-11-22")
        return run_config["ops"][omie_day_ahead_prices.op.name]["config"]

    assert config("omie_afternoon") == {"trailing_days": 7, "require_published": False}
    assert config("omie_evening") == {"trailing_days": 7, "require_published": True}


@pytest.fixture
def reloaded() -> Iterator[None]:
    yield
    importlib.reload(definitions)  # the environment of the other tests


def test_schedules_start_stopped_unless_turned_on(
    monkeypatch: pytest.MonkeyPatch, reloaded: None
) -> None:
    def statuses() -> set[dg.DefaultScheduleStatus]:
        defs = importlib.reload(definitions).defs
        return {defs.get_schedule_def(s.name).default_status for s in defs.schedules or []}

    assert statuses() == {dg.DefaultScheduleStatus.STOPPED}
    monkeypatch.setenv("IBERGRID_ORCHESTRATION_SCHEDULES_RUNNING", "true")
    assert statuses() == {dg.DefaultScheduleStatus.RUNNING}
