"""The Dagster definitions: they load, and their partitions, pools and schedules say what ADR 0005
says, DST days included."""

import importlib
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
import pytest

from ibergrid.lake.registry import MEDALLION_TABLES
from ibergrid.orchestration import definitions
from ibergrid.orchestration.assets import (
    OMIE_PARTITIONS,
    REDATA_ASSETS,
    REDATA_PARTITIONS,
    medallion_models,
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
    expected = (
        {f"bronze/redata_observations/{name}" for name in DATASETS}
        | {"bronze/omie_day_ahead_prices", "bronze/redata_demand_stream"}
        | {f"{spec.namespace}/{spec.name}" for spec in MEDALLION_TABLES}
    )
    assert keys == expected
    for job in (
        "redata_daily",
        "omie_afternoon",
        "omie_evening",
        "medallion_transform",
        "lake_maintenance",
        "sweep_audit_branches",
    ):
        assert definitions.defs.resolve_job_def(job).name == job


def test_redata_assets_share_one_pool_and_omie_and_transform_have_their_own() -> None:
    assert {asset.op.pool for asset in REDATA_ASSETS} == {"redata"}
    assert omie_day_ahead_prices.op.pool == "omie"
    assert medallion_models.op.pool == "transform"


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


def tick_config(request: dg.RunRequest, asset: dg.AssetsDefinition) -> dict[str, object]:
    config: dict[str, object] = request.run_config["ops"][asset.op.name]["config"]
    return config


@pytest.mark.parametrize("dst", [SPRING, FALL])
def test_redata_asks_for_yesterday_once_a_day_across_dst(dst: date) -> None:
    days = [dst + timedelta(days=n) for n in range(-2, 4)]

    requests = [r for day in days for r in requested("redata_daily_schedule", at(day, 9))]

    assert [r.partition_key for r in requests] == [str(d - timedelta(days=1)) for d in days]
    # Every dataset also ingests the 3 days before again (the tick carries the job's config).
    assert {tick_config(requests[0], asset)["trailing_days"] for asset in REDATA_ASSETS} == {3}


@pytest.mark.parametrize(
    ("schedule", "hour", "minute", "required"),
    [("omie_afternoon_schedule", 16, 0, False), ("omie_evening_schedule", 21, 30, True)],
)
@pytest.mark.parametrize("dst", [SPRING, FALL])
def test_omie_asks_for_tomorrow_once_a_day_across_dst(
    schedule: str, hour: int, minute: int, required: bool, dst: date
) -> None:
    days = [dst + timedelta(days=n) for n in range(-3, 3)]

    requests = [r for day in days for r in requested(schedule, at(day, hour, minute))]

    assert [r.partition_key for r in requests] == [str(d + timedelta(days=1)) for d in days]
    # The tick carries the job's config: a run launched by hand without it gets the defaults.
    assert tick_config(requests[0], omie_day_ahead_prices) == {
        "trailing_days": 7,
        "require_published": required,
    }


def test_the_schedules_run_at_their_madrid_times() -> None:
    times = {
        s.name: str(definitions.defs.get_schedule_def(s.name).cron_schedule).split()[:2]
        for s in definitions.defs.schedules or []
    }

    assert times == {
        "lake_maintenance_schedule": ["0", "4"],
        "redata_daily_schedule": ["0", "9"],
        "medallion_morning_schedule": ["30", "9"],
        "omie_afternoon_schedule": ["0", "16"],
        "medallion_afternoon_schedule": ["30", "16"],
        "omie_evening_schedule": ["30", "21"],
        "sweep_audit_branches_schedule": ["30", "5"],
    }


def test_scheduled_jobs_retry_a_failed_step_twice_and_backfills_do_not() -> None:
    for job in ("redata_daily", "omie_afternoon", "omie_evening", "medallion_transform"):
        policy = definitions.defs.resolve_job_def(job).op_retry_policy
        assert policy == dg.RetryPolicy(max_retries=2, delay=900), job
    assert definitions.defs.resolve_implicit_global_asset_job_def().op_retry_policy is None


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
