"""What the Dagster code server loads: assets, jobs, schedules and the resource (ADR 0005).

Schedules run in Madrid time, none between 02:00 and 03:00 (skipped or repeated on DST days), so
each fires once a day. They start stopped unless ``IBERGRID_ORCHESTRATION_SCHEDULES_RUNNING`` is
true (production), so a local stack never calls the sources on its own.
"""

import dagster as dg

from ibergrid.orchestration.assets import (
    REDATA_ASSETS,
    IngestConfig,
    OmieConfig,
    omie_day_ahead_prices,
)
from ibergrid.orchestration.resources import Lake
from ibergrid.orchestration.sweeper import sweep_audit_branches
from ibergrid.settings import OrchestrationSettings

TIMEZONE = "Europe/Madrid"
# Every scheduled REData run ingests the 3 days before yesterday again (late points, revisions,
# a failed day): usually the same one request per system, two across a month boundary.
REDATA_TRAILING_DAYS = 3
# Every scheduled OMIE run ingests the last 7 delivery days again: OMIE's listing shows every
# in-place rewrite and republication by D+2 (ADR 0004).
OMIE_TRAILING_DAYS = 7
# A scheduled run whose step fails runs it again twice, 15 minutes apart (a source not ready yet,
# a transient outage); backfills are not retried, since a retry redoes the whole range.
SCHEDULED_RETRIES = dg.RetryPolicy(max_retries=2, delay=900)
STATUS = (
    dg.DefaultScheduleStatus.RUNNING
    if OrchestrationSettings().schedules_running
    else dg.DefaultScheduleStatus.STOPPED
)


def _omie_config(require_published: bool) -> dg.RunConfig:
    config = OmieConfig(trailing_days=OMIE_TRAILING_DAYS, require_published=require_published)
    return dg.RunConfig(ops={omie_day_ahead_prices.op.name: config})


# Each job's partitions are its assets' (daily, Madrid).
redata_daily = dg.define_asset_job(
    "redata_daily",
    selection=REDATA_ASSETS,
    config=dg.RunConfig(
        ops={
            asset.op.name: IngestConfig(trailing_days=REDATA_TRAILING_DAYS)
            for asset in REDATA_ASSETS
        }
    ),
    op_retry_policy=SCHEDULED_RETRIES,
    description="Every REData dataset for yesterday (Madrid), and the 3 days before again.",
)
omie_afternoon = dg.define_asset_job(
    "omie_afternoon",
    selection=[omie_day_ahead_prices],
    config=_omie_config(require_published=False),
    op_retry_policy=SCHEDULED_RETRIES,
    description="Tomorrow's OMIE prices if already out, and the last 7 delivery days again.",
)
omie_evening = dg.define_asset_job(
    "omie_evening",
    selection=[omie_day_ahead_prices],
    config=_omie_config(require_published=True),
    op_retry_policy=SCHEDULED_RETRIES,
    description="Tomorrow's OMIE prices, which must be out by now, and the last 7 days again.",
)

defs = dg.Definitions(
    assets=[*REDATA_ASSETS, omie_day_ahead_prices],
    jobs=[redata_daily, omie_afternoon, omie_evening, sweep_audit_branches],
    schedules=[
        # Yesterday: on 2026-11-21 every dataset's D-1 was complete at 08:50 and 09:46, REData's
        # last update of hourly demand and daily generation having been at 06:47 (ADR 0005).
        dg.build_schedule_from_partitioned_job(
            redata_daily, hour_of_day=9, minute_of_hour=0, default_status=STATUS
        ),
        # Tomorrow, "not published yet" tolerated: 91 % of the 15-minute era's files were final by
        # 16:00 on D-1.
        dg.build_schedule_from_partitioned_job(
            omie_afternoon, hour_of_day=16, minute_of_hour=0, default_status=STATUS
        ),
        # Tomorrow, required: 97 % final by 21:30. A later file fails this run (retried at 21:45
        # and 22:00), and the next day's trailing window ingests it.
        dg.build_schedule_from_partitioned_job(
            omie_evening, hour_of_day=21, minute_of_hour=30, default_status=STATUS
        ),
        dg.ScheduleDefinition(
            job=sweep_audit_branches,
            cron_schedule="30 5 * * *",
            execution_timezone=TIMEZONE,
            default_status=STATUS,
        ),
    ],
    resources={"lake": Lake()},
    # One process per run, steps in sequence: a run's assets of one pool never run side by side.
    executor=dg.in_process_executor,
)
