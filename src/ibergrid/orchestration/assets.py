"""Bronze assets: one per REData dataset and one for OMIE, partitioned by Madrid day (ADR 0005).

A run hands its whole partition range to one connector call, which cuts it into the connector's
own chunks (calendar months, or years for daily generation) with one Write-Audit-Publish run each.
Partitions are recorded as materialized as those chunks publish, each with the measurements that
the snapshot summaries hold only until snapshot expiry: a run that fails part way keeps the
partitions it published, and the asset itself never yields an output.
"""

from collections import defaultdict
from collections.abc import Iterator
from datetime import date, timedelta

import dagster as dg
from pydantic import Field

from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES, REDATA_OBSERVATIONS
from ibergrid.orchestration.resources import Lake
from ibergrid.sources.omie import ingest as omie
from ibergrid.sources.redata import ingest as redata
from ibergrid.sources.redata.datasets import DATASETS, Dataset

START = "2023-01-01"  # OMIE's first daily file; the project's history starts there for both
FIRST_DAY = date.fromisoformat(START)
TIMEZONE = "Europe/Madrid"  # both sources' days
# The last partition is yesterday, the last complete Madrid day: for today REData answers 502, or
# provisional points once its morning update is out (ADR 0005).
REDATA_PARTITIONS = dg.DailyPartitionsDefinition(start_date=START, timezone=TIMEZONE)
# OMIE publishes delivery day D on D-1, so tomorrow is a partition (end_offset=1 ends at today).
OMIE_PARTITIONS = dg.DailyPartitionsDefinition(start_date=START, timezone=TIMEZONE, end_offset=2)
# A backfilled range is one run and one connector call. Dagster's multi_run would split it by a
# number of partitions, not at calendar months, and break the connectors' month and year chunks.
SINGLE_RUN = dg.BackfillPolicy.single_run()
# One writer per table and one request a second per source (ADR 0003, ADR 0004): dagster.yaml
# gives every pool a limit of 1.
REDATA_POOL = "redata"
OMIE_POOL = "omie"


class IngestConfig(dg.Config):
    """An asset's run configuration: the schedules set it (ADR 0005), a backfill keeps the
    defaults."""

    # Also re-ingest this many days before the range, for late points and corrections (REE's
    # revisions; OMIE's files rewritten up to D+2), never before the history starts.
    trailing_days: int = Field(default=0, ge=0)


class OmieConfig(IngestConfig):
    # Days after today that OMIE has not published fail the run instead of ending it quietly.
    require_published: bool = False


def _days(context: dg.AssetExecutionContext, trailing_days: int) -> tuple[date, date]:
    keys = context.partition_key_range
    start = date.fromisoformat(keys.start) - timedelta(days=trailing_days)
    return max(start, FIRST_DAY), date.fromisoformat(keys.end)


def redata_asset(dataset: Dataset) -> dg.AssetsDefinition:
    systems = ", ".join(system.name for system in dataset.systems)

    @dg.asset(
        key=dg.AssetKey(["bronze", "redata_observations", dataset.name]),
        description=(
            f"REData `{dataset.widget}` in bronze.redata_observations, one request per "
            f"{dataset.chunk} and system ({systems}); ADR 0003."
        ),
        group_name="bronze",
        kinds={"iceberg"},
        partitions_def=REDATA_PARTITIONS,
        backfill_policy=SINGLE_RUN,
        pool=REDATA_POOL,
        output_required=False,
    )
    def _asset(
        context: dg.AssetExecutionContext, config: IngestConfig, lake: Lake
    ) -> Iterator[dg.Output[None]]:
        start, end = _days(context, config.trailing_days)
        windows: dict[tuple[date, date], list[redata.ChunkReport]] = defaultdict(list)

        def on_chunk(report: redata.ChunkReport) -> None:
            # The plan goes window by window: a day is done once every system's chunk holding it is.
            reports = windows[(report.chunk.start, report.chunk.end)]
            reports.append(report)
            if len(reports) == len(dataset.systems):
                for day in report.chunk.days:
                    metadata = redata_metadata(day, reports)
                    context.log_event(
                        dg.AssetMaterialization(
                            context.asset_key, partition=str(day), metadata=metadata
                        )
                    )

        redata.ingest(
            lake.settings(),
            dataset.name,
            start,
            end,
            run_id_prefix=context.run.run_id,  # audit-<run id>-...: the sweeper's ownership test
            on_chunk=on_chunk,
            table=lake.table(REDATA_OBSERVATIONS),
            landing_prefix=lake.landing(redata.LANDING_PREFIX),
        )
        yield from ()

    return _asset


def redata_metadata(day: date, reports: list[redata.ChunkReport]) -> dict[str, dg.MetadataValue]:
    """The day's completeness per system, and its chunks' measurements (one per system)."""
    points = {}
    for report in reports:
        [(expected, received)] = [(e, r) for d, e, r in report.completeness.days if d == day]
        points[report.chunk.system.name] = {"expected": expected, "received": received}
    expected = sum(counts["expected"] for counts in points.values())
    received = sum(counts["received"] for counts in points.values())
    chunks = [
        {
            "system": report.chunk.system.name,
            "rows": report.rows,
            "rule_violations": report.violations,
            "dropped_series": list(report.dropped_series),
            "snapshot_id": str(report.snapshot_id),  # 63-bit ids lose digits as JSON numbers
            "run_id": report.run_id,
            "landing_path": report.landing_path,
        }
        for report in reports
    ]
    return {
        "complete": dg.MetadataValue.bool(expected == received),
        "expected_points": dg.MetadataValue.int(expected),
        "received_points": dg.MetadataValue.int(received),
        "points": dg.MetadataValue.json(points),
        "window": dg.MetadataValue.text(f"{reports[0].chunk.start}/{reports[0].chunk.end}"),
        "chunks": dg.MetadataValue.json(chunks),
    }


REDATA_ASSETS = [redata_asset(dataset) for dataset in DATASETS.values()]


@dg.asset(
    key=dg.AssetKey(["bronze", "omie_day_ahead_prices"]),
    description=(
        "OMIE marginalpdbc day-ahead prices (Spain and Portugal) in "
        "bronze.omie_day_ahead_prices, one file per delivery day; ADR 0004."
    ),
    group_name="bronze",
    kinds={"iceberg"},
    partitions_def=OMIE_PARTITIONS,
    backfill_policy=SINGLE_RUN,
    pool=OMIE_POOL,
    output_required=False,
)
def omie_day_ahead_prices(
    context: dg.AssetExecutionContext, config: OmieConfig, lake: Lake
) -> Iterator[dg.Output[None]]:
    first, end = _days(context, config.trailing_days)
    start = date.fromisoformat(context.partition_key_range.start)
    today = omie.madrid_today()
    settings = lake.settings()

    def on_chunk(report: omie.ChunkReport) -> None:
        if report.written is not None:
            for day in report.written.days:
                metadata = omie_metadata(report.written, report)
                context.log_event(
                    dg.AssetMaterialization(
                        context.asset_key, partition=str(day), metadata=metadata
                    )
                )

    def ingest(first: date, last: date, require_published: bool) -> list[omie.ChunkReport]:
        return omie.ingest(
            settings,
            first,
            last,
            run_id_prefix=context.run.run_id,  # audit-<run id>-...: the sweeper's ownership test
            on_chunk=on_chunk,
            table=lake.table(OMIE_DAY_AHEAD_PRICES),
            landing_prefix=lake.landing(omie.LANDING_PREFIX),
            require_published=require_published,
        )

    # 1. Days up to today, with the trailing re-ingest window. Their files exist (a missing one is
    #    an error whatever the flag), so publication is not in question.
    if first <= min(end, today):
        ingest(first, min(end, today), require_published=False)
    # 2. Days after today (tomorrow) in a call of their own: `require_published` discards the
    #    validated days of its own month chunk, so it must not cover the window above.
    first = max(start, today + timedelta(days=1))
    if first <= end:
        reports = ingest(first, end, require_published=config.require_published)
        if reports[-1].not_published_from is not None:
            context.log.info(
                f"OMIE has not published {reports[-1].not_published_from} yet: not materialized"
            )
    yield from ()


def omie_metadata(written: omie.Chunk, report: omie.ChunkReport) -> dict[str, dg.MetadataValue]:
    """The measurements of the chunk that published the day."""
    return {
        "window": dg.MetadataValue.text(f"{written.start}/{written.end}"),
        "rows": dg.MetadataValue.int(report.rows),
        "contract_versions": dg.MetadataValue.json(
            {f"v{version}": files for version, files in report.versions.items()}
        ),
        "rule_violations": dg.MetadataValue.json(report.violations),
        "snapshot_id": dg.MetadataValue.text(str(report.snapshot_id)),
        "run_id": dg.MetadataValue.text(str(report.run_id)),
    }
