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

from ibergrid.lake.registry import (
    GOLD_MIBEL_DAILY_SUMMARY,
    GOLD_MIBEL_HOURLY_MARKET,
    GOLD_OPTIMAL_CONSUMPTION_WINDOWS,
    GOLD_SYSTEM_DEMAND_DAILY,
    MEDALLION_TABLES,
    OMIE_DAY_AHEAD_PRICES,
    REDATA_OBSERVATIONS,
    SILVER_DEMAND_NATIONAL_5MIN,
    SILVER_DEMAND_SYSTEM_HOURLY,
    SILVER_GENERATION_DAILY,
    SILVER_MIBEL_PRICES_HOURLY,
    SILVER_MIBEL_PRICES_PERIOD,
)
from ibergrid.lake.tables import TableSpec
from ibergrid.orchestration.resources import Lake
from ibergrid.sources.omie import ingest as omie
from ibergrid.sources.redata import ingest as redata
from ibergrid.sources.redata.datasets import DATASETS, Dataset
from ibergrid.transform import run_medallion

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
# One writer per table and one request a second per source (ADR 0003, ADR 0004, ADR 0007):
# dagster.yaml gives every pool a limit of 1.
REDATA_POOL = "redata"
OMIE_POOL = "omie"
TRANSFORM_POOL = "transform"


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


# --- Medallion Silver & Gold assets (ADR 0007) ---------------------------------------------------

# External asset representation of the streaming lane's Bronze table (written continuously by
# stream-consumer, ADR 0006) so the Dagster lineage graph shows both batch and stream inputs.
REDATA_DEMAND_STREAM_ASSET = dg.AssetSpec(
    key=dg.AssetKey(["bronze", "redata_demand_stream"]),
    description=(
        "Append-only 5-minute national real-time demand from Redpanda in "
        "bronze.redata_demand_stream (written by stream-consumer; ADR 0006)."
    ),
    group_name="bronze",
    kinds={"iceberg", "redpanda"},
)

_BRONZE_DEMAND_REALTIME = dg.AssetKey(["bronze", "redata_observations", "demand_realtime"])
_BRONZE_DEMAND_HOURLY = dg.AssetKey(["bronze", "redata_observations", "demand_hourly"])
_BRONZE_GENERATION_DAILY = dg.AssetKey(["bronze", "redata_observations", "generation_daily"])
_BRONZE_MARKET_PRICES = dg.AssetKey(["bronze", "redata_observations", "market_prices"])
_BRONZE_OMIE = dg.AssetKey(["bronze", "omie_day_ahead_prices"])
_BRONZE_STREAM = REDATA_DEMAND_STREAM_ASSET.key


def _medallion_spec(
    table: TableSpec,
    deps: list[dg.AssetKey],
    description: str,
) -> dg.AssetSpec:
    return dg.AssetSpec(
        key=dg.AssetKey([table.namespace, table.name]),
        deps=deps,
        description=description,
        group_name=table.namespace,
        kinds={"dbt", "duckdb", "iceberg"},
    )


MEDALLION_SPECS: list[dg.AssetSpec] = [
    _medallion_spec(
        SILVER_DEMAND_NATIONAL_5MIN,
        [_BRONZE_DEMAND_REALTIME, _BRONZE_STREAM],
        "Reconciled 5-minute national demand (batch for closed Madrid days, stream for open days).",
    ),
    _medallion_spec(
        SILVER_DEMAND_SYSTEM_HOURLY,
        [_BRONZE_DEMAND_HOURLY],
        "Hourly demand per Spanish electric system with strict peninsular/non-peninsular scoping.",
    ),
    _medallion_spec(
        SILVER_MIBEL_PRICES_PERIOD,
        [_BRONZE_OMIE],
        "OMIE MIBEL day-ahead marginal prices at native resolution (v1 60 min, v2 15 min).",
    ),
    _medallion_spec(
        SILVER_MIBEL_PRICES_HOURLY,
        [dg.AssetKey(["silver", "mibel_prices_period"]), _BRONZE_MARKET_PRICES],
        "Standardized hourly MIBEL prices across v1/v2 enriched with REData PVPC and spot price.",
    ),
    _medallion_spec(
        SILVER_GENERATION_DAILY,
        [_BRONZE_GENERATION_DAILY],
        "Daily net generation by technology and electric system without double-counting totals.",
    ),
    _medallion_spec(
        GOLD_MIBEL_HOURLY_MARKET,
        [
            dg.AssetKey(["silver", "mibel_prices_hourly"]),
            dg.AssetKey(["silver", "demand_system_hourly"]),
            dg.AssetKey(["silver", "generation_daily"]),
        ],
        "Peninsular hourly market mart: MIBEL prices, peninsular demand, 1h ramps and mix shares.",
    ),
    _medallion_spec(
        GOLD_MIBEL_DAILY_SUMMARY,
        [
            dg.AssetKey(["gold", "mibel_hourly_market"]),
            dg.AssetKey(["silver", "demand_system_hourly"]),
        ],
        "Daily MIBEL executive summary keeping peninsular and non-peninsular demand separated.",
    ),
    _medallion_spec(
        GOLD_SYSTEM_DEMAND_DAILY,
        [
            dg.AssetKey(["silver", "demand_system_hourly"]),
            dg.AssetKey(["silver", "generation_daily"]),
        ],
        "Daily demand and renewable share per electric system (peninsular + 4 island/enclave).",
    ),
    _medallion_spec(
        GOLD_OPTIMAL_CONSUMPTION_WINDOWS,
        [dg.AssetKey(["gold", "mibel_hourly_market"])],
        "Calendar-month x day-type x Madrid-hour profile for industrial load shifting.",
    ),
]


@dg.multi_asset(
    name="medallion_models",
    specs=MEDALLION_SPECS,
    can_subset=True,
    pool=TRANSFORM_POOL,
)
def medallion_models(
    context: dg.AssetExecutionContext, lake: Lake
) -> Iterator[dg.MaterializeResult[None]]:
    """Run ``dbt build`` once in a staging DuckDB and publish every selected table via WAP."""
    selected_keys = list(context.selected_asset_keys)
    all_selected = len(selected_keys) == len(MEDALLION_TABLES)
    select = () if all_selected else tuple(key.path[-1] for key in selected_keys)
    report = run_medallion(
        lake.settings(),
        select=select,
        run_id_prefix=context.run.run_id,
        bronze_namespace=lake.namespace,
        target_namespace=lake.namespace,
    )
    bronze_meta = {
        name: (None if snap is None else str(snap))
        for name, snap in report.bronze_snapshots.items()
    }
    key_by_name = {spec.key.path[-1]: spec.key for spec in MEDALLION_SPECS}
    for table_report in report.tables:
        yield dg.MaterializeResult(
            asset_key=key_by_name[table_report.name],
            metadata={
                "rows": dg.MetadataValue.int(table_report.rows),
                "snapshot_id": dg.MetadataValue.text(
                    "none" if table_report.snapshot_id is None else str(table_report.snapshot_id)
                ),
                "run_id": dg.MetadataValue.text(table_report.run_id),
                "seconds": dg.MetadataValue.float(table_report.seconds),
                "dbt_models_built": dg.MetadataValue.int(report.dbt_models_built),
                "dbt_tests_passed": dg.MetadataValue.int(report.dbt_tests_passed),
                "bronze_snapshots": dg.MetadataValue.json(bronze_meta),
            },
        )
