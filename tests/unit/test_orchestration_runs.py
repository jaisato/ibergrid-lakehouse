"""Dagster runs in process: the assets, the scheduled OMIE jobs and the sweeper, on a real Iceberg
catalog (sqlite and local files) with REData and OMIE played back by the connectors' fakes."""

import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from pyarrow import fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES, REDATA_OBSERVATIONS, TABLES
from ibergrid.lake.tables import ensure_table
from ibergrid.orchestration import definitions, sweeper
from ibergrid.orchestration.assets import (
    REDATA_ASSETS,
    IngestConfig,
    OmieConfig,
    medallion_models,
    omie_day_ahead_prices,
)
from ibergrid.orchestration.resources import Lake
from ibergrid.orchestration.sweeper import sweep_audit_branches
from ibergrid.settings import base_url
from ibergrid.sources.omie import ingest as omie_ingest
from ibergrid.sources.omie.client import OmieClient
from ibergrid.sources.redata import ingest as redata_ingest
from ibergrid.sources.redata.client import RedataClient
from ibergrid.transform import runner as transform_runner

from .conftest import (
    FakeIcebergRest,
    FakeOmie,
    FakeRedata,
    LocalServer,
    Responder,
    fixture,
    omie_file,
    restamped,
)

Serve = Callable[[Responder], LocalServer]
ASSETS = {asset.key.path[-1]: asset for asset in REDATA_ASSETS}
OMIE_KEY = omie_day_ahead_prices.key
NOVEMBER = date(2026, 11, 18)  # the recorded day of every source


@pytest.fixture
def redata() -> FakeRedata:
    return FakeRedata()


@pytest.fixture
def omie() -> FakeOmie:
    return FakeOmie()


@pytest.fixture
def lake(
    catalog: SqlCatalog,
    tmp_path: Path,
    serve: Serve,
    redata: FakeRedata,
    omie: FakeOmie,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> Lake:
    """The resource as production builds it, pointed at the fakes; the catalog and the bucket are
    local, and the clients skip their one-second pace (the tests never sleep)."""
    for spec in TABLES:
        ensure_table(catalog, spec)
    root = tmp_path / "bucket-root"
    root.mkdir()
    landing = fs.SubTreeFileSystem(str(root), fs.LocalFileSystem())
    for module in (redata_ingest, omie_ingest):
        monkeypatch.setattr(module, "rest_catalog", lambda _settings: catalog)
        monkeypatch.setattr(module, "s3_filesystem", lambda *_args, **_kw: landing)
    monkeypatch.setattr(sweeper, "rest_catalog", lambda _settings: catalog)
    for client in (RedataClient, OmieClient):
        monkeypatch.setattr(
            client,
            "from_settings",
            classmethod(
                lambda cls, s: cls(base_url(s.base_url), timeout=5, max_rps=1e9, sleep=_no)
            ),
        )
    return Lake(redata_base_url=serve(redata).url, omie_base_url=serve(omie).url)


def _no(_seconds: float) -> None:
    pass


@pytest.fixture
def instance() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    """Every connector call (dataset or source, start, end, require_published), passed through."""
    seen: list[tuple[Any, ...]] = []
    redata, omie = redata_ingest.ingest, omie_ingest.ingest

    def redata_spy(settings: Any, dataset: str, start: date, end: date, **kwargs: Any) -> Any:
        seen.append((dataset, start, end))
        return redata(settings, dataset, start, end, **kwargs)

    def omie_spy(settings: Any, start: date, end: date, **kwargs: Any) -> Any:
        seen.append(("omie", start, end, kwargs["require_published"]))
        return omie(settings, start, end, **kwargs)

    monkeypatch.setattr(redata_ingest, "ingest", redata_spy)
    monkeypatch.setattr(omie_ingest, "ingest", omie_spy)
    return seen


def materialize(
    asset: dg.AssetsDefinition,
    lake: Lake,
    instance: dg.DagsterInstance,
    first: str,
    last: str | None = None,
    config: IngestConfig | None = None,
) -> dg.ExecuteInProcessResult:
    """One run of ``asset`` over partitions [first, last], as a backfill run gets them."""
    return dg.materialize(
        [asset],
        resources={"lake": lake},
        instance=instance,
        tags={
            "dagster/asset_partition_range_start": first,
            "dagster/asset_partition_range_end": last or first,
        },
        run_config=dg.RunConfig(ops={asset.op.name: config}) if config else None,
        raise_on_error=False,
    )


def materialized(instance: dg.DagsterInstance, key: dg.AssetKey) -> list[str]:
    return sorted(instance.get_materialized_partitions(key))


def metadata(instance: dg.DagsterInstance, key: dg.AssetKey, partition: str) -> dict[str, Any]:
    records = instance.fetch_materializations(
        dg.AssetRecordsFilter(asset_key=key, asset_partitions=[partition]), limit=1
    ).records
    materialization = records[0].asset_materialization
    assert materialization is not None
    return {name: entry.value for name, entry in materialization.metadata.items()}


def omie_day(omie: FakeOmie, day: date) -> None:
    """OMIE publishes ``day``: a recorded file of that day's contract (96 quarter-hours from
    2025-10-01, 24 hours before), restamped; not for DST days."""
    recorded = NOVEMBER if day >= date(2025, 10, 1) else date(2025, 9, 30)
    omie.files[f"marginalpdbc_{day:%Y%m%d}.1"] = omie_file(str(recorded)).replace(
        f"{recorded:%Y;%m;%d};".encode(), f"{day:%Y;%m;%d};".encode()
    )


def rows(catalog: SqlCatalog, identifier: tuple[str, str], where: str = "True") -> int:
    table: Table = catalog.load_table(identifier)
    return len(table.scan(row_filter=where).to_arrow())


# --- REData --------------------------------------------------------------------------------------


def test_a_redata_range_is_one_connector_call_and_each_day_gets_its_metadata(
    lake: Lake,
    instance: dg.DagsterInstance,
    redata: FakeRedata,
    calls: list[tuple[Any, ...]],
) -> None:
    """Two days across a month boundary: one call, two chunks (one per month), two partitions."""
    recorded = "market_prices-peninsular-2026-11-18_2026-11-18.json"
    for day in (date(2026, 10, 31), date(2026, 11, 1)):
        redata.payloads[f"market_prices-peninsular-{day}_{day}.json"] = restamped(recorded, day)

    result = materialize(ASSETS["market_prices"], lake, instance, "2026-10-31", "2026-11-01")

    assert result.success
    assert calls == [("market_prices", date(2026, 10, 31), date(2026, 11, 1))]
    key = ASSETS["market_prices"].key
    assert materialized(instance, key) == ["2026-10-31", "2026-11-01"]
    first = metadata(instance, key, "2026-10-31")
    assert first["complete"] is True
    assert (first["expected_points"], first["received_points"]) == (120, 120)  # PVPC 24, spot 96
    assert first["points"] == {"peninsular": {"expected": 120, "received": 120}}
    assert first["window"] == "2026-10-31/2026-10-31"
    [chunk] = first["chunks"]
    assert chunk["rows"] == 120
    assert chunk["run_id"].startswith(f"{result.run_id}-market_prices-peninsular-20261031-")
    assert chunk["landing_path"].endswith(".json")
    assert int(chunk["snapshot_id"]) > 0
    assert metadata(instance, key, "2026-11-01")["window"] == "2026-11-01/2026-11-01"


def test_a_day_is_materialized_once_every_system_published_it(
    lake: Lake, instance: dg.DagsterInstance, redata: FakeRedata, catalog: SqlCatalog
) -> None:
    peninsular = fixture("demand_hourly-peninsular-2026-11-18_2026-11-18.json")
    for system in ("baleares", "ceuta", "melilla"):  # answered like the peninsula's
        redata.payloads[f"demand_hourly-{system}-{NOVEMBER}_{NOVEMBER}.json"] = peninsular

    result = materialize(ASSETS["demand_hourly"], lake, instance, str(NOVEMBER))

    assert result.success
    found = metadata(instance, ASSETS["demand_hourly"].key, str(NOVEMBER))
    assert found["complete"] is True
    assert set(found["points"]) == {"peninsular", "canarias", "baleares", "ceuta", "melilla"}
    assert sorted(chunk["system"] for chunk in found["chunks"]) == sorted(found["points"])
    assert rows(catalog, REDATA_OBSERVATIONS.identifier) == 5 * 24


def test_a_run_that_fails_part_way_records_the_windows_it_completed(
    lake: Lake, instance: dg.DagsterInstance, redata: FakeRedata, catalog: SqlCatalog
) -> None:
    """Two days across a month boundary, five systems: the plan goes window by window, so
    October's five chunks publish and its day is recorded; in November Canarias' answer is refused
    after the peninsula's chunk published: the run fails and November's day is not recorded."""
    recorded = "demand_hourly-peninsular-2026-11-18_2026-11-18.json"
    for day in (date(2026, 10, 31), date(2026, 11, 1)):
        for system in ("peninsular", "canarias", "baleares", "ceuta", "melilla"):
            name = f"demand_hourly-{system}-{day}_{day}.json"
            redata.payloads[name] = restamped(recorded, day)
    redata.payloads["demand_hourly-canarias-2026-11-01_2026-11-01.json"] = b'{"data": {}}'

    result = materialize(ASSETS["demand_hourly"], lake, instance, "2026-10-31", "2026-11-01")

    assert not result.success
    assert materialized(instance, ASSETS["demand_hourly"].key) == ["2026-10-31"]
    assert len(metadata(instance, ASSETS["demand_hourly"].key, "2026-10-31")["chunks"]) == 5
    table = REDATA_OBSERVATIONS.identifier
    assert rows(catalog, table, "ts_local < '2026-11-01'") == 5 * 24
    assert rows(catalog, table, "electric_system = 'peninsular' AND ts_local >= '2026-11-01'") == 24
    assert rows(catalog, table, "electric_system <> 'peninsular' AND ts_local >= '2026-11-01'") == 0


def test_a_scheduled_redata_run_also_ingests_its_trailing_days_never_before_2023(
    lake: Lake,
    instance: dg.DagsterInstance,
    redata: FakeRedata,
    calls: list[tuple[Any, ...]],
) -> None:
    recorded = "market_prices-peninsular-2026-11-18_2026-11-18.json"
    for days in [(date(2026, 10, 31),), (date(2026, 11, 1),), (date(2023, 1, 1),)]:
        name = f"market_prices-peninsular-{days[0]}_{days[0]}.json"
        redata.payloads[name] = restamped(recorded, *days)
    config = IngestConfig(trailing_days=1)

    first = materialize(ASSETS["market_prices"], lake, instance, "2026-11-01", config=config)
    clamped = materialize(ASSETS["market_prices"], lake, instance, "2023-01-01", config=config)

    assert first.success
    assert clamped.success
    assert calls == [
        ("market_prices", date(2026, 10, 31), date(2026, 11, 1)),
        ("market_prices", date(2023, 1, 1), date(2023, 1, 1)),
    ]
    key = ASSETS["market_prices"].key
    assert materialized(instance, key) == ["2023-01-01", "2026-10-31", "2026-11-01"]


def test_the_resource_can_point_a_run_at_another_namespace_and_landing_prefix(
    lake: Lake, instance: dg.DagsterInstance, catalog: SqlCatalog, tmp_path: Path
) -> None:
    catalog.create_namespace("sandbox")
    ensure_table(catalog, replace(REDATA_OBSERVATIONS, namespace="sandbox"))
    elsewhere = Lake(
        redata_base_url=lake.redata_base_url, namespace="sandbox", landing_prefix="tests/run-1"
    )

    result = materialize(ASSETS["demand_realtime"], elsewhere, instance, str(NOVEMBER))

    assert result.success
    assert rows(catalog, ("sandbox", "redata_observations")) == 1152
    assert rows(catalog, REDATA_OBSERVATIONS.identifier) == 0
    [chunk] = metadata(instance, ASSETS["demand_realtime"].key, str(NOVEMBER))["chunks"]
    assert chunk["landing_path"].startswith("s3://ibergrid/tests/run-1/landing/redata/")


def test_the_resource_reads_the_environment_and_applies_its_overrides(credentials: None) -> None:
    assert str(Lake().settings().redata.base_url) == "https://apidatos.ree.es/"
    pointed = Lake(redata_base_url="http://127.0.0.1:1", omie_base_url="http://127.0.0.1:2")
    assert str(pointed.settings().redata.base_url) == "http://127.0.0.1:1/"
    assert str(pointed.settings().omie.base_url) == "http://127.0.0.1:2/"
    assert Lake().table(OMIE_DAY_AHEAD_PRICES) == ("bronze", "omie_day_ahead_prices")
    assert Lake().landing("landing/omie") == "landing/omie"


# --- OMIE ----------------------------------------------------------------------------------------

TODAY = date(2026, 11, 20)  # the tests' Madrid today; tomorrow is the 21st


@pytest.fixture
def today(monkeypatch: pytest.MonkeyPatch) -> date:
    monkeypatch.setattr(omie_ingest, "madrid_today", lambda: TODAY)
    return TODAY


def test_an_omie_backfill_is_one_call_cut_into_months(
    lake: Lake,
    instance: dg.DagsterInstance,
    omie: FakeOmie,
    today: date,
    calls: list[tuple[Any, ...]],
) -> None:
    days = [date(2026, 10, 30), date(2026, 10, 31), date(2026, 11, 1), date(2026, 11, 2)]
    for day in days:
        omie_day(omie, day)

    result = materialize(omie_day_ahead_prices, lake, instance, "2026-10-30", "2026-11-02")

    assert result.success
    assert calls == [("omie", days[0], days[-1], False)]
    assert materialized(instance, OMIE_KEY) == [str(day) for day in days]
    october = metadata(instance, OMIE_KEY, "2026-10-30")
    assert october["window"] == "2026-10-30/2026-10-31"
    assert (october["rows"], october["contract_versions"]) == (192, {"v2": 2})
    assert october["rule_violations"] == {}
    assert october["run_id"].startswith(f"{result.run_id}-omie-20261030-20261031-a1-")
    assert metadata(instance, OMIE_KEY, "2026-11-02")["window"] == "2026-11-01/2026-11-02"


def test_the_scheduled_omie_run_reingests_the_trailing_days_then_asks_for_tomorrow_alone(
    lake: Lake,
    instance: dg.DagsterInstance,
    omie: FakeOmie,
    today: date,
    calls: list[tuple[Any, ...]],
) -> None:
    for day in (17, 18, 19, 20, 21):
        omie_day(omie, date(2026, 11, day))
    config = OmieConfig(trailing_days=4, require_published=True)

    result = materialize(omie_day_ahead_prices, lake, instance, "2026-11-21", config=config)

    assert result.success
    assert calls == [
        ("omie", date(2026, 11, 17), TODAY, False),
        ("omie", date(2026, 11, 21), date(2026, 11, 21), True),
    ]
    assert materialized(instance, OMIE_KEY) == [f"2026-11-{day}" for day in (17, 18, 19, 20, 21)]


def test_the_omie_trailing_window_never_starts_before_2023(
    lake: Lake,
    instance: dg.DagsterInstance,
    omie: FakeOmie,
    today: date,
    calls: list[tuple[Any, ...]],
) -> None:
    omie_day(omie, date(2023, 1, 1))  # OMIE's first daily file; 2022 has only yearly archives

    result = materialize(
        omie_day_ahead_prices, lake, instance, "2023-01-01", config=OmieConfig(trailing_days=7)
    )

    assert result.success
    assert calls == [("omie", date(2023, 1, 1), date(2023, 1, 1), False)]


def test_tomorrow_not_published_yet_is_not_materialized_unless_required(
    lake: Lake, instance: dg.DagsterInstance, today: date, catalog: SqlCatalog
) -> None:
    tolerated = materialize(omie_day_ahead_prices, lake, instance, "2026-11-21")
    required = materialize(
        omie_day_ahead_prices,
        lake,
        instance,
        "2026-11-21",
        config=OmieConfig(require_published=True),
    )

    assert tolerated.success
    assert not required.success
    assert "has not published" in str(required.get_step_failure_events()[0].event_specific_data)
    assert materialized(instance, OMIE_KEY) == []
    assert rows(catalog, OMIE_DAY_AHEAD_PRICES.identifier) == 0


def test_a_required_tomorrow_fails_the_run_but_the_trailing_days_stay_published(
    lake: Lake,
    instance: dg.DagsterInstance,
    omie: FakeOmie,
    today: date,
    catalog: SqlCatalog,
    calls: list[tuple[Any, ...]],
) -> None:
    """The evening run's shape without its retry policy: today is re-ingested and recorded, then
    tomorrow, not out, fails the run."""
    omie_day(omie, TODAY)
    config = OmieConfig(trailing_days=1, require_published=True)

    result = materialize(omie_day_ahead_prices, lake, instance, "2026-11-21", config=config)

    assert not result.success
    assert calls == [
        ("omie", TODAY, TODAY, False),
        ("omie", date(2026, 11, 21), date(2026, 11, 21), True),
    ]
    assert materialized(instance, OMIE_KEY) == [str(TODAY)]
    assert rows(catalog, OMIE_DAY_AHEAD_PRICES.identifier) == 96


def test_the_scheduled_afternoon_job_carries_its_config(
    lake: Lake,
    instance: dg.DagsterInstance,
    omie: FakeOmie,
    today: date,
    calls: list[tuple[Any, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seven days re-ingested, then tomorrow, which OMIE has not published yet: tolerated. (The
    evening job's config is pinned on its tick; run here, its retries would wait 15 minutes.)"""
    for day in range(14, 21):
        omie_day(omie, date(2026, 11, day))
    monkeypatch.setenv("IBERGRID_OMIE_BASE_URL", str(lake.omie_base_url))

    result = definitions.defs.resolve_job_def("omie_afternoon").execute_in_process(
        partition_key="2026-11-21", instance=instance, raise_on_error=False
    )

    assert result.success
    assert calls == [
        ("omie", date(2026, 11, 14), TODAY, False),
        ("omie", date(2026, 11, 21), date(2026, 11, 21), False),
    ]
    assert materialized(instance, OMIE_KEY) == [f"2026-11-{day}" for day in range(14, 21)]


# --- The sweeper ---------------------------------------------------------------------------------


# What the sweeper may remove, spelled out rather than taken from sweeper.FINISHED: every other
# status, including any Dagster adds later, must keep its branch.
REMOVABLE = {dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED}


def runs_in_every_status(instance: dg.DagsterInstance) -> dict[dg.DagsterRunStatus, str]:
    # A queued run needs its code location's origin; Dagster has no public constructor for one.
    from dagster._core.remote_origin import (
        RegisteredCodeLocationOrigin,
        RemoteJobOrigin,
        RemoteRepositoryOrigin,
    )

    origin = RemoteJobOrigin(
        RemoteRepositoryOrigin(RegisteredCodeLocationOrigin("ibergrid"), "__repository__"),
        "redata_daily",
    )
    return {
        status: instance.add_run(
            dg.DagsterRun(
                job_name="redata_daily",
                run_id=str(uuid.uuid4()),
                status=status,
                remote_job_origin=origin,
            )
        ).run_id
        for status in dg.DagsterRunStatus
    }


def test_the_sweeper_removes_only_the_branches_of_finished_dagster_runs(
    lake: Lake, instance: dg.DagsterInstance, catalog: SqlCatalog
) -> None:
    runs = runs_in_every_status(instance)
    table: Table = catalog.load_table(OMIE_DAY_AHEAD_PRICES.identifier)
    owners = {status: f"{run}-omie-20261118-20261118-a1-beef" for status, run in runs.items()}
    strangers = [
        f"{uuid.uuid4()}-omie-20261118-20261118-a1-beef",  # no such run in this instance
        "20261121T080000Z-abcd-omie-20261118-20261118-a1-beef",  # the CLI's timestamped prefix
    ]
    for run_id in [*owners.values(), *strangers]:
        wap.start(table, run_id)

    result = sweep_audit_branches.execute_in_process(instance=instance, resources={"lake": lake})

    assert result.success
    kept = {f"audit-{owners[status]}" for status in dg.DagsterRunStatus if status not in REMOVABLE}
    assert dg.DagsterRunStatus.QUEUED in runs
    assert set(wap.audit_branches(table)) == kept | {f"audit-{name}" for name in strangers}
    removed = [line for line in logs(instance, result.run_id) if " removed " in line]
    assert sorted(removed) == sorted(
        f"bronze.omie_day_ahead_prices: removed audit-{owners[status]} (run {status.value})"
        for status in REMOVABLE
    )
    assert wap.audit_branches(catalog.load_table(REDATA_OBSERVATIONS.identifier)) == []


def test_a_branch_that_cannot_be_removed_does_not_stop_the_sweep(
    lake: Lake, instance: dg.DagsterInstance, catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = runs_in_every_status(instance)
    table: Table = catalog.load_table(OMIE_DAY_AHEAD_PRICES.identifier)
    stuck = f"audit-{runs[dg.DagsterRunStatus.FAILURE]}-omie-20261118-20261118-a1-beef"
    done = f"audit-{runs[dg.DagsterRunStatus.SUCCESS]}-omie-20261118-20261118-a1-beef"
    for name in sorted([stuck, done]):
        wap.start(table, name.removeprefix("audit-"))
    discard = wap.discard_branch

    def flaky(table: Table, name: str) -> bool:  # a concurrent commit on one branch
        if name == stuck:
            raise wap.WapError("the table changed concurrently")
        return discard(table, name)

    monkeypatch.setattr(wap, "discard_branch", flaky)  # the module the sweeper calls through

    result = sweep_audit_branches.execute_in_process(instance=instance, resources={"lake": lake})

    assert result.success
    assert wap.audit_branches(table) == [stuck]
    lines = logs(instance, result.run_id)
    assert (
        f"bronze.omie_day_ahead_prices: could not remove {stuck}: the table changed concurrently"
        in lines
    )
    assert f"bronze.omie_day_ahead_prices: removed {done} (run SUCCESS)" in lines


def logs(instance: dg.DagsterInstance, run_id: str) -> list[str]:
    return [entry.user_message for entry in instance.all_logs(run_id)]


def test_the_sweeper_skips_a_table_that_does_not_exist_yet(
    lake: Lake, instance: dg.DagsterInstance
) -> None:
    elsewhere = Lake(namespace="nowhere")

    result = sweep_audit_branches.execute_in_process(
        instance=instance, resources={"lake": elsewhere}
    )

    assert result.success


# --- Medallion transforms (ADR 0007) -------------------------------------------------------------


def test_medallion_models_materialize_all_and_subsets_in_process(
    lake: Lake,
    instance: dg.DagsterInstance,
    catalog: SqlCatalog,
    serve: Serve,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bronze ingestion + Dagster medallion materialization (full and subset) on SqlCatalog."""
    monkeypatch.setenv("IBERGRID_CATALOG_URL", serve(FakeIcebergRest(catalog)).url)
    monkeypatch.setattr(transform_runner, "rest_catalog", lambda _settings: catalog)

    assert materialize(omie_day_ahead_prices, lake, instance, str(NOVEMBER)).success

    full = dg.materialize(
        [medallion_models],
        resources={"lake": lake},
        instance=instance,
        raise_on_error=False,
    )
    assert full.success
    assert rows(catalog, ("silver", "mibel_prices_hourly")) == 24
    assert rows(catalog, ("gold", "mibel_daily_summary")) == 1

    # Subset materialization: runs dbt for +mibel_daily_summary and emits only that asset's result.
    subset = dg.materialize(
        [medallion_models],
        selection=[dg.AssetKey(["gold", "mibel_daily_summary"])],
        resources={"lake": lake},
        instance=instance,
        raise_on_error=False,
    )
    assert subset.success
    mat_events = subset.get_asset_materialization_events()
    assert [e.asset_key for e in mat_events] == [dg.AssetKey(["gold", "mibel_daily_summary"])]
    meta = {k: v.value for k, v in (mat_events[0].materialization.metadata or {}).items()}
    assert meta["rows"] == 1
    assert isinstance(meta["dbt_tests_passed"], int)
    assert meta["dbt_tests_passed"] > 0
    assert meta["snapshot_id"] != "none"
