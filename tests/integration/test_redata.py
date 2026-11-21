"""REData ingestion on the running stack, with REData played back from the recorded fixtures.

``lake init`` runs in the app image inside the network (it creates the real, empty Bronze table);
ingestion runs from the host against the published ports (ADR 0002), with the fake REData on
127.0.0.1, into a throwaway table and landing prefix that are removed afterwards.
"""

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any, NamedTuple

import duckdb
import pytest
from pyarrow import fs
from pydantic import HttpUrl

from ibergrid.lake import duck, registry
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.registry import REDATA_OBSERVATIONS
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import RedataSettings, Settings
from ibergrid.sources.redata.datasets import DATASETS, plan
from ibergrid.sources.redata.ingest import ChunkReport, ingest
from ibergrid.storage import s3_filesystem

from ..unit.conftest import FakeRedata, LocalServer, fixture
from .conftest import compose_run

pytestmark = pytest.mark.integration

NOVEMBER, LONG_DAY = date(2026, 11, 18), date(2026, 10, 25)
RUN_COLUMNS = ("fetched_at", "run_id")


class Target(NamedTuple):
    table: tuple[str, str]
    landing_prefix: str
    duck: str  # the table's name for DuckDB


@pytest.fixture(scope="module", autouse=True)
def initialised() -> None:
    """``lake init`` from the app image creates the Bronze tables once; reruns change nothing."""
    first = compose_run("doctor", "lake", "init")
    second = compose_run("doctor", "lake", "init")

    assert first.returncode == 0, first.stdout + first.stderr
    assert "lake: bronze.redata_observations " in first.stdout
    assert "lake: bronze.omie_day_ahead_prices " in first.stdout
    assert second.stdout.splitlines() == [
        f"lake: {spec.namespace}.{spec.name} unchanged" for spec in registry.TABLES
    ], second.stderr


@pytest.fixture
def wired(settings: Settings) -> Iterator[Settings]:
    server = LocalServer(FakeRedata())
    yield Settings(
        storage=settings.storage,
        catalog=settings.catalog,
        redata=RedataSettings(base_url=HttpUrl(server.url)),
    )
    server.close()


@pytest.fixture
def target(catalog: LakeCatalog, namespace: str, settings: Settings) -> Iterator[Target]:
    """The Bronze table's spec in the test's own namespace, and a landing prefix of its own; the
    namespace fixture drops the table, this one deletes the landed objects."""
    ensure_table(catalog, replace(REDATA_OBSERVATIONS, namespace=namespace))
    # Top level: deleting a nested prefix makes pyarrow leave a marker for its parent.
    prefix = f"it-landing-{namespace}"
    yield Target(
        (namespace, REDATA_OBSERVATIONS.name),
        prefix,
        f"{duck.CATALOG}.{namespace}.{REDATA_OBSERVATIONS.name}",
    )
    s3_filesystem(settings.storage, timeout=5).delete_dir(f"{settings.storage.bucket}/{prefix}")


def window_rows(
    con: duckdb.DuckDBPyConnection, table: str, dataset: str, day: date, *drop: str
) -> list[dict[str, Any]]:
    """Main's rows of one dataset and local day, read by DuckDB, in key order."""
    start, end = plan(DATASETS[dataset], day, day)[0].window_utc
    relation = duck.scan(con, table).filter(
        f"dataset = '{dataset}' AND ts_utc >= '{start.isoformat()}' "
        f"AND ts_utc < '{end.isoformat()}'"
    )
    rows = [
        {name: value for name, value in row.items() if name not in drop}
        for row in relation.to_arrow_table().to_pylist()
    ]
    return sorted(rows, key=lambda r: (r["electric_system"], r["series_id"], r["ts_utc"]))


def test_ingest_a_window_rerun_it_and_read_main_with_duckdb(
    wired: Settings, target: Target, con: duckdb.DuckDBPyConnection
) -> None:
    def run(prefix: str) -> list[ChunkReport]:
        return ingest(
            wired,
            "demand_hourly",
            NOVEMBER,
            NOVEMBER,
            ["peninsular", "canarias"],
            prefix,
            table=target.table,
            landing_prefix=target.landing_prefix,
        )

    first = run("it-first")
    before = window_rows(con, target.duck, "demand_hourly", NOVEMBER, *RUN_COLUMNS)
    second = run("it-again")

    assert [r.rows for r in first] == [r.rows for r in second] == [24, 24]
    assert window_rows(con, target.duck, "demand_hourly", NOVEMBER, *RUN_COLUMNS) == before
    rows = window_rows(con, target.duck, "demand_hourly", NOVEMBER)
    assert len(rows) == 48
    assert {(r["electric_system"], r["run_id"]) for r in rows} == {
        ("peninsular", second[0].run_id),
        ("canarias", second[1].run_id),
    }
    assert duck.describe(con, target.duck).refs[1:] == []  # no audit branch left
    assert_landed(wired, second, target.landing_prefix)


def test_the_25_hour_day_of_market_prices(
    wired: Settings,
    target: Target,
    catalog: LakeCatalog,
    con: duckdb.DuckDBPyConnection,
) -> None:
    [report] = ingest(
        wired,
        "market_prices",
        LONG_DAY,
        LONG_DAY,
        run_id_prefix="it-dst",
        table=target.table,
        landing_prefix=target.landing_prefix,
    )

    assert (report.rows, report.completeness.expected) == (125, 125)
    rows = window_rows(con, target.duck, "market_prices", LONG_DAY)
    assert {(r["series_id"], r["granularity"]) for r in rows} == {
        ("1001", "hour"),
        ("600", "15min"),
    }
    assert min(r["ts_utc"] for r in rows) == datetime(2026, 10, 24, 22, tzinfo=UTC)
    summary = catalog.load_table(target.table).current_snapshot().summary
    assert summary is not None
    assert summary["ibergrid.redata.window"] == "2026-10-25/2026-10-25"
    assert summary["ibergrid.redata.received-points"] == "125"
    assert_landed(wired, [report], target.landing_prefix)


def assert_landed(settings: Settings, reports: list[ChunkReport], prefix: str) -> None:
    """Each raw answer sits at its key under the prefix, outside the warehouse."""
    s3 = s3_filesystem(settings.storage, timeout=5)
    for report in reports:
        chunk = report.chunk
        path = report.landing_path.removeprefix("s3://")
        info = s3.get_file_info(path)
        name = f"{chunk.dataset.name}-{chunk.system.name}-{chunk.start}_{chunk.end}.json"
        assert info.type == fs.FileType.File, path
        assert info.size == len(fixture(name))
        assert path.startswith(f"{settings.storage.bucket}/{prefix}/")
        assert not path.startswith(f"{settings.storage.bucket}/{settings.catalog.warehouse}/")
