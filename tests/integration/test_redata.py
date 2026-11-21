"""REData ingestion on the running stack, with REData played back from the recorded fixtures.

``lake init`` runs in the app image inside the network; ingestion runs from the host against the
published ports (ADR 0002), with the fake REData on 127.0.0.1.
"""

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import duckdb
import pytest
from pyarrow import fs
from pydantic import HttpUrl

from ibergrid.lake import duck
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.settings import RedataSettings, Settings
from ibergrid.sources.redata.datasets import DATASETS, plan
from ibergrid.sources.redata.ingest import ChunkReport, ingest
from ibergrid.storage import s3_filesystem

from ..unit.conftest import FakeRedata, LocalServer, fixture
from .conftest import compose_run

pytestmark = pytest.mark.integration

TABLE = f"{duck.CATALOG}.bronze.redata_observations"
NOVEMBER, LONG_DAY = date(2026, 11, 18), date(2026, 10, 25)
RUN_COLUMNS = ("fetched_at", "run_id")


@pytest.fixture(scope="module", autouse=True)
def initialised() -> None:
    """``lake init`` from the app image creates the Bronze table once; later runs change nothing."""
    first = compose_run("doctor", "lake", "init")
    second = compose_run("doctor", "lake", "init")

    assert first.returncode == 0, first.stdout + first.stderr
    assert "lake: bronze.redata_observations " in first.stdout
    assert second.stdout == "lake: bronze.redata_observations unchanged\n", second.stderr


@pytest.fixture
def wired(settings: Settings) -> Iterator[Settings]:
    server = LocalServer(FakeRedata())
    yield Settings(
        storage=settings.storage,
        catalog=settings.catalog,
        redata=RedataSettings(base_url=HttpUrl(server.url)),
    )
    server.close()


def window_rows(
    con: duckdb.DuckDBPyConnection, dataset: str, day: date, *drop: str
) -> list[dict[str, Any]]:
    """Main's rows of one dataset and local day, read by DuckDB, in key order."""
    start, end = plan(DATASETS[dataset], day, day)[0].window_utc
    relation = duck.scan(con, TABLE).filter(
        f"dataset = '{dataset}' AND ts_utc >= '{start.isoformat()}' "
        f"AND ts_utc < '{end.isoformat()}'"
    )
    rows = [
        {name: value for name, value in row.items() if name not in drop}
        for row in relation.to_arrow_table().to_pylist()
    ]
    return sorted(rows, key=lambda r: (r["electric_system"], r["series_id"], r["ts_utc"]))


def test_ingest_a_window_rerun_it_and_read_main_with_duckdb(
    wired: Settings, catalog: LakeCatalog, con: duckdb.DuckDBPyConnection
) -> None:
    systems = ["peninsular", "canarias"]
    first = ingest(wired, "demand_hourly", NOVEMBER, NOVEMBER, systems, run_id_prefix="it-first")
    before = window_rows(con, "demand_hourly", NOVEMBER, *RUN_COLUMNS)
    second = ingest(wired, "demand_hourly", NOVEMBER, NOVEMBER, systems, run_id_prefix="it-again")

    assert [r.rows for r in first] == [r.rows for r in second] == [24, 24]
    assert window_rows(con, "demand_hourly", NOVEMBER, *RUN_COLUMNS) == before
    rows = window_rows(con, "demand_hourly", NOVEMBER)
    by_system = {(r["electric_system"], r["run_id"]) for r in rows}
    assert len(rows) == 48
    assert by_system == {("peninsular", second[0].run_id), ("canarias", second[1].run_id)}
    table = catalog.load_table(("bronze", "redata_observations"))
    assert not [ref for ref in table.metadata.refs if ref.startswith("audit-it-")]
    assert_landed(wired, second)


def test_the_25_hour_day_of_market_prices(
    wired: Settings, catalog: LakeCatalog, con: duckdb.DuckDBPyConnection
) -> None:
    [report] = ingest(wired, "market_prices", LONG_DAY, LONG_DAY, run_id_prefix="it-dst")

    assert (report.rows, report.completeness.expected) == (125, 125)
    rows = window_rows(con, "market_prices", LONG_DAY)
    assert {(r["series_id"], r["granularity"]) for r in rows} == {
        ("1001", "hour"),
        ("600", "15min"),
    }
    assert min(r["ts_utc"] for r in rows) == datetime(2026, 10, 24, 22, tzinfo=UTC)
    table = catalog.load_table(("bronze", "redata_observations"))
    summary = table.current_snapshot().summary
    assert summary is not None
    assert summary["ibergrid.redata.window"] == "2026-10-25/2026-10-25"
    assert summary["ibergrid.redata.received-points"] == "125"
    assert_landed(wired, [report])


def assert_landed(settings: Settings, reports: list[ChunkReport]) -> None:
    """Each raw answer sits at its deterministic key, outside the warehouse prefix."""
    s3 = s3_filesystem(settings.storage, timeout=5)
    for report in reports:
        chunk, key = report.chunk, report.chunk.landing_key
        info = s3.get_file_info(f"{settings.storage.bucket}/{key}")
        name = f"{chunk.dataset.name}-{chunk.system.name}-{chunk.start}_{chunk.end}.json"
        assert info.type == fs.FileType.File, key
        assert info.size == len(fixture(name))
        assert report.landing_path == f"s3://{settings.storage.bucket}/{key}"
        assert not key.startswith(settings.catalog.warehouse)
