"""OMIE ingestion on the running stack, with OMIE played back from the recorded files.

Ingestion runs from the host against the published ports (ADR 0002), with the fake OMIE on
127.0.0.1, into a throwaway table and landing prefix that are removed afterwards; DuckDB reads
the result from main. ``lake init`` from the app image is covered by test_redata.py.
"""

import hashlib
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any, NamedTuple

import duckdb
import pytest
from pyarrow import fs
from pydantic import HttpUrl

from ibergrid.lake import duck
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import OmieSettings, Settings
from ibergrid.sources.omie.ingest import ChunkReport, ingest
from ibergrid.storage import s3_filesystem

from ..unit.conftest import FakeOmie, LocalServer, omie_file

pytestmark = pytest.mark.integration

RUN_COLUMNS = ("fetched_at", "run_id")


class Target(NamedTuple):
    table: tuple[str, str]
    landing_prefix: str
    duck: str  # the table's name for DuckDB


@pytest.fixture
def wired(settings: Settings) -> Iterator[Settings]:
    server = LocalServer(FakeOmie())
    yield Settings(
        storage=settings.storage,
        catalog=settings.catalog,
        omie=OmieSettings(base_url=HttpUrl(server.url)),
    )
    server.close()


@pytest.fixture
def target(catalog: LakeCatalog, namespace: str, settings: Settings) -> Iterator[Target]:
    """The Bronze table's spec in the test's own namespace, and a landing prefix of its own; the
    namespace fixture drops the table, this one deletes the landed objects."""
    ensure_table(catalog, replace(OMIE_DAY_AHEAD_PRICES, namespace=namespace))
    # Top level: deleting a nested prefix makes pyarrow leave a marker for its parent.
    prefix = f"it-landing-{namespace}"
    yield Target(
        (namespace, OMIE_DAY_AHEAD_PRICES.name),
        prefix,
        f"{duck.CATALOG}.{namespace}.{OMIE_DAY_AHEAD_PRICES.name}",
    )
    s3_filesystem(settings.storage, timeout=5).delete_dir(f"{settings.storage.bucket}/{prefix}")


def main_rows(con: duckdb.DuckDBPyConnection, table: str, *drop: str) -> list[dict[str, Any]]:
    """Main's rows, read by DuckDB, in key order."""
    rows = [
        {name: value for name, value in row.items() if name not in drop}
        for row in duck.scan(con, table).to_arrow_table().to_pylist()
    ]
    return sorted(rows, key=lambda r: (r["delivery_date"], r["period"]))


def test_ingest_across_the_cut_over_rerun_it_and_read_main_with_duckdb(
    wired: Settings, target: Target, con: duckdb.DuckDBPyConnection
) -> None:
    def run(prefix: str) -> list[ChunkReport]:
        return ingest(
            wired,
            date(2025, 9, 30),
            date(2025, 10, 1),
            prefix,
            table=target.table,
            landing_prefix=target.landing_prefix,
        )

    first = run("it-first")
    before = main_rows(con, target.duck, *RUN_COLUMNS)
    second = run("it-again")

    assert [(r.rows, r.versions) for r in first] == [(24, {1: 1}), (96, {2: 1})]
    assert [r.rows for r in second] == [24, 96]
    assert main_rows(con, target.duck, *RUN_COLUMNS) == before
    rows = main_rows(con, target.duck)
    assert len(rows) == 120
    assert {(r["delivery_date"], r["run_id"]) for r in rows} == {
        (date(2025, 9, 30), second[0].run_id),
        (date(2025, 10, 1), second[1].run_id),
    }
    assert rows[23]["interval_end_utc"] == rows[24]["interval_start_utc"]  # 22:00 UTC, the cut-over
    assert duck.describe(con, target.duck).refs[1:] == []  # no audit branch left
    assert_landed(wired, rows, target.landing_prefix)


def test_the_100_quarter_hour_day(
    wired: Settings, target: Target, catalog: LakeCatalog, con: duckdb.DuckDBPyConnection
) -> None:
    [report] = ingest(
        wired,
        date(2025, 10, 26),
        date(2025, 10, 26),
        run_id_prefix="it-dst",
        table=target.table,
        landing_prefix=target.landing_prefix,
    )

    assert report.rows == 100
    rows = main_rows(con, target.duck)
    assert (rows[0]["interval_start_utc"], rows[-1]["interval_end_utc"]) == (
        datetime(2025, 10, 25, 22, tzinfo=UTC),
        datetime(2025, 10, 26, 23, tzinfo=UTC),
    )
    summary = catalog.load_table(target.table).current_snapshot().summary
    assert summary is not None
    assert summary["ibergrid.omie.window"] == "2025-10-26/2025-10-26"
    assert summary["ibergrid.omie.periods"] == "100"
    assert_landed(wired, rows, target.landing_prefix)


def assert_landed(settings: Settings, rows: list[dict[str, Any]], prefix: str) -> None:
    """Each original file sits, byte for byte, at its key under the prefix, outside the
    warehouse."""
    s3 = s3_filesystem(settings.storage, timeout=5)
    for day, path in {(r["delivery_date"], r["landing_path"]) for r in rows}:
        key = path.removeprefix("s3://")
        assert key.startswith(f"{settings.storage.bucket}/{prefix}/{day:%Y}/{day:%m}/")
        assert not key.startswith(f"{settings.storage.bucket}/{settings.catalog.warehouse}/")
        with s3.open_input_stream(key) as stored:
            body = stored.read()
        assert body == omie_file(str(day))
        assert key.endswith(f"-{hashlib.sha256(body).hexdigest()[:12]}.1")
        assert s3.get_file_info(key).type == fs.FileType.File
