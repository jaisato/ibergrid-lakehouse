"""Ingestion through Write-Audit-Publish on a real Iceberg catalog (sqlite + local files), with
REData played back from the fixtures and the landing zone on a local directory."""

import json
import re
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from pyarrow import fs
from pydantic import HttpUrl, SecretStr
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.update import AddSnapshotUpdate, RemoveSnapshotRefUpdate

from ibergrid.lake import wap
from ibergrid.lake.registry import REDATA_OBSERVATIONS
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings, StorageSettings
from ibergrid.sources.redata import ingest as ingest_module
from ibergrid.sources.redata.client import RedataClient, RedataError
from ibergrid.sources.redata.datasets import DATASETS, Chunk
from ibergrid.sources.redata.ingest import (
    AuditFailed,
    ChunkReport,
    _audit,
    ingest,
    ingest_chunk,
)
from ibergrid.sources.redata.normalize import InvalidResponse
from ibergrid.storage import s3_filesystem

from .conftest import FakeRedata, LocalServer, Responder, fixture

Serve = Callable[[Responder], LocalServer]
BUCKET = "ibergrid"
HOURLY, REALTIME = DATASETS["demand_hourly"], DATASETS["demand_realtime"]
DAY = date(2026, 11, 18)
PENINSULAR = Chunk(HOURLY, HOURLY.systems[0], DAY, DAY)
CANARIAS = Chunk(HOURLY, HOURLY.systems[1], DAY, DAY)
# Columns that differ between two runs of the same window by design.
RUN_COLUMNS = ("fetched_at", "run_id")


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, REDATA_OBSERVATIONS)
    loaded: Table = catalog.load_table(REDATA_OBSERVATIONS.identifier)
    return loaded


@pytest.fixture
def redata() -> FakeRedata:
    return FakeRedata()


@pytest.fixture
def redata_url(serve: Serve, redata: FakeRedata) -> str:
    return serve(redata).url


@pytest.fixture
def client(redata_url: str) -> RedataClient:
    """No pacing and no waits between retries: the tests never sleep."""
    return RedataClient(redata_url, timeout=5, max_rps=1e9, sleep=lambda _: None)


@pytest.fixture
def landing(tmp_path: Path) -> fs.FileSystem:
    root = tmp_path / "bucket-root"
    root.mkdir()
    return fs.SubTreeFileSystem(str(root), fs.LocalFileSystem())


def run(
    table: Table, client: RedataClient, landing: fs.FileSystem, chunk: Chunk, prefix: str = "t"
) -> ChunkReport:
    return ingest_chunk(table, client, landing, BUCKET, chunk, prefix)


def main_rows(table: Table, *drop: str) -> list[dict[str, Any]]:
    table.refresh()
    data = table.scan().to_arrow().drop_columns(list(drop))
    keys = ["dataset", "electric_system", "series_id", "ts_utc"]
    rows: list[dict[str, Any]] = data.sort_by([(k, "ascending") for k in keys]).to_pylist()
    return rows


def refs(table: Table) -> set[str]:
    table.refresh()
    return set(table.metadata.refs)


def test_a_chunk_is_landed_written_audited_and_published(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, CANARIAS, prefix="cli-1")

    assert report.rows == 24
    assert (report.completeness.expected, report.completeness.received) == (24, 24)
    assert report.run_id == "cli-1-demand_hourly-canarias-20261118-20261118-a1"
    key = "landing/redata/demand_hourly/canarias/2026/11/2026-11-18_2026-11-18.json"
    assert report.landing_path == f"s3://{BUCKET}/{key}"
    payload = fixture("demand_hourly-canarias-2026-11-18_2026-11-18.json")
    with landing.open_input_stream(f"{BUCKET}/{key}") as stored:
        assert stored.read() == payload

    table.refresh()
    assert report.snapshot_id == table.metadata.current_snapshot_id
    assert refs(table) == {"main"}  # the audit branch was published and removed
    rows = main_rows(table)
    assert len(rows) == 24
    assert {(r["dataset"], r["electric_system"], r["run_id"]) for r in rows} == {
        ("demand_hourly", "canarias", report.run_id)
    }
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert summary["ibergrid.run-id"] == report.run_id
    assert summary["ibergrid.redata.window"] == "2026-11-18/2026-11-18"
    assert (
        summary["ibergrid.redata.expected-points"],
        summary["ibergrid.redata.received-points"],
    ) == (
        "24",
        "24",
    )
    assert summary["ibergrid.redata.incomplete-days"] == "{}"
    assert summary["ibergrid.redata.payload-sha256"] == rows[0]["payload_sha256"]


def test_a_rerun_of_the_window_leaves_the_same_rows(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    first = run(table, client, landing, CANARIAS, prefix="first")
    before = main_rows(table, *RUN_COLUMNS)
    second = run(table, client, landing, CANARIAS, prefix="second")

    assert second.snapshot_id != first.snapshot_id
    assert main_rows(table, *RUN_COLUMNS) == before
    assert {row["run_id"] for row in main_rows(table)} == {second.run_id}


def test_a_rerun_replaces_only_its_own_dataset_system_and_window(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    run(table, client, landing, PENINSULAR)
    run(table, client, landing, CANARIAS)
    realtime = Chunk(REALTIME, REALTIME.systems[0], DAY, DAY)
    run(table, client, landing, realtime)
    peninsular_before = [
        r for r in main_rows(table, *RUN_COLUMNS) if r["electric_system"] == "peninsular"
    ]
    # REData now answers the realtime window with one series only: Bronze keeps what it gives.
    name = "demand_realtime-peninsular-2026-11-18_2026-11-18.json"
    document = json.loads(fixture(name))
    document["included"] = [item for item in document["included"] if item["id"] == "2037"]
    redata.payloads[name] = json.dumps(document).encode()

    report = run(table, client, landing, realtime)

    rows = main_rows(table, *RUN_COLUMNS)
    assert report.rows == 288
    assert len(rows) == 24 + 24 + 288
    assert {r["series_id"] for r in rows if r["dataset"] == "demand_realtime"} == {"2037"}
    hourly = [r for r in rows if r["dataset"] == "demand_hourly"]
    assert [r for r in hourly if r["electric_system"] == "peninsular"] == [
        r for r in peninsular_before if r["dataset"] == "demand_hourly"
    ]


def test_an_invalid_answer_lands_and_writes_nothing(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    run(table, client, landing, PENINSULAR)
    table.refresh()
    before = table.metadata.current_snapshot_id
    name = "demand_hourly-canarias-2026-11-18_2026-11-18.json"
    document = json.loads(fixture(name))
    document["included"][0]["attributes"]["values"][3]["value"] = -5
    redata.payloads[name] = json.dumps(document).encode()

    with pytest.raises(InvalidResponse, match=r"value -5\.0 is not > 0"):
        run(table, client, landing, CANARIAS)

    table.refresh()
    assert table.metadata.current_snapshot_id == before
    assert refs(table) == {"main"}
    assert landing.get_file_info(f"{BUCKET}/{CANARIAS.landing_key}").type == fs.FileType.NotFound


def test_a_window_without_data_fails_after_bounded_retries(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    future = Chunk(HOURLY, HOURLY.systems[1], date(2026, 12, 5), date(2026, 12, 5))

    with pytest.raises(RedataError, match="HTTP 502: The requested data is currently unavailable"):
        run(table, client, landing, future)

    assert refs(table) == set()  # nothing was started


def racing_publish(table: Table, monkeypatch: pytest.MonkeyPatch, times: int) -> list[str]:
    """Another process appends to main just before each of the next ``times`` publish commits
    (the commits that remove an audit branch and move main) reaches the catalog."""
    commit = table.catalog.commit_table
    other = table.catalog.load_table(REDATA_OBSERVATIONS.identifier)
    raced: list[str] = []

    def racing_commit(tbl: Table, requirements: Any, updates: Any) -> Any:
        removes = [u.ref_name for u in updates if isinstance(u, RemoveSnapshotRefUpdate)]
        moves_main = any(getattr(u, "ref_name", None) == "main" for u in updates)
        if removes and moves_main and len(raced) < times:
            raced.append(removes[0])
            other.refresh()
            other.append(other.scan().to_arrow().slice(0, 1))  # any commit moves main
        return commit(tbl, requirements, updates)

    monkeypatch.setattr(table.catalog, "commit_table", racing_commit)
    return raced


def test_a_run_that_loses_the_publish_race_restarts_on_the_new_main(
    table: Table, client: RedataClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, PENINSULAR)
    raced = racing_publish(table, monkeypatch, times=1)

    report = run(table, client, landing, CANARIAS, prefix="race")

    assert raced == ["audit-race-demand_hourly-canarias-20261118-20261118-a1"]
    assert report.run_id.endswith("-a2")
    assert refs(table) == {"main"}
    canarias = [r for r in main_rows(table) if r["electric_system"] == "canarias"]
    assert len(canarias) == 24
    assert {r["run_id"] for r in canarias} == {report.run_id}


def test_a_run_that_keeps_losing_gives_up_and_leaves_no_branch(
    table: Table, client: RedataClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, PENINSULAR)
    racing_publish(table, monkeypatch, times=3)

    with pytest.raises(wap.PublishConflict):
        run(table, client, landing, CANARIAS)

    assert refs(table) == {"main"}
    assert {r["electric_system"] for r in main_rows(table)} == {"peninsular"}


def failing_commits(table: Table, monkeypatch: pytest.MonkeyPatch, *, discard_too: bool) -> None:
    """The catalog refuses the run's data commit (and, with ``discard_too``, its discard)."""
    commit = table.catalog.commit_table

    def refusing(tbl: Table, requirements: Any, updates: Any) -> Any:
        writes = any(isinstance(u, AddSnapshotUpdate) for u in updates)
        discards = all(isinstance(u, RemoveSnapshotRefUpdate) for u in updates)
        if writes or (discard_too and discards):
            raise CommitFailedException("refused by the test")
        return commit(tbl, requirements, updates)

    monkeypatch.setattr(table.catalog, "commit_table", refusing)


def test_any_other_failure_discards_the_branch(
    table: Table, client: RedataClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, PENINSULAR)
    failing_commits(table, monkeypatch, discard_too=False)

    with pytest.raises(wap.WapError, match="the table changed concurrently"):
        run(table, client, landing, CANARIAS)

    assert refs(table) == {"main"}


def test_a_failed_discard_does_not_hide_the_original_failure(
    table: Table, client: RedataClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, PENINSULAR)
    failing_commits(table, monkeypatch, discard_too=True)

    with pytest.raises(wap.WapError, match="writing run"):
        run(table, client, landing, CANARIAS, prefix="stuck")

    assert refs(table) == {"main", "audit-stuck-demand_hourly-canarias-20261118-20261118-a1"}


def test_the_audit_refuses_a_branch_that_does_not_hold_the_batch(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, CANARIAS)
    window = CANARIAS.row_filter()

    _audit(table, report.snapshot_id, 24, window)  # what was published passes
    with pytest.raises(
        AuditFailed, match=r"holds 24 rows \(24 distinct keys\) in the window, expected 25"
    ):
        _audit(table, report.snapshot_id, 25, window)
    with pytest.raises(AuditFailed, match="no snapshot"):
        _audit(table, None, 24, window)


def test_duplicate_keys_on_the_branch_fail_the_audit(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    run(table, client, landing, CANARIAS)
    table.refresh()
    rows = table.scan().to_arrow()
    table.append(rows.slice(0, 1))  # the same key twice, written around the protocol
    table.refresh()

    with pytest.raises(AuditFailed, match=r"25 rows \(24 distinct keys\)"):
        _audit(table, table.metadata.current_snapshot_id, 25, CANARIAS.row_filter())


# --- ingest(): the library entry point the CLI and the orchestrator call ------------------------


@pytest.fixture
def wired(
    table: Table,
    catalog: SqlCatalog,
    landing: fs.FileSystem,
    redata_url: str,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> Settings:
    """Settings for the fake REData; the catalog and the bucket are the local ones."""
    monkeypatch.setenv("IBERGRID_REDATA_BASE_URL", redata_url)
    monkeypatch.setattr(ingest_module, "rest_catalog", lambda _settings: catalog)
    monkeypatch.setattr(ingest_module, "s3_filesystem", lambda *_args, **_kw: landing)
    return Settings()


def test_ingest_runs_every_chunk_and_reports_each(wired: Settings, table: Table) -> None:
    seen: list[str] = []

    reports = ingest(
        wired,
        "demand_hourly",
        DAY,
        DAY,
        ["peninsular", "canarias"],
        run_id_prefix="dagster-123",
        on_chunk=lambda report: seen.append(report.chunk.system.name),
    )

    assert seen == ["peninsular", "canarias"]
    assert [r.rows for r in reports] == [24, 24]
    assert all(r.run_id.startswith("dagster-123-demand_hourly-") for r in reports)
    assert len(main_rows(table)) == 48


def test_ingest_names_its_runs_when_no_prefix_is_given(wired: Settings) -> None:
    [report] = ingest(wired, "generation_daily", DAY, DAY, ["canarias"])

    assert re.fullmatch(
        r"\d{8}T\d{6}Z-[0-9a-f]{4}-generation_daily-canarias-20261118-20261118-a1", report.run_id
    )
    assert report.rows == 7


def test_ingest_refuses_a_bad_prefix_before_any_request(wired: Settings) -> None:
    with pytest.raises(ValueError, match="run id prefix 'bad prefix' must match"):
        ingest(wired, "demand_hourly", DAY, DAY, run_id_prefix="bad prefix")


def test_ingest_before_lake_init_says_so(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    monkeypatch.setattr(ingest_module, "rest_catalog", lambda _settings: catalog)

    with pytest.raises(RedataError, match="run `ibergrid lake init` first"):
        ingest(Settings(), "demand_hourly", DAY, DAY)


def test_landing_on_a_local_disk_creates_the_directories(tmp_path: Path) -> None:
    local = fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())

    path = ingest_module.land(local, BUCKET, "landing/a/b.json", b"{}")
    again = ingest_module.land(local, BUCKET, "landing/a/b.json", b"[]")  # a rerun overwrites

    assert path == again == "s3://ibergrid/landing/a/b.json"
    assert (tmp_path / "ibergrid" / "landing" / "a" / "b.json").read_bytes() == b"[]"


def test_landing_on_object_storage_creates_no_directories(closed_port: int) -> None:
    """On S3 the body goes straight to PutObject: here, to an endpoint that refuses it."""
    storage = StorageSettings(
        endpoint_url=HttpUrl(f"http://127.0.0.1:{closed_port}"),
        access_key=SecretStr("test-access-key"),
        secret_key=SecretStr("test-secret-key-do-not-log"),
    )
    s3 = s3_filesystem(storage, timeout=1, max_attempts=1)

    with pytest.raises(OSError, match=r"landing/a/b\.json"):
        ingest_module.land(s3, BUCKET, "landing/a/b.json", b"{}")
