"""Ingestion through Write-Audit-Publish on a real Iceberg catalog (sqlite + local files), with
REData played back from the fixtures and the landing zone on a local directory."""

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from pyarrow import fs
from pydantic import HttpUrl, SecretStr
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.update import (
    AddSnapshotUpdate,
    RemoveSnapshotRefUpdate,
    SetSnapshotRefUpdate,
)

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
CANARIAS_FIXTURE = "demand_hourly-canarias-2026-11-18_2026-11-18.json"
# Columns that differ between two runs of the same window by design.
RUN_COLUMNS = ("fetched_at", "run_id")


def days(dataset: str, system: str, *isodays: str) -> list[Chunk]:
    spec = DATASETS[dataset]
    [found] = [s for s in spec.systems if s.name == system]
    return [Chunk(spec, found, date.fromisoformat(d), date.fromisoformat(d)) for d in isodays]


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


def main_rows(table: Table, *drop: str, where: str | None = None) -> list[dict[str, Any]]:
    table.refresh()
    scan = table.scan() if where is None else table.scan(row_filter=where)
    data = scan.to_arrow().drop_columns(list(drop))
    keys = ["dataset", "electric_system", "series_id", "ts_utc"]
    rows: list[dict[str, Any]] = data.sort_by([(k, "ascending") for k in keys]).to_pylist()
    return rows


def refs(table: Table) -> set[str]:
    table.refresh()
    return set(table.metadata.refs)


def edit(redata: FakeRedata, name: str, change: Callable[[dict[str, Any]], object]) -> None:
    """REData answers ``name`` with the fixture as ``change`` leaves it."""
    document = json.loads(fixture(name))
    change(document)
    redata.payloads[name] = json.dumps(document).encode()


def test_a_chunk_is_landed_written_audited_and_published(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, CANARIAS, prefix="cli-1")

    assert report.rows == 24
    assert (report.completeness.expected, report.completeness.received) == (24, 24)
    assert (report.violations, report.dropped_series) == ({}, ())
    assert re.fullmatch(
        r"cli-1-demand_hourly-canarias-20261118-20261118-a1-[0-9a-f]{4}", report.run_id
    )
    payload = fixture(CANARIAS_FIXTURE)
    sha256 = hashlib.sha256(payload).hexdigest()
    key = f"landing/redata/demand_hourly/canarias/2026/11/2026-11-18_2026-11-18-{sha256[:12]}.json"
    assert report.landing_path == f"s3://{BUCKET}/{key}"
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
    assert summary["ibergrid.redata.expected-points"] == "24"
    assert summary["ibergrid.redata.received-points"] == "24"
    assert summary["ibergrid.redata.incomplete-days"] == "{}"
    assert summary["ibergrid.redata.rule-violations"] == "{}"
    assert summary["ibergrid.redata.dropped-series"] == "[]"
    assert summary["ibergrid.redata.payload-sha256"] == sha256 == rows[0]["payload_sha256"]


def test_a_rerun_of_the_window_leaves_the_same_rows(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    first = run(table, client, landing, CANARIAS, prefix="same")
    before = main_rows(table, *RUN_COLUMNS)
    second = run(table, client, landing, CANARIAS, prefix="same")

    assert second.snapshot_id != first.snapshot_id
    assert second.run_id != first.run_id  # same prefix, same attempt: the random part differs
    assert second.landing_path == first.landing_path  # the same answer, the same object
    assert main_rows(table, *RUN_COLUMNS) == before
    assert {row["run_id"] for row in main_rows(table)} == {second.run_id}


def test_a_rerun_of_one_day_leaves_the_other_days_alone(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    spring, autumn, november = days(
        "demand_hourly", "peninsular", "2026-03-29", "2026-10-25", "2026-11-18"
    )
    for chunk in (spring, autumn, november):
        run(table, client, landing, chunk)
    others = "ts_utc < '2026-11-17T23:00:00+00:00'"
    untouched = main_rows(table, where=others)

    def raise_every_value(document: dict[str, Any]) -> None:
        for point in document["included"][0]["attributes"]["values"]:
            point["value"] += 1

    edit(redata, "demand_hourly-peninsular-2026-11-18_2026-11-18.json", raise_every_value)
    run(table, client, landing, november)

    assert len(untouched) == 23 + 25
    assert main_rows(table, where=others) == untouched  # run ids and fetch times included
    rerun = main_rows(table, where=november.row_filter())
    original = json.loads(fixture("demand_hourly-peninsular-2026-11-18_2026-11-18.json"))
    values = [p["value"] for p in original["included"][0]["attributes"]["values"]]
    assert [r["value"] for r in rerun] == [v + 1 for v in values]


def test_a_rerun_replaces_only_its_own_dataset_system_and_window(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    run(table, client, landing, PENINSULAR)
    run(table, client, landing, CANARIAS)
    [realtime] = days("demand_realtime", "national", "2026-11-18")
    first = run(table, client, landing, realtime)
    peninsular_before = main_rows(table, *RUN_COLUMNS, where=PENINSULAR.row_filter())

    # REData now answers the realtime window with one series only: Bronze keeps what it gives,
    # and the run records which series went missing.
    def only_real(document: dict[str, Any]) -> None:
        document["included"] = [item for item in document["included"] if item["id"] == "2037"]

    edit(redata, "demand_realtime-national-2026-11-18_2026-11-18.json", only_real)
    report = run(table, client, landing, realtime)

    assert report.rows == 288
    assert report.dropped_series == ("2052", "2053", "2054")
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert summary["ibergrid.redata.dropped-series"] == '["2052", "2053", "2054"]'
    rows = main_rows(table, *RUN_COLUMNS)
    assert len(rows) == 24 + 24 + 288
    assert {r["series_id"] for r in rows if r["dataset"] == "demand_realtime"} == {"2037"}
    assert main_rows(table, *RUN_COLUMNS, where=PENINSULAR.row_filter()) == peninsular_before
    # A different answer got its own landing object; the first one is still there.
    assert report.landing_path != first.landing_path
    for path in (first.landing_path, report.landing_path):
        info = landing.get_file_info(path.removeprefix("s3://"))
        assert info.type == fs.FileType.File


def test_an_answer_with_a_structural_problem_lands_and_writes_nothing(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    run(table, client, landing, PENINSULAR)
    table.refresh()
    before = table.metadata.current_snapshot_id
    edit(
        redata,
        CANARIAS_FIXTURE,
        lambda d: d["included"][0]["attributes"]["values"][3].update(value="n/a"),
    )

    with pytest.raises(InvalidResponse, match="value 'n/a'"):
        run(table, client, landing, CANARIAS)

    table.refresh()
    assert table.metadata.current_snapshot_id == before
    assert refs(table) == {"main"}
    canarias = f"{BUCKET}/landing/redata/demand_hourly/canarias"
    assert landing.get_file_info(canarias).type == fs.FileType.NotFound


def test_value_rule_breaches_are_published_and_recorded(
    table: Table, client: RedataClient, landing: fs.FileSystem, redata: FakeRedata
) -> None:
    edit(
        redata,
        CANARIAS_FIXTURE,
        lambda d: d["included"][0]["attributes"]["values"][3].update(value=-5),
    )

    report = run(table, client, landing, CANARIAS)

    assert report.violations == {"10297": 1}
    assert sorted(r["value"] for r in main_rows(table))[0] == -5.0
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert summary["ibergrid.redata.rule-violations"] == '{"10297": 1}'


def test_a_window_without_data_fails_after_bounded_retries(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    [future] = days("demand_hourly", "canarias", "2026-12-05")

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

    [lost] = raced
    assert re.fullmatch(r"audit-race-demand_hourly-canarias-20261118-20261118-a1-[0-9a-f]{4}", lost)
    assert re.fullmatch(r"race-.*-a2-[0-9a-f]{4}", report.run_id)
    assert refs(table) == {"main"}
    canarias = main_rows(table, where=CANARIAS.row_filter())
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


def writing_onto_the_branch(table: Table, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Another writer appends one more copy of a row to the audit branch right after the run's
    data commit, before the audit reads it."""
    commit = table.catalog.commit_table
    other = table.catalog.load_table(REDATA_OBSERVATIONS.identifier)
    hit: list[str] = []

    def racing_commit(tbl: Table, requirements: Any, updates: Any) -> Any:
        answer = commit(tbl, requirements, updates)
        writes = any(isinstance(u, AddSnapshotUpdate) for u in updates)
        branches = [
            u.ref_name
            for u in updates
            if isinstance(u, SetSnapshotRefUpdate) and u.ref_name.startswith("audit-")
        ]
        if writes and branches and not hit:
            hit.append(branches[0])
            other.refresh()
            head = other.metadata.refs[branches[0]].snapshot_id
            row = other.scan(snapshot_id=head, row_filter=CANARIAS.row_filter()).to_arrow()
            other.append(row.slice(0, 1), branch=branches[0])
        return answer

    monkeypatch.setattr(table.catalog, "commit_table", racing_commit)
    return hit


def test_an_audit_failure_inside_the_run_publishes_nothing(
    table: Table, client: RedataClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, PENINSULAR)
    table.refresh()
    before = table.metadata.current_snapshot_id
    hit = writing_onto_the_branch(table, monkeypatch)

    with pytest.raises(AuditFailed, match=r"holds 25 rows \(24 distinct keys\) in the window"):
        run(table, client, landing, CANARIAS)

    assert len(hit) == 1
    table.refresh()
    assert table.metadata.current_snapshot_id == before
    assert refs(table) == {"main"}
    assert main_rows(table, where=CANARIAS.row_filter()) == []


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

    [left] = refs(table) - {"main"}
    assert left.startswith("audit-stuck-demand_hourly-canarias-20261118-20261118-a1-")


def test_the_audit_refuses_a_branch_that_does_not_hold_the_batch(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, CANARIAS)

    _audit(table, report.snapshot_id, 24, CANARIAS, 0)  # what was published passes
    with pytest.raises(
        AuditFailed, match=r"holds 24 rows \(24 distinct keys\) in the window, expected 25"
    ):
        _audit(table, report.snapshot_id, 25, CANARIAS, 0)
    with pytest.raises(AuditFailed, match="no snapshot"):
        _audit(table, None, 24, CANARIAS, 0)


def test_the_audit_refuses_a_branch_that_changed_rows_outside_the_window(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    spring, november = days("demand_hourly", "peninsular", "2026-03-29", "2026-11-18")
    run(table, client, landing, spring)
    report = run(table, client, landing, november)
    _audit(table, report.snapshot_id, 24, november, 23)  # the spring day: 23 rows, untouched
    table.delete(f"{spring.row_filter()} AND ts_utc = '2026-03-28T23:00:00+00:00'")
    table.refresh()

    with pytest.raises(
        AuditFailed, match="holds 22 rows of demand_hourly peninsular outside the window, 23"
    ):
        _audit(table, table.metadata.current_snapshot_id, 24, november, 23)


def test_duplicate_keys_on_the_branch_fail_the_audit(
    table: Table, client: RedataClient, landing: fs.FileSystem
) -> None:
    run(table, client, landing, CANARIAS)
    table.refresh()
    table.append(table.scan().to_arrow().slice(0, 1))  # the same key twice, around the protocol
    table.refresh()

    with pytest.raises(AuditFailed, match=r"25 rows \(24 distinct keys\)"):
        _audit(table, table.metadata.current_snapshot_id, 25, CANARIAS, 0)


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


def test_ingest_splits_a_long_daily_window_at_the_last_full_month(
    wired: Settings, table: Table
) -> None:
    """2025-02-10..2025-10-20 in one request would come back with October up to its 31st."""
    reports = ingest(wired, "generation_daily", date(2025, 2, 10), date(2025, 10, 20), ["canarias"])

    assert [(str(r.chunk.start), str(r.chunk.end)) for r in reports] == [
        ("2025-02-10", "2025-09-30"),
        ("2025-10-01", "2025-10-20"),
    ]
    stamps = [r["ts_local"][:10] for r in main_rows(table)]
    assert (min(stamps), max(stamps)) == ("2025-02-10", "2025-10-20")


def test_ingest_names_its_runs_when_no_prefix_is_given(wired: Settings) -> None:
    [report] = ingest(wired, "generation_daily", DAY, DAY, ["canarias"])

    assert re.fullmatch(
        r"\d{8}T\d{6}Z-[0-9a-f]{4}-generation_daily-canarias-20261118-20261118-a1-[0-9a-f]{4}",
        report.run_id,
    )
    assert report.rows == 7


def test_ingest_can_target_another_table_and_landing_prefix(
    wired: Settings, catalog: SqlCatalog, table: Table, landing: fs.FileSystem
) -> None:
    ensure_table(catalog, replace(REDATA_OBSERVATIONS, namespace="scratch"))

    [report] = ingest(
        wired,
        "demand_hourly",
        DAY,
        DAY,
        ["canarias"],
        table=("scratch", "redata_observations"),
        landing_prefix="tests/landing",
    )

    assert main_rows(table) == []
    assert len(main_rows(catalog.load_table(("scratch", "redata_observations")))) == 24
    assert report.landing_path.startswith(f"s3://{BUCKET}/tests/landing/demand_hourly/canarias/")
    assert landing.get_file_info(report.landing_path.removeprefix("s3://")).type == fs.FileType.File


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
