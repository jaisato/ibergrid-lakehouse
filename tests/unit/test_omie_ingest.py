"""OMIE ingestion through Write-Audit-Publish on a real Iceberg catalog (sqlite + local files), with
OMIE played back from the recorded files and the landing zone on a local directory."""

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyarrow import fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table
from pyiceberg.table.update import (
    AddSnapshotUpdate,
    RemoveSnapshotRefUpdate,
    SetSnapshotRefUpdate,
)

from ibergrid.lake import wap
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings
from ibergrid.sources.omie import ingest as ingest_module
from ibergrid.sources.omie.client import OmieClient, OmieError
from ibergrid.sources.omie.ingest import (
    AuditFailed,
    Chunk,
    ChunkReport,
    _audit,
    ingest,
    ingest_chunk,
    plan,
)
from ibergrid.sources.omie.marginalpdbc import MADRID, InvalidFile

from .conftest import FakeOmie, LocalServer, Responder, omie_file

Serve = Callable[[Responder], LocalServer]
BUCKET = "ibergrid"
TODAY = date(2026, 11, 21)  # the day these files were recorded
RUN_COLUMNS = ("fetched_at", "run_id")


def days(start: str, end: str | None = None) -> Chunk:
    return Chunk(date.fromisoformat(start), date.fromisoformat(end or start))


SEPT_30, OCT_1, OCT_26 = days("2025-09-30"), days("2025-10-01"), days("2025-10-26")


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, OMIE_DAY_AHEAD_PRICES)
    loaded: Table = catalog.load_table(OMIE_DAY_AHEAD_PRICES.identifier)
    return loaded


@pytest.fixture
def omie() -> FakeOmie:
    return FakeOmie()


@pytest.fixture
def omie_server(serve: Serve, omie: FakeOmie) -> LocalServer:
    return serve(omie)


@pytest.fixture
def client(omie_server: LocalServer) -> OmieClient:
    """No pacing and no waits between retries: the tests never sleep."""
    return OmieClient(omie_server.url, timeout=5, max_rps=1e9, sleep=lambda _: None)


@pytest.fixture
def landing(tmp_path: Path) -> fs.FileSystem:
    root = tmp_path / "bucket-root"
    root.mkdir()
    return fs.SubTreeFileSystem(str(root), fs.LocalFileSystem())


def run(
    table: Table,
    client: OmieClient,
    landing: fs.FileSystem,
    chunk: Chunk,
    prefix: str = "t",
    today: date = TODAY,
) -> ChunkReport:
    return ingest_chunk(table, client, landing, BUCKET, chunk, prefix, today=today)


def main_rows(table: Table, *drop: str, where: str | None = None) -> list[dict[str, Any]]:
    table.refresh()
    scan = table.scan() if where is None else table.scan(row_filter=where)
    data = scan.to_arrow().drop_columns(list(drop))
    rows: list[dict[str, Any]] = data.sort_by(
        [("delivery_date", "ascending"), ("period", "ascending")]
    ).to_pylist()
    return rows


def refs(table: Table) -> set[str]:
    table.refresh()
    return set(table.metadata.refs)


def landed(landing: fs.FileSystem) -> list[str]:
    selector = fs.FileSelector(BUCKET, recursive=True, allow_not_found=True)
    return sorted(i.path for i in landing.get_file_info(selector) if i.type == fs.FileType.File)


def test_a_chunk_is_landed_written_audited_and_published(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, OCT_1, prefix="cli-1")

    assert (report.rows, report.versions, report.violations) == (96, {2: 1}, {})
    assert (report.written, report.not_published_from) == (OCT_1, None)
    assert report.run_id is not None
    assert re.fullmatch(r"cli-1-omie-20251001-20251001-a1-[0-9a-f]{4}", report.run_id)
    payload = omie_file("2025-10-01")
    sha256 = hashlib.sha256(payload).hexdigest()
    key = f"landing/omie/marginalpdbc/2025/10/marginalpdbc_20251001-{sha256[:12]}.1"
    assert landed(landing) == [f"{BUCKET}/{key}"]
    with landing.open_input_stream(f"{BUCKET}/{key}") as stored:
        assert stored.read() == payload

    table.refresh()
    assert report.snapshot_id == table.metadata.current_snapshot_id
    assert refs(table) == {"main"}  # the audit branch was published and removed
    rows = main_rows(table)
    assert len(rows) == 96
    first = rows[0]
    assert first["delivery_date"] == date(2025, 10, 1)
    assert (first["period"], first["resolution_minutes"], first["contract_version"]) == (1, 15, 2)
    assert first["interval_start_utc"] == datetime(2025, 9, 30, 22, tzinfo=UTC)
    assert first["interval_end_utc"] == datetime(2025, 9, 30, 22, 15, tzinfo=UTC)
    assert {(r["source_file"], r["content_sha256"], r["run_id"]) for r in rows} == {
        ("marginalpdbc_20251001.1", sha256, report.run_id)
    }
    assert {r["landing_path"] for r in rows} == {f"s3://{BUCKET}/{key}"}
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert summary["ibergrid.run-id"] == report.run_id
    assert summary["ibergrid.omie.window"] == "2025-10-01/2025-10-01"
    assert (summary["ibergrid.omie.files"], summary["ibergrid.omie.periods"]) == ("1", "96")
    assert summary["ibergrid.omie.contract-versions"] == '{"2": 1}'
    assert summary["ibergrid.omie.rule-violations"] == "{}"
    assert "ibergrid.omie.not-published-from" not in summary.additional_properties


def test_a_rerun_of_the_window_leaves_the_same_rows(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    first = run(table, client, landing, OCT_26, prefix="same")
    before = main_rows(table, *RUN_COLUMNS)
    second = run(table, client, landing, OCT_26, prefix="same")

    assert second.snapshot_id != first.snapshot_id
    assert second.run_id != first.run_id  # same prefix, same attempt: the random part differs
    assert main_rows(table, *RUN_COLUMNS) == before
    assert len(before) == 100
    assert {row["run_id"] for row in main_rows(table)} == {second.run_id}
    assert len(landed(landing)) == 1  # the same bytes, the same object


def test_a_rerun_of_one_day_leaves_the_other_days_alone(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    run(table, client, landing, SEPT_30)
    run(table, client, landing, OCT_26)
    others = main_rows(table, where=OCT_26.outside_filter())

    report = run(table, client, landing, OCT_26)

    assert main_rows(table, where=OCT_26.outside_filter()) == others
    assert len(others) == 24
    assert {r["run_id"] for r in main_rows(table, where=OCT_26.row_filter())} == {report.run_id}


def test_a_file_omie_changed_replaces_the_rows_and_lands_beside_the_old_one(
    table: Table, client: OmieClient, landing: fs.FileSystem, omie: FakeOmie
) -> None:
    run(table, client, landing, SEPT_30)
    omie.files["marginalpdbc_20250930.1"] = omie_file("2025-09-30").replace(
        b"2025;09;30;1;", b"2025;09;30;1;1"
    )

    run(table, client, landing, SEPT_30)

    first = main_rows(table)[0]
    assert len(main_rows(table)) == 24
    assert str(first["price_pt_eur_mwh"]).startswith("1")
    assert len(landed(landing)) == 2
    assert first["landing_path"].removeprefix("s3://") in landed(landing)


def test_a_file_that_breaks_its_contract_lands_and_writes_nothing(
    table: Table, client: OmieClient, landing: fs.FileSystem, omie: FakeOmie
) -> None:
    """The first day is fine, the second is not: the chunk as a whole is refused."""
    omie.files["marginalpdbc_20250929.1"] = omie_file("2025-09-30").replace(b";09;30;", b";09;29;")
    omie.files["marginalpdbc_20250930.1"] = omie_file("2025-09-30").removesuffix(b"*\r\n")

    with pytest.raises(InvalidFile, match=r"marginalpdbc_20250930\.1: .* not the trailer"):
        run(table, client, landing, days("2025-09-29", "2025-09-30"))

    assert landed(landing) == []
    assert refs(table) == set()  # nothing was started


def test_a_missing_past_day_is_an_error_and_writes_nothing(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    with pytest.raises(OmieError, match=r"OMIE has no marginalpdbc_20251002\.1: delivery day"):
        run(table, client, landing, days("2025-10-01", "2025-10-02"))

    assert landed(landing) == []
    assert refs(table) == set()


def test_days_not_published_yet_end_the_chunk_where_they_start(
    table: Table, client: OmieClient, landing: fs.FileSystem, omie_server: LocalServer
) -> None:
    """Seen from 2026-11-17: the 18th is out (its file exists), the 19th is not yet."""
    report = run(table, client, landing, days("2026-11-18", "2026-11-30"), today=date(2026, 11, 17))

    assert (report.written, report.not_published_from) == (days("2026-11-18"), date(2026, 11, 19))
    assert report.rows == 96
    assert len(omie_server.requests) == 2  # nothing after the first missing day is asked for
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert summary["ibergrid.omie.window"] == "2026-11-18/2026-11-18"
    assert summary["ibergrid.omie.not-published-from"] == "2026-11-19"


def test_a_chunk_with_nothing_published_yet_writes_nothing(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, days("2026-11-19", "2026-11-30"), today=date(2026, 11, 17))

    assert report == ChunkReport(
        days("2026-11-19", "2026-11-30"), None, 0, {}, {}, date(2026, 11, 19), None, None
    )
    assert refs(table) == set()


def test_prices_outside_the_limits_are_published_and_recorded(
    table: Table, client: OmieClient, landing: fs.FileSystem, omie: FakeOmie
) -> None:
    omie.files["marginalpdbc_20250930.1"] = omie_file("2025-09-30").replace(
        b"2025;09;30;1;", b"2025;09;30;1;-60"
    )

    report = run(table, client, landing, SEPT_30)

    assert report.violations == {"price_pt_eur_mwh": 1}
    assert len(main_rows(table)) == 24
    summary = table.current_snapshot().summary  # type: ignore[union-attr]
    assert summary is not None
    assert json.loads(str(summary["ibergrid.omie.rule-violations"])) == {"price_pt_eur_mwh": 1}


def racing_publish(table: Table, monkeypatch: pytest.MonkeyPatch, times: int) -> list[str]:
    """Another process appends to main just before each of the next ``times`` publish commits
    (the commits that remove an audit branch and move main) reaches the catalog."""
    commit = table.catalog.commit_table
    other = table.catalog.load_table(OMIE_DAY_AHEAD_PRICES.identifier)
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
    table: Table, client: OmieClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, SEPT_30)
    raced = racing_publish(table, monkeypatch, times=1)

    report = run(table, client, landing, OCT_1, prefix="race")

    [lost] = raced
    assert re.fullmatch(r"audit-race-omie-20251001-20251001-a1-[0-9a-f]{4}", lost)
    assert report.run_id is not None
    assert re.fullmatch(r"race-.*-a2-[0-9a-f]{4}", report.run_id)
    assert refs(table) == {"main"}
    october = main_rows(table, where=OCT_1.row_filter())
    assert len(october) == 96
    assert {r["run_id"] for r in october} == {report.run_id}


def test_a_run_that_keeps_losing_gives_up_and_leaves_no_branch(
    table: Table, client: OmieClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, SEPT_30)
    racing_publish(table, monkeypatch, times=3)

    with pytest.raises(wap.PublishConflict):
        run(table, client, landing, OCT_1)

    assert refs(table) == {"main"}
    assert main_rows(table, where=OCT_1.row_filter()) == []


def writing_onto_the_branch(table: Table, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Another writer appends one more copy of a row to the audit branch right after the run's
    data commit, before the audit reads it."""
    commit = table.catalog.commit_table
    other = table.catalog.load_table(OMIE_DAY_AHEAD_PRICES.identifier)
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
            row = other.scan(snapshot_id=head, row_filter=OCT_1.row_filter()).to_arrow()
            other.append(row.slice(0, 1), branch=branches[0])
        return answer

    monkeypatch.setattr(table.catalog, "commit_table", racing_commit)
    return hit


def test_an_audit_failure_inside_the_run_publishes_nothing(
    table: Table, client: OmieClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, SEPT_30)
    table.refresh()
    before = table.metadata.current_snapshot_id
    hit = writing_onto_the_branch(table, monkeypatch)

    with pytest.raises(AuditFailed, match=r"holds 97 rows \(96 distinct keys\) in the window"):
        run(table, client, landing, OCT_1)

    assert len(hit) == 1
    table.refresh()
    assert table.metadata.current_snapshot_id == before
    assert refs(table) == {"main"}


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
    table: Table, client: OmieClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, SEPT_30)
    failing_commits(table, monkeypatch, discard_too=False)

    with pytest.raises(wap.WapError, match="the table changed concurrently"):
        run(table, client, landing, OCT_1)

    assert refs(table) == {"main"}


def test_a_failed_discard_does_not_hide_the_original_failure(
    table: Table, client: OmieClient, landing: fs.FileSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    run(table, client, landing, SEPT_30)
    failing_commits(table, monkeypatch, discard_too=True)

    with pytest.raises(wap.WapError, match="writing run"):
        run(table, client, landing, OCT_1, prefix="stuck")

    [left] = refs(table) - {"main"}
    assert left.startswith("audit-stuck-omie-20251001-20251001-a1-")


def test_the_audit_refuses_a_branch_that_does_not_hold_the_batch(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    report = run(table, client, landing, OCT_1)

    _audit(table, report.snapshot_id, 96, OCT_1, 0)  # what was published passes
    with pytest.raises(
        AuditFailed, match=r"holds 96 rows \(96 distinct keys\) in the window, expected 97"
    ):
        _audit(table, report.snapshot_id, 97, OCT_1, 0)
    with pytest.raises(AuditFailed, match="no snapshot"):
        _audit(table, None, 96, OCT_1, 0)


def test_the_audit_refuses_a_branch_that_changed_rows_outside_the_window(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    run(table, client, landing, SEPT_30)
    report = run(table, client, landing, OCT_1)
    _audit(table, report.snapshot_id, 96, OCT_1, 24)  # September's day: 24 rows, untouched
    table.delete(f"{SEPT_30.row_filter()} AND period = 1")
    table.refresh()

    with pytest.raises(
        AuditFailed, match=r"holds 23 rows outside 2025-10-01\.\.2025-10-01, 24 before"
    ):
        _audit(table, table.metadata.current_snapshot_id, 96, OCT_1, 24)


def test_duplicate_keys_on_the_branch_fail_the_audit(
    table: Table, client: OmieClient, landing: fs.FileSystem
) -> None:
    run(table, client, landing, OCT_1)
    table.refresh()
    table.append(table.scan().to_arrow().slice(0, 1))  # the same key twice, around the protocol
    table.refresh()

    with pytest.raises(AuditFailed, match=r"97 rows \(96 distinct keys\)"):
        _audit(table, table.metadata.current_snapshot_id, 97, OCT_1, 0)


# --- ingest(): the library entry point the CLI and the orchestrator call ------------------------


@pytest.fixture
def wired(
    table: Table,
    catalog: SqlCatalog,
    landing: fs.FileSystem,
    omie_server: LocalServer,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> Settings:
    """Settings for the fake OMIE; the catalog and the bucket are the local ones."""
    monkeypatch.setenv("IBERGRID_OMIE_BASE_URL", omie_server.url)
    monkeypatch.setattr(ingest_module, "rest_catalog", lambda _settings: catalog)
    monkeypatch.setattr(ingest_module, "s3_filesystem", lambda *_args, **_kw: landing)
    monkeypatch.setattr(ingest_module, "madrid_today", lambda: TODAY)
    return Settings()


def test_ingest_across_the_cut_over_runs_one_chunk_per_month(wired: Settings, table: Table) -> None:
    seen: list[str] = []

    reports = ingest(
        wired,
        date(2025, 9, 30),
        date(2025, 10, 1),
        run_id_prefix="dagster-123",
        on_chunk=lambda report: seen.append(str(report.chunk.start)),
    )

    assert seen == ["2025-09-30", "2025-10-01"]
    assert [(r.rows, r.versions) for r in reports] == [(24, {1: 1}), (96, {2: 1})]
    assert all(r.run_id and r.run_id.startswith("dagster-123-omie-") for r in reports)
    rows = main_rows(table)
    assert {(r["contract_version"], r["resolution_minutes"]) for r in rows} == {(1, 60), (2, 15)}
    # Contiguous across the cut-over: the last hour of the 30th ends where the 1st begins.
    hours, quarters = rows[:24], rows[24:]
    assert hours[-1]["interval_end_utc"] == quarters[0]["interval_start_utc"]


def test_ingest_stops_at_the_first_day_not_published_yet(
    wired: Settings, monkeypatch: pytest.MonkeyPatch, omie_server: LocalServer
) -> None:
    monkeypatch.setattr(ingest_module, "madrid_today", lambda: date(2026, 11, 17))

    reports = ingest(wired, date(2026, 11, 18), date(2027, 1, 31))

    [november] = reports  # December and January are not asked for
    assert november.not_published_from == date(2026, 11, 19)
    assert len(omie_server.requests) == 2


def test_ingest_names_its_runs_when_no_prefix_is_given(wired: Settings) -> None:
    [report] = ingest(wired, date(2025, 10, 26), date(2025, 10, 26))

    assert report.run_id is not None
    assert re.fullmatch(
        r"\d{8}T\d{6}Z-[0-9a-f]{4}-omie-20251026-20251026-a1-[0-9a-f]{4}", report.run_id
    )
    assert report.rows == 100


def test_ingest_can_target_another_table_and_landing_prefix(
    wired: Settings, catalog: SqlCatalog, table: Table, landing: fs.FileSystem
) -> None:
    ensure_table(catalog, replace(OMIE_DAY_AHEAD_PRICES, namespace="scratch"))

    [report] = ingest(
        wired,
        date(2025, 9, 30),
        date(2025, 9, 30),
        table=("scratch", "omie_day_ahead_prices"),
        landing_prefix="tests/landing",
    )

    assert main_rows(table) == []
    assert len(main_rows(catalog.load_table(("scratch", "omie_day_ahead_prices")))) == 24
    assert landed(landing)[0].startswith(f"{BUCKET}/tests/landing/2025/09/marginalpdbc_20250930-")
    assert report.rows == 24


def test_ingest_refuses_a_bad_prefix_before_any_request(wired: Settings) -> None:
    with pytest.raises(ValueError, match="run id prefix 'bad prefix' must match"):
        ingest(wired, date(2025, 9, 30), date(2025, 9, 30), run_id_prefix="bad prefix")


def test_ingest_before_lake_init_says_so(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    monkeypatch.setattr(ingest_module, "rest_catalog", lambda _settings: catalog)

    with pytest.raises(OmieError, match="run `ibergrid lake init` first"):
        ingest(Settings(), date(2025, 9, 30), date(2025, 9, 30))


def test_plan_cuts_calendar_months() -> None:
    assert plan(date(2024, 1, 30), date(2024, 3, 2)) == [
        days("2024-01-30", "2024-01-31"),
        days("2024-02-01", "2024-02-29"),
        days("2024-03-01", "2024-03-02"),
    ]
    assert plan(date(2025, 12, 31), date(2025, 12, 31)) == [days("2025-12-31")]
    with pytest.raises(ValueError, match="end 2025-09-29 is before start 2025-09-30"):
        plan(date(2025, 9, 30), date(2025, 9, 29))


def test_the_live_today_is_madrids() -> None:
    assert ingest_module.madrid_today() == datetime.now(MADRID).date()
