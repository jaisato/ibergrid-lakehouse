"""Write-Audit-Publish on a real Iceberg catalog (sqlite + local files)."""

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitStateUnknownException
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField, StringType

from ibergrid.lake import wap

SCHEMA = Schema(
    NestedField(1, "day", StringType(), required=False),
    NestedField(2, "value", LongType(), required=False),
)


def rows(*pairs: tuple[str, int]) -> pa.Table:
    return pa.table(
        {"day": [day for day, _ in pairs], "value": pa.array([v for _, v in pairs], pa.int64())}
    )


def content(table: pa.Table) -> list[tuple[str, int]]:
    return sorted(zip(table["day"].to_pylist(), table["value"].to_pylist(), strict=True))


def main_rows(table: Table) -> list[tuple[str, int]]:
    table.refresh()
    return content(table.scan().to_arrow())


def branch_rows(table: Table, audit: wap.AuditBranch) -> list[tuple[str, int]]:
    table.refresh()
    return content(table.scan(snapshot_id=table.metadata.refs[audit.name].snapshot_id).to_arrow())


def append_elsewhere(other: Table, data: pa.Table) -> None:
    """Another process appends to main with the metadata it has just loaded."""
    other.refresh()
    other.append(data)


def interleave(table: Table, writer: Callable[[], object], monkeypatch: pytest.MonkeyPatch) -> None:
    """Run ``writer`` (another process's commit) just before the next commit reaches the catalog."""
    commit = table.catalog.commit_table

    def racing_commit(*args: Any) -> Any:
        monkeypatch.setattr(table.catalog, "commit_table", commit)  # the writer commits too
        writer()
        return commit(*args)

    monkeypatch.setattr(table.catalog, "commit_table", racing_commit)


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace("ns")
    return catalog.create_table(("ns", "readings"), SCHEMA)


@pytest.fixture
def loaded(table: Table) -> Table:
    table.append(rows(("2026-01-01", 1), ("2026-01-02", 2)))
    return table


@pytest.fixture
def other(catalog: SqlCatalog, table: Table) -> Table:
    """The same table as another process sees it."""
    return catalog.load_table(("ns", "readings"))


# --- the protocol -------------------------------------------------------------------------------


def test_writes_land_on_the_branch_and_never_on_main(loaded: Table) -> None:
    main_before = loaded.metadata.current_snapshot_id

    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    head = wap.write(
        loaded, audit, rows(("2026-01-02", 20)), overwrite_filter="day == '2026-01-02'"
    )

    assert audit.base_snapshot_id == audit.root_snapshot_id == main_before
    assert loaded.metadata.current_snapshot_id == main_before
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    audited = wap.read(loaded, audit)
    assert audited.snapshot_id == head == wap.head(loaded, audit)
    assert content(audited.rows) == [("2026-01-01", 1), ("2026-01-02", 20), ("2026-01-03", 3)]
    assert head is not None
    snapshot = loaded.snapshot_by_id(head)
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary[wap.RUN_ID_PROPERTY] == "run-1"


def test_publish_moves_main_to_the_audited_snapshot_and_removes_the_branch(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    audited = wap.head(loaded, audit)

    assert wap.publish(loaded, audit, audited) == audited
    assert loaded.metadata.current_snapshot_id == audited
    assert set(loaded.metadata.refs) == {"main"}
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2), ("2026-01-03", 3)]


def test_publish_conflicts_when_main_moved_and_keeps_both_sides(
    loaded: Table, other: Table
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    append_elsewhere(other, rows(("2026-01-04", 4)))

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(loaded, audit, audited)

    assert loaded.metadata.current_snapshot_id == other.metadata.current_snapshot_id
    assert loaded.metadata.refs[audit.name].snapshot_id == audited
    assert ("2026-01-03", 3) in branch_rows(loaded, audit)


def test_publish_refuses_a_branch_written_after_its_audit(loaded: Table, other: Table) -> None:
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    audited = wap.head(loaded, audit)
    wap.write(other, audit, rows(("2026-01-09", 9)))  # unaudited

    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(loaded, audit, audited)

    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert ("2026-01-09", 9) in branch_rows(loaded, audit)


def test_the_catalog_rejects_a_publish_racing_a_write_to_main(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    interleave(loaded, lambda: append_elsewhere(other, rows(("2026-01-04", 4))), monkeypatch)

    with pytest.raises(wap.PublishConflict, match="nothing was published"):
        wap.publish(loaded, audit, audited, set_properties={"stream.offsets": "42"})

    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2), ("2026-01-04", 4)]
    assert loaded.metadata.refs[audit.name].snapshot_id == audited
    assert "stream.offsets" not in loaded.properties


def test_the_catalog_rejects_a_publish_racing_a_write_to_the_branch(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    interleave(loaded, lambda: wap.write(other, audit, rows(("2026-01-05", 5))), monkeypatch)

    with pytest.raises(wap.PublishConflict):
        wap.publish(loaded, audit, audited)

    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert ("2026-01-05", 5) in branch_rows(loaded, audit)


def test_publish_sets_properties_in_the_same_commit(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    versions = len(loaded.metadata.metadata_log)

    wap.publish(loaded, audit, audited, set_properties={"stream.offsets": "p0:42"})

    assert loaded.properties["stream.offsets"] == "p0:42"
    assert loaded.metadata.current_snapshot_id == audited
    assert len(loaded.metadata.metadata_log) == versions + 1  # one commit, one metadata file


def test_a_direct_write_commits_data_and_properties_in_one_transaction(loaded: Table) -> None:
    """The equivalent of ``publish(..., set_properties=...)`` for a write without WAP."""
    versions = len(loaded.metadata.metadata_log)

    with loaded.transaction() as tx:
        tx.set_properties({"stream.offsets": "p0:7"})
        tx.append(rows(("2026-01-03", 3)))

    assert loaded.properties["stream.offsets"] == "p0:7"
    assert ("2026-01-03", 3) in main_rows(loaded)
    assert len(loaded.metadata.metadata_log) == versions + 1


# --- a run that was published, discarded or re-created ------------------------------------------


def test_a_discard_racing_a_write_cannot_publish_the_rest_of_the_run(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    interleave(loaded, lambda: wap.discard(other, audit), monkeypatch)

    # PyIceberg retries the lost race on fresh metadata and re-creates the branch from nothing.
    with pytest.raises(wap.WapError, match="no longer descends from snapshot"):
        wap.write(loaded, audit, rows(("2026-01-04", 4)))

    recreated = loaded.metadata.refs[audit.name].snapshot_id
    assert loaded.snapshot_by_id(recreated).parent_snapshot_id is None  # type: ignore[union-attr]
    with pytest.raises(wap.WapError, match="no longer descends from snapshot"):
        wap.publish(loaded, audit, recreated)
    with pytest.raises(wap.WapError, match="no longer descends"):
        wap.read(loaded, audit)
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    wap.discard(loaded, audit)
    assert set(loaded.metadata.refs) == {"main"}


@pytest.mark.parametrize("ended_by", ["discard", "publish"])
@pytest.mark.parametrize("overwrite_filter", [None, "day == '2026-01-01'"])
def test_writes_to_an_ended_run_are_refused(
    loaded: Table, ended_by: str, overwrite_filter: str | None
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    if ended_by == "discard":
        wap.discard(loaded, audit)
    else:
        wap.publish(loaded, audit, audited)
    main_before = main_rows(loaded)

    with pytest.raises(wap.WapError, match="does not exist: the run was published or discarded"):
        wap.write(loaded, audit, rows(("2026-01-01", 100)), overwrite_filter=overwrite_filter)

    assert main_rows(loaded) == main_before
    assert set(loaded.metadata.refs) == {"main"}


def test_publishing_twice_returns_the_published_snapshot(loaded: Table, other: Table) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    wap.publish(loaded, audit, audited)
    append_elsewhere(other, rows(("2026-01-04", 4)))  # a later run published on top

    assert wap.publish(loaded, audit, audited) == audited
    assert main_rows(loaded) == [
        ("2026-01-01", 1),
        ("2026-01-02", 2),
        ("2026-01-03", 3),
        ("2026-01-04", 4),
    ]


def test_an_empty_run_can_be_published_twice(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.head(loaded, audit)

    assert audited == audit.base_snapshot_id
    assert wap.publish(loaded, audit, audited) == audited
    assert wap.publish(loaded, audit, audited) == audited
    assert set(loaded.metadata.refs) == {"main"}


def test_discard_removes_the_branch_and_leaves_main(loaded: Table) -> None:
    main_before = loaded.metadata.current_snapshot_id
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))

    wap.discard(loaded, audit)
    wap.discard(loaded, audit)  # idempotent

    assert set(loaded.metadata.refs) == {"main"}
    assert loaded.metadata.current_snapshot_id == main_before
    assert wap.start(loaded, "run-1").name == "audit-run-1"  # the run id is free again


def test_a_discarded_run_cannot_be_published(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    wap.discard(loaded, audit)

    with pytest.raises(wap.WapError, match="was discarded"):
        wap.publish(loaded, audit, audited)


def lose_the_answer(table: Table, monkeypatch: pytest.MonkeyPatch, *, landed: bool) -> None:
    """The next commit raises CommitStateUnknownException (a 5xx), after landing or not."""
    commit = table.catalog.commit_table

    def unknown(*args: Any) -> Any:
        monkeypatch.setattr(table.catalog, "commit_table", commit)
        if landed:
            commit(*args)
        raise CommitStateUnknownException("502 Bad Gateway")

    monkeypatch.setattr(table.catalog, "commit_table", unknown)


def test_a_publish_whose_answer_was_lost_is_recognised(
    loaded: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    lose_the_answer(loaded, monkeypatch, landed=True)

    assert wap.publish(loaded, audit, audited) == audited
    assert loaded.metadata.current_snapshot_id == audited
    assert wap.publish(loaded, audit, audited) == audited  # the caller's own retry


def test_a_publish_that_did_not_land_says_so(
    loaded: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    audited = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    lose_the_answer(loaded, monkeypatch, landed=False)

    with pytest.raises(wap.WapError, match="has not landed"):
        wap.publish(loaded, audit, audited)

    assert loaded.metadata.refs[audit.name].snapshot_id == audited
    assert wap.publish(loaded, audit, audited) == audited


# --- tables without snapshots -------------------------------------------------------------------


def test_an_empty_table_runs_on_an_empty_root_branch(table: Table) -> None:
    audit = wap.start(table, "first-load")

    assert audit.base_snapshot_id is None
    assert set(table.metadata.refs) == {audit.name}
    assert table.metadata.refs[audit.name].snapshot_id == audit.root_snapshot_id
    assert wap.head(table, audit) is None  # nothing to audit yet
    assert wap.read(table, audit).rows.num_rows == 0
    assert wap.write(table, audit, rows()) is None  # no rows, no snapshot

    wap.write(table, audit, rows(("2026-01-01", 1)))
    audited = wap.write(table, audit, rows(("2026-01-02", 2)))

    assert table.metadata.current_snapshot_id is None  # main still has no snapshot
    assert audited is not None
    assert wap.publish(table, audit, audited) == audited
    assert main_rows(table) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert set(table.metadata.refs) == {"main"}


def test_an_empty_run_on_an_empty_table_publishes_nothing(table: Table) -> None:
    audit = wap.start(table, "nothing")

    assert wap.publish(table, audit, None) is None
    assert wap.publish(table, audit, None) is None
    assert table.metadata.refs == {}
    assert table.metadata.current_snapshot_id is None


def test_an_empty_table_that_got_data_meanwhile_conflicts(table: Table, other: Table) -> None:
    audit = wap.start(table, "first-load")
    audited = wap.write(table, audit, rows(("2026-01-01", 1)))
    append_elsewhere(other, rows(("2026-01-09", 9)))

    with pytest.raises(wap.PublishConflict, match="from None to"):
        wap.publish(table, audit, audited)
    assert main_rows(table) == [("2026-01-09", 9)]


def test_a_discarded_run_on_an_empty_table_cannot_be_published(table: Table) -> None:
    audit = wap.start(table, "first-load")
    audited = wap.write(table, audit, rows(("2026-01-01", 1)))
    wap.discard(table, audit)

    with pytest.raises(wap.WapError, match="was discarded"):
        wap.publish(table, audit, audited)


def test_an_empty_append_commits_nothing(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")

    assert wap.write(loaded, audit, rows()) == audit.base_snapshot_id
    assert len(loaded.metadata.snapshots) == 1


# --- misuse and races on start and discard ------------------------------------------------------


@pytest.mark.parametrize("run_id", ["", "-starts-with-dash", "has space", "slash/", "x" * 101])
def test_run_ids_are_safe_ref_names(loaded: Table, run_id: str) -> None:
    with pytest.raises(ValueError, match="run id"):
        wap.start(loaded, run_id)


@pytest.mark.parametrize("fixture", ["table", "loaded"])
def test_a_run_id_cannot_be_reused_while_its_branch_exists(
    request: pytest.FixtureRequest, fixture: str
) -> None:
    table: Table = request.getfixturevalue(fixture)
    wap.start(table, "run-1")

    with pytest.raises(wap.WapError, match="already exists"):
        wap.start(table, "run-1")


def test_two_starts_racing_for_one_run_id_fail_cleanly(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    interleave(loaded, lambda: wap.start(other, "run-1"), monkeypatch)

    with pytest.raises(wap.WapError, match="changed concurrently"):
        wap.start(loaded, "run-1")


def test_a_discard_racing_a_write_fails_cleanly(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    interleave(loaded, lambda: wap.write(other, audit, rows(("2026-01-03", 3))), monkeypatch)

    with pytest.raises(wap.WapError, match="changed concurrently"):
        wap.discard(loaded, audit)
    assert ("2026-01-03", 3) in branch_rows(loaded, audit)


def test_a_run_cannot_touch_another_table(catalog: SqlCatalog, loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    other = catalog.create_table(("ns", "other"), SCHEMA)

    for operation in (wap.read, wap.head, wap.discard):
        with pytest.raises(wap.WapError, match="belongs to table"):
            operation(other, audit)
    with pytest.raises(wap.WapError, match="belongs to table"):
        wap.write(other, audit, rows(("2026-01-01", 1)))
    with pytest.raises(wap.WapError, match="belongs to table"):
        wap.publish(other, audit, audit.base_snapshot_id)
