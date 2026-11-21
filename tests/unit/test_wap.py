"""Write-Audit-Publish on a real Iceberg catalog (sqlite + local files)."""

from collections.abc import Callable
from typing import Any

import duckdb
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


def test_a_direct_transaction_is_atomic_but_not_fenced(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Why checkpoints go through publish only: two consumers that read the same offsets both
    land their batch, because PyIceberg retries the losing transaction on fresh metadata."""

    def consume_offsets_10_to_20(table: Table) -> None:
        table.refresh()
        with table.transaction() as tx:
            tx.set_properties({"stream.offsets": "p0:20"})
            tx.append(rows(("batch-10-20", 1)))

    interleave(loaded, lambda: consume_offsets_10_to_20(other), monkeypatch)
    consume_offsets_10_to_20(loaded)

    assert [r for r in main_rows(loaded) if r[0] == "batch-10-20"] == [("batch-10-20", 1)] * 2


def test_two_publishes_of_the_same_offsets_cannot_both_land(loaded: Table, other: Table) -> None:
    """The same race through WAP: the second publish of a batch conflicts."""
    first, second = wap.start(loaded, "consumer-a"), wap.start(other, "consumer-b")
    wap.publish(
        loaded,
        first,
        wap.write(loaded, first, rows(("batch-10-20", 1))),
        set_properties={"stream.offsets": "p0:20"},
    )

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(
            other,
            second,
            wap.write(other, second, rows(("batch-10-20", 1))),
            set_properties={"stream.offsets": "p0:20"},
        )
    assert [r for r in main_rows(loaded) if r[0] == "batch-10-20"] == [("batch-10-20", 1)]


def test_a_stale_checkpoint_without_data_conflicts(loaded: Table, other: Table) -> None:
    """Runs that wrote nothing still move main (a checkpoint snapshot), so they are ordered."""
    late, early = wap.start(loaded, "late"), wap.start(other, "early")
    checkpoint = wap.publish(
        other, early, wap.head(other, early), set_properties={"stream.offsets": "p0:20"}
    )

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(
            loaded, late, wap.head(loaded, late), set_properties={"stream.offsets": "p0:12"}
        )

    assert checkpoint is not None
    assert checkpoint != late.base_snapshot_id
    assert loaded.metadata.current_snapshot_id == checkpoint
    assert loaded.properties["stream.offsets"] == "p0:20"
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]  # no data change
    snapshot = loaded.snapshot_by_id(checkpoint)
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary[wap.RUN_ID_PROPERTY] == "early"


def test_a_checkpoint_after_its_branch_moved_conflicts(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    # A write lands between the audit check and the checkpoint snapshot: the snapshot goes on top
    # of it, so it is not the audited state any more.
    interleave(loaded, lambda: wap.write(other, audit, rows(("2026-01-09", 9))), monkeypatch)

    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(loaded, audit, audited, set_properties={"stream.offsets": "p0:5"})
    assert "stream.offsets" not in loaded.properties
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]


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


def test_an_empty_checkpoint_run_can_be_published_twice(loaded: Table) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    offsets = {"stream.offsets": "p0:5"}

    checkpoint = wap.publish(loaded, audit, audited, set_properties=offsets)

    assert wap.publish(loaded, audit, audited, set_properties=offsets) == checkpoint
    assert loaded.properties["stream.offsets"] == "p0:5"


def fail_commit(
    table: Table, monkeypatch: pytest.MonkeyPatch, number: int, error: Exception, *, landed: bool
) -> None:
    """The ``number``-th commit from now raises ``error``, after reaching the catalog or not."""
    commit = table.catalog.commit_table
    calls = {"count": 0}

    def failing(*args: Any) -> Any:
        calls["count"] += 1
        if calls["count"] < number:
            return commit(*args)
        monkeypatch.setattr(table.catalog, "commit_table", commit)
        if landed:
            commit(*args)
        raise error

    monkeypatch.setattr(table.catalog, "commit_table", failing)


OFFSETS = {"stream.offsets": "p0:5"}


@pytest.mark.parametrize(
    "failure",
    [CommitStateUnknownException("504 Gateway Timeout"), RuntimeError("process killed")],
    ids=["lost answer", "crash"],
)
def test_a_checkpoint_interrupted_after_its_append_resumes(
    loaded: Table, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    fail_commit(loaded, monkeypatch, 2, failure, landed=False)  # 1: the append, 2: the publish

    with pytest.raises((wap.WapError, RuntimeError), match=r"same arguments|killed"):
        wap.publish(loaded, audit, audited, set_properties=OFFSETS)
    assert loaded.metadata.current_snapshot_id == audit.base_snapshot_id
    assert loaded.metadata.refs[audit.name].snapshot_id != audited  # its checkpoint is staged

    checkpoint = wap.publish(loaded, audit, audited, set_properties=OFFSETS)

    assert loaded.metadata.current_snapshot_id == checkpoint != audited
    assert loaded.properties["stream.offsets"] == "p0:5"
    assert set(loaded.metadata.refs) == {"main"}
    assert wap.publish(loaded, audit, audited, set_properties=OFFSETS) == checkpoint


def test_a_checkpoint_whose_answer_was_lost_after_landing_is_recognised(
    loaded: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    fail_commit(loaded, monkeypatch, 2, CommitStateUnknownException("502"), landed=True)

    checkpoint = wap.publish(loaded, audit, audited, set_properties=OFFSETS)

    assert checkpoint == loaded.metadata.current_snapshot_id != audited
    assert loaded.properties["stream.offsets"] == "p0:5"


def test_a_foreign_write_after_an_interrupted_checkpoint_still_conflicts(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    fail_commit(loaded, monkeypatch, 2, RuntimeError("process killed"), landed=False)
    with pytest.raises(RuntimeError):
        wap.publish(loaded, audit, audited, set_properties=OFFSETS)
    wap.write(other, audit, rows(("2026-01-09", 9)))  # on top of the staged checkpoint

    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(loaded, audit, audited, set_properties=OFFSETS)
    assert "stream.offsets" not in loaded.properties


def test_a_straggler_write_is_not_taken_for_the_runs_checkpoint(loaded: Table) -> None:
    """Same run id and parent as a checkpoint, but no marker: it was never audited."""
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    wap.write(loaded, audit, rows(("2026-01-09", 9)))  # a straggler, after the empty audit
    main_before = loaded.metadata.refs["main"].snapshot_id

    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(loaded, audit, audited, set_properties=OFFSETS)
    loaded.refresh()
    assert loaded.metadata.refs["main"].snapshot_id == main_before
    assert "stream.offsets" not in loaded.properties


def test_a_marked_snapshot_that_adds_data_is_not_resumed(loaded: Table, other: Table) -> None:
    """The checkpoint marker is a claim: a marked snapshot that adds a file is refused."""
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    forged = {wap.RUN_ID_PROPERTY: "idle", wap.CHECKPOINT_PROPERTY: "true"}
    other.append(rows(("2026-01-09", 7)), snapshot_properties=forged, branch=audit.name)
    main_before = loaded.metadata.refs["main"].snapshot_id

    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(loaded, audit, audited, set_properties=OFFSETS)
    loaded.refresh()
    assert loaded.metadata.refs["main"].snapshot_id == main_before
    assert "stream.offsets" not in loaded.properties


def test_a_checkpoint_needs_live_data_in_main(loaded: Table) -> None:
    """After a published delete-everything run, main holds no data file: an empty append on it
    would have an empty manifest list, which DuckDB cannot read."""
    wipe = wap.start(loaded, "wipe")
    wap.publish(loaded, wipe, wap.write(loaded, wipe, rows(), overwrite_filter="value >= 0"))
    idle = wap.start(loaded, "idle")

    with pytest.raises(wap.WapError, match="a checkpoint needs data"):
        wap.publish(loaded, idle, wap.head(loaded, idle), set_properties=OFFSETS)
    assert "stream.offsets" not in loaded.properties
    con = duckdb.connect()
    con.load_extension("iceberg")
    loaded.refresh()
    assert con.execute("FROM iceberg_scan(?)", [loaded.metadata_location]).fetchall() == []


def test_a_reused_run_id_cannot_claim_the_previous_checkpoint(loaded: Table) -> None:
    first = wap.start(loaded, "batch-10")
    wap.publish(loaded, first, wap.head(loaded, first), set_properties={"stream.offsets": "p0:20"})
    second = wap.start(loaded, "batch-10")  # its base is the first run's checkpoint
    audited = wap.head(loaded, second)
    wap.discard(loaded, second)

    with pytest.raises(wap.WapError, match="was discarded"):
        wap.publish(loaded, second, audited, set_properties={"stream.offsets": "p0:30"})
    assert loaded.properties["stream.offsets"] == "p0:20"


def test_a_discarded_empty_run_cannot_claim_its_checkpoint(loaded: Table) -> None:
    audit = wap.start(loaded, "idle")
    audited = wap.head(loaded, audit)
    wap.discard(loaded, audit)

    with pytest.raises(wap.WapError, match="was discarded"):
        wap.publish(loaded, audit, audited, set_properties={"stream.offsets": "p0:5"})
    assert "stream.offsets" not in loaded.properties
    # Without properties an empty run changes nothing, published or discarded alike.
    assert wap.publish(loaded, audit, audited) == audit.base_snapshot_id


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


@pytest.mark.parametrize("discarded", [False, True])
def test_a_checkpoint_on_a_table_without_snapshots_needs_data(
    table: Table, discarded: bool
) -> None:
    audit = wap.start(table, "idle")
    if discarded:
        wap.discard(table, audit)

    with pytest.raises(wap.WapError, match="a checkpoint needs data"):
        wap.publish(table, audit, None, set_properties=OFFSETS)
    assert "stream.offsets" not in table.properties
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


def test_an_overwrite_of_no_rows_deletes_and_main_stays_readable(loaded: Table) -> None:
    """Empty data and a filter matching every row: a delete, never an empty manifest list."""
    audit = wap.start(loaded, "wipe")
    audited = wap.write(loaded, audit, rows(), overwrite_filter="value >= 0")

    assert audited is not None
    snapshot = loaded.snapshot_by_id(audited)
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.operation.value == "delete"
    wap.publish(loaded, audit, audited)
    con = duckdb.connect()
    con.install_extension("iceberg")
    con.load_extension("iceberg")
    loaded.refresh()
    assert con.execute("FROM iceberg_scan(?)", [loaded.metadata_location]).fetchall() == []


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


def test_two_starts_racing_on_a_table_without_snapshots_fail_cleanly(
    table: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    interleave(table, lambda: wap.start(other, "same"), monkeypatch)

    with pytest.raises(wap.WapError, match="another start of the same run id raced"):
        wap.start(table, "same")
    assert set(table.metadata.refs) == {"audit-same"}


def test_an_overwrite_racing_a_discard_fails_cleanly(
    loaded: Table, other: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    interleave(loaded, lambda: wap.discard(other, audit), monkeypatch)

    with pytest.raises(wap.WapError, match="changed concurrently"):
        wap.write(loaded, audit, rows(("2026-01-01", 100)), overwrite_filter="day == '2026-01-01'")
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]


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
