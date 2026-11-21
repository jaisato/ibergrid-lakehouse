"""Write-Audit-Publish on a real Iceberg catalog (sqlite + local files)."""

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
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


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace("ns")
    return catalog.create_table(("ns", "readings"), SCHEMA)


@pytest.fixture
def loaded(table: Table) -> Table:
    table.append(rows(("2026-01-01", 1), ("2026-01-02", 2)))
    return table


def test_writes_land_on_the_branch_and_never_on_main(loaded: Table) -> None:
    main_before = loaded.metadata.current_snapshot_id

    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    head = wap.write(
        loaded, audit, rows(("2026-01-02", 20)), overwrite_filter="day == '2026-01-02'"
    )

    assert audit.base_snapshot_id == main_before
    assert loaded.metadata.current_snapshot_id == main_before
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert content(wap.read(loaded, audit)) == [
        ("2026-01-01", 1),
        ("2026-01-02", 20),
        ("2026-01-03", 3),
    ]
    assert head is not None
    snapshot = loaded.snapshot_by_id(head)
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary[wap.RUN_ID_PROPERTY] == "run-1"


def test_publish_fast_forwards_main_and_removes_the_branch(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    head = wap.write(loaded, audit, rows(("2026-01-03", 3)))

    published = wap.publish(loaded, audit)

    assert published == head
    assert loaded.metadata.current_snapshot_id == head
    assert set(loaded.metadata.refs) == {"main"}
    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2), ("2026-01-03", 3)]


def test_publish_conflicts_when_main_moved_and_keeps_both_sides(
    catalog: SqlCatalog, loaded: Table
) -> None:
    audit = wap.start(loaded, "run-1")
    head = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    other_writer = catalog.load_table(("ns", "readings"))
    other_writer.append(rows(("2026-01-04", 4)))
    main_now = other_writer.metadata.current_snapshot_id

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(loaded, audit)

    assert loaded.metadata.current_snapshot_id == main_now
    assert loaded.metadata.refs[audit.name].snapshot_id == head
    assert ("2026-01-03", 3) in content(wap.read(loaded, audit))


def interleave(table: Table, writer: Callable[[], object], monkeypatch: pytest.MonkeyPatch) -> None:
    """Run ``writer`` (another process's commit) just before the next commit reaches the catalog."""
    commit = table.catalog.commit_table

    def racing_commit(*args: Any) -> Any:
        monkeypatch.setattr(table.catalog, "commit_table", commit)  # the writer commits too
        writer()
        return commit(*args)

    monkeypatch.setattr(table.catalog, "commit_table", racing_commit)


def test_the_catalog_rejects_a_publish_racing_a_write_to_main(
    catalog: SqlCatalog, loaded: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    head = wap.write(loaded, audit, rows(("2026-01-03", 3)))
    other_writer = catalog.load_table(("ns", "readings"))
    interleave(loaded, lambda: other_writer.append(rows(("2026-01-04", 4))), monkeypatch)

    with pytest.raises(wap.PublishConflict, match="nothing was published"):
        wap.publish(loaded, audit)

    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2), ("2026-01-04", 4)]
    assert loaded.metadata.refs[audit.name].snapshot_id == head


def test_the_catalog_rejects_a_publish_racing_a_write_to_the_branch(
    catalog: SqlCatalog, loaded: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit = wap.start(loaded, "run-1")
    wap.write(loaded, audit, rows(("2026-01-03", 3)))
    late_writer = catalog.load_table(("ns", "readings"))
    interleave(loaded, lambda: wap.write(late_writer, audit, rows(("2026-01-05", 5))), monkeypatch)

    with pytest.raises(wap.PublishConflict):
        wap.publish(loaded, audit)

    assert main_rows(loaded) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert ("2026-01-05", 5) in content(wap.read(loaded, audit))


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
    wap.discard(loaded, audit)

    with pytest.raises(wap.WapError, match="was discarded"):
        wap.publish(loaded, audit)


def test_an_empty_table_gets_its_branch_from_the_first_write(table: Table) -> None:
    audit = wap.start(table, "first-load")
    assert audit.base_snapshot_id is None
    assert table.metadata.refs == {}
    assert wap.read(table, audit).num_rows == 0
    assert wap.write(table, audit, rows()) is None  # no rows, no snapshot

    head = wap.write(table, audit, rows(("2026-01-01", 1)))
    wap.write(table, audit, rows(("2026-01-02", 2)))

    assert table.metadata.current_snapshot_id is None  # main still has no snapshot
    assert set(table.metadata.refs) == {audit.name}
    assert head is not None
    published = wap.publish(table, audit)
    assert published == table.metadata.current_snapshot_id
    assert main_rows(table) == [("2026-01-01", 1), ("2026-01-02", 2)]
    assert set(table.metadata.refs) == {"main"}


def test_an_empty_run_on_an_empty_table_publishes_nothing(table: Table) -> None:
    audit = wap.start(table, "nothing")

    assert wap.publish(table, audit) is None
    assert table.metadata.snapshots == []


def test_an_empty_table_that_got_data_meanwhile_conflicts(
    catalog: SqlCatalog, table: Table
) -> None:
    audit = wap.start(table, "first-load")
    wap.write(table, audit, rows(("2026-01-01", 1)))
    catalog.load_table(("ns", "readings")).append(rows(("2026-01-09", 9)))

    with pytest.raises(wap.PublishConflict, match="from None to"):
        wap.publish(table, audit)
    assert main_rows(table) == [("2026-01-09", 9)]


def test_an_empty_append_commits_nothing(loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")

    assert wap.write(loaded, audit, rows()) == audit.base_snapshot_id
    assert len(loaded.metadata.snapshots) == 1


@pytest.mark.parametrize("run_id", ["", "-starts-with-dash", "has space", "slash/", "x" * 101])
def test_run_ids_are_safe_ref_names(loaded: Table, run_id: str) -> None:
    with pytest.raises(ValueError, match="run id"):
        wap.start(loaded, run_id)


def test_a_run_id_cannot_be_reused_while_its_branch_exists(loaded: Table) -> None:
    wap.start(loaded, "run-1")

    with pytest.raises(wap.WapError, match="already exists"):
        wap.start(loaded, "run-1")


def test_a_run_cannot_touch_another_table(catalog: SqlCatalog, loaded: Table) -> None:
    audit = wap.start(loaded, "run-1")
    other = catalog.create_table(("ns", "other"), SCHEMA)

    for operation in (wap.read, wap.publish, wap.discard):
        with pytest.raises(wap.WapError, match="belongs to table"):
            operation(other, audit)
    with pytest.raises(wap.WapError, match="belongs to table"):
        wap.write(other, audit, rows(("2026-01-01", 1)))
