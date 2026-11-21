"""Snapshot expiry, orphan-file cleanup and maintenance on a real Iceberg catalog."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField

from ibergrid.lake import wap
from ibergrid.lake.maintenance import (
    collect_referenced_paths,
    expire_snapshots,
    maintain_table,
    remove_orphan_files,
)

# Snapshot timestamps come from the wall clock (which can step back on WSL): every cutoff is
# far from them, never "now".
LATER = datetime.now(UTC) + timedelta(days=1)
EARLIER = datetime.now(UTC) - timedelta(days=1)


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace("ns")
    table = catalog.create_table(("ns", "t"), Schema(NestedField(1, "x", LongType(), False)))
    for value in range(5):
        table.append(pa.table({"x": pa.array([value], pa.int64())}))
    return table


def ids(table: Table) -> list[int]:
    return [s.snapshot_id for s in table.metadata.snapshots]


def test_keeps_the_last_snapshots_of_main_and_the_data(table: Table) -> None:
    history = ids(table)

    expired = expire_snapshots(table, older_than=LATER, retain_last=2)

    assert sorted(expired) == sorted(history[:3])
    assert ids(table) == history[3:]
    assert sorted(table.scan().to_arrow()["x"].to_pylist()) == [0, 1, 2, 3, 4]


def test_nothing_newer_than_the_cutoff_is_expired(table: Table) -> None:
    assert expire_snapshots(table, older_than=EARLIER) == []
    assert len(ids(table)) == 5


def test_open_runs_keep_their_whole_lineage_back_to_main(table: Table) -> None:
    history = ids(table)
    audit = wap.start(table, "open-run")
    wap.write(table, audit, pa.table({"x": pa.array([10], pa.int64())}))
    wap.write(table, audit, pa.table({"x": pa.array([11], pa.int64())}))
    run = [s.snapshot_id for s in table.metadata.snapshots if s.snapshot_id not in history]

    expire_snapshots(table, older_than=LATER, retain_last=1)

    assert len(run) == 2
    assert set(ids(table)) == {*run, history[-1]}  # the run's snapshots and the fork
    assert sorted(wap.read(table, audit).rows["x"].to_pylist()) == [0, 1, 2, 3, 4, 10, 11]


def test_a_run_on_an_empty_table_keeps_its_root(catalog: SqlCatalog) -> None:
    catalog.create_namespace("empty")
    table = catalog.create_table(("empty", "t"), Schema(NestedField(1, "x", LongType(), False)))
    audit = wap.start(table, "first-load")
    audited = wap.write(table, audit, pa.table({"x": pa.array([1], pa.int64())}))

    assert expire_snapshots(table, older_than=LATER) == []
    assert wap.publish(table, audit, audited) == audited


def test_tag_heads_survive(table: Table) -> None:
    second = ids(table)[1]
    table.manage_snapshots().create_tag(second, "before-backfill").commit()

    expire_snapshots(table, older_than=LATER, retain_last=1)

    assert ids(table) == [second, table.metadata.current_snapshot_id]
    assert table.scan(snapshot_id=second).to_arrow().num_rows == 2


def test_a_naive_cutoff_is_refused(table: Table) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        expire_snapshots(table, older_than=datetime.now() + timedelta(days=1))


def test_an_empty_table_has_nothing_to_expire(catalog: SqlCatalog) -> None:
    catalog.create_namespace("empty")
    table = catalog.create_table(("empty", "t"), Schema(NestedField(1, "x", LongType(), False)))

    assert expire_snapshots(table, older_than=LATER) == []


def test_main_head_can_never_be_expired(table: Table) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        expire_snapshots(table, older_than=LATER, retain_last=0)


def _table_root(table: Table) -> Path:
    assert isinstance(table.io, PyArrowFileIO)
    _, _, path = PyArrowFileIO.parse_location(table.location(), table.io.properties)
    return Path(path)


def test_remove_orphan_files_protects_live_refs_and_grace_window(table: Table) -> None:
    # Add a tag and an open audit branch so their files must NEVER be treated as orphans.
    second = ids(table)[1]
    table.manage_snapshots().create_tag(second, "keep-me").commit()
    audit = wap.start(table, "open-audit")
    wap.write(table, audit, pa.table({"x": pa.array([99], pa.int64())}))

    root = _table_root(table)
    old_orphan = root / "data" / "orphan-old.parquet"
    recent_orphan = root / "data" / "orphan-recent.parquet"
    old_orphan.write_bytes(b"stale-orphan-payload")
    recent_orphan.write_bytes(b"recent-uncommitted-payload")

    # Age only old_orphan beyond the 48h grace window.
    old_ts = (datetime.now(UTC) - timedelta(hours=72)).timestamp()
    os.utime(old_orphan, (old_ts, old_ts))

    referenced = collect_referenced_paths(table)
    assert str(old_orphan) not in referenced
    assert str(recent_orphan) not in referenced

    # Dry run detects old_orphan, skips recent_orphan, and deletes nothing.
    dry = remove_orphan_files(table, grace_period=timedelta(hours=48), dry_run=True)
    assert dry.dry_run is True
    assert dry.orphan_files == (str(old_orphan),)
    assert dry.deleted_files == 0
    assert dry.reclaimed_bytes == len(b"stale-orphan-payload")
    assert dry.skipped_recent_files == 1
    assert old_orphan.exists()
    assert recent_orphan.exists()

    # Live run deletes ONLY old_orphan; recent_orphan, tag, main and audit branch stay intact!
    live = remove_orphan_files(table, grace_period=timedelta(hours=48), dry_run=False)
    assert live.dry_run is False
    assert live.orphan_files == (str(old_orphan),)
    assert live.deleted_files == 1
    assert live.reclaimed_bytes == len(b"stale-orphan-payload")
    assert live.skipped_recent_files == 1
    assert not old_orphan.exists()
    assert recent_orphan.exists()
    assert sorted(table.scan().to_arrow()["x"].to_pylist()) == [0, 1, 2, 3, 4]
    assert table.scan(snapshot_id=second).to_arrow().num_rows == 2
    assert sorted(wap.read(table, audit).rows["x"].to_pylist()) == [0, 1, 2, 3, 4, 99]


def test_remove_orphan_files_cleans_files_after_snapshot_expiry(table: Table) -> None:
    # Overwrite all rows (delete + append = 2 snapshots on top of 5) to create orphan files.
    table.overwrite(pa.table({"x": pa.array([42], pa.int64())}))
    expired = expire_snapshots(table, older_than=LATER, retain_last=1)
    assert len(expired) == 6

    # Using an explicit future older_than simulates aging past the grace window.
    result = remove_orphan_files(table, older_than=LATER, dry_run=False)
    assert result.deleted_files >= 5
    assert result.reclaimed_bytes > 0
    assert table.scan().to_arrow()["x"].to_pylist() == [42]


def test_remove_orphan_files_and_maintain_table_validation(table: Table) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        remove_orphan_files(table, grace_period=timedelta(seconds=-1))
    with pytest.raises(ValueError, match="older_than must be timezone-aware"):
        remove_orphan_files(table, older_than=datetime.now())
    with pytest.raises(ValueError, match="now must be timezone-aware"):
        remove_orphan_files(table, now=datetime.now())

    # maintain_table respects dry_run and open audit branches.
    dry_report = maintain_table(table, table_name="ns.t", compact=True, dry_run=True)
    assert dry_report.compaction is None
    assert dry_report.compaction_skipped_reason == "dry_run"
    assert dry_report.expired_snapshots == ()
    assert dry_report.orphans is not None
    assert dry_report.orphans.dry_run is True

    # Open audit branch skips compaction safely even when con is given.
    audit = wap.start(table, "busy")
    open_report = maintain_table(
        table,
        table_name="ns.t",
        duck_table_name="lake.ns.t",
        con=None,
        compact=True,
        expire_days=None,
        clean_orphans=False,
    )
    assert open_report.compaction_skipped_reason == "no_duckdb_connection"
    wap.discard(table, audit)

    with pytest.raises(ValueError, match="now must be timezone-aware"):
        maintain_table(table, table_name="ns.t", now=datetime.now())
