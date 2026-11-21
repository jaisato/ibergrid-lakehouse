"""Table maintenance: snapshot expiry (PyIceberg) and data-file compaction (DuckDB).

Neither deletes the files that stop being referenced (measured on Lakekeeper 0.15.1, ADR 0002),
so storage is reclaimed only by an orphan-file cleanup, which does not exist yet.
"""

from dataclasses import dataclass
from datetime import datetime

import duckdb
from pyiceberg.table import Table
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.table.snapshots import ancestors_of


class MaintenanceConflict(RuntimeError):
    """Another commit reached the table first; nothing was changed, run the maintenance again."""


def expire_snapshots(table: Table, older_than: datetime, retain_last: int = 1) -> list[int]:
    """Expire the snapshots older than ``older_than``, keeping the last ``retain_last`` of ``main``.

    PyIceberg rather than DuckDB: DuckDB 2.0.1 refuses tables with any ref besides ``main``, such
    as an open audit branch. Tag heads and the whole lineage of every other branch, back to where
    it forked from ``main``, are kept, so an open run can still prove what it descends from.
    ``older_than`` must be timezone-aware. Returns the expired ids.
    """
    if older_than.tzinfo is None:
        raise ValueError("older_than must be timezone-aware (snapshot times are UTC)")
    if retain_last < 1:
        raise ValueError("retain_last must be at least 1: main's head is never expired")
    table.refresh()
    main = [s.snapshot_id for s in ancestors_of(table.current_snapshot(), table.metadata)]
    keep = set(main[:retain_last])
    for name, ref in table.metadata.refs.items():
        keep.add(ref.snapshot_id)
        if name == MAIN_BRANCH or ref.snapshot_ref_type != SnapshotRefType.BRANCH:
            continue
        for snapshot in ancestors_of(table.snapshot_by_id(ref.snapshot_id), table.metadata):
            keep.add(snapshot.snapshot_id)
            if snapshot.snapshot_id in main:  # the fork point: kept, and older ones are main's
                break
    cutoff_ms = older_than.timestamp() * 1000
    expired = [
        s.snapshot_id
        for s in table.metadata.snapshots
        if s.timestamp_ms < cutoff_ms and s.snapshot_id not in keep
    ]
    if expired:
        table.maintenance.expire_snapshots().by_ids(expired).commit()
    return expired


@dataclass(frozen=True)
class Compaction:
    rewritten_files: int
    added_files: int
    rewritten_bytes: int
    failed_files: int


def rewrite_data_files(con: duckdb.DuckDBPyConnection, table: str) -> Compaction:
    """Compact small data files with DuckDB's ``iceberg_rewrite_data_files`` (PyIceberg 0.12 has
    none), with the extension's default thresholds. ``table`` is a catalog name
    (``lake.<namespace>.<table>``).

    The rewrite commits a ``replace`` snapshot to ``main``, guarded by the catalog (DuckDB asserts
    the table UUID, the schema and ``main``'s head). So it conflicts both ways with Write-Audit-
    Publish: a write to ``main`` between its plan and its commit raises ``MaintenanceConflict``
    (nothing changed), and a run open across it can no longer publish (``PublishConflict``).
    """
    try:
        [row] = con.execute(
            "SELECT rewritten_data_files_count, added_data_files_count, rewritten_bytes_count, "
            "failed_data_files_count FROM iceberg_rewrite_data_files($table)",
            {"table": table},
        ).fetchall()
    except duckdb.TransactionException as exc:
        raise MaintenanceConflict(f"{table} changed during the compaction: {exc}") from exc
    return Compaction(*(int(value) for value in row))
