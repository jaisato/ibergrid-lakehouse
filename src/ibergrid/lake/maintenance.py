"""Table maintenance: snapshot expiry (PyIceberg) and data-file compaction (DuckDB).

Both only rewrite metadata or replace files through a normal commit; neither deletes the files
that stop being referenced (measured on Lakekeeper 0.15.1, ADR 0002), so storage is reclaimed only
by an orphan-file cleanup, which does not exist yet.
"""

import itertools
from dataclasses import dataclass
from datetime import datetime

import duckdb
from pyiceberg.table import Table
from pyiceberg.table.snapshots import ancestors_of


def expire_snapshots(table: Table, older_than: datetime, retain_last: int = 1) -> list[int]:
    """Expire the snapshots older than ``older_than``, keeping the last ``retain_last`` of ``main``.

    PyIceberg rather than DuckDB: DuckDB 2.0.1 refuses tables with any ref besides ``main``, such
    as an open audit branch. Branch and tag heads are never expired. Returns the expired ids.
    """
    if retain_last < 1:
        raise ValueError("retain_last must be at least 1: main's head is never expired")
    table.refresh()
    keep = {
        s.snapshot_id
        for s in itertools.islice(
            ancestors_of(table.current_snapshot(), table.metadata), retain_last
        )
    }
    keep |= {ref.snapshot_id for ref in table.metadata.refs.values()}
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
    (``lake.<namespace>.<table>``): the rewrite commits through the catalog."""
    [row] = con.execute(
        "SELECT rewritten_data_files_count, added_data_files_count, rewritten_bytes_count, "
        "failed_data_files_count FROM iceberg_rewrite_data_files($table)",
        {"table": table},
    ).fetchall()
    return Compaction(*(int(value) for value in row))
