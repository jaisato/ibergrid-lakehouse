"""Table maintenance: snapshot expiry, data-file compaction and S3 orphan-file cleanup.

- ``expire_snapshots`` (PyIceberg): drops snapshots older than a cutoff while preserving ``main``'s
  last N snapshots, every tag head, and the full lineage of every open audit branch.
- ``rewrite_data_files`` (DuckDB): compacts small Parquet files via ``iceberg_rewrite_data_files``.
- ``remove_orphan_files`` (PyIceberg + PyArrow FileSystem, ADR 0008): reclaims unreferenced data,
  delete, manifest, manifest-list and metadata objects strictly older than a safety grace window
  (default 48 h) using a TOCTOU-safe double-collect across every snapshot and ref.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow.fs as pafs
import structlog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.table.snapshots import ancestors_of

from ibergrid.lake import wap

log = structlog.get_logger(__name__)

DEFAULT_ORPHAN_GRACE_PERIOD = timedelta(hours=48)


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
    (the catalog answered 409: nothing changed), and a run open across it can no longer publish
    (``PublishConflict``). Any other ``duckdb.TransactionException`` at commit (for example a 5xx)
    propagates unchanged: the compaction may or may not have landed, so look before retrying.
    """
    try:
        [row] = con.execute(
            "SELECT rewritten_data_files_count, added_data_files_count, rewritten_bytes_count, "
            "failed_data_files_count FROM iceberg_rewrite_data_files($table)",
            {"table": table},
        ).fetchall()
    except duckdb.TransactionException as exc:
        # Only the catalog's 409 means "nothing changed"; any other failure at commit (a 5xx,
        # a lost connection) leaves the outcome unknown and propagates as DuckDB raised it.
        if "Conflict_409" not in str(exc):
            raise
        raise MaintenanceConflict(f"{table} changed during the compaction: {exc}") from exc
    return Compaction(*(int(value) for value in row))


@dataclass(frozen=True)
class OrphanCleanupResult:
    """Summary of an orphan-file scan/deletion pass on one Iceberg table (ADR 0008)."""

    table: str
    scanned_files: int
    referenced_files: int
    orphan_files: tuple[str, ...]
    deleted_files: int
    reclaimed_bytes: int
    skipped_recent_files: int
    dry_run: bool


def collect_referenced_paths(table: Table) -> set[str]:
    """Return normalized FileSystem paths for every live object referenced by ``table``.

    Covers ``metadata_location``, ``metadata_log``, Puffin statistics files, and every snapshot's
    manifest list, manifest files, and live data/delete files (across ``main``, tags, and open
    ``audit-*`` branches).
    """
    io = table.io
    if not isinstance(io, PyArrowFileIO):
        raise TypeError(f"orphan cleanup requires PyArrowFileIO, got {type(io).__name__}")

    def _norm(uri: str) -> str:
        _, _, path = PyArrowFileIO.parse_location(uri, io.properties)
        return path.rstrip("/")

    referenced: set[str] = set()
    if table.metadata_location:
        referenced.add(_norm(table.metadata_location))
    for entry in table.metadata.metadata_log:
        referenced.add(_norm(entry.metadata_file))
    for stat in table.metadata.statistics:
        referenced.add(_norm(stat.statistics_path))
    for pstat in table.metadata.partition_statistics:
        referenced.add(_norm(pstat.statistics_path))

    seen_manifests: set[str] = set()
    for snapshot in table.metadata.snapshots:
        if snapshot.manifest_list:
            referenced.add(_norm(snapshot.manifest_list))
        for manifest in snapshot.manifests(io):
            norm_manifest = _norm(manifest.manifest_path)
            referenced.add(norm_manifest)
            if norm_manifest in seen_manifests:
                continue
            seen_manifests.add(norm_manifest)
            for manifest_entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
                referenced.add(_norm(manifest_entry.data_file.file_path))

    root_norm = _norm(table.location())
    referenced.add(f"{root_norm}/metadata/version-hint.text")
    return referenced


def remove_orphan_files(
    table: Table,
    *,
    older_than: datetime | None = None,
    grace_period: timedelta = DEFAULT_ORPHAN_GRACE_PERIOD,
    dry_run: bool = True,
    now: datetime | None = None,
) -> OrphanCleanupResult:
    """Scan ``table.location()`` and delete unreferenced objects strictly older than the cutoff.

    Safety invariants (ADR 0008):
    1. ``grace_period`` must be non-negative; ``older_than`` and ``now`` must be timezone-aware.
    2. TOCTOU protection: referenced paths are collected BEFORE and AFTER listing the table prefix,
       and their union is protected so a concurrent commit during listing is never deleted.
    3. Every listed path is verified to lie strictly under ``<table_location>/`` before deletion.
    4. Files whose storage ``mtime`` is missing or ``>= cutoff`` are counted in
       ``skipped_recent_files`` and never deleted.
    """
    if grace_period < timedelta(0):
        raise ValueError("grace_period must be non-negative")
    if older_than is not None and older_than.tzinfo is None:
        raise ValueError("older_than must be timezone-aware")
    if now is not None and now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    io = table.io
    if not isinstance(io, PyArrowFileIO):
        raise TypeError(f"orphan cleanup requires PyArrowFileIO, got {type(io).__name__}")

    reference_time = now if now is not None else datetime.now(UTC)
    cutoff = older_than if older_than is not None else reference_time - grace_period

    table.refresh()
    refs_before = collect_referenced_paths(table)

    scheme, netloc, raw_root = PyArrowFileIO.parse_location(table.location(), io.properties)
    root = raw_root.rstrip("/")
    fs = io.fs_by_scheme(scheme, netloc)
    prefix = f"{root}/"
    try:
        infos = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    except FileNotFoundError:
        infos = []

    table.refresh()
    refs_after = collect_referenced_paths(table)
    referenced = refs_before | refs_after

    scanned = 0
    skipped_recent = 0
    orphans: list[str] = []
    deleted = 0
    reclaimed_bytes = 0

    for info in infos:
        if info.type != pafs.FileType.File:
            continue
        norm_path = info.path.rstrip("/")
        if not norm_path.startswith(prefix):
            continue
        scanned += 1
        if norm_path in referenced:
            continue
        mtime = info.mtime
        if mtime is None:
            skipped_recent += 1
            continue
        mtime_utc = mtime if mtime.tzinfo is not None else mtime.replace(tzinfo=UTC)
        if mtime_utc >= cutoff:
            skipped_recent += 1
            continue
        orphans.append(norm_path)
        size = int(info.size or 0)
        if not dry_run:
            try:
                fs.delete_file(info.path)
            except FileNotFoundError:
                continue
            deleted += 1
            reclaimed_bytes += size
        else:
            reclaimed_bytes += size

    table_label = ".".join(table.name())
    result = OrphanCleanupResult(
        table=table_label,
        scanned_files=scanned,
        referenced_files=len(referenced),
        orphan_files=tuple(sorted(orphans)),
        deleted_files=deleted,
        reclaimed_bytes=reclaimed_bytes,
        skipped_recent_files=skipped_recent,
        dry_run=dry_run,
    )
    log.info(
        "lake.maintenance.orphans",
        table=table_label,
        scanned=scanned,
        orphans=len(orphans),
        deleted=deleted,
        reclaimed_bytes=reclaimed_bytes,
        skipped_recent=skipped_recent,
        dry_run=dry_run,
    )
    return result


@dataclass(frozen=True)
class TableMaintenanceReport:
    """Combined compaction, snapshot-expiry and orphan-cleanup outcome for one Iceberg table."""

    table: str
    compaction: Compaction | None
    compaction_skipped_reason: str | None
    expired_snapshots: tuple[int, ...]
    orphans: OrphanCleanupResult | None


def maintain_table(
    table: Table,
    *,
    table_name: str,
    duck_table_name: str | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
    compact: bool = True,
    expire_days: int | None = 7,
    retain_last: int = 5,
    clean_orphans: bool = True,
    orphan_grace_hours: float = 48.0,
    dry_run: bool = False,
    now: datetime | None = None,
) -> TableMaintenanceReport:
    """Run compaction, snapshot expiry and orphan-file cleanup safely on a single table."""
    current = now if now is not None else datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    table.refresh()
    compaction: Compaction | None = None
    skip_reason: str | None = None

    if compact:
        if dry_run:
            skip_reason = "dry_run"
        elif con is None or duck_table_name is None:
            skip_reason = "no_duckdb_connection"
        elif table.current_snapshot() is None:
            skip_reason = "empty_table"
        elif wap.audit_branches(table):
            skip_reason = "open_audit_branches"
        else:
            try:
                compaction = rewrite_data_files(con, duck_table_name)
            except MaintenanceConflict:
                skip_reason = "concurrent_write_conflict"

    expired: list[int] = []
    if expire_days is not None and not dry_run:
        cutoff = current - timedelta(days=expire_days)
        expired = expire_snapshots(table, older_than=cutoff, retain_last=retain_last)

    orphans: OrphanCleanupResult | None = None
    if clean_orphans:
        orphans = remove_orphan_files(
            table,
            grace_period=timedelta(hours=orphan_grace_hours),
            dry_run=dry_run,
            now=current,
        )

    return TableMaintenanceReport(
        table=table_name,
        compaction=compaction,
        compaction_skipped_reason=skip_reason,
        expired_snapshots=tuple(expired),
        orphans=orphans,
    )
