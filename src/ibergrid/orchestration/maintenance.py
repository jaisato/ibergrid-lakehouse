"""Automated Iceberg table maintenance job for Dagster (ADR 0008).

Runs compaction (DuckDB ``iceberg_rewrite_data_files``), snapshot expiry (PyIceberg, retaining
recent history plus tags and open audit branches), and safety-windowed S3 orphan-file cleanup
across every registered Bronze, Silver, Gold and Quarantine table, emitting per-table metrics and
OpenLineage run events. Scheduled at 04:00 Europe/Madrid (outside the 02:00-03:00 DST gap/repeat
window and before daily REData/OMIE ingestion).
"""

import dagster as dg
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

from ibergrid.lake import duck
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.maintenance import TableMaintenanceReport, maintain_table
from ibergrid.lake.registry import TABLES
from ibergrid.lineage import LineageEmitter, build_dataset
from ibergrid.orchestration.resources import Lake


class MaintenanceConfig(dg.Config):
    """Per-run overrides for ``lake_maintenance``; defaults come from ``MaintenanceSettings``."""

    compact: bool = True
    expire_days: int | None = Field(default=None, ge=1)
    retain_last: int | None = Field(default=None, ge=1)
    clean_orphans: bool = True
    orphan_grace_hours: float | None = Field(default=None, ge=0.0)
    dry_run: bool = False


@dg.op(description="Compact small files, expire old snapshots and clean orphan objects (ADR 0008).")
def maintain_lake_tables(
    context: dg.OpExecutionContext, config: MaintenanceConfig, lake: Lake
) -> list[dict[str, object]]:
    settings = lake.settings()
    mcfg = settings.maintenance
    expire_days = mcfg.snapshot_retention_days if config.expire_days is None else config.expire_days
    retain_last = mcfg.retain_last_snapshots if config.retain_last is None else config.retain_last
    grace_hours = (
        mcfg.orphan_grace_hours if config.orphan_grace_hours is None else config.orphan_grace_hours
    )

    emitter = LineageEmitter.from_settings(settings)
    emitter.emit("START", job_name="lake_maintenance", run_id=context.run_id)

    catalog = rest_catalog(settings)
    con = duck.connect(settings) if (config.compact and not config.dry_run) else None
    summaries: list[dict[str, object]] = []
    lineage_outputs = []

    try:
        for spec in TABLES:
            identifier = lake.table(spec)
            qualified = ".".join(identifier)
            try:
                table = catalog.load_table(identifier)
            except NoSuchTableError:
                context.log.info(f"{qualified}: no such table yet")
                continue

            report: TableMaintenanceReport = maintain_table(
                table,
                table_name=qualified,
                duck_table_name=duck.table_name(qualified) if con is not None else None,
                con=con,
                compact=config.compact,
                expire_days=expire_days,
                retain_last=retain_last,
                clean_orphans=config.clean_orphans,
                orphan_grace_hours=grace_hours,
                dry_run=config.dry_run,
            )
            entry = {
                "table": qualified,
                "compacted_files": 0
                if report.compaction is None
                else report.compaction.rewritten_files,
                "added_files": 0 if report.compaction is None else report.compaction.added_files,
                "compaction_skipped": report.compaction_skipped_reason,
                "expired_snapshots": len(report.expired_snapshots),
                "orphan_files": 0 if report.orphans is None else len(report.orphans.orphan_files),
                "deleted_orphans": 0 if report.orphans is None else report.orphans.deleted_files,
                "reclaimed_bytes": 0 if report.orphans is None else report.orphans.reclaimed_bytes,
                "dry_run": config.dry_run,
            }
            summaries.append(entry)
            context.log.info(f"maintenance: {entry}")
            snap = table.current_snapshot()
            lineage_outputs.append(
                build_dataset(
                    spec,
                    warehouse_uri=emitter.warehouse_uri,
                    snapshot_id=None if snap is None else snap.snapshot_id,
                    namespace_override=identifier[0],
                )
            )
    except Exception as exc:
        emitter.emit(
            "FAIL", job_name="lake_maintenance", run_id=context.run_id, error_message=str(exc)
        )
        raise
    finally:
        if con is not None:
            con.close()

    emitter.emit(
        "COMPLETE",
        job_name="lake_maintenance",
        run_id=context.run_id,
        outputs=lineage_outputs,
    )
    return summaries


@dg.job(
    description="Daily: Iceberg compaction, snapshot expiry and orphan-file cleanup (ADR 0008)."
)
def lake_maintenance() -> None:
    maintain_lake_tables()
