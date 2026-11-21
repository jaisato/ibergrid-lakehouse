"""ACID quarantine writer for ``quarantine.rejected_records`` via Iceberg WAP (ADR 0008)."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa
import structlog
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.registry import QUARANTINE_REJECTED_RECORDS

log = structlog.get_logger(__name__)
MAX_PUBLISH_ATTEMPTS = 5


@dataclass(frozen=True)
class QuarantineRecord:
    """One quality violation persisted in ``quarantine.rejected_records``."""

    run_id: str
    source_table: str
    rule_name: str
    severity: str
    reason: str
    record_key: str | None = None
    payload_json: str | None = None
    audit_branch: str | None = None
    snapshot_id: int | None = None
    rejected_at_utc: datetime | None = None


def write_quarantine(
    catalog: Catalog,
    records: Sequence[QuarantineRecord],
    *,
    run_id_prefix: str = "quarantine",
    target_namespace: str | None = None,
) -> int | None:
    """Append rejected records to ``quarantine.rejected_records`` through WAP.

    Quarantine is strictly append-only, so a concurrent ``PublishConflict`` safely restarts on a
    fresh audit branch without losing or overwriting another run's quarantined records.
    """
    if not records:
        return None

    spec = QUARANTINE_REJECTED_RECORDS
    identifier = (target_namespace or spec.namespace, spec.name)
    try:
        table: Table = catalog.load_table(identifier)
    except NoSuchTableError as exc:
        raise RuntimeError(
            f"{'.'.join(identifier)} does not exist in catalog; run `ibergrid lake init` first"
        ) from exc

    now = datetime.now(UTC)
    arrow_schema = spec.schema.as_arrow()
    rows = pa.table(
        {
            "rejected_at_utc": pa.array(
                [r.rejected_at_utc or now for r in records], type=pa.timestamp("us", "UTC")
            ),
            "run_id": pa.array([r.run_id for r in records], type=pa.string()),
            "source_table": pa.array([r.source_table for r in records], type=pa.string()),
            "rule_name": pa.array([r.rule_name for r in records], type=pa.string()),
            "severity": pa.array([r.severity for r in records], type=pa.string()),
            "reason": pa.array([r.reason for r in records], type=pa.string()),
            "record_key": pa.array([r.record_key for r in records], type=pa.string()),
            "payload_json": pa.array([r.payload_json for r in records], type=pa.string()),
            "audit_branch": pa.array([r.audit_branch for r in records], type=pa.string()),
            "snapshot_id": pa.array([r.snapshot_id for r in records], type=pa.int64()),
        },
        schema=arrow_schema,
    )

    properties = {
        "ibergrid.quarantine.records": str(len(records)),
        "ibergrid.quarantine.sources": ",".join(sorted({r.source_table for r in records})),
    }
    for attempt in range(1, MAX_PUBLISH_ATTEMPTS + 1):
        run_id = f"{run_id_prefix}-quarantine-a{attempt}-{uuid.uuid4().hex[:6]}"
        audit = wap.start(table, run_id)
        try:
            audited = wap.write(table, audit, rows, snapshot_properties=properties)
            published = wap.publish(table, audit, audited)
        except wap.PublishConflict:
            wap.discard(table, audit)
            if attempt == MAX_PUBLISH_ATTEMPTS:
                raise
            continue
        except Exception:
            wap.discard(table, audit)
            raise
        log.info(
            "quality.quarantine.written",
            table=".".join(identifier),
            records=len(records),
            snapshot_id=published,
            run_id=run_id,
        )
        return published
    return None  # pragma: no cover - loop returns or raises
