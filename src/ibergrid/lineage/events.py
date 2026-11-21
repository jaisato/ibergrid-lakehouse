"""OpenLineage v2 RunEvent builder and emitter (ADR 0008).

Emits spec-compliant OpenLineage 2-0-2 JSON ``START`` / ``COMPLETE`` / ``FAIL`` events with
dataset schema, datasource, output statistics, Iceberg snapshot provenance, and data-quality
assertion facets. Events are always emitted to structured logs, optionally appended to a local
JSONL sink (``IBERGRID_LINEAGE_SINK_PATH``), and optionally POSTed to an OpenLineage HTTP server
such as Marquez (``IBERGRID_LINEAGE_ENDPOINT_URL``) with non-fatal transport error handling so
observability never breaks a data pipeline commit.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.error import URLError
from urllib.request import Request, urlopen

import structlog

from ibergrid.lake.tables import TableSpec
from ibergrid.settings import LineageSettings, Settings, base_url

log = structlog.get_logger(__name__)

OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
PRODUCER_URI = "https://github.com/jaisato/ibergrid-lakehouse"
SCHEMA_FACET_URL = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
DATASOURCE_FACET_URL = "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json"
OUTPUT_STATS_FACET_URL = (
    "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json"
)
QUALITY_ASSERTIONS_FACET_URL = (
    "https://openlineage.io/spec/facets/1-0-1/DataQualityAssertionsDatasetFacet.json"
)
ERROR_FACET_URL = "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json"

EventType = Literal["START", "COMPLETE", "FAIL"]


@dataclass(frozen=True)
class LineageAssertion:
    """One data-quality assertion attached to a dataset's OpenLineage facets."""

    assertion: str
    success: bool
    column: str | None = None


@dataclass(frozen=True)
class LineageDataset:
    """An input or output Iceberg table in an OpenLineage RunEvent."""

    namespace: str
    name: str
    facets: Mapping[str, Any]
    output_facets: Mapping[str, Any] | None = None

    def to_dict(self, *, is_output: bool) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "namespace": self.namespace,
            "name": self.name,
            "facets": dict(self.facets),
        }
        if is_output and self.output_facets:
            payload["outputFacets"] = dict(self.output_facets)
        return payload


def build_dataset(
    spec: TableSpec,
    *,
    warehouse_uri: str,
    snapshot_id: int | None = None,
    audit_branch: str | None = None,
    row_count: int | None = None,
    size_bytes: int | None = None,
    assertions: Sequence[LineageAssertion] = (),
    namespace_override: str | None = None,
) -> LineageDataset:
    """Construct an OpenLineage dataset descriptor from an IberGrid ``TableSpec``."""
    fields = [
        {
            "name": f.name,
            "type": str(f.field_type),
            **({"description": f.doc} if f.doc else {}),
        }
        for f in spec.schema.fields
    ]
    facets: dict[str, Any] = {
        "schema": {
            "_producer": PRODUCER_URI,
            "_schemaURL": SCHEMA_FACET_URL,
            "fields": fields,
        },
        "dataSource": {
            "_producer": PRODUCER_URI,
            "_schemaURL": DATASOURCE_FACET_URL,
            "name": warehouse_uri,
            "uri": warehouse_uri,
        },
    }
    if snapshot_id is not None or audit_branch is not None:
        facets["ibergridSnapshot"] = {
            "_producer": PRODUCER_URI,
            "_schemaURL": f"{PRODUCER_URI}/facets/IbergridSnapshotFacet.json",
            "snapshotId": None if snapshot_id is None else str(snapshot_id),
            "auditBranch": audit_branch,
        }
    if assertions:
        facets["dataQualityAssertions"] = {
            "_producer": PRODUCER_URI,
            "_schemaURL": QUALITY_ASSERTIONS_FACET_URL,
            "assertions": [
                {
                    "assertion": item.assertion,
                    "success": item.success,
                    **({"column": item.column} if item.column else {}),
                }
                for item in assertions
            ],
        }

    output_facets: dict[str, Any] = {}
    if row_count is not None or size_bytes is not None:
        stats: dict[str, Any] = {
            "_producer": PRODUCER_URI,
            "_schemaURL": OUTPUT_STATS_FACET_URL,
        }
        if row_count is not None:
            stats["rowCount"] = row_count
        if size_bytes is not None:
            stats["size"] = size_bytes
        output_facets["outputStatistics"] = stats

    ns = namespace_override or spec.namespace
    return LineageDataset(
        namespace=warehouse_uri,
        name=f"{ns}.{spec.name}",
        facets=facets,
        output_facets=output_facets or None,
    )


def build_run_event(
    event_type: EventType,
    *,
    job_namespace: str,
    job_name: str,
    run_id: str | None = None,
    inputs: Sequence[LineageDataset] = (),
    outputs: Sequence[LineageDataset] = (),
    error_message: str | None = None,
    event_time: datetime | None = None,
) -> dict[str, Any]:
    """Build an OpenLineage v2 RunEvent dictionary ready for JSON serialization."""
    resolved_run_id = _ensure_uuid(run_id)
    now = event_time or datetime.now(UTC)
    run_facets: dict[str, Any] = {}
    if error_message is not None:
        run_facets["errorMessage"] = {
            "_producer": PRODUCER_URI,
            "_schemaURL": ERROR_FACET_URL,
            "message": error_message,
            "programmingLanguage": "PYTHON",
        }
    return {
        "eventType": event_type,
        "eventTime": now.astimezone(UTC).isoformat(),
        "run": {"runId": resolved_run_id, "facets": run_facets},
        "job": {"namespace": job_namespace, "name": job_name, "facets": {}},
        "inputs": [ds.to_dict(is_output=False) for ds in inputs],
        "outputs": [ds.to_dict(is_output=True) for ds in outputs],
        "producer": PRODUCER_URI,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
    }


class LineageEmitter:
    """Emits OpenLineage v2 events to structlog, optional JSONL sink, and optional HTTP endpoint."""

    def __init__(self, settings: LineageSettings, warehouse_uri: str) -> None:
        self.settings = settings
        self.warehouse_uri = warehouse_uri

    @classmethod
    def from_settings(cls, settings: Settings) -> LineageEmitter:
        uri = f"{base_url(settings.catalog.url)}/catalog/{settings.catalog.warehouse}"
        return cls(settings.lineage, warehouse_uri=uri)

    def emit(
        self,
        event_type: EventType,
        *,
        job_name: str,
        run_id: str | None = None,
        inputs: Sequence[LineageDataset] = (),
        outputs: Sequence[LineageDataset] = (),
        error_message: str | None = None,
    ) -> dict[str, Any]:
        """Build and emit an OpenLineage RunEvent; transport failures warn without raising."""
        event = build_run_event(
            event_type,
            job_namespace=self.settings.namespace,
            job_name=job_name,
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            error_message=error_message,
        )
        log.info(
            "lineage.event",
            event_type=event_type,
            job=job_name,
            run_id=event["run"]["runId"],
            inputs=len(inputs),
            outputs=len(outputs),
        )
        payload = json.dumps(event, separators=(",", ":"), sort_keys=True)

        if self.settings.sink_path:
            try:
                sink = Path(self.settings.sink_path)
                sink.parent.mkdir(parents=True, exist_ok=True)
                with sink.open("a", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
            except OSError as exc:
                log.warning(
                    "lineage.sink.failed",
                    sink_path=self.settings.sink_path,
                    error=str(exc),
                )

        if self.settings.endpoint_url is not None:
            url = f"{base_url(self.settings.endpoint_url)}/api/v1/lineage"
            req = Request(  # noqa: S310 (HttpUrl guarantees http/https)
                url,
                data=payload.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urlopen(req, timeout=self.settings.timeout) as resp:  # noqa: S310 (HttpUrl)
                    status = getattr(resp, "status", 200)
                    if status >= 400:
                        log.warning("lineage.http.rejected", url=url, status=status)
            except (URLError, OSError) as exc:
                log.warning("lineage.http.failed", url=url, error=str(exc))

        return event


def _ensure_uuid(raw: str | None) -> str:
    if raw is None:
        return str(uuid.uuid4())
    try:
        return str(uuid.UUID(raw))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ibergrid:{raw}"))
