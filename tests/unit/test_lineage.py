"""OpenLineage v2 RunEvent generation and non-fatal multi-sink emission (ADR 0008)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import structlog.testing

from ibergrid.lake import registry
from ibergrid.lineage import (
    OPENLINEAGE_SCHEMA_URL,
    LineageAssertion,
    LineageEmitter,
    build_dataset,
    build_run_event,
)
from ibergrid.settings import Settings

from .conftest import LocalServer, Recorded, Responder

Serve = Callable[[Responder], LocalServer]


def test_build_dataset_and_run_event_conform_to_openlineage_v2() -> None:
    inp = build_dataset(
        registry.OMIE_DAY_AHEAD_PRICES,
        warehouse_uri="http://catalog:8181/catalog/lakehouse",
        snapshot_id=101,
    )
    out = build_dataset(
        registry.GOLD_MIBEL_HOURLY_MARKET,
        warehouse_uri="http://catalog:8181/catalog/lakehouse",
        snapshot_id=202,
        audit_branch="audit-r1",
        row_count=34080,
        size_bytes=1_234_567,
        assertions=(
            LineageAssertion(assertion="dbt_tests_passed", success=True),
            LineageAssertion(
                assertion="cross_source_spot_reconciliation",
                success=True,
                column="price_es_eur_mwh",
            ),
        ),
    )

    event = build_run_event(
        "COMPLETE",
        job_namespace="ibergrid",
        job_name="medallion_transform",
        run_id="not-a-uuid-run-1",
        inputs=[inp],
        outputs=[out],
    )
    assert event["eventType"] == "COMPLETE"
    assert event["schemaURL"] == OPENLINEAGE_SCHEMA_URL
    assert event["job"] == {"namespace": "ibergrid", "name": "medallion_transform", "facets": {}}
    # Non-UUID run ids are deterministically converted to UUIDv5.
    assert len(event["run"]["runId"]) == 36
    assert (
        event["run"]["runId"]
        == build_run_event(
            "COMPLETE",
            job_namespace="ibergrid",
            job_name="medallion_transform",
            run_id="not-a-uuid-run-1",
        )["run"]["runId"]
    )

    assert len(event["inputs"]) == 1
    assert event["inputs"][0]["name"] == "bronze.omie_day_ahead_prices"
    assert event["inputs"][0]["facets"]["ibergridSnapshot"]["snapshotId"] == "101"
    assert len(event["inputs"][0]["facets"]["schema"]["fields"]) == len(
        registry.OMIE_DAY_AHEAD_PRICES.schema.fields
    )

    out_payload = event["outputs"][0]
    assert out_payload["name"] == "gold.mibel_hourly_market"
    assert out_payload["outputFacets"]["outputStatistics"]["rowCount"] == 34080
    assert out_payload["outputFacets"]["outputStatistics"]["size"] == 1_234_567
    assert len(out_payload["facets"]["dataQualityAssertions"]["assertions"]) == 2
    assert out_payload["facets"]["ibergridSnapshot"]["auditBranch"] == "audit-r1"

    fail_event = build_run_event(
        "FAIL",
        job_namespace="ibergrid",
        job_name="medallion_transform",
        error_message="dbt test failed",
    )
    assert fail_event["run"]["facets"]["errorMessage"]["message"] == "dbt test failed"


def test_emitter_writes_jsonl_posts_http_and_tolerates_transport_errors(
    tmp_path: Path, serve: Serve, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    received: list[dict[str, Any]] = []
    status_code = 200

    def handler(req: Recorded) -> tuple[int, Any]:
        assert req.method == "POST"
        assert req.path == "/api/v1/lineage"
        received.append(req.body if isinstance(req.body, dict) else json.loads(req.body))
        return status_code, {}

    server = serve(handler)
    sink = tmp_path / "nested" / "events.jsonl"
    monkeypatch.setenv("IBERGRID_LINEAGE_SINK_PATH", str(sink))
    monkeypatch.setenv("IBERGRID_LINEAGE_ENDPOINT_URL", server.url)

    emitter = LineageEmitter.from_settings(Settings())
    with structlog.testing.capture_logs() as captured:
        emitter.emit("START", job_name="medallion_transform", run_id="run-1")
        emitter.emit(
            "COMPLETE",
            job_name="medallion_transform",
            run_id="run-1",
            outputs=[
                build_dataset(
                    registry.GOLD_MIBEL_HOURLY_MARKET,
                    warehouse_uri=emitter.warehouse_uri,
                    snapshot_id=77,
                    row_count=24,
                )
            ],
        )

    lineage_logs = [entry for entry in captured if entry.get("event") == "lineage.event"]
    assert len(lineage_logs) == 2
    logged_complete = lineage_logs[1]["openlineage_event"]
    assert logged_complete["eventType"] == "COMPLETE"
    assert logged_complete["outputs"][0]["name"] == "gold.mibel_hourly_market"
    assert logged_complete["outputs"][0]["facets"]["ibergridSnapshot"]["snapshotId"] == "77"

    lines = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert [e["eventType"] for e in lines] == ["START", "COMPLETE"]
    assert [e["eventType"] for e in received] == ["START", "COMPLETE"]

    # Transport 500 or unreachable server warns without raising (never breaks commits).
    status_code = 500
    emitter.emit("FAIL", job_name="medallion_transform", run_id="run-1", error_message="boom")

    monkeypatch.setenv("IBERGRID_LINEAGE_ENDPOINT_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("IBERGRID_LINEAGE_TIMEOUT", "0.5")
    # Unwritable sink path (directory instead of file) also warns cleanly.
    monkeypatch.setenv("IBERGRID_LINEAGE_SINK_PATH", str(tmp_path))
    resilient = LineageEmitter.from_settings(Settings())
    event = resilient.emit("START", job_name="lake_maintenance")
    assert event["eventType"] == "START"
