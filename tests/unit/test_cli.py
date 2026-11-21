import json
import re
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType
from typer.testing import CliRunner

from ibergrid import cli
from ibergrid.cli import app
from ibergrid.lake import registry
from ibergrid.lake.tables import TableSpec, ensure_table
from ibergrid.sources.omie import ingest as omie_ingest
from ibergrid.sources.redata import ingest as ingest_module

from .conftest import (
    FakeIcebergRest,
    FakeLakekeeper,
    FakeOmie,
    FakeRedata,
    LocalServer,
    Responder,
    error,
    fixture,
    omie_file,
)

Serve = Callable[[Responder], LocalServer]
runner = CliRunner()


@pytest.fixture
def stack(
    serve: Serve, open_port: int, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> FakeLakekeeper:
    """Storage, catalog and broker endpoints on 127.0.0.1, wired through IBERGRID_* variables."""
    lakekeeper = FakeLakekeeper()
    monkeypatch.setenv("IBERGRID_S3_ENDPOINT_URL", serve(lambda _: (200, None)).url)
    monkeypatch.setenv("IBERGRID_CATALOG_URL", serve(lakekeeper).url)
    monkeypatch.setenv("IBERGRID_BROKER_BOOTSTRAP_SERVERS", f"127.0.0.1:{open_port}")
    return lakekeeper


def test_version_prints_the_package_version() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout == f"ibergrid {version('ibergrid')}\n"


@pytest.mark.usefixtures("stack")
def test_doctor_passes_when_everything_answers() -> None:
    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0, result.output
    assert [line.split()[:2] for line in result.stdout.splitlines()[:3]] == [
        ["ok", "storage"],
        ["ok", "catalog"],
        ["ok", "broker"],
    ]
    assert result.stdout.endswith("doctor: all checks passed\n")


@pytest.mark.usefixtures("stack")
def test_doctor_exits_1_and_names_what_is_down(
    monkeypatch: pytest.MonkeyPatch, closed_port: int
) -> None:
    monkeypatch.setenv("IBERGRID_BROKER_BOOTSTRAP_SERVERS", f"127.0.0.1:{closed_port}")

    result = runner.invoke(app, ["doctor", "--timeout", "1"])

    assert result.exit_code == 1
    assert "FAIL  broker" in result.stdout
    assert "doctor: 1 check(s) failed: broker" in result.stderr


def test_invalid_configuration_exits_2_without_leaking_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IBERGRID_S3_SECRET_KEY", "test-secret-key-do-not-log")

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 2
    assert "IBERGRID_S3_ACCESS_KEY: Field required" in result.stderr
    assert "test-secret-key-do-not-log" not in result.output


def test_catalog_bootstrap_is_idempotent(
    stack: FakeLakekeeper, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("IBERGRID_LOG_FORMAT", "json")

    first = runner.invoke(app, ["catalog", "bootstrap"])
    second = runner.invoke(app, ["catalog", "bootstrap"])

    assert first.exit_code == 0, first.output
    assert first.stdout == "catalog: server bootstrapped now; warehouse 'lakehouse' created\n"
    assert second.exit_code == 0, second.output
    assert second.stdout == (
        "catalog: server already bootstrapped; warehouse 'lakehouse' already exists\n"
    )
    assert list(stack.warehouses) == ["lakehouse"]
    events = [json.loads(line) for line in first.stderr.splitlines()]
    assert [(e["event"], e["changed"]) for e in events] == [
        ("catalog.bootstrap", True),
        ("catalog.warehouse", True),
    ]
    assert "test-secret-key-do-not-log" not in first.output + second.output


@pytest.mark.usefixtures("stack")
def test_catalog_bootstrap_exits_1_when_the_catalog_does_not_answer(
    monkeypatch: pytest.MonkeyPatch, closed_port: int
) -> None:
    monkeypatch.setenv("IBERGRID_CATALOG_URL", f"http://127.0.0.1:{closed_port}")

    result = runner.invoke(app, ["catalog", "bootstrap"])

    assert result.exit_code == 1
    assert result.stderr.startswith("error: no answer from the catalog: GET http://127.0.0.1")


def test_update_credential_stores_rotated_keys(
    stack: FakeLakekeeper, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert runner.invoke(app, ["catalog", "bootstrap"]).exit_code == 0
    monkeypatch.setenv("IBERGRID_S3_SECRET_KEY", "rotated-secret-do-not-log")

    result = runner.invoke(app, ["catalog", "update-credential"])

    assert result.exit_code == 0, result.output
    assert result.stdout == "catalog: warehouse 'lakehouse' now uses the current S3 keys\n"
    credential = stack.warehouses["lakehouse"]["storage-credential"]
    assert credential["secret-access-key"] == "rotated-secret-do-not-log"
    assert "rotated-secret-do-not-log" not in result.output


@pytest.mark.usefixtures("stack")
def test_update_credential_exits_1_before_bootstrap() -> None:
    result = runner.invoke(app, ["catalog", "update-credential"])

    assert result.exit_code == 1
    assert "warehouse 'lakehouse' does not exist" in result.stderr


# A registered table for the lake commands, small enough to read in a test.
SPEC = TableSpec(
    namespace="ops",
    name="probe",
    schema=Schema(
        NestedField(1, "run_id", StringType(), required=False),
        NestedField(2, "written_at", TimestamptzType(), required=False),
    ),
)


@pytest.fixture
def lake(catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None) -> SqlCatalog:
    """``lake init`` with one registered table, against a real local catalog for the REST one."""
    monkeypatch.setattr(cli, "rest_catalog", lambda _settings: catalog)
    monkeypatch.setattr(registry, "TABLES", (SPEC,))
    return catalog


@pytest.mark.usefixtures("credentials")
def test_lake_init_with_an_empty_registry_creates_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(registry, "TABLES", ())

    result = runner.invoke(app, ["lake", "init"])

    assert result.exit_code == 0, result.output
    assert result.stdout == "lake: no tables registered\n"


def test_lake_init_creates_the_medallion_tables(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    monkeypatch.setattr(cli, "rest_catalog", lambda _settings: catalog)

    first = runner.invoke(app, ["lake", "init"])
    second = runner.invoke(app, ["lake", "init"])

    expected_names = [f"{spec.namespace}.{spec.name}" for spec in registry.TABLES]
    assert first.stdout.splitlines() == [f"lake: {name} created" for name in expected_names], (
        first.output
    )
    assert second.stdout.splitlines() == [f"lake: {name} unchanged" for name in expected_names]
    table = catalog.load_table(("bronze", "redata_observations"))
    assert [f.name for f in table.spec().fields] == ["dataset", "ts_utc_month"]
    assert [str(f.transform) for f in table.spec().fields] == ["identity", "month"]
    omie = catalog.load_table(("bronze", "omie_day_ahead_prices"))
    assert [(f.name, str(f.transform)) for f in omie.spec().fields] == [
        ("delivery_date_month", "month")
    ]
    silver_prices = catalog.load_table(("silver", "mibel_prices_hourly"))
    assert [(f.name, str(f.transform)) for f in silver_prices.spec().fields] == [
        ("hour_utc_month", "month")
    ]
    gold_summary = catalog.load_table(("gold", "mibel_daily_summary"))
    assert list(gold_summary.spec().fields) == []


def test_lake_init_creates_the_registry_tables_once(lake: SqlCatalog) -> None:
    first = runner.invoke(app, ["lake", "init"])
    second = runner.invoke(app, ["lake", "init"])

    assert first.exit_code == 0, first.output
    assert first.stdout == "lake: ops.probe created\n"
    assert second.stdout == "lake: ops.probe unchanged\n"
    assert lake.table_exists(SPEC.identifier)


def test_lake_init_reports_additive_changes(
    lake: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert runner.invoke(app, ["lake", "init"]).exit_code == 0
    grown = replace(
        SPEC,
        schema=Schema(*SPEC.schema.fields, NestedField(3, "note", StringType())),
        properties={"write.metadata.previous-versions-max": "10"},
    )
    monkeypatch.setattr(registry, "TABLES", (grown,))

    result = runner.invoke(app, ["lake", "init"])

    assert result.exit_code == 0, result.output
    assert result.stdout == (
        "lake: ops.probe added columns note; "
        "updated properties write.metadata.previous-versions-max\n"
    )


def test_lake_init_exits_1_on_an_incompatible_change(
    lake: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert runner.invoke(app, ["lake", "init"]).exit_code == 0
    dropped = replace(SPEC, schema=Schema(SPEC.schema.fields[0]))
    monkeypatch.setattr(registry, "TABLES", (dropped,))

    result = runner.invoke(app, ["lake", "init"])

    assert result.exit_code == 1
    assert result.stderr.startswith("error: ops.probe: column 'written_at' is missing")


@pytest.mark.usefixtures("credentials")
def test_lake_init_exits_1_when_the_catalog_does_not_answer(
    monkeypatch: pytest.MonkeyPatch, closed_port: int
) -> None:
    monkeypatch.setattr(registry, "TABLES", (SPEC,))
    monkeypatch.setenv("IBERGRID_CATALOG_URL", f"http://127.0.0.1:{closed_port}")

    result = runner.invoke(app, ["lake", "init"])

    assert result.exit_code == 1
    assert result.stderr.startswith("error: ")


@pytest.mark.usefixtures("credentials")
@pytest.mark.parametrize("status", [401, 404, 500])
def test_lake_init_explains_catalog_errors_in_one_line(
    serve: Serve, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """Before bootstrap Lakekeeper answers 404; a proxy or a broken catalog, 401 or 500."""
    answer = error("WarehouseNotFound", "Warehouse lakehouse not found", status)
    monkeypatch.setattr(registry, "TABLES", (SPEC,))
    monkeypatch.setenv("IBERGRID_CATALOG_URL", serve(lambda _: (status, answer)).url)

    result = runner.invoke(app, ["lake", "init"])

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert result.stderr == "error: WarehouseNotFound: Warehouse lakehouse not found\n"


@pytest.fixture
def rest(
    serve: Serve, catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> SqlCatalog:
    """A REST endpoint for DuckDB's ATTACH, serving the tables of a real local catalog."""
    monkeypatch.setenv("IBERGRID_CATALOG_URL", serve(FakeIcebergRest(catalog)).url)
    return catalog


def test_lake_describe_shows_snapshot_refs_and_files(rest: SqlCatalog) -> None:
    ensure_table(rest, SPEC)
    table = rest.load_table(SPEC.identifier)
    table.append(
        pa.table({"run_id": ["r1"], "written_at": pa.array([None], pa.timestamp("us", "UTC"))})
    )
    head = table.metadata.current_snapshot_id

    result = runner.invoke(app, ["lake", "describe", "ops.probe"])

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert lines[0] == "table:      ops.probe"
    assert lines[1].startswith(f"snapshot:   {head} (append, ")
    assert lines[2] == f"refs:       main -> {head} (branch)"
    assert lines[3].startswith("data files: 1 (")


def test_lake_describe_an_empty_table(rest: SqlCatalog) -> None:
    ensure_table(rest, SPEC)

    result = runner.invoke(app, ["lake", "describe", "ops.probe"])

    assert result.exit_code == 0, result.output
    assert "snapshot:   none (the table has no snapshot yet)" in result.stdout
    assert "refs:       none" in result.stdout
    assert "data files: 0 (0 bytes)" in result.stdout


@pytest.mark.usefixtures("rest")
@pytest.mark.parametrize(("table", "message"), [("ops.missing", "error: "), ("probe", "expected")])
def test_lake_describe_exits_1_on_an_unknown_table(table: str, message: str) -> None:
    result = runner.invoke(app, ["lake", "describe", table])

    assert result.exit_code == 1
    assert result.stderr.startswith("error: ")
    assert message in result.stderr


@pytest.fixture
def redata_stack(
    serve: Serve,
    catalog: SqlCatalog,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> FakeRedata:
    """``ingest redata`` against the recorded answers, a real local catalog and a local bucket."""
    fake = FakeRedata()
    monkeypatch.setenv("IBERGRID_REDATA_BASE_URL", serve(fake).url)
    ensure_table(catalog, registry.REDATA_OBSERVATIONS)
    monkeypatch.setattr(ingest_module, "rest_catalog", lambda _settings: catalog)
    landing = fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())
    monkeypatch.setattr(ingest_module, "s3_filesystem", lambda *_args, **_kw: landing)
    return fake


INGEST = ["ingest", "redata", "--dataset", "demand_hourly", "--start", "2026-11-18"]


@pytest.mark.usefixtures("redata_stack")
def test_ingest_redata_prints_one_line_per_chunk() -> None:
    args = [*INGEST, "--end", "2026-11-18", "--system", "peninsular", "--system", "canarias"]

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert re.fullmatch(
        r"redata: demand_hourly peninsular 2026-11-18\.\.2026-11-18: 24 rows, "
        r"24/24 points \(100\.0 %\), snapshot \d+",
        lines[0],
    )
    assert lines[1].startswith("redata: demand_hourly canarias 2026-11-18..2026-11-18: 24 rows")
    assert lines[2:] == ["redata: 2 chunk(s) published"]


def test_ingest_redata_names_incomplete_days(redata_stack: FakeRedata) -> None:
    name = "demand_hourly-canarias-2026-11-18_2026-11-18.json"
    document = json.loads(fixture(name))
    document["included"][0]["attributes"]["values"] = document["included"][0]["attributes"][
        "values"
    ][:20]
    redata_stack.payloads[name] = json.dumps(document).encode()

    result = runner.invoke(app, [*INGEST, "--end", "2026-11-18", "--system", "canarias"])

    assert result.exit_code == 0, result.output
    assert "20 rows, 20/24 points (83.3 %)" in result.stdout
    assert result.stdout.splitlines()[0].endswith("; incomplete days: 2026-11-18 20/24")


def test_ingest_redata_exits_1_at_the_first_failed_chunk(redata_stack: FakeRedata) -> None:
    name = "demand_hourly-canarias-2026-11-18_2026-11-18.json"
    redata_stack.payloads[name] = b'{"included": []}'
    args = [*INGEST, "--end", "2026-11-18", "--system", "peninsular", "--system", "canarias"]

    result = runner.invoke(app, args)

    assert result.exit_code == 1
    assert result.stdout.startswith("redata: demand_hourly peninsular")  # published, and kept
    assert "chunk(s) published" not in result.stdout
    # stderr also carries the log events; the error is the last line.
    error_line = result.stderr.splitlines()[-1]
    assert error_line.startswith("error: demand_hourly-canarias-20261118-20261118: invalid")


@pytest.mark.usefixtures("redata_stack")
@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--end", "2026-11-18", "--system", "mallorca"], "has no system 'mallorca'"),
        (["--end", "2026-11-17"], "end 2026-11-17 is before start 2026-11-18"),
    ],
)
def test_ingest_redata_refuses_bad_arguments(extra: list[str], message: str) -> None:
    result = runner.invoke(app, [*INGEST, *extra])

    assert result.exit_code == 1
    assert message in result.stderr


@pytest.mark.usefixtures("redata_stack")
def test_ingest_redata_refuses_an_unknown_dataset() -> None:
    args = [
        "ingest",
        "redata",
        "--dataset",
        "balance",
        "--start",
        "2026-11-18",
        "--end",
        "2026-11-18",
    ]

    result = runner.invoke(app, args)

    assert result.exit_code == 1
    assert result.stderr.startswith("error: unknown dataset 'balance'; expected one of: ")


def test_ingest_redata_names_rule_breaches_and_dropped_series(redata_stack: FakeRedata) -> None:
    args = ["ingest", "redata", "--dataset", "demand_realtime", "--start", "2026-11-18"]
    assert runner.invoke(app, [*args, "--end", "2026-11-18"]).exit_code == 0
    name = "demand_realtime-national-2026-11-18_2026-11-18.json"
    document = json.loads(fixture(name))
    document["included"] = [item for item in document["included"] if item["id"] == "2037"]
    document["included"][0]["attributes"]["values"][0]["value"] = -1
    redata_stack.payloads[name] = json.dumps(document).encode()

    result = runner.invoke(app, [*args, "--end", "2026-11-18"])

    assert result.exit_code == 0, result.output
    line = result.stdout.splitlines()[0]
    assert line.startswith("redata: demand_realtime national 2026-11-18..2026-11-18: 288 rows")
    assert line.endswith(
        "; points breaking the value rules: 2037 1"
        "; series no longer in the answer: 2052, 2053, 2054"
    )


def test_ingest_redata_names_five_incomplete_days_and_counts_the_rest(
    redata_stack: FakeRedata,
) -> None:
    # Twelve days asked for, only the first one answered.
    name = "demand_hourly-canarias-2026-11-18_2026-11-29.json"
    redata_stack.payloads[name] = fixture("demand_hourly-canarias-2026-11-18_2026-11-18.json")

    result = runner.invoke(app, [*INGEST, "--end", "2026-11-29", "--system", "canarias"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines()[0].endswith(
        "; incomplete days: 2026-11-19 0/24, 2026-11-20 0/24, 2026-11-21 0/24, 2026-11-22 0/24, "
        "2026-11-23 0/24 and 6 more"
    )


@pytest.fixture
def omie_stack(
    serve: Serve,
    catalog: SqlCatalog,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    credentials: None,
) -> FakeOmie:
    """``ingest omie`` against the recorded files, a real local catalog and a local bucket, on
    2026-11-21 (Madrid), the day the files were recorded."""
    fake = FakeOmie()
    monkeypatch.setenv("IBERGRID_OMIE_BASE_URL", serve(fake).url)
    ensure_table(catalog, registry.OMIE_DAY_AHEAD_PRICES)
    monkeypatch.setattr(omie_ingest, "rest_catalog", lambda _settings: catalog)
    landing = fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())
    monkeypatch.setattr(omie_ingest, "s3_filesystem", lambda *_args, **_kw: landing)
    monkeypatch.setattr(omie_ingest, "madrid_today", lambda: date(2026, 11, 21))
    return fake


@pytest.mark.usefixtures("omie_stack")
def test_ingest_omie_prints_one_line_per_chunk() -> None:
    result = runner.invoke(app, ["ingest", "omie", "--start", "2025-09-30", "--end", "2025-10-01"])

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert re.fullmatch(
        r"omie: 2025-09-30\.\.2025-09-30: 1 file\(s\) \(v1 1\), 24 rows, snapshot \d+", lines[0]
    )
    assert re.fullmatch(
        r"omie: 2025-10-01\.\.2025-10-01: 1 file\(s\) \(v2 1\), 96 rows, snapshot \d+", lines[1]
    )
    assert lines[2:] == ["omie: 2 chunk(s) published"]


def test_ingest_omie_stops_at_the_first_day_not_published_yet(
    omie_stack: FakeOmie, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(omie_ingest, "madrid_today", lambda: date(2026, 11, 17))
    args = ["ingest", "omie", "--start", "2026-11-18", "--end", "2026-12-31"]

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    lines = result.stdout.splitlines()
    assert lines[0].startswith("omie: 2026-11-18..2026-11-18: 1 file(s) (v2 1), 96 rows, snapshot ")
    assert lines[0].endswith("; not published yet from 2026-11-19")
    assert lines[1:] == ["omie: 1 chunk(s) published; not published yet from 2026-11-19"]


@pytest.mark.usefixtures("omie_stack")
def test_ingest_omie_says_when_nothing_is_published_yet() -> None:
    result = runner.invoke(app, ["ingest", "omie", "--start", "2026-11-22", "--end", "2026-11-23"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == [
        "omie: 2026-11-22..2026-11-23: nothing published yet",
        "omie: 0 chunk(s) published; not published yet from 2026-11-22",
    ]


def test_ingest_omie_names_prices_outside_the_limits(omie_stack: FakeOmie) -> None:
    omie_stack.files["marginalpdbc_20250930.1"] = omie_file("2025-09-30").replace(
        b"2025;09;30;1;", b"2025;09;30;1;9999"
    )

    result = runner.invoke(app, ["ingest", "omie", "--start", "2025-09-30", "--end", "2025-09-30"])

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines()[0].endswith(
        "; prices outside -500..4000 EUR/MWh: price_pt_eur_mwh 1"
    )


@pytest.mark.usefixtures("omie_stack")
def test_ingest_omie_exits_1_at_a_missing_past_day() -> None:
    result = runner.invoke(app, ["ingest", "omie", "--start", "2025-09-30", "--end", "2025-10-02"])

    assert result.exit_code == 1
    assert result.stdout.startswith("omie: 2025-09-30..2025-09-30: ")  # published, and kept
    assert "chunk(s) published" not in result.stdout
    error_line = result.stderr.splitlines()[-1]
    assert error_line.startswith("error: OMIE has no marginalpdbc_20251002.1 to .5: delivery day")


def test_ingest_omie_can_require_publication(
    omie_stack: FakeOmie, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(omie_ingest, "madrid_today", lambda: date(2026, 11, 17))
    args = ["ingest", "omie", "--start", "2026-11-18", "--end", "2026-11-19", "--require-published"]

    result = runner.invoke(app, args)

    assert result.exit_code == 1
    assert "chunk(s) published" not in result.stdout
    assert result.stderr.splitlines()[-1] == (
        "error: OMIE has not published marginalpdbc_20261119.1 to .5 yet (delivery day "
        "2026-11-19, today is 2026-11-17 in Madrid) and publication was required"
    )


@pytest.mark.usefixtures("omie_stack")
def test_ingest_omie_finds_a_republished_day() -> None:
    result = runner.invoke(app, ["ingest", "omie", "--start", "2023-05-28", "--end", "2023-05-28"])

    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("omie: 2023-05-28..2023-05-28: 1 file(s) (v1 1), 24 rows, ")


def test_lake_maintain_and_quality_check_cli(
    catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    from ibergrid.quality.gates import GateReport, QualityViolation
    from ibergrid.quality.metadata import MetadataAuditResult, TableMetadataSummary

    ensure_table(catalog, registry.OMIE_DAY_AHEAD_PRICES)
    monkeypatch.setattr(cli, "rest_catalog", lambda _s: catalog)

    res = runner.invoke(
        app,
        [
            "lake",
            "maintain",
            "--table",
            "bronze.omie_day_ahead_prices",
            "--table",
            "bronze.missing_table",
            "--no-compact",
            "--dry-run",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "maintain: bronze.omie_day_ahead_prices:" in res.stdout
    assert "[dry-run]" in res.stdout
    assert "maintain: bronze.missing_table: not found (skipped)" in res.stdout

    summary = TableMetadataSummary(
        table="lake.silver.mibel_prices_hourly",
        snapshot_id=42,
        snapshot_committed_utc=None,
        operation="overwrite",
        data_files=1,
        delete_files=0,
        total_records=24,
        total_data_bytes=1024,
        data_bytes_scanned=0,
        partitions=1,
        columns={},
        elapsed_ms=1.5,
    )
    ok_report = GateReport(
        table="silver.mibel_prices_hourly",
        contract_path="contracts/soda/silver_mibel_prices_hourly.yml",
        snapshot_id=42,
        audit_branch="main",
        passed=True,
        checks_passed=10,
        checks_failed=0,
        violations=(),
        metadata_result=MetadataAuditResult(
            summary=summary, passed=True, violations=(), elapsed_ms=1.5
        ),
        decoupled_periods_verified=4,
        elapsed_ms=12.3,
    )
    monkeypatch.setattr(cli, "check_tables", lambda *_a, **_kw: [ok_report])

    ok_text = runner.invoke(app, ["quality", "check", "--metadata-only"])
    assert ok_text.exit_code == 0, ok_text.output
    assert (
        "ok   silver.mibel_prices_hourly: passed=10 failed=0, decoupled_verified=4"
        in ok_text.stdout
    )

    ok_json = runner.invoke(app, ["quality", "check", "--json"])
    assert ok_json.exit_code == 0, ok_json.output
    parsed = json.loads(ok_json.stdout)
    assert parsed[0]["data_bytes_scanned"] == 0
    assert parsed[0]["decoupled_periods_verified"] == 4

    bad_report = GateReport(
        table="silver.mibel_prices_hourly",
        contract_path=None,
        snapshot_id=42,
        audit_branch="main",
        passed=False,
        checks_passed=9,
        checks_failed=1,
        violations=(QualityViolation("silver.mibel_prices_hourly", "soda.dup", "error", "1 dup"),),
        elapsed_ms=5.0,
    )
    monkeypatch.setattr(cli, "check_tables", lambda *_a, **_kw: [bad_report])
    fail_res = runner.invoke(app, ["quality", "check"])
    assert fail_res.exit_code == 1
    assert "FAIL silver.mibel_prices_hourly" in fail_res.stdout
    assert "soda.dup: 1 dup" in fail_res.stdout


def test_portal_export_and_api_serve_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    import builtins
    import sys

    from ibergrid.portal.export import PortalExportReport

    captured: dict[str, object] = {}

    def _fake_export(_settings: object, **kwargs: object) -> PortalExportReport:
        captured.update(kwargs)
        return PortalExportReport(
            exported_at_utc="2026-11-21T20:00:00Z",
            local_files=(tmp_path / "summary.json",),
            s3_uris=(),
            daily_market_rows=10,
            optimal_window_rows=24,
            systems_monthly_rows=5,
            blackout_hourly_rows=72,
        )

    monkeypatch.setattr(cli, "export_portal_bundle", _fake_export)
    res = runner.invoke(app, ["portal", "export"])
    assert res.exit_code == 0, res.output
    assert "portal: exported 1 local file(s), 0 S3 object(s)" in res.stdout
    assert captured["output_dir"] == Path("portal/data")

    def _bad_export(_settings: object, **_kwargs: object) -> PortalExportReport:
        raise ValueError("s3_prefix must not contain '..' path segments")

    monkeypatch.setattr(cli, "export_portal_bundle", _bad_export)
    err = runner.invoke(app, ["portal", "export", "--s3-prefix", "../bad"])
    assert err.exit_code == 1
    assert "error: s3_prefix" in err.stderr

    # api serve calls uvicorn.run with the FastAPI app and configured host/port.
    uvicorn_calls: list[dict[str, object]] = []

    class _FakeUvicorn:
        @staticmethod
        def run(application: object, **kwargs: object) -> None:
            uvicorn_calls.append({"app": application, **kwargs})

    monkeypatch.setitem(sys.modules, "uvicorn", _FakeUvicorn)
    served = runner.invoke(app, ["api", "serve", "--host", "127.0.0.1", "--port", "9009"])
    assert served.exit_code == 0, served.output
    assert uvicorn_calls[0]["host"] == "127.0.0.1"
    assert uvicorn_calls[0]["port"] == 9009

    # Missing uvicorn/fastapi exits 1 with a clear actionable error.
    real_import = builtins.__import__

    def _deny_uvicorn(name: str, *args: object, **kwargs: object) -> object:
        if name == "uvicorn":
            raise ImportError("No module named 'uvicorn'")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.delitem(sys.modules, "uvicorn", raising=False)
    monkeypatch.setattr(builtins, "__import__", _deny_uvicorn)
    missing = runner.invoke(app, ["api", "serve"])
    assert missing.exit_code == 1
    assert "API dependencies are not installed" in missing.stderr
