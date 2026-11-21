import json
from collections.abc import Callable
from dataclasses import replace
from importlib.metadata import version

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType
from typer.testing import CliRunner

from ibergrid import cli
from ibergrid.cli import app
from ibergrid.lake import registry
from ibergrid.lake.tables import TableSpec, ensure_table

from .conftest import FakeIcebergRest, FakeLakekeeper, LocalServer, Responder, error

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


# A registered table for the lake commands; the real registry is empty until the connectors.
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
def test_lake_init_with_an_empty_registry_creates_nothing() -> None:
    result = runner.invoke(app, ["lake", "init"])

    assert registry.TABLES == ()
    assert result.exit_code == 0, result.output
    assert result.stdout == "lake: no tables registered\n"


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
