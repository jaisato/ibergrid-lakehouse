import json
from collections.abc import Callable
from importlib.metadata import version

import pytest
from typer.testing import CliRunner

from ibergrid.cli import app

from .conftest import FakeLakekeeper, LocalServer, Responder

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
