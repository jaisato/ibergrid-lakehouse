"""Against the running compose stack: `make up`, then `make test-integration`.

The app image runs inside the compose network (``docker compose run``); the catalog's management
API is read from the host through its published port.
"""

import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from ibergrid.catalog_admin import CatalogAdmin

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
CATALOG_URL = os.environ.get("IBERGRID_TEST_CATALOG_URL", "http://127.0.0.1:28181")
WAREHOUSE = os.environ.get("IBERGRID_CATALOG_WAREHOUSE", "lakehouse")
BUCKET = os.environ.get("IBERGRID_S3_BUCKET", "ibergrid")


def compose_run(service: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", service, *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def warehouses() -> list[dict[str, Any]]:
    payload = CatalogAdmin(CATALOG_URL).expect("GET", "/management/v1/warehouse", ok=200)
    return list(payload["warehouses"])


def test_doctor_passes_inside_the_network() -> None:
    result = compose_run("doctor")

    assert result.returncode == 0, result.stdout + result.stderr
    for check in ("storage", "catalog", "broker"):
        assert f"ok    {check}" in result.stdout


def test_warehouse_exists_on_the_bucket() -> None:
    [warehouse] = [w for w in warehouses() if w["name"] == WAREHOUSE]

    profile = warehouse["storage-profile"]
    assert warehouse["status"] == "active"
    assert (profile["type"], profile["flavor"]) == ("s3", "s3-compat")
    assert (profile["bucket"], profile["key-prefix"]) == (BUCKET, WAREHOUSE)


def test_catalog_bootstrap_is_idempotent() -> None:
    before = warehouses()

    for _ in range(2):
        result = compose_run("catalog-init")
        assert result.returncode == 0, result.stdout + result.stderr
        assert "server already bootstrapped" in result.stdout
        assert f"warehouse {WAREHOUSE!r} already exists" in result.stdout

    assert warehouses() == before


def test_update_credential_is_accepted_by_the_catalog() -> None:
    """The rotation step of .env.example, with unchanged keys, against the real management API."""

    def without_timestamp() -> list[dict[str, Any]]:
        return [{k: v for k, v in w.items() if k != "updated-at"} for w in warehouses()]

    before = without_timestamp()

    result = compose_run("catalog-init", "catalog", "update-credential")

    assert result.returncode == 0, result.stdout + result.stderr
    assert f"warehouse {WAREHOUSE!r} now uses the current S3 keys" in result.stdout
    # Only Lakekeeper's `updated-at` moves: same warehouse, same storage profile.
    assert without_timestamp() == before
