"""The running compose stack (`make up`), reached from the host through its published ports.

Host clients take the keys from `.env` and use the published endpoints; the warehouse itself
names the in-network endpoint, which ADR 0002's client configuration overrides.
"""

import os
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from pydantic import HttpUrl, SecretStr

from ibergrid.lake import duck
from ibergrid.lake.catalog import LakeCatalog, rest_catalog
from ibergrid.settings import CatalogSettings, Settings, StorageSettings

REPO = Path(__file__).resolve().parents[2]
CATALOG_URL = os.environ.get("IBERGRID_TEST_CATALOG_URL", "http://127.0.0.1:28181")
S3_URL = os.environ.get("IBERGRID_TEST_S3_URL", "http://127.0.0.1:29000")


def compose_run(
    service: str, *args: str, entrypoint: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the app image inside the compose network, as `make smoke` does."""
    override = [] if entrypoint is None else ["--entrypoint", entrypoint]
    return subprocess.run(
        ["docker", "compose", "run", "--rm", "--no-deps", *override, service, *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


@pytest.fixture(scope="session")
def settings() -> Settings:
    """The stack's keys from `.env` (written by `make env`), with the host's endpoints."""
    lines = (REPO / ".env").read_text().splitlines()
    env = dict(line.split("=", 1) for line in lines if "=" in line and not line.startswith("#"))
    return Settings(
        storage=StorageSettings(
            endpoint_url=HttpUrl(S3_URL),
            access_key=SecretStr(env["IBERGRID_S3_ACCESS_KEY"]),
            secret_key=SecretStr(env["IBERGRID_S3_SECRET_KEY"]),
        ),
        catalog=CatalogSettings(url=HttpUrl(CATALOG_URL)),
    )


@pytest.fixture(scope="session")
def catalog(settings: Settings) -> LakeCatalog:
    return rest_catalog(settings)


@pytest.fixture
def namespace(catalog: LakeCatalog) -> Iterator[str]:
    """A namespace of its own for each test, dropped with its tables afterwards."""
    name = f"it_{uuid.uuid4().hex[:12]}"
    catalog.create_namespace(name)
    yield name
    for identifier in catalog.list_tables(name):
        catalog.purge_table(identifier)
    catalog.drop_namespace(name)


@pytest.fixture
def con(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duck.connect(settings)
    yield connection
    connection.close()
