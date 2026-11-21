"""The running compose stack (`make up`), reached from the host through its published ports.

Host clients take the keys from `.env` and use the published endpoints; the warehouse itself
names the in-network endpoint, which ADR 0002's client configuration overrides.
"""

import json
import os
import subprocess
import threading
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

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


class CatalogProxy:
    """Lakekeeper behind a local proxy: records commit requests and can run a hook (another
    process's commit) just before the next one reaches the catalog."""

    def __init__(self) -> None:
        self.commits: list[dict[str, Any]] = []
        self.before_next_commit: Callable[[], object] | None = None
        # One entry per coming commit: None forwards it, a status answers it unforwarded.
        self.fail_commits: list[int | None] = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def handle_any(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                # DuckDB commits through /transactions/commit, PyIceberg through /tables/<name>.
                is_commit = self.command == "POST" and (
                    "/transactions/commit" in self.path or "/tables/" in self.path
                )
                if is_commit and proxy.before_next_commit is not None:
                    hook, proxy.before_next_commit = proxy.before_next_commit, None
                    hook()
                failure = proxy.fail_commits.pop(0) if is_commit and proxy.fail_commits else None
                if failure is not None:
                    error = {"type": "BadGateway", "message": "injected", "code": failure}
                    self._answer(failure, json.dumps({"error": error}).encode())
                    return
                # The Host header goes along: Lakekeeper advertises the URI the client used, so
                # the client keeps talking to the proxy.
                # Accept-Encoding stays behind: the answer is passed on without its encoding.
                skip = {"content-length", "accept-encoding"}
                headers = {k: v for k, v in self.headers.items() if k.lower() not in skip}
                request = urllib.request.Request(  # noqa: S310 (fixed http URL)
                    CATALOG_URL + self.path, data=body or None, method=self.command, headers=headers
                )
                try:
                    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
                        status, answer = response.status, response.read()
                except urllib.error.HTTPError as exc:
                    status, answer = exc.code, exc.read()
                if is_commit:
                    proxy.commits.append({"body": json.loads(body), "status": status})
                self._answer(status, answer)

            def _answer(self, status: int, answer: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(answer)))
                self.end_headers()
                self.wfile.write(answer)

            do_GET = do_POST = do_DELETE = do_HEAD = handle_any

            def log_message(self, format: str, *args: Any) -> None:
                """Keep test output quiet."""

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._httpd.server_port}"
        threading.Thread(target=self._httpd.serve_forever, args=(0.01,), daemon=True).start()

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def proxy() -> Iterator[CatalogProxy]:
    catalog_proxy = CatalogProxy()
    yield catalog_proxy
    catalog_proxy.close()
