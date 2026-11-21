"""Real local servers instead of mocks: the code under test talks HTTP and TCP to 127.0.0.1."""

import json
import os
import socket
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError


@dataclass(frozen=True)
class Recorded:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


# A responder returns (status, payload): a JSON-serialisable value, raw bytes, or None for no body.
Responder = Callable[[Recorded], tuple[int, Any]]


class LocalServer:
    """An HTTP server on an ephemeral 127.0.0.1 port that records requests and answers them."""

    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.requests: list[Recorded] = []
        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self._httpd.server_port}"
        # A short poll interval keeps shutdown() (and so each test teardown) fast.
        threading.Thread(target=self._httpd.serve_forever, args=(0.01,), daemon=True).start()

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def handle_any(self) -> None:
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                headers = {name.lower(): value for name, value in self.headers.items()}
                request = Recorded(
                    self.command, self.path, headers, json.loads(raw) if raw else None
                )
                server.requests.append(request)
                status, payload = server.responder(request)
                if payload is None:
                    body = b""
                elif isinstance(payload, bytes):
                    body = payload
                else:
                    body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            do_GET = do_POST = do_HEAD = handle_any

            def log_message(self, format: str, *args: Any) -> None:
                """Keep test output quiet."""

        return Handler


class FakeLakekeeper:
    """The slice of Lakekeeper's API that ``catalog bootstrap`` and ``doctor`` use."""

    def __init__(self, unhealthy_answers: int = 0) -> None:
        self.unhealthy_answers = unhealthy_answers
        self.bootstrapped = False
        self.warehouses: dict[str, Any] = {}

    def __call__(self, request: Recorded) -> tuple[int, Any]:
        match request.method, request.path:
            case "GET", "/health":
                if self.unhealthy_answers:
                    self.unhealthy_answers -= 1
                    return 503, None
                return 200, {"status": "ok"}
            case "GET", "/management/v1/info":
                return 200, {"bootstrapped": self.bootstrapped, "version": "0.15.1"}
            case "POST", "/management/v1/bootstrap":
                if self.bootstrapped:
                    return 400, error("CatalogAlreadyBootstrapped", "already bootstrapped", 400)
                self.bootstrapped = True
                return 204, None
            case "GET", "/management/v1/warehouse":
                listing = [{"name": name, "id": f"id-{name}"} for name in self.warehouses]
                return 200, {"warehouses": listing}
            case "POST", "/management/v1/warehouse":
                name = request.body["warehouse-name"]
                self.warehouses[name] = request.body
                return 201, {"name": name, "id": f"id-{name}"}
            case "POST", path if path.endswith("/storage-credential"):
                name = path.removeprefix("/management/v1/warehouse/id-").split("/")[0]
                if name not in self.warehouses:
                    return 404, error("WarehouseNotFound", f"no warehouse {name}", 404)
                credential = request.body["new-storage-credential"]
                self.warehouses[name]["storage-credential"] = credential
                return 200, {"name": name, "id": f"id-{name}"}
        return 404, error("NotFound", f"no route for {request.method} {request.path}", 404)


class FakeIcebergRest:
    """The read side of the Iceberg REST API (what DuckDB's ATTACH uses), over a real catalog.

    Table metadata and files stay local, so DuckDB reads them without any object storage.
    """

    def __init__(self, catalog: SqlCatalog) -> None:
        self.catalog = catalog

    def __call__(self, request: Recorded) -> tuple[int, Any]:
        path = request.path.partition("?")[0]
        if path == "/catalog/v1/config":
            return 200, {"defaults": {"prefix": "wh-id"}, "overrides": {}}
        match path.removeprefix("/catalog/v1/wh-id/").split("/"):
            case ["namespaces", namespace, "tables", name]:
                try:
                    table = self.catalog.load_table((namespace, name))
                except NoSuchTableError:
                    return 404, error("NoSuchTableException", f"no table {name}", 404)
                return 200, {
                    "metadata-location": table.metadata_location,
                    "metadata": json.loads(table.metadata.model_dump_json()),
                    "config": {},
                }
        return 404, error("NotFound", f"no route for {request.method} {request.path}", 404)


def error(kind: str, message: str, code: int) -> dict[str, Any]:
    return {"error": {"type": kind, "message": message, "code": code}}


@pytest.fixture
def serve() -> Iterator[Callable[[Responder], LocalServer]]:
    servers: list[LocalServer] = []

    def start(responder: Responder) -> LocalServer:
        servers.append(LocalServer(responder))
        return servers[-1]

    yield start
    for server in servers:
        server.close()


@pytest.fixture
def open_port() -> Iterator[int]:
    """A port with a listening TCP socket."""
    with socket.create_server(("127.0.0.1", 0)) as listener:
        yield listener.getsockname()[1]


@pytest.fixture
def closed_port() -> int:
    """A port that was free a moment ago: connecting to it is refused."""
    with socket.create_server(("127.0.0.1", 0)) as listener:
        return int(listener.getsockname()[1])


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """No IBERGRID_* variable from the developer's shell or the CI runner leaks into a test."""
    for name in list(os.environ):
        if name.startswith("IBERGRID_"):
            monkeypatch.delenv(name)


@pytest.fixture
def credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBERGRID_S3_ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("IBERGRID_S3_SECRET_KEY", "test-secret-key-do-not-log")


@pytest.fixture
def catalog(tmp_path: Path) -> SqlCatalog:
    """A real Iceberg catalog without Docker: sqlite for the pointers, files for the warehouse."""
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path / 'catalog.db'}", warehouse=warehouse.as_uri()
    )
