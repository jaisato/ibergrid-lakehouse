"""Real local servers instead of mocks: the code under test talks HTTP and TCP to 127.0.0.1."""

import json
import os
import socket
import threading
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
import structlog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from ibergrid.sources.redata.datasets import DATASETS


@dataclass(frozen=True)
class Recorded:
    method: str
    path: str
    headers: dict[str, str]
    body: Any


# A responder returns (status, payload) or (status, payload, headers); the payload is a
# JSON-serialisable value, raw bytes, or None for no body.
Responder = Callable[[Recorded], tuple[int, Any] | tuple[int, Any, dict[str, str]]]


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
                status, payload, *extra = server.responder(request)
                if payload is None:
                    body = b""
                elif isinstance(payload, bytes):
                    body = payload
                else:
                    body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                for name, value in (extra[0] if extra else {}).items():
                    self.send_header(name, value)
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
            case "GET", path if path.startswith("/management/v1/warehouse/id-"):
                name = path.removeprefix("/management/v1/warehouse/id-")
                if name not in self.warehouses:
                    return 404, error("WarehouseNotFound", f"no warehouse {name}", 404)
                return 200, {"name": name, **self.warehouses[name]}
            case "POST", path if path.endswith("/storage"):
                name = path.removeprefix("/management/v1/warehouse/id-").split("/")[0]
                if name not in self.warehouses:
                    return 404, error("WarehouseNotFound", f"no warehouse {name}", 404)
                self.warehouses[name] |= request.body
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


REDATA_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "redata"
# What REData answers, with 502, for a window it has no data for (and, the same, when it fails).
UNAVAILABLE = {
    "errors": [
        {
            "status": "502",
            "title": "Internal error",
            "detail": "The requested data is currently unavailable. Please try again later.",
        }
    ]
}


class FakeRedata:
    """REData's ``/en/datos/`` widgets answered from the recorded fixtures
    (``tests/fixtures/redata``); a window without a fixture gets REData's no-data 502.

    ``payloads`` replaces a fixture's body by file name, for answers the API never gave.
    """

    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}

    def __call__(self, request: Recorded) -> tuple[int, Any]:
        path, _, query = request.path.partition("?")
        params = dict(urllib.parse.parse_qsl(query))
        [dataset] = [d for d in DATASETS.values() if path == f"/en/datos/{d.widget}"]
        unscoped = [s.name for s in dataset.systems if s.geo_id is None]
        system = params.get("geo_limit") or (unscoped[0] if unscoped else None)
        if system is None:  # like demanda/evolucion asked without geo parameters
            return 400, json.loads(fixture("error-400.json"))
        name = f"{dataset.name}-{system}-{params['start_date'][:10]}_{params['end_date'][:10]}.json"
        if name in self.payloads:
            return 200, self.payloads[name]
        answer = REDATA_FIXTURES / name
        return (200, answer.read_bytes()) if answer.exists() else (502, UNAVAILABLE)


def fixture(name: str) -> bytes:
    return (REDATA_FIXTURES / name).read_bytes()


OMIE_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "omie"
# The start of what OMIE answers, with 404, for a file it has not published (26 KB of HTML).
NOT_PUBLISHED = b'<!DOCTYPE html>\n<html lang="es" dir="ltr">\n  <head>\n'


class FakeOmie:
    """OMIE's file downloads answered from the recorded files (``tests/fixtures/omie``); a day
    without one gets OMIE's 404 page. ``files`` replaces or adds a day's bytes by file name."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def __call__(self, request: Recorded) -> tuple[int, Any]:
        path, _, query = request.path.partition("?")
        params = dict(urllib.parse.parse_qsl(query))
        if path != "/es/file-download" or params.get("parents") != "marginalpdbc":
            return 404, NOT_PUBLISHED
        name = params.get("filename", "")
        if name in self.files:
            return 200, self.files[name]
        answer = OMIE_FIXTURES / name
        return (
            (200, answer.read_bytes())
            if "/" not in name and answer.is_file()
            else (404, NOT_PUBLISHED)
        )


def omie_file(day: str) -> bytes:
    """The recorded file of an ISO delivery day."""
    return (OMIE_FIXTURES / f"marginalpdbc_{day.replace('-', '')}.1").read_bytes()


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


@pytest.fixture(autouse=True)
def default_logging() -> Iterator[None]:
    """The CLI points structlog at the test runner's stderr, which is closed once a test ends."""
    yield
    structlog.reset_defaults()


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
