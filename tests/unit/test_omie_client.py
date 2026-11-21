"""The OMIE client against a local server: the file under its version, OMIE's 404 as "no file",
retries, redirects.

The retry policy itself is ``ibergrid._http.PoliteClient``'s, covered in depth through REData's
client tests; these cover what OMIE's client adds.
"""

from collections.abc import Callable
from datetime import date
from typing import Any

import pytest
import structlog.testing
from pydantic import HttpUrl

from ibergrid.settings import OmieSettings
from ibergrid.sources.omie.client import MAX_VERSION, OmieClient, OmieError, OmieFile, file_name

from .conftest import NOT_PUBLISHED, FakeOmie, LocalServer, Recorded, Responder, omie_file

Serve = Callable[[Responder], LocalServer]
DAY = date(2025, 10, 1)


def client(server: LocalServer, delays: list[float], **options: Any) -> OmieClient:
    """A client that records its waits instead of sleeping; pacing (1 ns apart) never waits."""
    return OmieClient(server.url, timeout=2, max_rps=1e9, sleep=delays.append, **options)


def names(server: LocalServer) -> list[str]:
    return [request.path.rpartition("filename=")[2] for request in server.requests]


def test_fetch_downloads_the_days_file(serve: Serve) -> None:
    server = serve(FakeOmie())
    delays: list[float] = []

    found = client(server, delays).fetch(DAY)

    assert found == OmieFile(DAY, 1, omie_file("2025-10-01"))
    assert found.name == "marginalpdbc_20251001.1"
    [request] = server.requests
    assert request.path == "/es/file-download?parents=marginalpdbc&filename=marginalpdbc_20251001.1"
    assert request.headers["accept"] == "*/*"
    assert request.headers["user-agent"].startswith("ibergrid-lakehouse/")
    assert delays == []


@pytest.mark.parametrize(("day", "version"), [(date(2023, 5, 28), 2), (date(2025, 10, 30), 3)])
def test_a_republished_day_is_found_under_its_version(
    serve: Serve, day: date, version: int
) -> None:
    """OMIE's listing holds one file per day: these two days only under .2 and .3."""
    server = serve(FakeOmie())

    found = client(server, []).fetch(day)

    assert found is not None
    assert (found.version, found.name) == (version, file_name(day, version))
    assert names(server) == [file_name(day, v) for v in range(1, version + 1)]


def test_a_day_without_any_version_is_none_after_trying_each(serve: Serve) -> None:
    server = serve(FakeOmie())
    delays: list[float] = []

    assert client(server, delays).fetch(date(2099, 1, 1)) is None
    assert names(server) == [f"marginalpdbc_20990101.{v}" for v in range(1, MAX_VERSION + 1)]
    assert delays == []  # a 404 is an answer, not retried


def test_a_redirect_is_followed(serve: Serve) -> None:
    target = serve(FakeOmie())

    def redirect(request: Recorded) -> tuple[int, Any, dict[str, str]]:
        return 302, None, {"Location": target.url + request.path}

    found = client(serve(redirect), []).fetch(DAY)

    assert found is not None
    assert found.payload == omie_file("2025-10-01")
    assert len(target.requests) == 1


def test_5xx_is_retried_then_reported_with_the_file_name(serve: Serve) -> None:
    server = serve(lambda _: (503, b"busy"))
    delays: list[float] = []

    with (
        structlog.testing.capture_logs() as logs,
        pytest.raises(
            OmieError, match=r"^OMIE marginalpdbc_20251001\.1: HTTP 503: busy after 3 attempts$"
        ),
    ):
        client(server, delays, max_attempts=3).fetch(DAY)

    assert len(server.requests) == 3
    assert len(delays) == 2
    assert [(log["event"], log["file"], log["attempt"]) for log in logs] == [
        ("omie.retry", "marginalpdbc_20251001.1", 1),
        ("omie.retry", "marginalpdbc_20251001.1", 2),
    ]


@pytest.mark.parametrize("status", [400, 403, 410])
def test_other_4xx_are_permanent(serve: Serve, status: int) -> None:
    server = serve(lambda _: (status, NOT_PUBLISHED))

    with pytest.raises(
        OmieError,
        match=rf"^OMIE marginalpdbc_20251001\.1: HTTP {status}: <!DOCTYPE html>.*"
        r"\(permanent, not retried\)$",
    ):
        client(server, []).fetch(DAY)

    assert len(server.requests) == 1


def test_from_settings() -> None:
    settings = OmieSettings(base_url=HttpUrl("http://127.0.0.1:1/"), timeout=5, max_rps=0.5)

    built = OmieClient.from_settings(settings)

    assert (built.base, built.timeout, built.interval) == ("http://127.0.0.1:1", 5, 2.0)
    assert built.url(DAY, 3) == (
        "http://127.0.0.1:1/es/file-download?parents=marginalpdbc&filename=marginalpdbc_20251001.3"
    )
