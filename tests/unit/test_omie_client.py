"""The OMIE client against a local server: the file, OMIE's 404 as "no file", retries, redirects.

The retry policy itself is ``ibergrid._http.PoliteClient``'s, covered in depth through REData's
client tests; these cover what OMIE's client adds.
"""

from collections.abc import Callable
from datetime import date
from typing import Any

import pytest
from pydantic import HttpUrl

from ibergrid.settings import OmieSettings
from ibergrid.sources.omie.client import OmieClient, OmieError

from .conftest import NOT_PUBLISHED, FakeOmie, LocalServer, Recorded, Responder, omie_file

Serve = Callable[[Responder], LocalServer]
DAY = date(2025, 10, 1)


def client(server: LocalServer, delays: list[float], **options: Any) -> OmieClient:
    """A client that records its waits instead of sleeping; pacing (1 ns apart) never waits."""
    return OmieClient(server.url, timeout=2, max_rps=1e9, sleep=delays.append, **options)


def test_fetch_downloads_the_days_file(serve: Serve) -> None:
    server = serve(FakeOmie())
    delays: list[float] = []

    assert client(server, delays).fetch(DAY) == omie_file("2025-10-01")
    [request] = server.requests
    assert request.path == "/es/file-download?parents=marginalpdbc&filename=marginalpdbc_20251001.1"
    assert request.headers["accept"] == "*/*"
    assert request.headers["user-agent"].startswith("ibergrid-lakehouse/")
    assert delays == []


def test_a_file_omie_does_not_have_is_none_and_not_retried(serve: Serve) -> None:
    server = serve(FakeOmie())
    delays: list[float] = []

    assert client(server, delays).fetch(date(2099, 1, 1)) is None
    assert len(server.requests) == 1
    assert delays == []


def test_a_redirect_is_followed(serve: Serve) -> None:
    target = serve(FakeOmie())

    def redirect(request: Recorded) -> tuple[int, Any, dict[str, str]]:
        return 302, None, {"Location": target.url + request.path}

    assert client(serve(redirect), []).fetch(DAY) == omie_file("2025-10-01")
    assert len(target.requests) == 1


def test_5xx_is_retried_then_reported_with_the_file_name(serve: Serve) -> None:
    server = serve(lambda _: (503, b"busy"))
    delays: list[float] = []

    with pytest.raises(
        OmieError, match=r"^OMIE marginalpdbc_20251001\.1: HTTP 503: busy after 3 attempts$"
    ):
        client(server, delays, max_attempts=3).fetch(DAY)

    assert len(server.requests) == 3
    assert len(delays) == 2


@pytest.mark.parametrize("status", [400, 403, 410])
def test_other_4xx_are_permanent(serve: Serve, status: int) -> None:
    server = serve(lambda _: (status, NOT_PUBLISHED))

    with pytest.raises(
        OmieError, match=rf"HTTP {status}: <!DOCTYPE html>.*\(permanent, not retried\)"
    ):
        client(server, []).fetch(DAY)

    assert len(server.requests) == 1


def test_from_settings() -> None:
    settings = OmieSettings(base_url=HttpUrl("http://127.0.0.1:1/"), timeout=5, max_rps=0.5)

    built = OmieClient.from_settings(settings)

    assert (built.base, built.timeout, built.interval) == ("http://127.0.0.1:1", 5, 2.0)
    assert built.url(DAY).startswith("http://127.0.0.1:1/es/file-download?parents=marginalpdbc&")
