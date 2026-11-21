"""The REData client against a local HTTP server: retries, backoff, permanent errors, pacing."""

import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import pytest
from pydantic import HttpUrl

from ibergrid.settings import RedataSettings
from ibergrid.sources.redata.client import RedataClient, RedataError
from ibergrid.sources.redata.datasets import DATASETS, Chunk

from .conftest import UNAVAILABLE, FakeRedata, LocalServer, Recorded, Responder, fixture

Serve = Callable[[Responder], LocalServer]
HOURLY = DATASETS["demand_hourly"]
CANARIAS = Chunk(HOURLY, HOURLY.systems[1], date(2026, 11, 18), date(2026, 11, 18))


def client(
    server: LocalServer, delays: list[float], *, timeout: float = 2, **options: Any
) -> RedataClient:
    """A client that records its waits instead of sleeping; pacing (1 ns apart) never waits."""
    return RedataClient(server.url, timeout=timeout, max_rps=1e9, sleep=delays.append, **options)


def answers(*replies: tuple[int, Any] | tuple[int, Any, dict[str, str]]) -> Responder:
    """Answer the requests in order, then repeat the last reply."""
    queue = list(replies)

    def responder(_: Recorded) -> tuple[int, Any] | tuple[int, Any, dict[str, str]]:
        return queue.pop(0) if len(queue) > 1 else queue[0]

    return responder


def test_fetch_asks_for_the_chunk_in_english(serve: Serve) -> None:
    server = serve(FakeRedata())
    delays: list[float] = []

    body = client(server, delays).fetch(CANARIAS)

    assert body == fixture("demand_hourly-canarias-2026-11-18_2026-11-18.json")
    [request] = server.requests
    assert request.path == (
        "/en/datos/demanda/evolucion?start_date=2026-11-18T00%3A00&end_date=2026-11-18T23%3A59"
        "&time_trunc=hour&geo_trunc=electric_system&geo_limit=canarias&geo_ids=8742"
    )
    assert request.headers["accept"] == "application/json"
    assert request.headers["user-agent"].startswith("ibergrid-lakehouse/")
    assert delays == []


def test_5xx_is_retried_with_growing_jittered_backoff(serve: Serve) -> None:
    server = serve(answers((502, UNAVAILABLE), (500, None), (503, b"busy"), (200, b"{}")))
    delays: list[float] = []

    assert client(server, delays, backoff=1.0).fetch(CANARIAS) == b"{}"

    assert len(server.requests) == 4
    assert 0.5 <= delays[0] <= 1
    assert 1 <= delays[1] <= 2
    assert 2 <= delays[2] <= 4


def test_retries_are_bounded_and_the_error_names_widget_params_and_detail(serve: Serve) -> None:
    server = serve(answers((502, UNAVAILABLE)))
    delays: list[float] = []

    with pytest.raises(RedataError) as failure:
        client(server, delays, max_attempts=3).fetch(CANARIAS)

    assert len(server.requests) == 3
    assert len(delays) == 2
    message = str(failure.value)
    assert message.startswith("REData demanda/evolucion {'start_date': '2026-11-18T00:00'")
    assert "'geo_limit': 'canarias'" in message
    assert message.endswith(
        "HTTP 502: The requested data is currently unavailable. Please try again later. "
        "after 3 attempts"
    )


def test_backoff_is_capped(serve: Serve) -> None:
    server = serve(answers((500, None)))
    delays: list[float] = []

    with pytest.raises(RedataError):
        client(server, delays, max_attempts=5, backoff=10, max_delay=15).fetch(CANARIAS)

    assert all(delay <= 15 for delay in delays)
    assert max(delays) >= 7.5


@pytest.mark.parametrize("status", [400, 404, 403])
def test_4xx_is_permanent(serve: Serve, status: int) -> None:
    server = serve(answers((status, fixture("error-400.json"))))
    delays: list[float] = []

    with pytest.raises(RedataError, match=rf"HTTP {status}: The requested data is currently"):
        client(server, delays).fetch(CANARIAS)

    assert len(server.requests) == 1
    assert delays == []


def test_a_body_that_is_not_redata_json_is_quoted(serve: Serve) -> None:
    server = serve(answers((418, b"<html>teapot</html>")))

    with pytest.raises(RedataError, match=r"HTTP 418: <html>teapot</html> \(permanent"):
        client(server, []).fetch(CANARIAS)


def test_429_waits_as_long_as_retry_after_says(serve: Serve) -> None:
    server = serve(answers((429, None, {"Retry-After": "7"}), (200, b"{}")))
    delays: list[float] = []

    assert client(server, delays).fetch(CANARIAS) == b"{}"
    assert delays == [7.0]


def test_retry_after_can_be_an_http_date(serve: Serve) -> None:
    later = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)
    server = serve(answers((503, None, {"Retry-After": later}), (200, b"{}")))
    delays: list[float] = []

    client(server, delays).fetch(CANARIAS)

    assert 25 <= delays[0] <= 30


def test_an_unusable_retry_after_falls_back_to_backoff(serve: Serve) -> None:
    server = serve(answers((503, None, {"Retry-After": "soon"}), (200, b"{}")))
    delays: list[float] = []

    client(server, delays, backoff=1.0).fetch(CANARIAS)

    assert 0.5 <= delays[0] <= 1


def test_a_retry_after_beyond_the_cap_fails_now(serve: Serve) -> None:
    server = serve(answers((429, None, {"Retry-After": "3600"})))

    with pytest.raises(RedataError, match="Retry-After 3600 s is too long"):
        client(server, [], max_delay=60).fetch(CANARIAS)

    assert len(server.requests) == 1


def test_timeouts_are_retried(serve: Serve) -> None:
    def slow(_: Recorded) -> tuple[int, Any]:
        time.sleep(0.5)
        return 200, b"{}"

    server = serve(slow)
    delays: list[float] = []

    with pytest.raises(RedataError, match=r"no answer \(GET .* timed out\) after 2 attempts"):
        client(server, delays, max_attempts=2, timeout=0.1).fetch(CANARIAS)

    assert len(delays) == 1


def test_requests_are_paced(serve: Serve) -> None:
    server = serve(FakeRedata())
    paced = RedataClient(server.url, timeout=2, max_rps=5)

    started = time.monotonic()
    for _ in range(3):
        paced.fetch(CANARIAS)

    assert time.monotonic() - started >= 0.4  # two waits of 1/5 s between three requests


def test_from_settings() -> None:
    settings = RedataSettings(base_url=HttpUrl("http://127.0.0.1:1/"), timeout=5, max_rps=0.5)

    built = RedataClient.from_settings(settings)

    assert (built.base, built.timeout, built.interval) == ("http://127.0.0.1:1", 5, 2.0)
    assert built.url(CANARIAS).startswith("http://127.0.0.1:1/en/datos/demanda/evolucion?")


def test_a_retry_after_date_without_a_zone_is_read_as_utc(serve: Serve) -> None:
    later = (datetime.now(UTC) + timedelta(seconds=30)).strftime("%a, %d %b %Y %H:%M:%S -0000")
    server = serve(answers((503, None, {"Retry-After": later}), (200, b"{}")))
    delays: list[float] = []

    client(server, delays).fetch(CANARIAS)

    assert 25 <= delays[0] <= 30
