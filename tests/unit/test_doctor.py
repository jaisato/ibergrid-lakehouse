from collections.abc import Callable

import pytest
from pydantic import HttpUrl, SecretStr

from ibergrid.doctor import (
    check_broker,
    check_catalog,
    check_storage,
    run_checks,
)
from ibergrid.settings import BrokerSettings, CatalogSettings, Settings, StorageSettings

from .conftest import FakeLakekeeper, LocalServer, Responder

Serve = Callable[[Responder], LocalServer]


def storage_at(url: str, **overrides: str) -> StorageSettings:
    return StorageSettings(
        endpoint_url=HttpUrl(url),
        bucket=overrides.get("bucket", "ibergrid"),
        region=overrides.get("region", "us-east-1"),
        access_key=SecretStr(overrides.get("access_key", "test-access-key")),
        secret_key=SecretStr(overrides.get("secret_key", "test-secret-key-do-not-log")),
    )


def test_storage_check_sends_a_signed_head_bucket(serve: Serve) -> None:
    server = serve(lambda _: (200, None))

    result = check_storage(storage_at(server.url), timeout=2)

    assert result.ok, result.detail
    request = server.requests[0]
    assert (request.method, request.path) == ("HEAD", "/ibergrid")
    assert request.headers["host"] == server.url.removeprefix("http://")
    assert request.headers["authorization"].startswith(
        "AWS4-HMAC-SHA256 Credential=test-access-key/"
    )
    assert "test-secret-key-do-not-log" not in str(request.headers)


@pytest.mark.parametrize(
    ("status", "hint"),
    [
        (403, "credentials rejected"),
        (404, "bucket 'ibergrid' does not exist"),
        (500, "HTTP status 500"),
    ],
)
def test_storage_check_explains_failures(serve: Serve, status: int, hint: str) -> None:
    server = serve(lambda _: (status, None))

    result = check_storage(storage_at(server.url), timeout=2)

    assert not result.ok
    assert hint in result.detail


def test_storage_check_reports_an_unreachable_endpoint(closed_port: int) -> None:
    result = check_storage(storage_at(f"http://127.0.0.1:{closed_port}"), timeout=2)

    assert not result.ok
    assert result.detail.startswith(f"HeadBucket http://127.0.0.1:{closed_port}/ibergrid: ")
    assert "NETWORK_CONNECTION" in result.detail


def test_catalog_check_uses_the_health_endpoint(serve: Serve) -> None:
    server = serve(FakeLakekeeper())

    result = check_catalog(CatalogSettings(url=HttpUrl(server.url)), timeout=2)

    assert result.ok
    assert result.detail.startswith(f"GET {server.url}/health -> 200")


def test_catalog_check_fails_on_an_unhealthy_answer(serve: Serve) -> None:
    server = serve(FakeLakekeeper(unhealthy_answers=1))

    result = check_catalog(CatalogSettings(url=HttpUrl(server.url)), timeout=2)

    assert not result.ok
    assert "-> 503" in result.detail


def test_unreachable_endpoint_is_reported_not_raised(closed_port: int) -> None:
    catalog = CatalogSettings(url=HttpUrl(f"http://127.0.0.1:{closed_port}"))

    result = check_catalog(catalog, timeout=2)

    assert not result.ok
    assert "refused" in result.detail.lower()


def test_broker_check_connects_to_every_server(open_port: int, closed_port: int) -> None:
    up = BrokerSettings(bootstrap_servers=f"127.0.0.1:{open_port}")
    half = BrokerSettings(bootstrap_servers=f"127.0.0.1:{open_port},127.0.0.1:{closed_port}")

    assert check_broker(up, timeout=2).ok
    failed = check_broker(half, timeout=2)
    assert not failed.ok
    assert failed.detail.startswith(f"TCP connect failed: 127.0.0.1:{closed_port} (")


def test_run_checks_reports_storage_catalog_and_broker_in_order(
    serve: Serve, open_port: int
) -> None:
    s3 = serve(lambda _: (200, None))
    lakekeeper = serve(FakeLakekeeper())
    settings = Settings(
        storage=storage_at(s3.url),
        catalog=CatalogSettings(url=HttpUrl(lakekeeper.url)),
        broker=BrokerSettings(bootstrap_servers=f"127.0.0.1:{open_port}"),
    )

    results = run_checks(settings, timeout=2)

    assert [(r.name, r.ok) for r in results] == [
        ("storage", True),
        ("catalog", True),
        ("broker", True),
    ]
