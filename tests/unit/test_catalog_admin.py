from collections.abc import Callable
from typing import Any

import pytest
from pydantic import HttpUrl, SecretStr

from ibergrid.catalog_admin import CatalogAdmin, CatalogError, bootstrap_catalog
from ibergrid.settings import CatalogSettings, StorageSettings

from .conftest import FakeLakekeeper, LocalServer, Recorded, Responder, error

Serve = Callable[[Responder], LocalServer]

CATALOG = CatalogSettings(warehouse="lakehouse")
STORAGE = StorageSettings(
    endpoint_url=HttpUrl("http://storage:9000"),
    region="us-east-1",
    bucket="ibergrid",
    access_key=SecretStr("test-access-key"),
    secret_key=SecretStr("test-secret-key-do-not-log"),
    path_style=True,
)


def admin_for(server: LocalServer) -> CatalogAdmin:
    return CatalogAdmin(server.url, timeout=2)


def test_fresh_catalog_is_bootstrapped_and_gets_the_warehouse(serve: Serve) -> None:
    lakekeeper = FakeLakekeeper()
    server = serve(lakekeeper)

    result = bootstrap_catalog(admin_for(server), CATALOG, STORAGE)

    assert (result.bootstrapped, result.warehouse_created) == (True, True)
    assert lakekeeper.bootstrapped
    assert lakekeeper.warehouses["lakehouse"] == {
        "warehouse-name": "lakehouse",
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": "ibergrid",
            "key-prefix": "lakehouse",
            "region": "us-east-1",
            "endpoint": "http://storage:9000",
            "path-style-access": True,
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": "test-access-key",
            "secret-access-key": "test-secret-key-do-not-log",
        },
    }
    bootstrap_call = next(r for r in server.requests if r.path == "/management/v1/bootstrap")
    assert bootstrap_call.body == {"accept-terms-of-use": True}


def test_second_run_only_reads(serve: Serve) -> None:
    server = serve(FakeLakekeeper())
    bootstrap_catalog(admin_for(server), CATALOG, STORAGE)
    server.requests.clear()

    result = bootstrap_catalog(admin_for(server), CATALOG, STORAGE)

    assert (result.bootstrapped, result.warehouse_created) == (False, False)
    assert {r.method for r in server.requests} == {"GET"}


def test_existing_server_still_gets_a_missing_warehouse(serve: Serve) -> None:
    lakekeeper = FakeLakekeeper()
    lakekeeper.bootstrapped = True
    server = serve(lakekeeper)

    result = bootstrap_catalog(admin_for(server), CATALOG, STORAGE)

    assert (result.bootstrapped, result.warehouse_created) == (False, True)


def test_wait_retries_with_capped_exponential_backoff(serve: Serve) -> None:
    server = serve(FakeLakekeeper(unhealthy_answers=6))
    delays: list[float] = []

    admin_for(server).wait_until_healthy(attempts=10, sleep=delays.append)

    assert delays == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0]


def test_wait_gives_up_with_the_last_problem(serve: Serve) -> None:
    server = serve(FakeLakekeeper(unhealthy_answers=99))
    delays: list[float] = []

    with pytest.raises(
        CatalogError, match=r"not healthy after 3 attempts: GET /health returned 503"
    ):
        admin_for(server).wait_until_healthy(attempts=3, sleep=delays.append)
    assert len(delays) == 2


def test_unreachable_catalog_is_a_catalog_error(closed_port: int) -> None:
    admin = CatalogAdmin(f"http://127.0.0.1:{closed_port}", timeout=2)

    with pytest.raises(CatalogError, match="not healthy after 2 attempts: GET /health: no answer"):
        admin.wait_until_healthy(attempts=2, sleep=lambda _: None)


def test_error_answer_carries_lakekeeper_type_and_message(serve: Serve) -> None:
    def reject_warehouse(request: Recorded) -> tuple[int, Any]:
        if request.method == "POST":
            return 400, error("StorageProfileValidationFailed", "bucket does not exist", 400)
        return FakeLakekeeper()(request)

    server = serve(reject_warehouse)

    with pytest.raises(CatalogError) as caught:
        bootstrap_catalog(admin_for(server), CATALOG, STORAGE)
    message = str(caught.value)
    assert "returned 400: StorageProfileValidationFailed: bucket does not exist" in message


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        ((502, None), "returned 502: no error details"),
        ((500, {"detail": "boom"}), 'returned 500: {"detail": "boom"}'),
        ((200, b"<html>proxy page</html>"), "returned 200 with a non-JSON body"),
        ((200, ["not", "an", "object"]), "returned 200 with a non-object JSON body"),
    ],
)
def test_unexpected_answers_are_explained(
    serve: Serve, answer: tuple[int, Any], expected: str
) -> None:
    server = serve(lambda _: answer)

    with pytest.raises(CatalogError, match=expected.replace("{", r"\{").replace("}", r"\}")):
        admin_for(server).expect("GET", "/management/v1/info", ok=200)
