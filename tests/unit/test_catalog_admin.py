from collections.abc import Callable
from typing import Any

import pytest
from pydantic import HttpUrl, SecretStr
from structlog.testing import capture_logs

from ibergrid.catalog_admin import (
    CatalogAdmin,
    CatalogError,
    bootstrap_catalog,
    update_credential,
)
from ibergrid.settings import CatalogSettings, StorageSettings

from .conftest import FakeLakekeeper, LocalServer, Recorded, Responder, error

Serve = Callable[[Responder], LocalServer]

SECRET = "test-secret-key-do-not-log"
CATALOG = CatalogSettings(warehouse="lakehouse")
STORAGE = StorageSettings(
    endpoint_url=HttpUrl("http://storage:9000"),
    region="us-east-1",
    bucket="ibergrid",
    access_key=SecretStr("test-access-key"),
    secret_key=SecretStr(SECRET),
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
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": "test-access-key",
            "secret-access-key": SECRET,
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


def test_unreachable_catalog_is_one_clear_error(closed_port: int) -> None:
    admin = CatalogAdmin(f"http://127.0.0.1:{closed_port}", timeout=2)

    with pytest.raises(CatalogError, match=r"^no answer from the catalog: GET http://127\.0\.0\.1"):
        bootstrap_catalog(admin, CATALOG, STORAGE)


def test_rejected_warehouse_reports_lakekeeper_error_with_the_secret_masked(serve: Serve) -> None:
    lakekeeper = FakeLakekeeper()

    def reject_warehouse(request: Recorded) -> tuple[int, Any]:
        if (request.method, request.path) == ("POST", "/management/v1/warehouse"):
            # A server that echoes the request must still not leak the key through our error.
            echoed = f"cannot use secret-access-key {SECRET}"
            return 400, error("StorageProfileValidationFailed", echoed, 400)
        return lakekeeper(request)

    server = serve(reject_warehouse)

    with pytest.raises(CatalogError) as caught:
        bootstrap_catalog(admin_for(server), CATALOG, STORAGE)

    message = str(caught.value)
    assert message == (
        "POST /management/v1/warehouse returned 400: "
        "StorageProfileValidationFailed: cannot use secret-access-key **********"
    )
    assert caught.value.__cause__ is None
    assert lakekeeper.bootstrapped, "the warehouse call comes after a successful bootstrap"


def test_update_credential_replaces_the_stored_keys(serve: Serve) -> None:
    lakekeeper = FakeLakekeeper()
    server = serve(lakekeeper)
    bootstrap_catalog(admin_for(server), CATALOG, STORAGE)
    rotated = STORAGE.model_copy(update={"secret_key": SecretStr("rotated-secret")})

    update_credential(admin_for(server), CATALOG, rotated)

    stored = lakekeeper.warehouses["lakehouse"]
    assert stored["storage-credential"]["secret-access-key"] == "rotated-secret"
    assert stored["storage-credential"]["credential-type"] == "access-key"
    assert stored["storage-profile"]["remote-signing-enabled"] is False


def test_update_credential_also_turns_remote_signing_off(serve: Serve) -> None:
    """A warehouse created before ADR 0002 gets the current profile (remote signing off)."""
    lakekeeper = FakeLakekeeper()
    server = serve(lakekeeper)
    bootstrap_catalog(admin_for(server), CATALOG, STORAGE)
    lakekeeper.warehouses["lakehouse"]["storage-profile"]["remote-signing-enabled"] = True

    update_credential(admin_for(server), CATALOG, STORAGE)

    assert lakekeeper.warehouses["lakehouse"]["storage-profile"]["remote-signing-enabled"] is False
    [call] = [r for r in server.requests if r.path.endswith("/storage")]
    assert set(call.body) == {"storage-profile", "storage-credential"}


def test_update_credential_keeps_the_profile_fields_it_does_not_own(serve: Serve) -> None:
    lakekeeper = FakeLakekeeper()
    server = serve(lakekeeper)
    bootstrap_catalog(admin_for(server), CATALOG, STORAGE)
    profile = lakekeeper.warehouses["lakehouse"]["storage-profile"]
    profile |= {"remote-signing-url-style": "path", "push-s3-delete-disabled": False}
    profile |= {"endpoint": "http://storage:9000/", "region": "eu-south-2"}

    with capture_logs() as logs:
        update_credential(admin_for(server), CATALOG, STORAGE)

    assert logs[-1]["profile"] == ["region"]  # the endpoint only lost its trailing slash
    profile = lakekeeper.warehouses["lakehouse"]["storage-profile"]
    assert (profile["remote-signing-url-style"], profile["push-s3-delete-disabled"]) == (
        "path",
        False,
    )
    assert (profile["region"], profile["endpoint"]) == ("us-east-1", "http://storage:9000")


def test_update_credential_needs_an_existing_warehouse(serve: Serve) -> None:
    server = serve(FakeLakekeeper())

    with pytest.raises(CatalogError, match="warehouse 'lakehouse' does not exist"):
        update_credential(admin_for(server), CATALOG, STORAGE)


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        ((502, None), "returned 502: no error details"),
        ((500, {"detail": SECRET}), "returned 500: unexpected dict body"),
        ((200, b"<html>proxy page</html>"), "returned 200 with a non-JSON body"),
        ((200, ["not", "an", "object"]), "returned 200 with a non-object JSON body"),
    ],
)
def test_unexpected_answers_are_explained_without_echoing_bodies(
    serve: Serve, answer: tuple[int, Any], expected: str
) -> None:
    server = serve(lambda _: answer)

    with pytest.raises(CatalogError) as caught:
        admin_for(server).expect("GET", "/management/v1/info", ok=200)

    assert str(caught.value).endswith(expected)
    assert SECRET not in str(caught.value)
