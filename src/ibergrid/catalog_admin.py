"""Lakekeeper management API: bootstrap the server, create the warehouse, rotate its credential.

Compose starts ``catalog-init`` only once the catalog is healthy, so nothing here retries: an
unreachable catalog is one clear error.
"""

import json
from dataclasses import dataclass
from typing import Any

import structlog

from ibergrid import _http
from ibergrid.settings import CatalogSettings, StorageSettings, base_url

log = structlog.get_logger(__name__)


class CatalogError(RuntimeError):
    """The catalog is unreachable or answered something unexpected."""


@dataclass(frozen=True)
class BootstrapResult:
    bootstrapped: bool
    warehouse_created: bool


class CatalogAdmin:
    """A thin JSON client for Lakekeeper's ``/management/v1`` endpoints."""

    def __init__(self, url: str, *, timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, Any]:
        """Send one request and return ``(status, parsed JSON or None)``."""
        try:
            status, raw = _http.send(
                method,
                self.url + path,
                timeout=self.timeout,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                body=None if body is None else json.dumps(body).encode(),
            )
        except _http.Unreachable as exc:
            raise CatalogError(f"no answer from the catalog: {exc}") from exc
        try:
            return status, json.loads(raw) if raw else None
        except ValueError as exc:
            raise CatalogError(f"{method} {path} returned {status} with a non-JSON body") from exc

    def expect(
        self, method: str, path: str, body: dict[str, Any] | None = None, *, ok: int
    ) -> dict[str, Any]:
        """Send one request and return its JSON object, or raise unless the status is ``ok``."""
        status, payload = self.request(method, path, body)
        if status != ok:
            raise CatalogError(f"{method} {path} returned {status}: {_describe(payload)}")
        if payload is None:
            return {}
        if not isinstance(payload, dict):
            raise CatalogError(f"{method} {path} returned {status} with a non-object JSON body")
        return payload


def storage_credential(storage: StorageSettings) -> dict[str, str]:
    """Lakekeeper's static S3 access-key credential. It holds the secret key: never log it."""
    return {
        "type": "s3",
        "credential-type": "access-key",
        "access-key-id": storage.access_key.get_secret_value(),
        "secret-access-key": storage.secret_key.get_secret_value(),
    }


def warehouse_request(catalog: CatalogSettings, storage: StorageSettings) -> dict[str, Any]:
    """The ``POST /management/v1/warehouse`` body."""
    return {
        "warehouse-name": catalog.warehouse,
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": storage.bucket,
            "key-prefix": catalog.warehouse,
            "region": storage.region,
            "endpoint": base_url(storage.endpoint_url),
            # RustFS and Cloudflare R2 both accept path-style addressing.
            "path-style-access": True,
            "sts-enabled": False,
            # Every client uses its own S3 keys (ADR 0002): no signing service on the open catalog.
            "remote-signing-enabled": False,
        },
        "storage-credential": storage_credential(storage),
    }


def bootstrap_catalog(
    admin: CatalogAdmin, catalog: CatalogSettings, storage: StorageSettings
) -> BootstrapResult:
    """Bootstrap Lakekeeper and create the warehouse; each step is skipped if already done.

    An existing warehouse keeps its stored credential: rotate keys with ``update_credential``.
    """
    info = admin.expect("GET", "/management/v1/info", ok=200)
    bootstrapped = not info.get("bootstrapped", False)
    if bootstrapped:
        admin.expect("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True}, ok=204)
    log.info("catalog.bootstrap", changed=bootstrapped)

    created = _warehouse_id(admin, catalog) is None
    if created:
        body = warehouse_request(catalog, storage)
        _send_secret(admin, "/management/v1/warehouse", body, storage, ok=201)
    log.info(
        "catalog.warehouse", warehouse=catalog.warehouse, bucket=storage.bucket, changed=created
    )
    return BootstrapResult(bootstrapped=bootstrapped, warehouse_created=created)


def update_credential(
    admin: CatalogAdmin, catalog: CatalogSettings, storage: StorageSettings
) -> None:
    """Replace the warehouse's stored S3 credential with the keys in ``storage``."""
    warehouse_id = _warehouse_id(admin, catalog)
    if warehouse_id is None:
        raise CatalogError(f"warehouse {catalog.warehouse!r} does not exist: bootstrap it first")
    path = f"/management/v1/warehouse/{warehouse_id}/storage-credential"
    _send_secret(
        admin, path, {"new-storage-credential": storage_credential(storage)}, storage, ok=200
    )
    log.info("catalog.credential", warehouse=catalog.warehouse, changed=True)


def _warehouse_id(admin: CatalogAdmin, catalog: CatalogSettings) -> str | None:
    listing = admin.expect("GET", "/management/v1/warehouse", ok=200)
    ids = [w.get("id") for w in listing.get("warehouses", []) if w.get("name") == catalog.warehouse]
    return str(ids[0]) if ids else None


def _send_secret(
    admin: CatalogAdmin, path: str, body: dict[str, Any], storage: StorageSettings, *, ok: int
) -> None:
    """POST a body that carries the secret key; an error that echoes it gets it masked."""
    try:
        admin.expect("POST", path, body, ok=ok)
    except CatalogError as exc:
        secret = storage.secret_key.get_secret_value()
        raise CatalogError(str(exc).replace(secret, "**********")) from None


def _describe(payload: Any) -> str:
    # Lakekeeper answers errors in the Iceberg REST shape: {"error": {"type", "message", "code"}}.
    # Anything else is summarised, never echoed: an unexpected body could hold anything.
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict):
        return f"{error.get('type')}: {error.get('message')}"
    return "no error details" if payload is None else f"unexpected {type(payload).__name__} body"
