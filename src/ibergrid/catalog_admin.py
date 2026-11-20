"""Lakekeeper management API: bootstrap the server and create the warehouse, idempotently.

Standard library only (``urllib``): this runs in a one-shot init container and from the deploy job,
where a smaller dependency surface is worth more than a nicer HTTP client.
"""

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import structlog

from ibergrid.settings import CatalogSettings, StorageSettings, base_url

log = structlog.get_logger(__name__)


class CatalogError(RuntimeError):
    """The catalog is unreachable or answered something unexpected."""


@dataclass(frozen=True)
class BootstrapResult:
    bootstrapped: bool
    warehouse_created: bool


class CatalogAdmin:
    """A thin JSON client for Lakekeeper's ``/health`` and ``/management/v1`` endpoints."""

    def __init__(self, url: str, *, timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, Any]:
        """Send one request and return ``(status, parsed JSON or None)``; no retries."""
        # The URL comes from an HttpUrl setting, so only http and https can reach urlopen.
        req = urllib.request.Request(  # noqa: S310
            self.url + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
                status, raw = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, raw = exc.code, exc.read()
        except OSError as exc:  # URLError, refused connections and timeouts
            reason = getattr(exc, "reason", exc)
            raise CatalogError(f"{method} {path}: no answer from {self.url} ({reason})") from exc
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

    def wait_until_healthy(
        self, *, attempts: int = 30, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        """Poll ``/health`` with capped exponential backoff while the catalog starts."""
        delay, problem = 0.5, "no attempt made"
        for attempt in range(1, attempts + 1):
            try:
                status, _ = self.request("GET", "/health")
            except CatalogError as exc:
                problem = str(exc)
            else:
                if status == 200:
                    return
                problem = f"GET /health returned {status}"
            if attempt < attempts:
                log.info("catalog.waiting", attempt=attempt, problem=problem, retry_in_s=delay)
                sleep(delay)
                delay = min(delay * 2, 5.0)
        raise CatalogError(f"catalog not healthy after {attempts} attempts: {problem}")


def warehouse_request(catalog: CatalogSettings, storage: StorageSettings) -> dict[str, Any]:
    """The ``POST /management/v1/warehouse`` body. It carries the secret key: never log it."""
    return {
        "warehouse-name": catalog.warehouse,
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": storage.bucket,
            "key-prefix": catalog.warehouse,
            "region": storage.region,
            "endpoint": base_url(storage.endpoint_url),
            "path-style-access": storage.path_style,
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "access-key-id": storage.access_key.get_secret_value(),
            "secret-access-key": storage.secret_key.get_secret_value(),
        },
    }


def bootstrap_catalog(
    admin: CatalogAdmin, catalog: CatalogSettings, storage: StorageSettings
) -> BootstrapResult:
    """Bootstrap Lakekeeper and create the warehouse; each step is skipped if already done."""
    info = admin.expect("GET", "/management/v1/info", ok=200)
    bootstrapped = not info.get("bootstrapped", False)
    if bootstrapped:
        admin.expect("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True}, ok=204)
    log.info("catalog.bootstrap", changed=bootstrapped)

    listing = admin.expect("GET", "/management/v1/warehouse", ok=200)
    existing = {w.get("name") for w in listing.get("warehouses", [])}
    created = catalog.warehouse not in existing
    if created:
        admin.expect(
            "POST", "/management/v1/warehouse", warehouse_request(catalog, storage), ok=201
        )
    log.info(
        "catalog.warehouse", warehouse=catalog.warehouse, bucket=storage.bucket, changed=created
    )
    return BootstrapResult(bootstrapped=bootstrapped, warehouse_created=created)


def _describe(payload: Any) -> str:
    # Lakekeeper answers errors in the Iceberg REST shape: {"error": {"type", "message", "code"}}.
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict):
        return f"{error.get('type')}: {error.get('message')}"
    return "no error details" if payload is None else json.dumps(payload)[:300]
