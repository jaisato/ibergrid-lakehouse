"""Readiness checks for object storage, the catalog and the broker.

``ibergrid doctor`` runs them inside the compose network (``make smoke``) and after a deploy.
"""

import socket
import time
from dataclasses import dataclass

from pyarrow import fs

from ibergrid import _http
from ibergrid.settings import BrokerSettings, CatalogSettings, Settings, StorageSettings, base_url
from ibergrid.storage import s3_filesystem


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def run_checks(settings: Settings, timeout: float) -> list[CheckResult]:
    return [
        check_storage(settings.storage, timeout),
        check_catalog(settings.catalog, timeout),
        check_broker(settings.broker, timeout),
    ]


def check_storage(storage: StorageSettings, timeout: float) -> CheckResult:
    """A HeadBucket through pyarrow's S3 client: the endpoint answers, the keys work and the bucket
    exists.

    RustFS also serves an anonymous ``/health/ready`` (the compose healthcheck uses it), but
    Cloudflare R2 has no equivalent, so the check that works on both is an authenticated
    HeadBucket.
    """
    s3 = s3_filesystem(storage, timeout=timeout, max_attempts=1)
    what = f"HeadBucket {base_url(storage.endpoint_url)}/{storage.bucket}"
    started = time.monotonic()
    try:
        info = s3.get_file_info(storage.bucket)
    except OSError as exc:
        # "When getting information for bucket 'x': AWS Error ACCESS_DENIED during HeadBucket ..."
        reason = str(exc).partition(": ")[2] or str(exc)
        hint = "credentials rejected" if "ACCESS_DENIED" in reason else reason
        return CheckResult("storage", False, f"{what}: {hint}")
    elapsed = f"({round((time.monotonic() - started) * 1000)} ms)"
    if info.type == fs.FileType.NotFound:
        return CheckResult(
            "storage", False, f"{what} {elapsed}: bucket {storage.bucket!r} does not exist"
        )
    return CheckResult("storage", True, f"{what} {elapsed}")


def check_catalog(catalog: CatalogSettings, timeout: float) -> CheckResult:
    url = f"{base_url(catalog.url)}/health"
    started = time.monotonic()
    try:
        status, _ = _http.send("GET", url, timeout=timeout)
    except _http.Unreachable as exc:
        return CheckResult("catalog", False, str(exc))
    elapsed_ms = round((time.monotonic() - started) * 1000)
    return CheckResult("catalog", status == 200, f"GET {url} -> {status} ({elapsed_ms} ms)")


def check_broker(broker: BrokerSettings, timeout: float) -> CheckResult:
    """A TCP connect to every bootstrap server; the Kafka protocol is checked in later phases."""
    failures = []
    for host, port in broker.addresses():
        try:
            with socket.create_connection((host, port), timeout=timeout):
                pass
        except OSError as exc:
            failures.append(f"{host}:{port} ({exc})")
    if failures:
        return CheckResult("broker", False, "TCP connect failed: " + ", ".join(failures))
    return CheckResult("broker", True, f"TCP connect to {broker.bootstrap_servers}")
