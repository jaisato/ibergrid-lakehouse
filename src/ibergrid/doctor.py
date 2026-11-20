"""Readiness checks for object storage, the catalog and the broker.

``ibergrid doctor`` runs them inside the compose network (``make smoke``) and after a deploy.
Standard library only, like :mod:`ibergrid.catalog_admin`.
"""

import hashlib
import hmac
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime

from ibergrid.settings import BrokerSettings, CatalogSettings, Settings, StorageSettings, base_url

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


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
    """A signed ``HEAD /<bucket>``: the endpoint answers, the keys work and the bucket exists.

    RustFS also serves an anonymous ``/health/ready`` (the compose healthcheck uses it), but
    Cloudflare R2 has no equivalent, so the check that works on both is an authenticated
    HeadBucket. Path-style addressing, which RustFS and R2 both accept.
    """
    url = f"{base_url(storage.endpoint_url)}/{urllib.parse.quote(storage.bucket)}"
    headers = sigv4_headers("HEAD", url, storage, datetime.now(UTC))
    hints = {403: "credentials rejected", 404: f"bucket {storage.bucket!r} does not exist"}
    return _probe("storage", "HEAD", url, timeout, headers, hints)


def check_catalog(catalog: CatalogSettings, timeout: float) -> CheckResult:
    return _probe("catalog", "GET", f"{base_url(catalog.url)}/health", timeout, {}, {})


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


def sigv4_headers(method: str, url: str, storage: StorageSettings, now: datetime) -> dict[str, str]:
    """AWS Signature Version 4 headers for a body-less S3 request (``url`` already encoded)."""
    parts = urllib.parse.urlsplit(url)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    scope = f"{amz_date[:8]}/{storage.region}/s3/aws4_request"
    # Lowercase names in sorted order, as the canonical request requires.
    headers = {"host": parts.netloc, "x-amz-content-sha256": EMPTY_SHA256, "x-amz-date": amz_date}
    signed = ";".join(headers)
    canonical = "\n".join(
        [method, parts.path or "/", parts.query]
        + [f"{name}:{value}" for name, value in headers.items()]
        + ["", signed, EMPTY_SHA256]
    )
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = f"AWS4{storage.secret_key.get_secret_value()}".encode()
    for part in (amz_date[:8], storage.region, "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    credential = f"{storage.access_key.get_secret_value()}/{scope}"
    return headers | {
        "authorization": (
            f"AWS4-HMAC-SHA256 Credential={credential}, "
            f"SignedHeaders={signed}, Signature={signature}"
        )
    }


def _probe(
    name: str,
    method: str,
    url: str,
    timeout: float,
    headers: dict[str, str],
    hints: dict[int, str],
) -> CheckResult:
    # The URL comes from an HttpUrl setting, so only http and https can reach urlopen.
    request = urllib.request.Request(url, method=method, headers=headers)  # noqa: S310
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    except OSError as exc:  # URLError, refused connections and timeouts
        return CheckResult(name, False, f"{method} {url}: {getattr(exc, 'reason', exc)}")
    elapsed_ms = round((time.monotonic() - started) * 1000)
    detail = f"{method} {url} -> {status} ({elapsed_ms} ms)"
    if status in hints:
        detail += f": {hints[status]}"
    return CheckResult(name, status == 200, detail)
