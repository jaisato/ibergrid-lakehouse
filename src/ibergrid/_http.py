"""The single HTTP helper behind the catalog client, the readiness checks and the source clients.

Standard library only (``urllib``): the CLI runs in one-shot containers and from the deploy job,
where a small dependency surface is worth more than a nicer HTTP client.
"""

import urllib.error
import urllib.request
from collections.abc import Iterable


class Unreachable(Exception):
    """No HTTP answer at all: refused connection, unknown host or timeout."""


def send(
    method: str,
    url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, bytes]:
    """Send one request, without retries; every HTTP status is a normal ``(status, body)``."""
    status, _, payload = exchange(method, url, timeout=timeout, headers=headers, body=body)
    return status, payload


def exchange(
    method: str,
    url: str,
    *,
    timeout: float,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """``send``, plus the response headers (lower-case names), for callers that need them."""
    # Callers build URLs from HttpUrl settings, so only http and https can reach urlopen.
    request = urllib.request.Request(url, data=body, method=method, headers=headers or {})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return response.status, _lower(response.headers.items()), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, _lower(exc.headers.items()), exc.read()
    except OSError as exc:  # URLError, refused connections and timeouts
        raise Unreachable(f"{method} {url}: {getattr(exc, 'reason', exc)}") from exc


def _lower(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    return {name.lower(): value for name, value in items}
