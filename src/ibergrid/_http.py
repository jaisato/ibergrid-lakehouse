"""The single HTTP helper behind the catalog client, the readiness checks and the source clients.

Standard library only (``urllib``): the CLI runs in one-shot containers and from the deploy job,
where a small dependency surface is worth more than a nicer HTTP client.
"""

import http.client
import urllib.error
import urllib.request
from collections.abc import Iterable


class Unreachable(Exception):
    """No complete HTTP answer: refused connection, unknown host, timeout or a broken response."""


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
        try:
            error_body = exc.read()
        except (OSError, http.client.HTTPException):
            # An error answer whose body is cut short still has its status (and Retry-After):
            # keep them, so the caller's retry policy decides instead of a transport error.
            error_body = b""
        return exc.code, _lower(exc.headers.items()), error_body
    # OSError: URLError, refused connections and timeouts; HTTPException: a response cut short
    # (IncompleteRead) or not HTTP at all.
    except (OSError, http.client.HTTPException) as exc:
        raise Unreachable(f"{method} {url}: {getattr(exc, 'reason', exc)!s}") from exc


def _lower(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    return {name.lower(): value for name, value in items}
