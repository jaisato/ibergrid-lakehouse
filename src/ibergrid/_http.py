"""The single HTTP helper behind the catalog client, the readiness checks and the source clients.

Standard library only (``urllib``): the CLI runs in one-shot containers and from the deploy job,
where a small dependency surface is worth more than a nicer HTTP client.
"""

import email.utils
import http.client
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from importlib.metadata import version

import structlog

log = structlog.get_logger(__name__)


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


class PoliteClient:
    """The source clients' GET: at most ``max_rps`` requests a second per instance, and bounded
    retries on timeouts, broken responses, 429 and 5xx, with exponential backoff and jitter (or
    after ``Retry-After``, up to ``max_delay``). Subclasses set ``error`` and may override
    ``detail``; what a final status means is theirs to decide."""

    error: type[Exception] = RuntimeError
    source = "http"  # names the retry log event
    accept = "*/*"

    def __init__(
        self,
        base: str,
        *,
        timeout: float,
        max_rps: float = 1.0,
        max_attempts: int = 4,
        backoff: float = 1.0,
        max_delay: float = 60.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.interval = 1 / max_rps
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.max_delay = max_delay
        self._sleep = sleep
        self._last_request: float | None = None
        self._headers = {
            "Accept": self.accept,
            "User-Agent": f"ibergrid-lakehouse/{version('ibergrid')}",
        }

    def detail(self, body: bytes) -> str:
        """What an error answer says, for messages: by default the start of its body."""
        return body[:200].decode(errors="replace") or "(empty body)"

    def get(self, url: str, what: str, **context: str) -> tuple[int, bytes]:
        """The first answer that is not 429 or 5xx, as ``(status, body)``; ``self.error`` naming
        ``what`` once the attempts run out or ``Retry-After`` asks for too long. ``context`` goes
        into each retry's log event (``<source>.retry``)."""
        attempt = 0
        while True:
            attempt += 1
            self._pace()
            try:
                status, headers, body = exchange(
                    "GET", url, timeout=self.timeout, headers=self._headers
                )
            except Unreachable as exc:
                failure, delay = f"no answer ({exc})", None
            else:
                if status != 429 and status < 500:
                    return status, body
                failure = f"HTTP {status}: {self.detail(body)}"
                delay = _retry_after(headers.get("retry-after"))
            if attempt >= self.max_attempts:
                raise self.error(f"{what}: {failure} after {self.max_attempts} attempts")
            if delay is None:
                # Exponential backoff with jitter, so that parallel runs do not retry in step.
                base = min(self.max_delay, self.backoff * 2 ** (attempt - 1))
                delay = base * random.uniform(0.5, 1.0)  # noqa: S311 (jitter, not security)
            elif delay > self.max_delay:
                raise self.error(f"{what}: {failure}; Retry-After {delay:.0f} s is too long")
            log.warning(f"{self.source}.retry", **context, attempt=attempt, error=failure)
            self._sleep(delay)

    def _pace(self) -> None:
        if self._last_request is not None:
            wait = self._last_request + self.interval - time.monotonic()
            if wait > 0:
                self._sleep(wait)
        self._last_request = time.monotonic()


def _retry_after(value: str | None) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (seconds or an HTTP date), if usable."""
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - datetime.now(UTC)).total_seconds())


def _lower(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    return {name.lower(): value for name, value in items}
