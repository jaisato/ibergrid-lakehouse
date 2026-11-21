"""REData over ``ibergrid._http``: at most ``max_rps`` requests a second and bounded retries.

Timeouts, 429 and 5xx are retried with exponential backoff and jitter (or after ``Retry-After``);
any other answer but 200 is permanent. REData answers 502 with "The requested data is currently
unavailable" both when it fails and when a window has no data (a future day, before a series
starts), so such a window fails after the last attempt, a few seconds later.
"""

import email.utils
import json
import random
import time
import urllib.parse
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version

import structlog

from ibergrid import _http
from ibergrid.settings import RedataSettings, base_url
from ibergrid.sources.redata.datasets import Chunk

log = structlog.get_logger(__name__)


class RedataError(Exception):
    """REData refused a request or never answered it, or its answer cannot be stored."""


class RedataClient:
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
            "Accept": "application/json",
            "User-Agent": f"ibergrid-lakehouse/{version('ibergrid')}",
        }

    @classmethod
    def from_settings(cls, settings: RedataSettings) -> "RedataClient":
        return cls(base_url(settings.base_url), timeout=settings.timeout, max_rps=settings.max_rps)

    def url(self, chunk: Chunk) -> str:
        # /en/: English series titles; the series ids are the same in every language.
        query = urllib.parse.urlencode(chunk.params())
        return f"{self.base}/en/datos/{chunk.dataset.widget}?{query}"

    def fetch(self, chunk: Chunk) -> bytes:
        """The response body of a 200, or ``RedataError`` naming the widget and the parameters."""
        url = self.url(chunk)
        what = f"REData {chunk.dataset.widget} {chunk.params()}"
        attempt = 0
        while True:
            attempt += 1
            self._pace()
            try:
                status, headers, body = _http.exchange(
                    "GET", url, timeout=self.timeout, headers=self._headers
                )
            except _http.Unreachable as exc:
                failure, delay = f"no answer ({exc})", None
            else:
                if status == 200:
                    return body
                failure = f"HTTP {status}: {_detail(body)}"
                if status != 429 and status < 500:
                    raise RedataError(f"{what}: {failure} (permanent, not retried)")
                delay = _retry_after(headers.get("retry-after"))
            if attempt >= self.max_attempts:
                raise RedataError(f"{what}: {failure} after {self.max_attempts} attempts")
            if delay is None:
                # Exponential backoff with jitter, so that parallel runs do not retry in step.
                base = min(self.max_delay, self.backoff * 2 ** (attempt - 1))
                delay = base * random.uniform(0.5, 1.0)  # noqa: S311 (jitter, not security)
            elif delay > self.max_delay:
                raise RedataError(f"{what}: {failure}; Retry-After {delay:.0f} s is too long")
            log.warning("redata.retry", widget=chunk.dataset.widget, attempt=attempt, error=failure)
            self._sleep(delay)

    def _pace(self) -> None:
        if self._last_request is not None:
            wait = self._last_request + self.interval - time.monotonic()
            if wait > 0:
                self._sleep(wait)
        self._last_request = time.monotonic()


def _detail(body: bytes) -> str:
    """REData's own message (``errors[0].detail``), or the start of the body."""
    try:
        return str(json.loads(body)["errors"][0]["detail"])
    except (ValueError, KeyError, IndexError, TypeError):
        return body[:200].decode(errors="replace") or "(empty body)"


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
