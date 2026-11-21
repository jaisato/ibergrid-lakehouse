"""REData over ``ibergrid._http``: at most ``max_rps`` requests a second and bounded retries.

Timeouts, 429 and 5xx are retried with exponential backoff and jitter (or after ``Retry-After``);
any other answer but 200 is permanent. REData answers 502 with "The requested data is currently
unavailable" both when it fails and when a window has no data (a future day, before a series
starts), so such a window fails after the last attempt, a few seconds later.
"""

import json
import urllib.parse

from ibergrid import _http
from ibergrid.settings import RedataSettings, base_url
from ibergrid.sources.redata.datasets import Chunk


class RedataError(Exception):
    """REData refused a request or never answered it, or its answer cannot be stored."""


class RedataClient(_http.PoliteClient):
    error = RedataError
    source = "redata"
    accept = "application/json"

    @classmethod
    def from_settings(cls, settings: RedataSettings) -> "RedataClient":
        return cls(base_url(settings.base_url), timeout=settings.timeout, max_rps=settings.max_rps)

    def url(self, chunk: Chunk) -> str:
        # /en/: English series titles; the series ids are the same in every language.
        query = urllib.parse.urlencode(chunk.params())
        return f"{self.base}/en/datos/{chunk.dataset.widget}?{query}"

    def fetch(self, chunk: Chunk) -> bytes:
        """The response body of a 200, or ``RedataError`` naming the widget and the parameters."""
        what = f"REData {chunk.dataset.widget} {chunk.params()}"
        status, body = self.get(self.url(chunk), what, widget=chunk.dataset.widget)
        if status != 200:
            failure = f"HTTP {status}: {self.detail(body)}"
            raise RedataError(f"{what}: {failure} (permanent, not retried)")
        return body

    def detail(self, body: bytes) -> str:
        """REData's own message (``errors[0].detail``), or the start of the body."""
        try:
            return str(json.loads(body)["errors"][0]["detail"])
        except (ValueError, KeyError, IndexError, TypeError):
            return super().detail(body)
