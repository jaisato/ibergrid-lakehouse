"""OMIE's daily ``marginalpdbc`` files over ``ibergrid._http``: paced, with bounded retries.

Timeouts, 429 and 5xx are retried (``ibergrid._http.PoliteClient``); a 404 is its own outcome,
``None``, because OMIE answers 404 (an HTML page) for a file it has not published, a delivery day
whose results are not out yet as well as one in the far future (measured on 2026-11-21). Whether a
404 is "not yet" or an error depends on the day, so ``ingest`` decides. Redirects are followed
(``urllib``'s default).
"""

from datetime import date

from ibergrid import _http
from ibergrid.settings import OmieSettings, base_url


class OmieError(Exception):
    """OMIE refused a request or never answered it, or a file cannot be stored."""


class OmieClient(_http.PoliteClient):
    error = OmieError
    source = "omie"

    @classmethod
    def from_settings(cls, settings: OmieSettings) -> "OmieClient":
        return cls(base_url(settings.base_url), timeout=settings.timeout, max_rps=settings.max_rps)

    def url(self, day: date) -> str:
        return f"{self.base}/es/file-download?parents=marginalpdbc&filename={file_name(day)}"

    def fetch(self, day: date) -> bytes | None:
        """The file's bytes, ``None`` if OMIE has no file for ``day`` (404), or ``OmieError``."""
        what = f"OMIE {file_name(day)}"
        status, body = self.get(self.url(day), what)
        if status == 404:
            return None
        if status != 200:
            raise OmieError(f"{what}: HTTP {status}: {self.detail(body)} (permanent, not retried)")
        return body

    def detail(self, body: bytes) -> str:
        """OMIE's error answers are HTML pages: their start, on one line."""
        return " ".join(super().detail(body).split())


def file_name(day: date) -> str:
    """OMIE's name for the day-ahead marginal prices of delivery day ``day``."""
    return f"marginalpdbc_{day:%Y%m%d}.1"
