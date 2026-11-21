"""OMIE's daily ``marginalpdbc`` files over ``ibergrid._http``: paced, with bounded retries.

A day's file is ``marginalpdbc_<YYYYMMDD>.<version>``. OMIE keeps one file per day and a
republication replaces it under the next version (``.1`` almost always; ``.2`` and ``.3`` seen,
ADR 0004), so ``fetch`` asks for ``.1``, then ``.2`` and so on up to ``MAX_VERSION``, and takes the
first that exists. OMIE answers 404 (an HTML page) for a name it does not have: a day whose
results are not out yet answers 404 for every version. Whether that is "not yet" or an error
depends on the day, so ``ingest`` decides. Timeouts, 429 and 5xx are retried
(``ibergrid._http.PoliteClient``); redirects are followed (``urllib``'s default).
"""

from dataclasses import dataclass
from datetime import date

from ibergrid import _http
from ibergrid.settings import OmieSettings, base_url

MAX_VERSION = 5  # the highest seen is 3 (2023-01-01..2026-11-21)


class OmieError(Exception):
    """OMIE refused a request or never answered it, or a file cannot be stored."""


@dataclass(frozen=True)
class OmieFile:
    day: date
    version: int
    payload: bytes

    @property
    def name(self) -> str:
        return file_name(self.day, self.version)


class OmieClient(_http.PoliteClient):
    error = OmieError
    source = "omie"

    @classmethod
    def from_settings(cls, settings: OmieSettings) -> "OmieClient":
        return cls(base_url(settings.base_url), timeout=settings.timeout, max_rps=settings.max_rps)

    def url(self, day: date, version: int = 1) -> str:
        name = file_name(day, version)
        return f"{self.base}/es/file-download?parents=marginalpdbc&filename={name}"

    def fetch(self, day: date) -> OmieFile | None:
        """The day's file under its first version that exists, ``None`` if OMIE answers 404 for
        ``.1`` to ``.MAX_VERSION`` (each a request), or ``OmieError``."""
        for version in range(1, MAX_VERSION + 1):
            name = file_name(day, version)
            status, body = self.get(self.url(day, version), f"OMIE {name}", file=name)
            if status == 200:
                return OmieFile(day, version, body)
            if status != 404:
                raise OmieError(
                    f"OMIE {name}: HTTP {status}: {self.detail(body)} (permanent, not retried)"
                )
        return None

    def detail(self, body: bytes) -> str:
        """OMIE's error answers are HTML pages: their start, on one line."""
        return " ".join(super().detail(body).split())


def file_name(day: date, version: int = 1) -> str:
    """OMIE's name for version ``version`` of delivery day ``day``'s marginal prices."""
    return f"marginalpdbc_{day:%Y%m%d}.{version}"


def all_versions(day: date) -> str:
    """The names ``fetch`` tries, for messages."""
    return f"{file_name(day)} to .{MAX_VERSION}"
