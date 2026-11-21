"""Serving schemas, query store and rate limiter for IberGrid Lakehouse (ADR 0010).

Note: ``ibergrid.api.app`` (which imports FastAPI) is intentionally NOT imported here so that
``ibergrid.portal`` and ``ibergrid.cli`` remain importable inside the lean ``ibergrid-app``
container image where the optional ``api`` dependency group is not installed.
"""

from ibergrid.api.rate_limit import RateLimitDecision, SlidingWindowRateLimiter
from ibergrid.api.schemas import (
    ATTRIBUTION_MIBEL_AND_REE,
    ATTRIBUTION_REE_ONLY,
    NON_COMMERCIAL_NOTICE,
    HealthResponse,
    MarketDailyResponse,
    MarketHourlyResponse,
    MetadataTablesResponse,
    OptimalWindowsResponse,
    RealtimeDemandResponse,
    SystemDailyResponse,
)
from ibergrid.api.store import LakeQueryStore

__all__ = [
    "ATTRIBUTION_MIBEL_AND_REE",
    "ATTRIBUTION_REE_ONLY",
    "NON_COMMERCIAL_NOTICE",
    "HealthResponse",
    "LakeQueryStore",
    "MarketDailyResponse",
    "MarketHourlyResponse",
    "MetadataTablesResponse",
    "OptimalWindowsResponse",
    "RateLimitDecision",
    "RealtimeDemandResponse",
    "SlidingWindowRateLimiter",
    "SystemDailyResponse",
]
