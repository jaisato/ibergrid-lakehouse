"""Thread-safe sliding-window per-IP rate limiter for FastAPI (ADR 0010)."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

MAX_TRACKED_CLIENTS = 10_000


@dataclass(frozen=True)
class RateLimitDecision:
    """Result of checking one client request against its sliding-window budget."""

    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int


class SlidingWindowRateLimiter:
    """In-memory sliding-window per-key rate limiter with bounded client cardinality."""

    def __init__(
        self,
        *,
        limit: int,
        window_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
        max_clients: int = MAX_TRACKED_CLIENTS,
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if max_clients < 1:
            raise ValueError("max_clients must be >= 1")
        self._limit = limit
        self._window = window_seconds
        self._clock = clock or time.monotonic
        self._max_clients = max_clients
        self._lock = threading.Lock()
        self._buckets: dict[str, deque[float]] = {}

    @property
    def limit(self) -> int:
        return self._limit

    def check(self, client_key: str) -> RateLimitDecision:
        """Consume one token for ``client_key`` if within budget, or compute retry delay."""
        now = self._clock()
        cutoff = now - self._window
        with self._lock:
            bucket = self._buckets.get(client_key)
            if bucket is None:
                if len(self._buckets) >= self._max_clients:
                    self._evict_stale(cutoff)
                bucket = deque()
                self._buckets[client_key] = bucket

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= self._limit:
                oldest = bucket[0]
                retry_after = max(1, math.ceil(oldest + self._window - now))
                return RateLimitDecision(
                    allowed=False,
                    limit=self._limit,
                    remaining=0,
                    retry_after_seconds=retry_after,
                )

            bucket.append(now)
            remaining = max(0, self._limit - len(bucket))
            return RateLimitDecision(
                allowed=True,
                limit=self._limit,
                remaining=remaining,
                retry_after_seconds=0,
            )

    def _evict_stale(self, cutoff: float) -> None:
        stale = [key for key, q in self._buckets.items() if not q or q[-1] <= cutoff]
        for key in stale:
            del self._buckets[key]
        if len(self._buckets) >= self._max_clients:
            # Drop the oldest-seen client entry to keep memory strictly bounded under IP floods.
            oldest_key = min(
                self._buckets,
                key=lambda k: self._buckets[k][-1] if self._buckets[k] else -math.inf,
            )
            del self._buckets[oldest_key]
