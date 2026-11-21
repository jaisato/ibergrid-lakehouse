"""Statistical timing and memory helpers for the IberGrid benchmark suite (ADR 0009)."""

from __future__ import annotations

import math
import resource
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LatencySummary:
    """Distribution summary across repeated warm benchmark iterations."""

    warmup: int
    iterations: int
    min_ms: float
    p50_ms: float
    p95_ms: float
    mean_ms: float
    max_ms: float
    peak_rss_mib: float


def percentile(values: Sequence[float], p: float) -> float:
    """Linear-interpolated percentile ``p`` in ``[0, 100]`` over a non-empty sequence."""
    if not values:
        raise ValueError("percentile requires at least one measurement")
    if not (0.0 <= p <= 100.0):
        raise ValueError(f"percentile p must be in [0, 100], got {p}")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (p / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(ordered[low])
    weight = rank - low
    return float(ordered[low] + weight * (ordered[high] - ordered[low]))


def current_peak_rss_mib() -> float:
    """Process peak resident set size in MiB.

    On Linux, reads ``VmHWM`` from ``/proc/self/status`` (the peak RSS of the current ``mm_struct``
    after ``execve``) because ``getrusage(RUSAGE_SELF).ru_maxrss`` inherits the parent's
    ``signal->maxrss`` across ``fork()`` + ``execve()``. Falls back to ``ru_maxrss`` elsewhere.
    """
    if sys.platform.startswith("linux"):
        try:
            for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024.0, 2)
        except OSError:
            pass
    raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return round(raw / (1024.0 * 1024.0), 2)
    return round(raw / 1024.0, 2)


def summarize_timings(
    samples_ms: Sequence[float],
    *,
    warmup: int,
    peak_rss_mib: float | None = None,
) -> LatencySummary:
    """Build a :class:`LatencySummary` from millisecond timing samples."""
    if not samples_ms:
        raise ValueError("summarize_timings requires at least one sample")
    if warmup < 0:
        raise ValueError(f"warmup must be >= 0, got {warmup}")
    rss = current_peak_rss_mib() if peak_rss_mib is None else round(peak_rss_mib, 2)
    return LatencySummary(
        warmup=warmup,
        iterations=len(samples_ms),
        min_ms=round(min(samples_ms), 3),
        p50_ms=round(percentile(samples_ms, 50.0), 3),
        p95_ms=round(percentile(samples_ms, 95.0), 3),
        mean_ms=round(sum(samples_ms) / len(samples_ms), 3),
        max_ms=round(max(samples_ms), 3),
        peak_rss_mib=rss,
    )


def time_callable[T](
    fn: Callable[[], T],
    *,
    warmup: int = 2,
    iterations: int = 10,
) -> tuple[T, LatencySummary]:
    """Run ``warmup`` untimed calls followed by ``iterations`` timed calls of ``fn``."""
    if warmup < 0:
        raise ValueError(f"warmup must be >= 0, got {warmup}")
    if iterations < 1:
        raise ValueError(f"iterations must be >= 1, got {iterations}")

    for _ in range(warmup):
        fn()

    samples_ms: list[float] = []
    last_result: T | None = None
    for _ in range(iterations):
        started = time.perf_counter()
        last_result = fn()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        samples_ms.append(elapsed_ms)

    assert last_result is not None  # noqa: S101 - iterations >= 1 guarantees assignment
    return last_result, summarize_timings(samples_ms, warmup=warmup)
