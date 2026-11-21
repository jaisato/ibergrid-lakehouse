"""Heartbeats of the long-running stream services, the check their healthchecks run, and the
watchdog that ends a stuck process.

Each service rewrites its heartbeat file (a JSON object with its latest counters) whenever its
loop goes round, with ``failing_since`` set while its iterations keep failing (a catalog down, a
broker unreachable or refusing the password, dead letters refused) and ``refused`` while the
consumer refuses to start (a topic that changed under its checkpoint: only an operator moves on).
It is healthy while the file is fresh, nothing was refused, and it has not been failing for longer
than ``--max-failing``. The compose healthcheck runs inside the service's own
cgroup every 30 s, so it runs this module (``python -m ibergrid.stream.health``, standard library
only) instead of the full CLI, which costs 0.9 s of CPU and 133 MiB of memory per start in the app
image; ``ibergrid stream health`` runs the same check.

Docker restarts exited containers, not unhealthy ones, and a request that never returns (PyIceberg
0.12's REST client has no timeout) stops a loop without ending its process: the ``Watchdog`` ends
it with exit code 70 when the loop has not gone round for its timeout.

Standard library only: ``ibergrid.settings`` imports this module.
"""

import argparse
import json
import os
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path

HEARTBEAT_PATH = "/tmp/ibergrid-stream.heartbeat"  # noqa: S108 (a container's own /tmp)
MAX_AGE = 120.0
MAX_FAILING = 600.0
WATCHDOG_EXIT = 70


def beat(path: str | Path, **stats: object) -> None:
    """Replace the heartbeat atomically, so a check never reads half a file."""
    target = Path(path)
    payload = json.dumps({"at": time.time(), **stats}, default=str)
    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    with os.fdopen(fd, "w") as out:
        out.write(payload)
    os.replace(tmp, target)


def check(
    path: str | Path, max_age: float, max_failing: float = MAX_FAILING, *, now: float | None = None
) -> tuple[bool, str]:
    """Whether the heartbeat is at most ``max_age`` seconds old and its loop has not been failing
    for more than ``max_failing`` seconds, and what it says."""
    now = time.time() if now is None else now
    try:
        content = Path(path).read_text()
        payload = json.loads(content)
        age = now - float(payload["at"])
        failing = payload.get("failing_since")
        failing_for = None if failing is None else now - float(failing)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        return False, f"no usable heartbeat at {path}: {exc}"
    if age > max_age:
        return False, f"heartbeat {age:.0f} s old (limit {max_age:.0f} s): {content}"
    if payload.get("refused"):
        return False, f"refused to start: {content}"
    if failing_for is not None and failing_for > max_failing:
        return False, f"failing for {failing_for:.0f} s (limit {max_failing:.0f} s): {content}"
    return True, f"heartbeat {age:.0f} s old: {content}"


class Watchdog:
    """Ends the process unless ``touch`` is called at least every ``timeout`` seconds."""

    def __init__(
        self,
        timeout: float,
        *,
        exit: Callable[[int], object] = os._exit,
        every: float | None = None,
    ) -> None:
        self.timeout = timeout
        self._exit = exit
        self._every = min(30.0, timeout / 4) if every is None else every
        self._last = time.monotonic()

    def touch(self) -> None:
        self._last = time.monotonic()

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._watch, name="watchdog", daemon=True)
        thread.start()
        return thread

    def _watch(self) -> None:
        while True:
            time.sleep(self._every)
            stuck = time.monotonic() - self._last
            if stuck > self.timeout:
                event = {"event": "stream.watchdog.exit", "level": "error", "stuck": round(stuck)}
                print(json.dumps(event), file=sys.stderr, flush=True)
                self._exit(WATCHDOG_EXIT)
                return


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exit 0 while the heartbeat is fresh and fine.")
    parser.add_argument("--path", default=HEARTBEAT_PATH)
    parser.add_argument("--max-age", type=float, default=MAX_AGE, help="seconds")
    parser.add_argument("--max-failing", type=float, default=MAX_FAILING, help="seconds")
    args = parser.parse_args(argv)
    ok, detail = check(args.path, args.max_age, args.max_failing)
    print(detail, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
