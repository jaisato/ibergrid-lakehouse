"""Heartbeats of the long-running stream services, and the check their healthchecks run.

Each service rewrites its heartbeat file (a JSON object with its latest counters) whenever its
loop goes round; it is healthy while the file is fresh. The compose healthcheck runs inside the
service's own cgroup every 30 s, so it runs this module (``python -m ibergrid.stream.health``,
standard library only) instead of the full CLI, which costs 0.9 s of CPU and 133 MiB of memory
per start in the app image; ``ibergrid stream health`` runs the same check.

Standard library only: ``ibergrid.settings`` imports this module.
"""

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

HEARTBEAT_PATH = "/tmp/ibergrid-stream.heartbeat"  # noqa: S108 (a container's own /tmp)
MAX_AGE = 120.0


def beat(path: str | Path, **stats: object) -> None:
    """Replace the heartbeat atomically, so a check never reads half a file."""
    target = Path(path)
    payload = json.dumps({"at": time.time(), **stats}, default=str)
    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    with os.fdopen(fd, "w") as out:
        out.write(payload)
    os.replace(tmp, target)


def check(path: str | Path, max_age: float, *, now: float | None = None) -> tuple[bool, str]:
    """Whether the heartbeat is at most ``max_age`` seconds old, and what it says."""
    try:
        content = Path(path).read_text()
        age = (time.time() if now is None else now) - float(json.loads(content)["at"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return False, f"no usable heartbeat at {path}: {exc}"
    if age > max_age:
        return False, f"heartbeat {age:.0f} s old (limit {max_age:.0f} s): {content}"
    return True, f"heartbeat {age:.0f} s old: {content}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Exit 0 while the heartbeat is fresh.")
    parser.add_argument("--path", default=HEARTBEAT_PATH)
    parser.add_argument("--max-age", type=float, default=MAX_AGE, help="seconds")
    args = parser.parse_args(argv)
    ok, detail = check(args.path, args.max_age)
    print(detail, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
