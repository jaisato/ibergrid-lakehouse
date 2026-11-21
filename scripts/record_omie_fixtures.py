"""Record the OMIE files the tests replay: `uv run python scripts/record_omie_fixtures.py`.

It downloads each day's marginalpdbc file through the connector's own client (one request a
second), checks it against its contract and writes the bytes unchanged (Latin-1, CRLF) to
tests/fixtures/omie/marginalpdbc_<YYYYMMDD>.1. Arguments, if any, are ISO days to record again
(the rest are left as they are). The files are public data; no credential is involved. Recorded
on 2026-11-21.
"""

import sys
from datetime import date
from pathlib import Path

from ibergrid.sources.omie.client import OmieClient, file_name
from ibergrid.sources.omie.marginalpdbc import parse

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "omie"

# (delivery day, why the tests need it)
FIXTURES = [
    ("2024-03-31", "v1, spring forward: 23 hours"),
    ("2024-10-27", "v1, fall back: 25 hours"),
    ("2025-04-28", "v1, the blackout day: Spain and Portugal decoupled"),
    ("2025-09-30", "v1, the last hourly day"),
    ("2025-10-01", "v2, the first quarter-hourly day: 96"),
    ("2025-10-26", "v2, fall back: 100"),
    ("2026-03-29", "v2, spring forward: 92"),
    ("2026-11-18", "v2, a recent normal day (REData's fixtures have the same day)"),
]


def main(only: set[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    client = OmieClient("https://www.omie.es", timeout=60)
    for iso, _why in FIXTURES:
        if only and iso not in only:
            continue
        day = date.fromisoformat(iso)
        payload = client.fetch(day)
        if payload is None:
            raise SystemExit(f"OMIE has no {file_name(day)}")
        day_file = parse(payload, day)  # refuses a file that breaks its contract
        (OUT / file_name(day)).write_bytes(payload)
        print(f"{file_name(day)}: v{day_file.contract.version}, {len(day_file.periods)} periods")


if __name__ == "__main__":
    main(set(sys.argv[1:]))
