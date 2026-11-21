"""Record the REData answers the tests replay: `uv run python scripts/record_redata_fixtures.py`.

It asks the live API through the connector's own client (one request a second), keeps only the
series listed for the large answers, and writes compact JSON to
tests/fixtures/redata/<dataset>-<system>-<start>_<end>.json. Arguments, if any, are file names
to record again (the rest are left as they are). The answers are public data; no credential is
involved. Recorded on 2026-11-21.
"""

import json
import sys
import time
from datetime import date
from pathlib import Path

from ibergrid import _http
from ibergrid.sources.redata.client import RedataClient
from ibergrid.sources.redata.datasets import DATASETS, Chunk

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "redata"
BASE = "https://apidatos.ree.es"
REAL, FORECASTED, TOTAL_SCHEDULED, GAS_TURBINE = "2037", "2052", "2054", "1450"

# (dataset, system, start, end, series kept (None: all), why the tests need it)
FIXTURES = [
    ("demand_realtime", "national", "2026-11-18", "2026-11-18", None, "with Total scheduled"),
    ("demand_realtime", "national", "2023-06-15", "2023-06-15", (REAL, FORECASTED), "without"),
    ("demand_realtime", "national", "2026-03-29", "2026-03-29", (REAL,), "23 hours: 276 points"),
    ("demand_realtime", "national", "2026-10-25", "2026-10-25", (REAL,), "25 hours: 300 points"),
    ("demand_hourly", "peninsular", "2026-11-18", "2026-11-18", None, "a normal day"),
    ("demand_hourly", "canarias", "2026-11-18", "2026-11-18", None, "a non-peninsular system"),
    ("demand_hourly", "peninsular", "2026-03-29", "2026-03-29", None, "23 hours"),
    ("demand_hourly", "peninsular", "2026-10-25", "2026-10-25", None, "25 hours"),
    ("generation_daily", "national", "2026-11-18", "2026-11-18", None, "no geo parameters"),
    ("generation_daily", "canarias", "2026-11-18", "2026-11-18", None, "its own technologies"),
    # A long daily window ending mid-month comes back up to the month's end for some series
    # (gas turbine among them); the two windows plan() makes instead come back exact.
    ("generation_daily", "canarias", "2025-02-10", "2025-10-20", (GAS_TURBINE,), "leaks"),
    ("generation_daily", "canarias", "2025-02-10", "2025-09-30", (GAS_TURBINE,), "exact"),
    ("generation_daily", "canarias", "2025-10-01", "2025-10-20", (GAS_TURBINE,), "exact"),
    ("market_prices", "peninsular", "2026-11-18", "2026-11-18", None, "96 spot, 24 PVPC"),
    ("market_prices", "peninsular", "2026-03-29", "2026-03-29", None, "92 and 23"),
    ("market_prices", "peninsular", "2026-10-25", "2026-10-25", None, "100 and 25"),
    ("market_prices", "peninsular", "2024-12-15", "2024-12-15", None, "spot still hourly"),
    # Days OMIE's Spanish and Portuguese prices differ: the cross-source check sees a column swap.
    ("market_prices", "peninsular", "2025-04-28", "2025-04-28", None, "blackout, decoupled"),
    ("market_prices", "peninsular", "2025-10-01", "2025-10-01", None, "first 15-minute day"),
]


def main(only: set[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    client = RedataClient(BASE, timeout=120)
    for dataset_name, system_name, start, end, keep, _why in FIXTURES:
        name = f"{dataset_name}-{system_name}-{start}_{end}.json"
        if only and name not in only:
            continue
        dataset = DATASETS[dataset_name]
        [system] = [s for s in dataset.systems if s.name == system_name]
        chunk = Chunk(dataset, system, date.fromisoformat(start), date.fromisoformat(end))
        document = json.loads(client.fetch(chunk))
        ids = [item["id"] for item in document["included"]]
        if start == "2023-06-15" and TOTAL_SCHEDULED in ids:  # the drift the tests rely on
            raise SystemExit(f"{start} now carries Total scheduled: {ids}")
        if keep is not None:
            document["included"] = [item for item in document["included"] if item["id"] in keep]
        _write(name, document)
    if only and "error-400.json" not in only:
        return
    # A widget asked without the parameters it needs: 400 and REData's error body.
    query = "start_date=2026-11-18T00:00&end_date=2026-11-18T23:59&time_trunc=hour"
    time.sleep(1)  # the client's pace, for this one request outside it
    status, body = _http.send("GET", f"{BASE}/en/datos/demanda/evolucion?{query}", timeout=60)
    if status != 400:
        raise SystemExit(f"expected 400 without geo parameters, got {status}")
    _write("error-400.json", json.loads(body))


def _write(name: str, document: object) -> None:
    text = json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n"
    (OUT / name).write_text(text, encoding="utf-8")
    print(f"{name}: {len(text)} bytes")


if __name__ == "__main__":
    main(set(sys.argv[1:]))
