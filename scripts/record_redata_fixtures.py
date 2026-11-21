"""Record the REData answers the tests replay: `uv run python scripts/record_redata_fixtures.py`.

It asks the live API through the connector's own client (one request a second), keeps only the
series listed for the large five-minute answers, and writes compact JSON to
tests/fixtures/redata/<dataset>-<system>-<start>_<end>.json. The answers are public data; no
credential is involved. Recorded on 2026-11-21.
"""

import json
import time
from datetime import date
from pathlib import Path

from ibergrid import _http
from ibergrid.sources.redata.client import RedataClient
from ibergrid.sources.redata.datasets import DATASETS, Chunk

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "redata"
BASE = "https://apidatos.ree.es"
REAL, FORECASTED, TOTAL_SCHEDULED = "2037", "2052", "2054"

# (dataset, system, day, series kept (None: all), why the tests need it)
FIXTURES = [
    ("demand_realtime", "peninsular", "2026-11-18", None, "a normal day, with Total scheduled"),
    ("demand_realtime", "peninsular", "2023-06-15", (REAL, FORECASTED), "before Total scheduled"),
    ("demand_realtime", "peninsular", "2026-03-29", (REAL,), "23-hour day: 276 points"),
    ("demand_realtime", "peninsular", "2026-10-25", (REAL,), "25-hour day: 300 points"),
    ("demand_hourly", "peninsular", "2026-11-18", None, "a normal day"),
    ("demand_hourly", "canarias", "2026-11-18", None, "a non-peninsular system"),
    ("demand_hourly", "peninsular", "2026-03-29", None, "23 hours"),
    ("demand_hourly", "peninsular", "2026-10-25", None, "25 hours"),
    ("generation_daily", "national", "2026-11-18", None, "no geo parameters"),
    ("generation_daily", "canarias", "2026-11-18", None, "its own technologies"),
    ("market_prices", "peninsular", "2026-11-18", None, "96 spot quarter-hours, 24 PVPC hours"),
    ("market_prices", "peninsular", "2026-03-29", None, "92 and 23"),
    ("market_prices", "peninsular", "2026-10-25", None, "100 and 25"),
    ("market_prices", "peninsular", "2024-12-15", None, "spot still hourly"),
]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    client = RedataClient(BASE, timeout=120)
    for dataset_name, system_name, day, keep, _why in FIXTURES:
        dataset = DATASETS[dataset_name]
        [system] = [s for s in dataset.systems if s.name == system_name]
        chunk = Chunk(dataset, system, date.fromisoformat(day), date.fromisoformat(day))
        document = json.loads(client.fetch(chunk))
        ids = [item["id"] for item in document["included"]]
        if day == "2023-06-15" and TOTAL_SCHEDULED in ids:  # the drift the tests rely on
            raise SystemExit(f"{day} now carries Total scheduled: {ids}")
        if keep is not None:
            document["included"] = [item for item in document["included"] if item["id"] in keep]
        _write(f"{dataset_name}-{system_name}-{day}_{day}.json", document)
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
    main()
