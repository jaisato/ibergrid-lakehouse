"""Reproducible Data Chaos Proof (`make chaos-data`, ADR 0008).

Runs in a throwaway compose project (`ibergrid-chaos-data`, its own volumes and subnets
`10.214.0.0/16`, removed in `finally:`) so it NEVER touches the real `ibergrid` stack or backfilled
volumes. Seeds a clean multi-day baseline covering normal days, a 23-hour DST spring-forward day
(2025-03-30), the real decoupled Iberian blackout day (2025-04-28: ES 47.94 vs PT 40.70 EUR/MWh),
a 25-hour DST fall-back day (2025-10-26), and Peninsular + Canarias systems.

Then injects 5 deterministic data-corruption scenarios into Iceberg WAP audit branches:
1. Duplicate keys in `bronze.omie_day_ahead_prices` -> blocked by Soda `duplicate` gate.
2. OMIE ES/PT column swap on 2025-04-28 in `silver.mibel_prices_hourly` -> caught by cross-source
   REData spot reconciliation gate (`abs(diff) > 0.02 EUR/MWh`).
3. Peninsular / Canarias scope mix & unverified Canarias timestamp in `silver.demand_system_hourly`
   -> caught by peninsular isolation and Canarias verification gates.
4. Missing DST hour (22h on 23h day 2025-03-30) in `gold.mibel_hourly_market` -> caught by DST
   continuity gate.
5. Out-of-range price (`9999.0 EUR/MWh`) in `bronze.omie_day_ahead_prices` -> caught by zero-scan
   Iceberg metadata bounds check (`data_bytes_scanned == 0`).

Proves 100 % WAP blocking before `wap.publish`, `main` snapshot unchanged on every table, zero
audit branches left behind, and every violation recorded in `quarantine.rejected_records`.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
PROJECT = "ibergrid-chaos-data"
NETWORKS = ("storage", "catalog", "broker", "catalog-db", "orchestration", "orchestration-db")
SUBNETS = "networks:\n" + "".join(
    f"  {name}:\n    ipam:\n      config:\n        - subnet: 10.214.{n}.0/24\n"
    for n, name in enumerate(NETWORKS, start=1)
)

# Executed inside the `quality` container (orchestration image: PyIceberg, DuckDB, Soda v4,
# /opt/ibergrid/contracts/soda). Seeds clean baseline tables, runs all 5 chaos scenarios on real
# WAP audit branches, and prints a structured JSON summary.
CHAOS_RUNNER = r"""
import json
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa

from ibergrid.lake import duck, registry, wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.quality import (
    QualityGateError,
    audit_branch_and_quarantine,
    find_contracts_dir,
)
from ibergrid.settings import Settings

MADRID = ZoneInfo("Europe/Madrid")
NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
ATTRIBUTION = "Source: OMIE (omie.es) & Red Eléctrica (REData) — non-commercial portfolio project"


def madrid_day_hours(day: date) -> list[datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=MADRID).astimezone(UTC)
    end = datetime(
        (day + timedelta(days=1)).year,
        (day + timedelta(days=1)).month,
        (day + timedelta(days=1)).day,
        tzinfo=MADRID,
    ).astimezone(UTC)
    out = []
    cur = start
    while cur < end:
        out.append(cur)
        cur += timedelta(hours=1)
    return out


def make_omie_rows(days: list[date]) -> pa.Table:
    cols: dict[str, list] = {f.name: [] for f in registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow()}
    for day in days:
        hours = madrid_day_hours(day)
        for idx, start_utc in enumerate(hours, start=1):
            es = 47.94 if (day == date(2025, 4, 28) and idx == 8) else 50.0 + idx
            pt = 40.70 if (day == date(2025, 4, 28) and idx == 8) else es
            cols["delivery_date"].append(day)
            cols["period"].append(idx)
            cols["resolution_minutes"].append(60)
            cols["interval_start_utc"].append(start_utc)
            cols["interval_end_utc"].append(start_utc + timedelta(hours=1))
            cols["price_es_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(pt)
            cols["contract_version"].append(1)
            cols["source_file"].append(f"marginalpdbc_{day:%Y%m%d}.1")
            cols["file_version"].append(1)
            cols["content_sha256"].append("a" * 64)
            cols["fetched_at"].append(NOW)
            cols["run_id"].append("seed")
            cols["landing_path"].append(f"landing/omie/{day:%Y%m%d}.1")
    return pa.table(cols, schema=registry.OMIE_DAY_AHEAD_PRICES.schema.as_arrow())


def make_mibel_hourly_rows(days: list[date]) -> pa.Table:
    schema = registry.SILVER_MIBEL_PRICES_HOURLY.schema.as_arrow()
    cols: dict[str, list] = {f.name: [] for f in schema}
    for day in days:
        for idx, start_utc in enumerate(madrid_day_hours(day), start=1):
            local = start_utc.astimezone(MADRID)
            es = 47.94 if (day == date(2025, 4, 28) and idx == 8) else 50.0 + idx
            pt = 40.70 if (day == date(2025, 4, 28) and idx == 8) else es
            cols["hour_utc"].append(start_utc)
            cols["delivery_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["contract_version"].append(1)
            cols["periods_in_hour"].append(1)
            cols["price_es_eur_mwh"].append(es)
            cols["price_es_min_eur_mwh"].append(es)
            cols["price_es_max_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(pt)
            cols["spread_es_minus_pt_eur_mwh"].append(round(es - pt, 4))
            cols["is_decoupled"].append(abs(es - pt) > 0.01)
            cols["is_zero_or_negative_es"].append(es <= 0.0)
            cols["pvpc_eur_mwh"].append(es + 25.0)
            cols["redata_spot_eur_mwh"].append(es)
            cols["omie_redata_diff_eur_mwh"].append(0.0)
    return pa.table(cols, schema=schema)


def make_demand_system_rows(day: date) -> pa.Table:
    schema = registry.SILVER_DEMAND_SYSTEM_HOURLY.schema.as_arrow()
    cols: dict[str, list] = {f.name: [] for f in schema}
    for sys_name, scope, verified, base_mw in (
        ("peninsular", "peninsular", True, 25000.0),
        ("canarias", "non_peninsular", False, 1100.0),
    ):
        for start_utc in madrid_day_hours(day):
            local = start_utc.astimezone(MADRID)
            cols["ts_utc"].append(start_utc)
            cols["madrid_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["electric_system"].append(sys_name)
            cols["system_scope"].append(scope)
            cols["ts_utc_verified"].append(verified)
            cols["demand_mw"].append(base_mw + local.hour * 100.0)
            cols["series_last_update_utc"].append(NOW)
    return pa.table(cols, schema=schema)


def make_gold_market_rows(days: list[date]) -> pa.Table:
    schema = registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow()
    cols: dict[str, list] = {f.name: [] for f in schema}
    for day in days:
        day_type = "weekend" if day.weekday() >= 5 else "weekday"
        for idx, start_utc in enumerate(madrid_day_hours(day), start=1):
            local = start_utc.astimezone(MADRID)
            es = 47.94 if (day == date(2025, 4, 28) and idx == 8) else 50.0 + idx
            pt = 40.70 if (day == date(2025, 4, 28) and idx == 8) else es
            cols["hour_utc"].append(start_utc)
            cols["delivery_date"].append(day)
            cols["madrid_hour"].append(local.hour)
            cols["day_type"].append(day_type)
            cols["price_es_eur_mwh"].append(es)
            cols["price_pt_eur_mwh"].append(pt)
            cols["spread_es_minus_pt_eur_mwh"].append(round(es - pt, 4))
            cols["is_decoupled"].append(abs(es - pt) > 0.01)
            cols["is_zero_or_negative_es"].append(es <= 0.0)
            cols["pvpc_eur_mwh"].append(es + 25.0)
            cols["peninsular_demand_mw"].append(25000.0 + local.hour * 100.0)
            cols["demand_ramp_1h_mw"].append(100.0)
            cols["price_ramp_1h_eur_mwh"].append(1.0)
            cols["intraday_price_rank"].append(idx)
            cols["is_evening_ramp_window"].append(18 <= local.hour <= 21)
            cols["peninsular_renewable_share_pct"].append(58.0)
            cols["peninsular_solar_share_pct"].append(22.0)
            cols["peninsular_wind_share_pct"].append(24.0)
            cols["source_attribution"].append(ATTRIBUTION)
            cols["updated_at_utc"].append(NOW)
    return pa.table(cols, schema=schema)


def publish_seed(table, rows: pa.Table, run_id: str) -> int:
    audit = wap.start(table, run_id)
    head = wap.write(table, audit, rows)
    published = wap.publish(table, audit, head)
    assert published is not None
    return published


settings = Settings()
catalog = rest_catalog(settings)
con = duck.connect(settings)
contracts_dir = find_contracts_dir(settings)

days = [date(2025, 3, 30), date(2025, 4, 28), date(2025, 10, 26)]
t_omie = catalog.load_table(registry.OMIE_DAY_AHEAD_PRICES.identifier)
t_hourly = catalog.load_table(registry.SILVER_MIBEL_PRICES_HOURLY.identifier)
t_demand = catalog.load_table(registry.SILVER_DEMAND_SYSTEM_HOURLY.identifier)
t_gold = catalog.load_table(registry.GOLD_MIBEL_HOURLY_MARKET.identifier)

omie_rows = make_omie_rows(days)
hourly_rows = make_mibel_hourly_rows(days)
demand_rows = make_demand_system_rows(date(2025, 4, 28))
gold_rows = make_gold_market_rows(days)

seed_snaps = {
    "bronze.omie_day_ahead_prices": publish_seed(t_omie, omie_rows, "seed-omie"),
    "silver.mibel_prices_hourly": publish_seed(t_hourly, hourly_rows, "seed-hourly"),
    "silver.demand_system_hourly": publish_seed(t_demand, demand_rows, "seed-demand"),
    "gold.mibel_hourly_market": publish_seed(t_gold, gold_rows, "seed-gold"),
}

# Verify clean baseline passes 100 % including verified decoupled blackout hour.
baseline_audit = wap.start(t_hourly, "baseline-check")
baseline_report = audit_branch_and_quarantine(
    catalog,
    t_hourly,
    baseline_audit,
    table_name="silver.mibel_prices_hourly",
    con=con,
    table_ref=duck.table_name("silver.mibel_prices_hourly"),
    contracts_dir=contracts_dir,
    run_id="baseline-check",
)
wap.discard(t_hourly, baseline_audit)
assert baseline_report.passed, baseline_report.violations
assert baseline_report.decoupled_periods_verified == 1

scenarios = []


def run_chaos(
    name: str,
    table,
    table_name: str,
    bad_rows: pa.Table,
    *,
    overwrite: bool = False,
) -> None:
    before_snap = table.metadata.current_snapshot_id
    run_id = f"chaos-{name}"
    audit = wap.start(table, run_id)
    if overwrite:
        from pyiceberg.expressions import AlwaysTrue
        wap.write(table, audit, bad_rows, overwrite_filter=AlwaysTrue())
    else:
        wap.write(table, audit, bad_rows)

    blocked = False
    rules = []
    meta_bytes = None
    quarantine_snap = None
    try:
        audit_branch_and_quarantine(
            catalog,
            table,
            audit,
            table_name=table_name,
            con=con,
            table_ref=duck.table_name(table_name),
            contracts_dir=contracts_dir,
            run_id=run_id,
        )
    except QualityGateError as exc:
        blocked = True
        rules = [v.rule_name for v in exc.report.violations]
        meta_bytes = (
            exc.report.metadata_result.summary.data_bytes_scanned
            if exc.report.metadata_result is not None
            else None
        )
        quarantine_snap = exc.report.quarantine_snapshot_id

    table.refresh()
    after_snap = table.metadata.current_snapshot_id
    branches_left = wap.audit_branches(table)
    scenarios.append(
        {
            "scenario": name,
            "table": table_name,
            "blocked": blocked,
            "main_unchanged": before_snap == after_snap == seed_snaps[table_name],
            "audit_branches_left": len(branches_left),
            "rules_triggered": rules,
            "metadata_bytes_scanned": meta_bytes,
            "quarantine_snapshot_id": quarantine_snap,
        }
    )


# 1. Duplicate keys in bronze.omie_day_ahead_prices
run_chaos("duplicate-omie-key", t_omie, "bronze.omie_day_ahead_prices", omie_rows.slice(0, 1))

# 2. OMIE ES/PT column swap on decoupled blackout day 2025-04-28 in silver.mibel_prices_hourly
es_idx = hourly_rows.schema.get_field_index("price_es_eur_mwh")
pt_idx = hourly_rows.schema.get_field_index("price_pt_eur_mwh")
swapped = hourly_rows.set_column(
    es_idx, hourly_rows.schema.field(es_idx), hourly_rows.column("price_pt_eur_mwh")
).set_column(
    pt_idx, hourly_rows.schema.field(pt_idx), hourly_rows.column("price_es_eur_mwh")
)
run_chaos("omie-es-pt-swap", t_hourly, "silver.mibel_prices_hourly", swapped, overwrite=True)

# 3. Peninsular / Canarias scope mix & unverified timestamp breach in silver.demand_system_hourly
scope_idx = demand_rows.schema.get_field_index("system_scope")
ver_idx = demand_rows.schema.get_field_index("ts_utc_verified")
mixed = demand_rows.set_column(
    scope_idx,
    demand_rows.schema.field(scope_idx),
    pa.array(["peninsular"] * demand_rows.num_rows, demand_rows.schema.field(scope_idx).type),
).set_column(
    ver_idx,
    demand_rows.schema.field(ver_idx),
    pa.array([True] * demand_rows.num_rows, demand_rows.schema.field(ver_idx).type),
)
run_chaos("canarias-peninsular-mix", t_demand, "silver.demand_system_hourly", mixed, overwrite=True)

# 4. DST missing hour (drop 1 hour from 23h spring-forward day 2025-03-30 -> 22h)
missing_dst = gold_rows.slice(1)
run_chaos("dst-missing-hour", t_gold, "gold.mibel_hourly_market", missing_dst, overwrite=True)

# 5. Out-of-range MIBEL price (9999.0 EUR/MWh) caught with 0 data bytes scanned by metadata check
price_idx = omie_rows.schema.get_field_index("price_es_eur_mwh")
period_idx = omie_rows.schema.get_field_index("period")
bad_price = omie_rows.slice(0, 1).set_column(
    period_idx, omie_rows.schema.field(period_idx), pa.array([99], pa.int32())
).set_column(
    price_idx, omie_rows.schema.field(price_idx), pa.array([9999.0], pa.float64())
)
run_chaos("zero-scan-price-bounds", t_omie, "bronze.omie_day_ahead_prices", bad_price)

# Query quarantine.rejected_records via DuckDB to verify ACID persistence.
q_rows = con.execute(
    "SELECT source_table, rule_name, count(*) FROM lake.quarantine.rejected_records "
    "GROUP BY source_table, rule_name ORDER BY source_table, rule_name"
).fetchall()
total_quarantined = sum(int(r[2]) for r in q_rows)

print(
    json.dumps(
        {
            "baseline_decoupled_verified": baseline_report.decoupled_periods_verified,
            "scenarios": scenarios,
            "quarantine_summary": [
                {"source_table": r[0], "rule_name": r[1], "records": int(r[2])} for r in q_rows
            ],
            "total_quarantined_records": total_quarantined,
        }
    )
)
"""


class DataChaos:
    def __init__(self, work: Path) -> None:
        (work / "subnets.yaml").write_text(SUBNETS)
        self.base = [
            "docker",
            "compose",
            "-p",
            PROJECT,
            "--env-file",
            str(work / ".env"),
            "-f",
            "compose.yaml",
            "-f",
            str(work / "subnets.yaml"),
            "--profile",
            "local-storage",
        ]

    def compose(self, *args: str) -> str:
        result = subprocess.run(  # noqa: S603 (fixed docker compose command)
            [*self.base, *args], cwd=REPO, capture_output=True, text=True, timeout=600, check=False
        )
        if result.returncode:
            sys.exit(f"docker compose {' '.join(args[:4])} failed:\n{result.stderr[-3000:]}")
        return result.stdout

    def down(self) -> None:
        subprocess.run(  # noqa: S603
            [*self.base, "down", "--volumes", "--remove-orphans"],
            cwd=REPO,
            capture_output=True,
            check=False,
        )


def expect(what: str, actual: object, expected: object) -> None:
    status = "ok  " if actual == expected else "FAIL"
    print(f"{status} {what}: {actual}" + ("" if actual == expected else f" (expected {expected})"))
    if actual != expected:
        raise SystemExit(1)


def main() -> None:
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        subprocess.run([REPO / "scripts" / "gen-env.sh", work / ".env"], check=True)  # noqa: S603
        chaos = DataChaos(work)
        try:
            print(f"chaos-data: starting throwaway project {PROJECT}")
            chaos.compose("up", "-d", "--wait", "catalog-init")
            chaos.compose("run", "--rm", "--no-deps", "doctor", "lake", "init")
            out = chaos.compose(
                "run", "--rm", "--no-deps", "--entrypoint", "python", "quality", "-c", CHAOS_RUNNER
            )
            payload: dict[str, Any] = json.loads(out.strip().splitlines()[-1])

            expect("baseline decoupled hours verified", payload["baseline_decoupled_verified"], 1)
            scenarios: list[dict[str, Any]] = payload["scenarios"]
            expect("scenarios executed", len(scenarios), 5)

            for item in scenarios:
                label = item["scenario"]
                expect(f"{label}: blocked before publish", item["blocked"], True)
                expect(f"{label}: main snapshot unchanged", item["main_unchanged"], True)
                expect(f"{label}: audit branches left", item["audit_branches_left"], 0)
                expect(
                    f"{label}: zero data bytes metadata check", item["metadata_bytes_scanned"], 0
                )
                if item["quarantine_snapshot_id"] is None:
                    sys.exit(f"FAIL {label}: missing quarantine snapshot id")
                print(f"     rules triggered: {', '.join(item['rules_triggered'])}")

            blocked_count = sum(1 for s in scenarios if s["blocked"])
            expect("WAP blocking rate", f"{blocked_count}/{len(scenarios)} (100 %)", "5/5 (100 %)")
            print(
                f"ok   quarantine.rejected_records persisted: "
                f"{payload['total_quarantined_records']} record(s) across "
                f"{len(payload['quarantine_summary'])} rule group(s)"
            )
            for row in payload["quarantine_summary"]:
                print(f"     - {row['source_table']} | {row['rule_name']} ({row['records']})")
            print(f"chaos-data: passed in {time.monotonic() - started:.1f} s")
        finally:
            chaos.down()


if __name__ == "__main__":
    main()
