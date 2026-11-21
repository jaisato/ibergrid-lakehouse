"""Export precalculated Gold/Silver portal bundles to local disk or S3/R2 (ADR 0010)."""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
from pyarrow import fs

from ibergrid.api.schemas import ATTRIBUTION_MIBEL_AND_REE, NON_COMMERCIAL_NOTICE
from ibergrid.api.store import LakeQueryStore
from ibergrid.lake import duck
from ibergrid.settings import Settings
from ibergrid.storage import land, s3_filesystem

BLACKOUT_START = date(2025, 4, 27)
BLACKOUT_END = date(2025, 4, 29)
PORTAL_ASSET_TYPES: dict[str, str] = {
    "index.html": "text/html; charset=utf-8",
    "styles.css": "text/css; charset=utf-8",
    "app.js": "application/javascript; charset=utf-8",
}


@dataclass(frozen=True)
class PortalExportReport:
    """Summary of files written to local disk and/or uploaded to S3/R2."""

    exported_at_utc: str
    local_files: tuple[Path, ...]
    s3_uris: tuple[str, ...]
    daily_market_rows: int
    optimal_window_rows: int
    systems_monthly_rows: int
    blackout_hourly_rows: int


def find_benchmarks_json(settings: Settings) -> Path | None:
    """Locate ``benchmarks/results/latest.json`` from settings or repository checkout."""
    if settings.api.benchmarks_path is not None:
        candidate = Path(settings.api.benchmarks_path)
        return candidate if candidate.is_file() else None
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "benchmarks" / "results" / "latest.json"
        if candidate.is_file():
            return candidate
    return None


def find_portal_assets_dir(settings: Settings) -> Path | None:
    """Locate static portal assets (``index.html``, ``styles.css``, ``app.js``)."""
    if settings.api.portal_dir is not None:
        configured = Path(settings.api.portal_dir)
        return configured if (configured / "index.html").is_file() else None
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "portal"
        if (candidate / "index.html").is_file():
            return candidate
    return None


def _normalize_s3_prefix(prefix: str) -> str:
    cleaned = prefix.strip().strip("/")
    if not cleaned:
        raise ValueError("s3_prefix must not be empty")
    if ".." in cleaned.split("/"):
        raise ValueError("s3_prefix must not contain '..' path segments")
    return cleaned


def _to_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _to_json_bytes(payload: Any, *, compact: bool = False) -> bytes:
    if compact:
        return (json.dumps(payload, separators=(",", ":"), default=str) + "\n").encode("utf-8")
    return (json.dumps(payload, indent=2, default=str) + "\n").encode("utf-8")


def _annual_mibel_rollup(daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_year: dict[int, list[dict[str, Any]]] = {}
    for row in daily_rows:
        year = int(str(row["delivery_date"])[:4])
        by_year.setdefault(year, []).append(row)
    out: list[dict[str, Any]] = []
    for year in sorted(by_year):
        grp = by_year[year]
        ren_vals = [
            float(r["peninsular_renewable_share_pct"])
            for r in grp
            if r["peninsular_renewable_share_pct"] is not None
        ]
        sol_vals = [
            float(r["peninsular_solar_share_pct"])
            for r in grp
            if r["peninsular_solar_share_pct"] is not None
        ]
        out.append(
            {
                "year": year,
                "days": len(grp),
                "price_es_mean_eur_mwh": round(
                    sum(float(r["price_es_mean_eur_mwh"]) for r in grp) / len(grp), 2
                ),
                "price_pt_mean_eur_mwh": round(
                    sum(float(r["price_pt_mean_eur_mwh"]) for r in grp) / len(grp), 2
                ),
                "zero_or_negative_es_hours": sum(int(r["zero_or_negative_es_hours"]) for r in grp),
                "decoupled_hours": sum(int(r["decoupled_hours"]) for r in grp),
                "peninsular_renewable_share_pct": (
                    round(sum(ren_vals) / len(ren_vals), 2) if ren_vals else None
                ),
                "peninsular_solar_share_pct": (
                    round(sum(sol_vals) / len(sol_vals), 2) if sol_vals else None
                ),
            }
        )
    return out


def _systems_rollup(
    sys_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_system: dict[str, list[dict[str, Any]]] = {}
    by_month_sys: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in sys_rows:
        sys_name = str(row["electric_system"])
        month_key = str(row["demand_date"])[:7]
        by_system.setdefault(sys_name, []).append(row)
        by_month_sys.setdefault((month_key, sys_name), []).append(row)

    overview: list[dict[str, Any]] = []
    for sys_name in sorted(by_system, key=lambda s: (s != "peninsular", s)):
        grp = by_system[sys_name]
        ren = [float(r["renewable_share_pct"]) for r in grp if r["renewable_share_pct"] is not None]
        sol = [float(r["solar_share_pct"]) for r in grp if r["solar_share_pct"] is not None]
        wnd = [float(r["wind_share_pct"]) for r in grp if r["wind_share_pct"] is not None]
        overview.append(
            {
                "electric_system": sys_name,
                "system_scope": grp[0]["system_scope"],
                "ts_utc_verified": bool(grp[0]["ts_utc_verified"]),
                "days": len(grp),
                "avg_daily_demand_mwh": round(
                    sum(float(r["demand_total_mwh"]) for r in grp) / len(grp), 1
                ),
                "peak_demand_mw": round(max(float(r["demand_peak_mw"]) for r in grp), 1),
                "avg_renewable_share_pct": round(sum(ren) / len(ren), 2) if ren else None,
                "avg_solar_share_pct": round(sum(sol) / len(sol), 2) if sol else None,
                "avg_wind_share_pct": round(sum(wnd) / len(wnd), 2) if wnd else None,
            }
        )

    monthly: list[dict[str, Any]] = []
    for month_key, sys_name in sorted(by_month_sys):
        grp = by_month_sys[(month_key, sys_name)]
        ren = [float(r["renewable_share_pct"]) for r in grp if r["renewable_share_pct"] is not None]
        monthly.append(
            {
                "month": month_key,
                "electric_system": sys_name,
                "system_scope": grp[0]["system_scope"],
                "ts_utc_verified": bool(grp[0]["ts_utc_verified"]),
                "days": len(grp),
                "avg_daily_demand_mwh": round(
                    sum(float(r["demand_total_mwh"]) for r in grp) / len(grp), 1
                ),
                "avg_renewable_share_pct": round(sum(ren) / len(ren), 2) if ren else None,
            }
        )
    return overview, monthly


def _quarantine_summary(
    con: duckdb.DuckDBPyConnection, table_resolver: Callable[[str], str]
) -> dict[str, Any]:
    q_ref = table_resolver("quarantine.rejected_records")
    try:
        rows = con.execute(
            "SELECT rule_name, source_table, CAST(count(*) AS BIGINT) "
            "FROM iceberg_scan($table) GROUP BY 1, 2 ORDER BY 3 DESC, 1 ASC",
            {"table": q_ref},
        ).fetchall()
    except duckdb.Error:
        return {"total_rejected_records": 0, "by_rule": []}
    by_rule = [
        {"rule_name": str(r[0]), "source_table": str(r[1]), "count": int(r[2])} for r in rows
    ]
    return {
        "total_rejected_records": sum(item["count"] for item in by_rule),
        "by_rule": by_rule,
    }


def export_portal_bundle(
    settings: Settings,
    *,
    output_dir: Path | None = None,
    s3_prefix: str | None = None,
    benchmarks_path: Path | None = None,
    include_assets: bool = True,
    connection_factory: Callable[[Settings], duckdb.DuckDBPyConnection] | None = None,
    table_resolver: Callable[[str], str] | None = None,
    filesystem: fs.FileSystem | None = None,
) -> PortalExportReport:
    """Materialize static JSON/CSV analytical bundles from Gold & Silver tables.

    Writes to ``output_dir`` (e.g. ``portal/data``) and/or uploads directly to
    ``s3://<bucket>/<s3_prefix>/`` on RustFS or Cloudflare R2.
    """
    if output_dir is None and s3_prefix is None:
        raise ValueError("specify at least one of output_dir or s3_prefix")
    clean_prefix = _normalize_s3_prefix(s3_prefix) if s3_prefix is not None else None

    exported_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    resolve = table_resolver or duck.table_name
    store = LakeQueryStore(
        settings,
        connection_factory=connection_factory,
        table_resolver=resolve,
    )
    try:
        daily_resp = store.market_daily(date(2023, 1, 1), date(2030, 12, 31))
        windows_resp = store.optimal_windows(None, None)
        systems_resp = store.systems_daily(None, date(2023, 1, 1), date(2030, 12, 31))
        blackout_resp = store.market_hourly(BLACKOUT_START, BLACKOUT_END)
        realtime_resp = store.realtime_demand(24)
        metadata_resp = store.metadata_tables()

        con = (connection_factory or duck.connect)(settings)
        try:
            quarantine = _quarantine_summary(con, resolve)
        finally:
            con.close()
    finally:
        store.close()

    daily_rows = [item.model_dump(mode="json") for item in daily_resp.items]
    window_rows = [item.model_dump(mode="json") for item in windows_resp.items]
    system_rows = [item.model_dump(mode="json") for item in systems_resp.items]
    blackout_rows = [item.model_dump(mode="json") for item in blackout_resp.items]
    realtime_rows = [item.model_dump(mode="json") for item in realtime_resp.items]

    annual_mibel = _annual_mibel_rollup(daily_rows)
    systems_overview, systems_monthly = _systems_rollup(system_rows)
    blackout_daily = [
        row
        for row in daily_rows
        if str(BLACKOUT_START) <= str(row["delivery_date"]) <= str(BLACKOUT_END)
    ]

    summary_payload: dict[str, Any] = {
        "exported_at_utc": exported_at,
        "source_attribution": ATTRIBUTION_MIBEL_AND_REE,
        "non_commercial_notice": NON_COMMERCIAL_NOTICE,
        "coverage_start": daily_rows[0]["delivery_date"] if daily_rows else None,
        "coverage_end": daily_rows[-1]["delivery_date"] if daily_rows else None,
        "total_days": len(daily_rows),
        "annual_mibel": annual_mibel,
        "systems_overview": systems_overview,
        "blackout_2025_04_28_daily": blackout_daily,
        "realtime_demand": {
            "hours": realtime_resp.hours,
            "batch_points": realtime_resp.batch_points,
            "stream_points": realtime_resp.stream_points,
            "updated_at_utc": realtime_resp.updated_at_utc,
            "items": realtime_rows,
        },
        "metadata_inventory": metadata_resp.model_dump(mode="json"),
        "quarantine_summary": quarantine,
    }

    data_artifacts: dict[str, tuple[bytes, str]] = {
        "summary.json": (_to_json_bytes(summary_payload), "application/json; charset=utf-8"),
        "optimal_windows.json": (
            _to_json_bytes(
                {
                    "exported_at_utc": exported_at,
                    "source_attribution": ATTRIBUTION_MIBEL_AND_REE,
                    "items": window_rows,
                },
                compact=True,
            ),
            "application/json; charset=utf-8",
        ),
        "optimal_windows.csv": (_to_csv_bytes(window_rows), "text/csv; charset=utf-8"),
        "daily_market.json": (
            _to_json_bytes(
                {
                    "exported_at_utc": exported_at,
                    "source_attribution": ATTRIBUTION_MIBEL_AND_REE,
                    "items": daily_rows,
                },
                compact=True,
            ),
            "application/json; charset=utf-8",
        ),
        "daily_market.csv": (_to_csv_bytes(daily_rows), "text/csv; charset=utf-8"),
        "blackout_2025_04_28.json": (
            _to_json_bytes(
                {
                    "exported_at_utc": exported_at,
                    "source_attribution": ATTRIBUTION_MIBEL_AND_REE,
                    "start": str(BLACKOUT_START),
                    "end": str(BLACKOUT_END),
                    "daily": blackout_daily,
                    "hourly": blackout_rows,
                }
            ),
            "application/json; charset=utf-8",
        ),
        "systems_daily.json": (
            _to_json_bytes(
                {
                    "exported_at_utc": exported_at,
                    "source_attribution": "Source: Red Eléctrica, REData",
                    "overview": systems_overview,
                    "monthly": systems_monthly,
                }
            ),
            "application/json; charset=utf-8",
        ),
    }

    bench_file = benchmarks_path if benchmarks_path is not None else find_benchmarks_json(settings)
    if bench_file is not None and bench_file.is_file():
        data_artifacts["benchmarks.json"] = (
            bench_file.read_bytes(),
            "application/json; charset=utf-8",
        )

    local_files: list[Path] = []
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for name, (payload, _ctype) in data_artifacts.items():
            dest = output_dir / name
            dest.write_bytes(payload)
            local_files.append(dest)

    s3_uris: list[str] = []
    if clean_prefix is not None:
        target_fs = filesystem or s3_filesystem(settings.storage, timeout=30.0)
        for name, (payload, ctype) in data_artifacts.items():
            uri = land(
                target_fs,
                settings.storage.bucket,
                f"{clean_prefix}/data/{name}",
                payload,
                content_type=ctype,
            )
            s3_uris.append(uri)
        if include_assets:
            assets_dir = find_portal_assets_dir(settings)
            if assets_dir is not None:
                for asset_name, ctype in PORTAL_ASSET_TYPES.items():
                    asset_path = assets_dir / asset_name
                    if asset_path.is_file():
                        uri = land(
                            target_fs,
                            settings.storage.bucket,
                            f"{clean_prefix}/{asset_name}",
                            asset_path.read_bytes(),
                            content_type=ctype,
                        )
                        s3_uris.append(uri)

    return PortalExportReport(
        exported_at_utc=exported_at,
        local_files=tuple(local_files),
        s3_uris=tuple(s3_uris),
        daily_market_rows=len(daily_rows),
        optimal_window_rows=len(window_rows),
        systems_monthly_rows=len(systems_monthly),
        blackout_hourly_rows=len(blackout_rows),
    )
