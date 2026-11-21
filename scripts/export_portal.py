#!/usr/bin/env python3
"""Export precalculated Gold/Silver portal data bundles from the local compose stack (ADR 0010).

Usage:
    make portal-export
    uv run python -u scripts/export_portal.py --s3-prefix portal
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ibergrid.benchmarks.runner import load_host_settings
from ibergrid.portal.export import export_portal_bundle

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / "portal" / "data"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export precalculated Gold/Silver portal JSON & CSV bundles."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUT,
        help="Local output directory for JSON/CSV portal data (default: portal/data).",
    )
    parser.add_argument(
        "--s3-prefix",
        default=None,
        help="Optional S3/R2 key prefix to also upload the static portal and data bundle.",
    )
    parser.add_argument(
        "--catalog-url",
        default=os.environ.get("IBERGRID_TEST_CATALOG_URL", "http://127.0.0.1:28181"),
        help="Lakekeeper catalog base URL.",
    )
    parser.add_argument(
        "--s3-url",
        default=os.environ.get("IBERGRID_TEST_S3_URL", "http://127.0.0.1:29000"),
        help="S3/RustFS endpoint URL.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_host_settings(
        REPO_ROOT,
        catalog_url=args.catalog_url,
        s3_url=args.s3_url,
    )
    report = export_portal_bundle(
        settings,
        output_dir=args.output_dir,
        s3_prefix=args.s3_prefix,
        benchmarks_path=REPO_ROOT / "benchmarks" / "results" / "latest.json",
    )
    print(
        f"[portal-export] Wrote {len(report.local_files)} local file(s), "
        f"{len(report.s3_uris)} S3 object(s) "
        f"(daily={report.daily_market_rows}, windows={report.optimal_window_rows}, "
        f"systems_monthly={report.systems_monthly_rows}, "
        f"blackout_hours={report.blackout_hourly_rows})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
