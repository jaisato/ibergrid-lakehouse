#!/usr/bin/env python3
"""Run the reproducible IberGrid benchmark suite against the local compose stack (ADR 0009).

Usage:
    make benchmark
    uv run python -u benchmarks/run.py --iterations 10 --warmup 2
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ibergrid.benchmarks.report import render_markdown_report, write_json_report
from ibergrid.benchmarks.runner import load_host_settings, run_live_benchmark_suite

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_JSON_OUT = REPO_ROOT / "benchmarks" / "results" / "latest.json"
DEFAULT_MD_OUT = REPO_ROOT / "docs" / "benchmarks.md"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run IberGrid Lakehouse benchmarks against the running local stack."
    )
    parser.add_argument("--warmup", type=int, default=2, help="Warm-up runs per measurement.")
    parser.add_argument("--iterations", type=int, default=10, help="Timed runs per measurement.")
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
    parser.add_argument(
        "--json-out",
        type=Path,
        default=DEFAULT_JSON_OUT,
        help="Destination path for structured JSON benchmark results.",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=DEFAULT_MD_OUT,
        help="Destination path for the rendered Markdown benchmark report.",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="Skip rewriting docs/benchmarks.md (only write JSON).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = load_host_settings(
        REPO_ROOT,
        catalog_url=args.catalog_url,
        s3_url=args.s3_url,
    )
    result = run_live_benchmark_suite(
        settings,
        warmup=args.warmup,
        iterations=args.iterations,
        on_progress=lambda msg: print(f"[benchmark] {msg}", flush=True),
    )

    write_json_report(result, args.json_out)
    print(f"[benchmark] Wrote JSON report: {args.json_out}", flush=True)

    if not args.no_markdown:
        md = render_markdown_report(result)
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(md, encoding="utf-8")
        print(f"[benchmark] Wrote Markdown report: {args.markdown_out}", flush=True)

    all_verified = all(r.answer_verified for r in result.query_benchmarks) and all(
        r.counts_match for r in result.metadata_benchmarks
    )
    if not all_verified:
        print("[benchmark] ERROR: cross-engine or metadata answer mismatch detected!", flush=True)
        return 1

    print("[benchmark] All cross-engine and zero-scan metadata results verified.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
