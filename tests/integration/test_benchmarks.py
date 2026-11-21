"""Multi-engine DuckDB + Polars benchmark suite integration test on Lakekeeper + RustFS."""

from __future__ import annotations

import contextlib
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.io.pyarrow import PyArrowFileIO

from ibergrid.benchmarks import (
    BENCHMARK_QUERIES,
    benchmark_metadata_vs_full_scan,
    benchmark_query_across_layouts,
    build_compacted_clone,
    export_unpartitioned_parquet,
    polars_s3_storage_options,
)
from ibergrid.lake import duck, registry
from ibergrid.lake.catalog import LakeCatalog, s3_properties
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings

pytestmark = pytest.mark.integration
NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
ATTRIBUTION = "Source: OMIE (omie.es) & Red Eléctrica (REData)"


def _sample_gold_mibel() -> pa.Table:
    schema = registry.GOLD_MIBEL_HOURLY_MARKET.schema.as_arrow()
    cols: dict[str, list[object]] = {f.name: [] for f in schema}
    rows = [
        (datetime(2024, 5, 1, 12, 0, tzinfo=UTC), date(2024, 5, 1), 14, 0.0, 0.0, False, True),
        (datetime(2025, 4, 28, 7, 0, tzinfo=UTC), date(2025, 4, 28), 9, 47.94, 40.70, True, False),
        (datetime(2025, 10, 1, 10, 0, tzinfo=UTC), date(2025, 10, 1), 12, -1.5, -1.5, False, True),
    ]
    for ts_utc, dday, mhour, es, pt, dec, zneg in rows:
        cols["hour_utc"].append(ts_utc)
        cols["delivery_date"].append(dday)
        cols["madrid_hour"].append(mhour)
        cols["day_type"].append("weekday")
        cols["price_es_eur_mwh"].append(es)
        cols["price_pt_eur_mwh"].append(pt)
        cols["spread_es_minus_pt_eur_mwh"].append(round(es - pt, 4))
        cols["is_decoupled"].append(dec)
        cols["is_zero_or_negative_es"].append(zneg)
        cols["pvpc_eur_mwh"].append(80.0)
        cols["peninsular_demand_mw"].append(27000.0)
        cols["demand_ramp_1h_mw"].append(100.0)
        cols["price_ramp_1h_eur_mwh"].append(2.0)
        cols["intraday_price_rank"].append(1)
        cols["is_evening_ramp_window"].append(False)
        cols["peninsular_renewable_share_pct"].append(62.5)
        cols["peninsular_solar_share_pct"].append(25.0)
        cols["peninsular_wind_share_pct"].append(22.0)
        cols["source_attribution"].append(ATTRIBUTION)
        cols["updated_at_utc"].append(NOW)
    return pa.table(cols, schema=schema)


def test_duckdb_and_polars_benchmark_layouts_on_rustfs(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection, settings: Settings
) -> None:
    spec = replace(registry.GOLD_MIBEL_HOURLY_MARKET, namespace=namespace)
    ensure_table(catalog, spec)
    live_tbl = catalog.load_table(spec.identifier)
    sample = _sample_gold_mibel()
    live_tbl.append(sample.slice(0, 2))
    live_tbl.append(sample.slice(2))

    live_duck = f"{duck.CATALOG}.{namespace}.mibel_hourly_market"
    storage_opts = polars_s3_storage_options(settings.storage)
    io = PyArrowFileIO(s3_properties(settings.storage))
    s3_fs = io.fs_by_scheme("s3", settings.storage.bucket)
    tmp_prefix = f"{settings.storage.bucket}/it-bench-{uuid.uuid4().hex[:8]}"
    parquet_uri = f"s3://{tmp_prefix}/gold_mibel.parquet"

    try:
        export_unpartitioned_parquet(con, live_duck, parquet_uri)
        info = s3_fs.get_file_info(f"{tmp_prefix}/gold_mibel.parquet")
        comp_tbl = build_compacted_clone(
            catalog,
            con,
            source_table=live_tbl,
            source_spec=spec,
            bench_namespace=namespace,
            clone_duck_name=f"{duck.CATALOG}.{namespace}.{namespace}_mibel_hourly_market",
        )
        comp_duck = f"{duck.CATALOG}.{namespace}.{namespace}_mibel_hourly_market"
        q2 = next(q for q in BENCHMARK_QUERIES if q.query_id == "Q2")

        rows = benchmark_query_across_layouts(
            con,
            q2,
            parquet_uri=parquet_uri,
            parquet_bytes=int(info.size or 0),
            live_table=live_tbl,
            live_duck_source=live_duck,
            compacted_table=comp_tbl,
            compacted_duck_source=comp_duck,
            storage_options=storage_opts,
            warmup=0,
            iterations=2,
        )
        assert len(rows) == 6
        assert all(r.answer_verified for r in rows)
        assert all(r.row_count == 2 for r in rows)

        meta = benchmark_metadata_vs_full_scan(
            con,
            logical_table="gold.mibel_hourly_market",
            duck_table=live_duck,
            time_col="hour_utc",
            metric_col="price_es_eur_mwh",
            warmup=0,
            iterations=2,
        )
        assert meta is not None
        assert meta.metadata_bytes_scanned == 0
        assert meta.counts_match is True
        assert meta.total_records == 3
    finally:
        with contextlib.suppress(FileNotFoundError):
            s3_fs.delete_dir(tmp_prefix)
