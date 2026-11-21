# IberGrid Lakehouse — Benchmarks and EU Cost Analysis

Measured on `2026-11-21T19:19:06Z` against the live 3.9-year backfilled dataset (`2023-01-01`..`2026-11-21`, RustFS S3 + Lakekeeper Iceberg REST catalog). Reproducible via `make benchmark` (`benchmarks/run.py`, JSON artifact at [benchmarks/results/latest.json](../benchmarks/results/latest.json)); architectural rationale in [ADR 0009](adr/0009-duckdb-polars-vs-spark.md).

## 1. Environment and 3.9-Year Dataset Footprint

- **Host:** `Linux 6.18.58.1-microsoft-standard-WSL2 (x86_64)`, `12` vCPU, `47.0` GiB RAM
- **Engines:** Python `3.12.11`, DuckDB `2.0.1`, Polars `2.1.0`, PyIceberg `0.12.0`
- **Fresh-process cold start (median of 3 runs):** DuckDB `120.0 ms`, Polars `227.1 ms` (vs 15,000-90,000 ms for JVM/Spark driver + cluster bootstrap)
- **Lakehouse Iceberg tables:** `2,546,472` total rows across `13` tables (`1,320` live Parquet data files, `47.98 MiB` Parquet)
- **Total S3 bucket footprint (Iceberg + manifests + raw JSON/OMIE landing):** `4,844` objects, `229.25 MiB`

| Table | Rows | Data files | Partitions | Parquet size |
|---|---:|---:|---:|---:|
| `bronze.redata_observations` | 1,690,379 | 971 | 192 | 32.13 MiB |
| `bronze.omie_day_ahead_prices` | 64,131 | 48 | 47 | 1.59 MiB |
| `bronze.redata_demand_stream` | 2,671 | 12 | 1 | 212.5 KiB |
| `silver.demand_national_5min` | 409,248 | 48 | 48 | 6.67 MiB |
| `silver.demand_system_hourly` | 170,400 | 48 | 48 | 1.26 MiB |
| `silver.mibel_prices_period` | 64,131 | 47 | 47 | 1.47 MiB |
| `silver.mibel_prices_hourly` | 34,104 | 48 | 48 | 1.24 MiB |
| `silver.generation_daily` | 68,207 | 47 | 47 | 1.28 MiB |
| `gold.mibel_hourly_market` | 34,104 | 48 | 48 | 1.60 MiB |
| `gold.mibel_daily_summary` | 1,421 | 1 | 1 | 131.3 KiB |
| `gold.system_demand_daily` | 7,100 | 1 | 1 | 388.4 KiB |
| `gold.optimal_consumption_windows` | 576 | 1 | 1 | 28.4 KiB |
| `quarantine.rejected_records` | 0 | 0 | 0 | 0 B |

## 2. Benchmark 1 — Storage Layout & Multi-Engine Latency (DuckDB vs Polars)

All three layouts live on the **same RustFS S3 bucket** (apples-to-apples HTTP S3 storage: no mixing local disk against object storage). Each `(query, layout, engine)` runs 2 warm-up iterations followed by 10 timed iterations; DuckDB and Polars results are verified row-for-row.

- **`unpartitioned_parquet`**: single ZSTD Parquet dump on S3 (no Iceberg metadata, no partition pruning).
- **`partitioned_iceberg`**: live Lakekeeper Iceberg table as produced by daily/chunked ingestion and medallion transforms.
- **`compacted_iceberg`**: throwaway Lakekeeper Iceberg clone with identical schema and partition spec compacted via DuckDB `iceberg_rewrite_data_files` (purged after measurement so backfilled volumes stay untouched).

- **Isolated Peak RSS** is measured in a dedicated clean child process per `(query, layout, engine)` so setup allocations never contaminate query memory.

| Query | Layout | Engine | p50 (ms) | p95 (ms) | Files scanned | Bytes scanned | Rows | Peak RSS (MiB) | Verified |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| **Q1** | `unpartitioned_parquet` | `duckdb` | 6.24 | 8.14 | 1/1 | 16.95 MiB | 3 | 221.5 | Yes |
| **Q1** | `unpartitioned_parquet` | `polars` | 18.15 | 24.91 | 1/1 | 16.95 MiB | 3 | 199.9 | Yes |
| **Q1** | `partitioned_iceberg` | `duckdb` | 25.19 | 30.26 | 2/971 | 428.9 KiB | 3 | 228.9 | Yes |
| **Q1** | `partitioned_iceberg` | `polars` | 20.92 | 24.46 | 2/971 | 428.9 KiB | 3 | 207.0 | Yes |
| **Q1** | `compacted_iceberg` | `duckdb` | 12.45 | 13.51 | 1/192 | 421.3 KiB | 3 | 222.2 | Yes |
| **Q1** | `compacted_iceberg` | `polars` | 18.26 | 22.27 | 1/192 | 421.3 KiB | 3 | 203.2 | Yes |
| **Q2** | `unpartitioned_parquet` | `duckdb` | 4.94 | 6.69 | 1/1 | 897.0 KiB | 4 | 212.9 | Yes |
| **Q2** | `unpartitioned_parquet` | `polars` | 9.16 | 15.59 | 1/1 | 897.0 KiB | 4 | 185.8 | Yes |
| **Q2** | `partitioned_iceberg` | `duckdb` | 18.24 | 20.44 | 48/48 | 1.60 MiB | 4 | 249.1 | Yes |
| **Q2** | `partitioned_iceberg` | `polars` | 17.26 | 21.13 | 48/48 | 1.60 MiB | 4 | 203.7 | Yes |
| **Q2** | `compacted_iceberg` | `duckdb` | 17.87 | 25.10 | 48/48 | 1.60 MiB | 4 | 247.5 | Yes |
| **Q2** | `compacted_iceberg` | `polars` | 19.26 | 23.89 | 48/48 | 1.60 MiB | 4 | 203.0 | Yes |
| **Q3** | `unpartitioned_parquet` | `duckdb` | 8.91 | 10.88 | 1/1 | 4.08 MiB | 32 | 217.6 | Yes |
| **Q3** | `unpartitioned_parquet` | `polars` | 13.35 | 16.59 | 1/1 | 4.08 MiB | 32 | 193.2 | Yes |
| **Q3** | `partitioned_iceberg` | `duckdb` | 15.45 | 19.12 | 1/48 | 143.9 KiB | 32 | 224.2 | Yes |
| **Q3** | `partitioned_iceberg` | `polars` | 18.69 | 23.61 | 1/48 | 143.9 KiB | 32 | 200.3 | Yes |
| **Q3** | `compacted_iceberg` | `duckdb` | 20.33 | 22.70 | 1/48 | 143.9 KiB | 32 | 226.2 | Yes |
| **Q3** | `compacted_iceberg` | `polars` | 22.13 | 26.34 | 1/48 | 143.9 KiB | 32 | 200.8 | Yes |
| **Q4** | `unpartitioned_parquet` | `duckdb` | 11.40 | 12.90 | 1/1 | 894.7 KiB | 2,840 | 219.8 | Yes |
| **Q4** | `unpartitioned_parquet` | `polars` | 18.37 | 21.55 | 1/1 | 894.7 KiB | 2,840 | 196.6 | Yes |
| **Q4** | `partitioned_iceberg` | `duckdb` | 19.66 | 24.84 | 48/48 | 1.26 MiB | 2,840 | 251.7 | Yes |
| **Q4** | `partitioned_iceberg` | `polars` | 23.02 | 28.29 | 48/48 | 1.26 MiB | 2,840 | 211.4 | Yes |
| **Q4** | `compacted_iceberg` | `duckdb` | 21.63 | 29.32 | 48/48 | 1.26 MiB | 2,840 | 253.0 | Yes |
| **Q4** | `compacted_iceberg` | `polars` | 21.48 | 27.38 | 48/48 | 1.26 MiB | 2,840 | 210.0 | Yes |

**Key empirical observations (Benchmark 1):**
- **Partition pruning:** On selective monthly queries (`Q1` on `bronze.redata_observations` and `Q3` on `silver.demand_national_5min`), Iceberg prunes over **96 %-98 % of Parquet bytes** relative to full-table or unpartitioned Parquet scans by reading only the matching monthly partition file(s).
- **Compaction impact:** Compacting fragmented tables (such as `bronze.redata_observations` from 971 files down to 192 files, 1 per `(dataset, month)` partition) shrinks Iceberg manifest metadata ~5x and cuts DuckDB `Q1` latency sharply.
- **Localhost loopback vs WAN object storage:** On local loopback RustFS (<0.5 ms RTT, multi-GB/s), reading a single 1-17 MiB unpartitioned Parquet file avoids the ~7-10 ms REST catalog `loadTable` + Avro manifest overhead. Over WAN object storage (Cloudflare R2, 15-30 ms RTT) or multi-GB tables, Iceberg's >96 % byte reduction dominates total latency and Class B read costs, while monthly partitioning on tiny (<2 MiB) Gold tables (`Q2`) illustrates why small Gold marts (`mibel_daily_summary`, `system_demand_daily`, `optimal_consumption_windows`) are intentionally kept as 1-file unpartitioned tables.
- **Multi-engine equivalence & memory:** DuckDB and Polars match 100 % row-for-row across all `24` `(query, layout, engine)` runs with isolated peak RSS <= `253.0 MiB` (DuckDB) and <= `211.4 MiB` (Polars).

## 3. Benchmark 2 — Zero-Scan Iceberg Metadata Checks vs Full SQL Data Scans

Compares `ibergrid.quality.metadata.inspect_table_metadata` (DuckDB `iceberg_snapshots` + `iceberg_files` + `iceberg_column_stats`, **0 Parquet data bytes scanned**) against an equivalent full SQL scan (`count(*)`, null count, `min`/`max` on metric and timestamp columns) across all populated tables.

| Table | Rows | Data files | Parquet size | Metadata p50 / p95 (ms) | Full SQL p50 / p95 (ms) | Speedup (p50) | Data bytes scanned | Exact match |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `bronze.redata_observations` | 1,690,379 | 971 | 32.13 MiB | 253.34 / 275.69 | 110.31 / 118.91 | **0.44x** | **0 B** vs 32.13 MiB | Yes |
| `bronze.omie_day_ahead_prices` | 64,131 | 48 | 1.59 MiB | 40.50 / 45.21 | 17.46 / 23.48 | **0.43x** | **0 B** vs 1.59 MiB | Yes |
| `bronze.redata_demand_stream` | 2,671 | 12 | 212.5 KiB | 37.59 / 40.83 | 15.78 / 18.45 | **0.42x** | **0 B** vs 212.5 KiB | Yes |
| `silver.demand_national_5min` | 409,248 | 48 | 6.67 MiB | 37.33 / 40.67 | 18.33 / 19.37 | **0.49x** | **0 B** vs 6.67 MiB | Yes |
| `silver.demand_system_hourly` | 170,400 | 48 | 1.26 MiB | 36.06 / 49.59 | 29.09 / 119.18 | **0.81x** | **0 B** vs 1.26 MiB | Yes |
| `silver.mibel_prices_period` | 64,131 | 47 | 1.47 MiB | 40.59 / 51.52 | 18.92 / 42.27 | **0.47x** | **0 B** vs 1.47 MiB | Yes |
| `silver.mibel_prices_hourly` | 34,104 | 48 | 1.24 MiB | 40.99 / 46.95 | 18.94 / 23.48 | **0.46x** | **0 B** vs 1.24 MiB | Yes |
| `silver.generation_daily` | 68,207 | 47 | 1.28 MiB | 36.73 / 40.43 | 18.43 / 24.28 | **0.50x** | **0 B** vs 1.28 MiB | Yes |
| `gold.mibel_hourly_market` | 34,104 | 48 | 1.60 MiB | 44.22 / 46.85 | 19.25 / 21.72 | **0.44x** | **0 B** vs 1.60 MiB | Yes |
| `gold.mibel_daily_summary` | 1,421 | 1 | 131.3 KiB | 30.12 / 32.78 | 11.17 / 13.69 | **0.37x** | **0 B** vs 131.3 KiB | Yes |
| `gold.system_demand_daily` | 7,100 | 1 | 388.4 KiB | 31.43 / 35.08 | 11.16 / 12.26 | **0.36x** | **0 B** vs 388.4 KiB | Yes |
| `gold.optimal_consumption_windows` | 576 | 1 | 28.4 KiB | 29.19 / 31.53 | 10.99 / 12.28 | **0.38x** | **0 B** vs 28.4 KiB | Yes |

**Key empirical observations (Benchmark 2):**
- **100 % data-file I/O eliminated:** `inspect_table_metadata` validates row counts, null counts and column min/max bounds across **all** columns of every table with **0 Parquet data bytes scanned** (`0 B` vs `47.98 MiB` total Parquet across all populated tables), matching full SQL scans 100 %.
- **Local loopback latency vs WAN/multi-GB scaling:** `inspect_table_metadata` issues three metadata table-function calls (`iceberg_snapshots/refs`, `iceberg_files`, `iceberg_column_stats`), trading fixed manifest-parsing overhead on tiny localhost loopback tables for **0 Parquet data GETs**. Over WAN object storage (Cloudflare R2) or as tables scale past ~100 MiB, zero-scan metadata stays O(manifests) while full SQL scans scale linearly with table bytes.

## 4. Benchmark 3 — Stream & Data Chaos Proof Summary

| Suite | Injected fault | Target table | Measured outcome | Evidence |
|---|---|---|---|---|
| Stream Chaos (make chaos-stream, ADR 0006) | SIGKILL consumer mid-flush across 3 crash windows (pre-publish, post-publish, checkpoint) | `bronze.redata_demand_stream` | **22 / 22 assertions passed (0 duplicates, 0 lost rows)** | Throwaway compose stack; exactly-once proved via atomic WAP checkpoint properties in 66 s. |
| Data Chaos #1 (make chaos-data, ADR 0008) | Duplicate (delivery_date, period) primary key injection | `bronze.omie_day_ahead_prices` | **100 % blocked before wap.publish** | Soda no_duplicate_values + DST continuity gate; audit branch discarded, main untouched. |
| Data Chaos #2 (make chaos-data, ADR 0008) | OMIE Spain/Portugal price column swap on decoupled blackout day (2025-04-28) | `silver.mibel_prices_hourly` | **100 % blocked before wap.publish** | Cross-source OMIE ES vs REData spot gate (<= 0.02 EUR/MWh); decoupled check catches swap. |
| Data Chaos #3 (make chaos-data, ADR 0008) | Canarias misclassified as peninsular with ts_utc_verified = TRUE | `silver.demand_system_hourly` | **100 % blocked before wap.publish** | Peninsular/non-peninsular isolation + Canarias clock-flag gates; 0 leakage to main. |
| Data Chaos #4 (make chaos-data, ADR 0008) | Missing DST spring-forward hour (22h on 23h day 2025-03-30) | `gold.mibel_hourly_market` | **100 % blocked before wap.publish** | DST 23/24/25h continuity gate; offending date sampled into quarantine.rejected_records. |
| Data Chaos #5 (make chaos-data, ADR 0008) | Out-of-range MIBEL price (9999.0 EUR/MWh > 4000 EUR/MWh cap) | `bronze.omie_day_ahead_prices` | **100 % blocked before wap.publish (0 data bytes scanned)** | Zero-scan metadata bounds gate + Soda range check; 47 forensic rows persisted across 5 runs. |

## 5. Real European Cloud Monthly Cost Breakdown

### Cloudflare R2 Storage Scaling (EU Jurisdiction, $0.00 Egress)

Published Standard Storage rates: 10 GB free tier ($0.015/GB-month above), 1M Class A ops/month free ($4.50/M above), 10M Class B ops/month free ($0.36/M above), $0.00/GB egress.

| Scale scenario | Storage (GiB) | Class A ops/mo | Class B ops/mo | Egress (GB/mo) | Monthly R2 (USD) | Monthly R2 (EUR) |
|---|---:|---:|---:|---:|---:|---:|
| Actual 3.9-year IberGrid dataset (1x) | 0.2239 | 182,850 | 740,700 | 50 | $0.00 | €0.00 |
| 10-year + high-frequency expansion (~10 GB) | 10.0000 | 365,700 | 2,222,100 | 250 | $0.00 | €0.00 |
| Pan-European ENTSO-E scale (~100 GB) | 100.0000 | 914,250 | 7,407,000 | 1000 | $1.35 | €1.24 |
| Sub-terabyte ceiling (1 TB) | 1024.0000 | 1,828,500 | 14,814,000 | 2000 | $20.67 | €19.02 |

### End-to-End European Compute + Storage Comparison

| Scenario | Compute (EU) | Storage | Compute €/mo | Storage €/mo | Egress €/mo | **Total €/mo** | Notes |
|---|---|---|---:|---:|---:|---:|---|
| **IberGrid minimal EU (batch + serving)** | Hetzner Cloud CAX11 (2 Arm64 vCPU, 4 GB RAM, 40 GB NVMe, Falkenstein) | Cloudflare R2 Standard (EU jurisdiction, 0 egress) | €3.79 | €0.00 | €0.00 | **€3.79** | Ex-VAT list price (~4.59 EUR incl. 21 % ES VAT); 20 TB traffic included. |
| **IberGrid full stack EU (batch + Redpanda + Dagster + API)** | Hetzner Cloud CAX21 (4 Arm64 vCPU, 8 GB RAM, 80 GB NVMe, Falkenstein) | Cloudflare R2 Standard (EU jurisdiction, 0 egress) | €6.49 | €0.00 | €0.00 | **€6.49** | Ex-VAT list price (~7.85 EUR incl. 21 % ES VAT); comfortably fits Lakekeeper, Postgres, Redpanda, Dagster, dbt, Soda and FastAPI. |
| **Managed Spark baseline (reference, EU region)** | Managed Spark cluster / Databricks Jobs + Managed Kafka + RDS Postgres catalog | AWS S3 Standard + NAT/egress | €340.00 | €2.50 | €18.00 | **€360.50** | Conservative lower bound for always-on/scheduled EU managed Spark + broker + catalog; 30-90 s JVM cold starts and ~55x higher fixed cost for <1 TB data. |
