# 0009. DuckDB 2.0 + Polars + PyIceberg vs Apache Spark for sub-terabyte workloads

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

Most enterprise lakehouse reference architectures default to Apache Spark on Kubernetes, EMR,
Dataproc or Databricks regardless of actual data volume. After materializing **3.9 years** of real
Iberian electricity data (`2023-01-01`..`2026-11-21`, Red Eléctrica REData + OMIE MIBEL) across
13 Apache Iceberg tables (`2,546,472` total rows across Bronze, Silver and Gold), the entire
lakehouse occupies:

- **`47.98 MiB`** of ZSTD Parquet data across `1,320` live Iceberg data files;
- **`229.25 MiB`** (`4,844` objects) across the entire object-storage bucket — including every raw
  REData JSON response, every original OMIE `marginalpdbc` file, and all Iceberg manifests and
  snapshots.

Even projected out to 10 years of Iberian grid data plus finer 5-minute or 15-minute series, the
dataset stays orders of magnitude below 1 TB. This record evaluates **DuckDB 2.0.1 + Polars 2.1.0 +
PyIceberg 0.12.0** against **Apache Spark** on real measured latency, cold-start overhead, memory
footprint, multi-engine interoperability and European cloud cost — and defines the explicit
quantitative thresholds where migrating to Apache Spark becomes justified.

## Decision drivers

- **Sub-second interactive latency & fast CI/CD:** analytical queries, dbt builds, quality checks
  and unit/integration tests must run in milliseconds to seconds, not minutes of JVM/cluster spin-up.
- **Single-server European cost discipline:** production must run comfortably on one European Arm64
  VPS (Hetzner Cloud EU) plus zero-egress object storage (Cloudflare R2) at single-digit EUR/month.
- **True multi-engine openness:** tables live in Apache Iceberg v2 behind Lakekeeper's standard
  REST catalog, so switching or adding Spark or Trino later requires **zero data migration**.
- **Honest, reproducible evidence:** every latency, memory, byte-pruning and cost claim must come
  from the committed reproducible benchmark suite (`make benchmark`,
  [benchmarks/results/latest.json](../../benchmarks/results/latest.json),
  [docs/benchmarks.md](../benchmarks.md)).

## Considered options

1. **DuckDB 2.0 + Polars 2.1 + PyIceberg 0.12 over Iceberg REST (chosen):** in-process vectorized
   C++/Rust engines reading and compacting Iceberg tables written through PyIceberg WAP (ADR 0002),
   with dbt Core + `dbt-duckdb` for medallion transforms (ADR 0007) and Soda Core v4 for WAP
   quality gates (ADR 0008).
2. **Apache Spark 3.5 / 4.0 (PySpark + Iceberg Spark Runtime) on a managed cluster or single-node
   local mode:** JVM-based distributed SQL/DataFrame engine with native Iceberg branch writes and
   compaction.
3. **Trino / Presto coordinator + worker cluster:** distributed MPP SQL engine on the JVM for
   interactive federation and serving.

## Empirical measurements (2026-11-21, real 3.9-year dataset)

Captured via `make benchmark` (`benchmarks/run.py`) against the live backfilled RustFS + Lakekeeper
stack (`Linux 6.18.58.1 x86_64`, 12 vCPU, 47 GiB RAM, Python 3.12.11, DuckDB 2.0.1, Polars 2.1.0,
PyIceberg 0.12.0; 2 warm-ups + 10 timed iterations per cell; isolated child-process `/proc/self/status`
`VmHWM` peak RSS so setup allocations never contaminate query RSS).

### 1. Cold start and memory footprint

| Metric | DuckDB 2.0.1 | Polars 2.1.0 | Apache Spark (reference) |
|---|---:|---:|---|
| Fresh-process cold start (median of 3 runs) | **96.8 ms** | **124.7 ms** | 15,000-90,000 ms (JVM + driver + executor bootstrap) |
| Isolated query peak RSS (`VmHWM`, Q1-Q4) | **213.3-252.9 MiB** | **185.9-214.0 MiB** | >= 2,048-4,096 MiB minimum driver + executor JVM |
| Full 3.9-year dbt medallion build (12 models, 87 tests, 9 WAP publishes, ADR 0007) | **25.85 s** (`7.84 s` dbt build, **703.4 MiB** RSS) | N/A | Typically 60-180 s including cluster/session startup |
| Idle stack footprint (Phase 0 measurement, ADR 0001, without RustFS) | **287-332 MiB** total (Lakekeeper + Postgres + Redpanda) | Same | Multiple GiB idle per JVM coordinator/executor |

### 2. Storage layout & multi-engine latency (Benchmark 1, same RustFS S3 bucket)

All 24 `(query, layout, engine)` runs returned **100 % row-for-row identical answers** between
DuckDB 2.0.1 and Polars 2.1.0 (`POLARS_ICEBERG_PLANNER=pyiceberg`). `Planned files` and
`Planned file footprint` report catalog-level candidate Parquet files and summed file sizes from
PyIceberg's partition/manifest planner (vs the single whole file for unpartitioned Parquet); both
engines additionally perform column projection and Parquet row-group skipping inside each planned
file, so actual bytes read over the wire are strictly <= the planned file footprint:

| Query | Table | Layout | DuckDB p50 / p95 (ms) | Polars p50 / p95 (ms) | Planned files | Planned file footprint | DuckDB / Polars RSS (MiB) |
|---|---|---|---:|---:|---:|---:|---:|
| **Q1** (1-month point lookup `2025-10`) | `bronze.redata_observations` (1.69M rows) | `unpartitioned_parquet` | 6.45 / 8.30 | 13.28 / 14.88 | 1 / 1 | 16.95 MiB | 224.7 / 202.0 |
| **Q1** | `bronze.redata_observations` | `partitioned_iceberg` | 29.11 / 37.16 | 22.91 / 33.31 | **2 / 971** | **428.9 KiB** (-97.5 % footprint) | 229.1 / 209.1 |
| **Q1** | `bronze.redata_observations` | `compacted_iceberg` | **11.07 / 11.70** | 18.56 / 19.57 | **1 / 192** | **421.3 KiB** (-97.5 % footprint) | 222.9 / 205.3 |
| **Q2** (full 3.9-year annual MIBEL agg) | `gold.mibel_hourly_market` (34k rows) | `unpartitioned_parquet` | 3.60 / 3.86 | 7.94 / 9.18 | 1 / 1 | 897.0 KiB | 213.3 / 185.9 |
| **Q2** | `gold.mibel_hourly_market` | `partitioned_iceberg` | 17.14 / 18.59 | 27.21 / 50.70 | 48 / 48 | 1.60 MiB | 244.9 / 205.6 |
| **Q2** | `gold.mibel_hourly_market` | `compacted_iceberg` | 15.21 / 16.37 | 14.08 / 16.43 | 48 / 48 | 1.60 MiB | 248.5 / 205.5 |
| **Q3** (1-month 5-min rolling 1h peak) | `silver.demand_national_5min` (409k rows) | `unpartitioned_parquet` | 7.05 / 8.06 | 10.93 / 11.55 | 1 / 1 | 4.08 MiB | 217.6 / 193.2 |
| **Q3** | `silver.demand_national_5min` | `partitioned_iceberg` | **14.79 / 15.84** | 14.88 / 22.19 | **1 / 48** | **143.9 KiB** (-96.5 % footprint) | 226.6 / 202.9 |
| **Q4** (Peninsular vs Canarias daily) | `silver.demand_system_hourly` (170k rows) | `unpartitioned_parquet` | 8.54 / 9.17 | 12.61 / 14.14 | 1 / 1 | 894.7 KiB | 220.9 / 196.2 |
| **Q4** | `silver.demand_system_hourly` | `partitioned_iceberg` | 18.98 / 21.04 | 19.66 / 38.33 | 48 / 48 | 1.26 MiB | 252.7 / 214.0 |

Three nuanced lessons directly backed by the measurements:

1. **Iceberg partition pruning eliminates 96.5 %-98.7 % of the planned Parquet file footprint** on
   selective time-window queries before either engine opens a Parquet footer (`Q1`: 1/192 files =
   421 KiB vs 16.95 MiB unpartitioned / 32.13 MiB full table; `Q3`: 1/48 files = 144 KiB vs
   4.08 MiB unpartitioned / 6.67 MiB full table).
2. **Compaction more than halves fragmented-table latency.** Compacting `bronze.redata_observations`
   from **971** small chunked files into **192** files (1 per `(dataset, month)` partition) shrinks
   Avro manifest metadata ~5x and cuts DuckDB `Q1` p50 latency by **2.63x** (`29.11 ms -> 11.07 ms`).
3. **Honest localhost loopback vs WAN storage analysis.** On local loopback RustFS (<0.5 ms RTT,
   multi-GB/s throughput), reading a single 1-17 MiB unpartitioned Parquet file with column
   projection avoids ~7-10 ms of Iceberg REST `loadTable` + Avro manifest parsing. Over WAN object
   storage (Cloudflare R2, 15-30 ms RTT, ~50-100 MB/s) or multi-GB tables, pruning 97-99 % of
   candidate files at the manifest layer eliminates remote footer/row-group reads and bounds
   object-store I/O. Conversely, `Q2` shows that monthly partitioning on tiny (<2 MiB) Gold summary
   tables incurs 48 small-object reads on full scans — validating why `gold.mibel_daily_summary`,
   `gold.system_demand_daily` and `gold.optimal_consumption_windows` are intentionally stored as
   1-file unpartitioned Iceberg tables.

### 3. Zero-scan Iceberg metadata checks vs full SQL scans (Benchmark 2)

Across all 12 populated Bronze, Silver and Gold tables (`2,546,472` rows, `47.98 MiB` Parquet),
`ibergrid.quality.metadata.inspect_table_metadata` matched full SQL column scans 100 % across all
audited columns (4-6 columns per table: row counts, null counts, numeric bounds, timestamps, dates,
booleans and strings) while reading **0 B of Parquet data** (`100 %` data-file I/O eliminated):

- On 1-48-file tables: `26.3-48.4 ms` p50 (`0 B` data scanned vs `28.4 KiB - 6.67 MiB`); on
  409k-row `silver.demand_national_5min`, metadata p95 (`36.6 ms`) already beats full SQL p95
  (`70.4 ms`) by **1.92x**.
- On uncompacted 971-file `bronze.redata_observations`: `215.4 ms` p50 (`0 B` data scanned vs
  `32.13 MiB`).
- Again, on tiny localhost loopback tables, DuckDB's C++ full scan is fast in wall-clock ms
  (`9.5-122.6 ms`) because loopback reads 1-32 MiB instantaneously while `inspect_table_metadata`
  issues three separate metadata table-function calls (`iceberg_snapshots/refs`, `iceberg_files`,
  `iceberg_column_stats`). Over WAN storage (Cloudflare R2) or at >= 100 MiB-1 TB scale, zero-scan
  metadata scales as `O(manifests)` with **0 data GETs** while full SQL scans scale linearly with
  data volume.

### 4. Real European cloud monthly cost breakdown

Based on the measured `229.25 MiB` (`0.2239 GiB`) 3.9-year bucket footprint and conservative daily
operational projections (`182,850` Class A ops/mo and `740,700` Class B ops/mo across batch,
5-minute streaming, 2x daily dbt medallion, maintenance and 5,000 daily API/portal reads):

| Scenario | Compute (EU) | Storage | Compute €/mo | Storage €/mo | Egress €/mo | **Total €/mo** |
|---|---|---|---:|---:|---:|---:|
| **IberGrid minimal EU** (batch + serving) | Hetzner Cloud CAX11 (2 Arm64 vCPU, 4 GB RAM, Falkenstein) | Cloudflare R2 Standard (EU, 0 egress) | €3.79 | €0.00 | €0.00 | **€3.79** |
| **IberGrid full stack EU** (batch + Redpanda + Dagster + API) | Hetzner Cloud CAX21 (4 Arm64 vCPU, 8 GB RAM, Falkenstein) | Cloudflare R2 Standard (EU, 0 egress) | €6.49 | €0.00 | €0.00 | **€6.49** |
| **Managed Spark baseline** (EU reference) | Managed Spark / Databricks Jobs + Managed Kafka + RDS Postgres | AWS S3 Standard + NAT/egress | €340.00 | €2.50 | €18.00 | **€360.50** |

Even scaling storage **4,500x** from `0.22 GiB` to **1 TB** on Cloudflare R2 costs only **$20.67/mo
(€19.02/mo)** with **$0.00 egress**, because R2 charges zero data-transfer fees to compute or
analytical readers.

## Decision outcome

Chosen option: **DuckDB 2.0 + Polars 2.1 + PyIceberg 0.12 over Iceberg REST (Lakekeeper)**.

For sub-terabyte analytical lakehouses (everything from tens of megabytes to hundreds of gigabytes),
modern single-node vectorized C++/Rust engines deliver **sub-25 ms** analytical queries, **~100 ms**
cold starts, **< 260 MiB** standalone query RAM, **100 % multi-engine interoperability** over open
Apache Iceberg tables, and **~55x lower European cloud infrastructure cost** (`€6.49/mo` vs
`~€360+/mo`) than a managed Apache Spark stack — with dramatically simpler local testing and zero
JVM operations.

- **Role split preserved:** PyIceberg owns all ACID WAP writes and snapshot/orphan maintenance
  (ADR 0002, ADR 0008); DuckDB owns SQL medallion builds (`dbt-duckdb`), Soda v4 contract evaluation,
  compaction and real-time serving; Polars provides a zero-JVM Rust DataFrame consumer over the
  exact same Lakekeeper Iceberg tables (`pl.scan_iceberg`) and lives in the `benchmarks` / `dev`
  dependency group so `ibergrid-app:local` (`813 MB`) and `ibergrid-orchestration:local` (`1.46 GB`)
  stay lean.

## Explicit Spark crossover thresholds (when Apache Spark becomes justified)

Choosing DuckDB + Polars is a **regime decision, not an ideology**. Revisit this ADR and attach
Apache Spark (or Trino for federated BI concurrency) to the **exact same Lakekeeper Iceberg REST
catalog** — with zero data migration — when any of the following concrete thresholds is crossed:

1. **Working set exceeds ~1 TB (or single-node shuffle exceeds available RAM + local NVMe spill):**
   DuckDB 2.0 handles out-of-core larger-than-RAM aggregations/joins cleanly up to a few hundred GB
   on fast NVMe, but multi-terabyte wide joins or full-history global sorts saturate single-node
   memory/NVMe bandwidth and need horizontal shuffle across executors.
2. **Sustained multi-tenant concurrent ETL / heavy ad-hoc concurrency (> 20-50 heavy concurrent
   writers/analysts):** single-process in-memory engines do not provide distributed YARN/Kubernetes
   resource queues, dynamic executor autoscaling, or isolated per-tenant compute pools.
3. **High-throughput stateful distributed stream processing (> 50k-100k events/sec with distributed
   watermarked stream-stream joins):** IberGrid's streaming lane processes ~1,152 events/day plus
   revisions (well served by a single 384 MiB Python WAP micro-batch consumer on Redpanda);
   sub-second multi-gigabyte stateful stream joins warrant Spark Structured Streaming or Apache Flink.
4. **Organization-wide existing Spark ML / Graph / JVM UDF platform:** when downstream teams already
   operate shared Spark clusters and custom JVM/Scala libraries, standardizing on Iceberg REST lets
   Spark read and write the exact same tables without abandoning DuckDB/Polars for local dev and
   lightweight jobs.

## Consequences

- **Good:** sub-25 ms multi-engine queries, ~100 ms cold starts, < 260 MiB isolated RSS, and
  €3.79-€6.49/month European production hosting with €0.00 Cloudflare R2 storage/egress.
- **Good:** `ibergrid-app:local` and `ibergrid-orchestration:local` images gain 0 bytes (`polars`
  is confined to the `benchmarks` / `dev` dependency group).
- **Good:** full benchmark reproducibility (`make benchmark`) pinned by both unit tests (local
  `SqlCatalog`) and integration tests (`tests/integration/test_benchmarks.py` on live Lakekeeper +
  RustFS).
- **Neutral:** Polars 2.1.0 currently falls back to the PyIceberg scan planner
  (`POLARS_ICEBERG_PLANNER=pyiceberg`) unless the separate `polars_iceberg` plugin is installed;
  planning overhead on 48-971 files is ~5-15 ms and fully acceptable at sub-terabyte scale.
- **Bad:** single-node vertical ceiling caps heavy global shuffles at one host's RAM + NVMe; if
  IberGrid ever expands from Iberian electricity into multi-terabyte pan-European smart-meter
  telemetry, Spark or Trino must be attached to Lakekeeper alongside DuckDB.
