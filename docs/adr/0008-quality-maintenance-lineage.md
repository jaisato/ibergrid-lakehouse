# 0008. Quality gates, Iceberg maintenance, OpenLineage and data chaos proof

Date: 2026-11-21
Status: Accepted

## Context

With batch ingestion (ADRs 0003–0005), the exactly-once streaming lane (ADR 0006) and the dbt
Silver/Gold medallion (ADR 0007) live across 3.9 years of Iberian electricity data
(`2023-01-01`..`2026-11-21`), four production gaps remained:

1. **Declarative WAP quality gates and ACID quarantine.** Bronze connectors checked structure and
   counted anomalies, and dbt ran SQL assertions inside a staging DuckDB, but there was no
   declarative contract layer able to audit arbitrary Iceberg WAP audit branches before
   `wap.publish`, catch OMIE Spain/Portugal column swaps explicitly by verifying decoupled periods,
   or persist rejected records into an ACID quarantine table for forensics.
2. **Zero-scan metadata auditing.** Iceberg Avro manifests already store per-file record counts,
   null/NaN counts and column lower/upper bounds. Reading them through DuckDB's `iceberg_files` and
   `iceberg_column_stats` validates schema/range/freshness invariants with **0 Parquet data bytes
   scanned**.
3. **Storage reclamation.** Neither `expire_snapshots` (PyIceberg) nor `rewrite_data_files`
   (DuckDB) removes unreferenced S3 objects from RustFS or Cloudflare R2 (ADR 0002). Without an
   orphan-file cleanup guarded by a safety window and automated scheduling, object storage grows
   monotonically.
4. **Lineage observability and reproducible fault proof.** Pipeline runs needed standard
   OpenLineage v2 events (inputs, outputs, Iceberg snapshots, row counts and quality assertions)
   without forcing an extra database/service into the local stack, plus a reproducible data chaos
   suite (`make chaos-data`) proving 100 % WAP blocking and quarantine under deterministic
   corruption scenarios.

## Decision

### 1. Zero-scan Iceberg metadata checks (`ibergrid.quality.metadata`)

- `inspect_table_metadata(con, table, *, snapshot_id=None)` and
  `audit_table_metadata(con, table, rule, *, snapshot_id=None)` query ONLY DuckDB 2.0.1's
  `iceberg_snapshots`, `iceberg_refs`, `iceberg_files` and `iceberg_column_stats` table functions
  with `snapshot_from_id => $id`, so both `main` and open WAP audit branches are audited with
  `data_bytes_scanned == 0`.
- Column lower/upper bounds across multi-file tables are parsed according to `column_type` before
  computing global min/max (numeric `float` parsing for `DOUBLE`/`INTEGER`/`BIGINT`, never lexical
  `MIN`/`MAX` on VARCHAR strings where `"9.5" > "100.0"`).
- Default domain rules (`TABLE_METADATA_RULES`) enforce non-null keys and harmonised MIBEL limits
  (`-500..4000 EUR/MWh`, `demand > 0`, `hours_in_day` in `23..25`).

### 2. Soda Core v4 declarative contracts and ACID quarantine

- **Soda v4 (`soda-core==4.29.0` + `soda-duckdb==4.29.0`) over legacy Soda 3.x.** Legacy
  `soda-core-duckdb 3.x` pins `opentelemetry-api<1.23` and conflicts with `dbt-core==1.12.7`
  (`>=1.26`). Soda v4 resolves cleanly alongside `duckdb==2.0.1`, `dbt-core==1.12.7`,
  `dbt-duckdb==1.11.1`, `dagster==1.13.31` and `pyiceberg==0.12.0` with zero downgrades, and
  exposes `DuckDBDataSourceImpl.from_existing_cursor(con, "ibergrid")` so contracts run directly
  over an existing Iceberg-attached DuckDB session.
- **Image boundary preserved.** `soda-core` and `soda-duckdb` live in the `quality` dependency
  group included in `orchestration` (and `dev`), keeping `ibergrid-app:local` unchanged at
  **813 MB** (zero Soda, dbt or Dagster bytes). `ibergrid-orchestration:local` grows by ~20 MB to
  **1.46 GB** and bakes `contracts/soda/` at `/opt/ibergrid/contracts/soda`.
- **Five declarative YAML contracts (`contracts/soda/*.yml`):**
  - `bronze_omie_day_ahead_prices.yml`: strict 14-column schema, MIBEL price limits
    (`-500..4000`), duplicate `(delivery_date, period)` check and DST 23/24/25h (92/96/100
    quarter-hour) continuity.
  - `bronze_redata_observations.yml`: strict 16-column schema, valid electric system & granularity,
    duplicate key check, positive demand check and market price range check.
  - `silver_mibel_prices_hourly.yml`: strict 15-column schema, MIBEL bounds, duplicate `hour_utc`,
    DST hourly continuity, and the **cross-source OMIE ES vs REData peninsular spot reconciliation
    gate** (`abs(price_es_eur_mwh - redata_spot_eur_mwh) <= 0.02 EUR/MWh`), also counting
    `decoupled_periods_verified` so ES/PT column swaps cannot slip past coupled days (ADR 0004).
  - `silver_demand_system_hourly.yml`: strict 8-column schema, `demand_mw > 0`, duplicate key check,
    strict peninsular vs non-peninsular `system_scope` isolation, and the **Canarias
    `ts_utc_verified = FALSE` gate** (ADR 0003).
  - `gold_mibel_hourly_market.yml`: strict 20-column schema, MIBEL bounds, duplicate `hour_utc`,
    DST hourly continuity, positive peninsular demand and required REE/OMIE source attribution.
- **ACID quarantine (`quarantine.rejected_records`):** registered in `ibergrid.lake.registry`
  (partitioned by `month(rejected_at_utc)`). `ibergrid.transform.runner._publish_table` invokes
  `audit_branch_and_quarantine` directly on every staged Silver and Gold WAP audit branch before
  `wap.publish` (Bronze remains "checked, not gated" per ADRs 0003 and 0004 so raw source anomalies
  stay preserved in Bronze). A missing YAML contract for any table in `CONTRACT_MAP` fails closed
  (`FileNotFoundError`) and discards the audit branch rather than publishing with reduced checks.
  Whenever a gate fails, violations and up to 20 sampled offending rows (`record_key` +
  `payload_json`) are appended to `quarantine.rejected_records` via `ibergrid.lake.wap` (retrying
  append-only `PublishConflict`), the audit branch is discarded via `wap.discard`, and
  `QualityGateError` is raised before `wap.publish` is ever reached.

### 3. S3 orphan-file cleanup and automated maintenance (`ibergrid.lake.maintenance`)

- `collect_referenced_paths(table)` traverses `metadata_location`, `metadata_log`, Puffin
  statistics files, `version-hint.text`, and EVERY snapshot in `table.metadata.snapshots` (all
  manifest lists, manifest files and live data/delete files across `main`, tags and open `audit-*`
  branches), normalizing URIs via `PyArrowFileIO.parse_location` so the same logic works on local
  file catalogs, RustFS and Cloudflare R2.
- `remove_orphan_files(table, *, grace_period=timedelta(hours=48), dry_run=True)` closes the
  object-storage TOCTOU race by collecting referenced paths **both before and after** recursively
  listing `table.location()` and protecting their union, fences every path strictly inside
  `<table_location>/`, refuses negative grace periods or naive datetimes, and never deletes files
  whose storage `mtime` is missing or newer than the grace cutoff.
- `maintain_table` combines DuckDB compaction (safely skipped if `dry_run`, empty table, open
  audit branches or `MaintenanceConflict`), PyIceberg `expire_snapshots` (defaults: 7 days, retain
  at least 5 snapshots), and `remove_orphan_files`.
- **CLI and Dagster:** `ibergrid lake maintain [--compact/--no-compact] [--expire-days N]
  [--clean-orphans/--no-clean-orphans] [--dry-run]`, `ibergrid quality check [--metadata-only]
  [--quarantine] [--json]`, on-demand `quality` compose service (`make quality-check`,
  `make maintain`), and Dagster's `lake_maintenance` job scheduled daily at **04:00 Europe/Madrid**
  (outside the 02:00–03:00 DST gap/repeat window and before daily ingestion).

### 4. OpenLineage v2 observability (`ibergrid.lineage`)

- Emits spec-compliant OpenLineage 2-0-2 JSON `START`, `COMPLETE` and `FAIL` RunEvents carrying
  `schema`, `dataSource`, `outputStatistics`, `dataQualityAssertions`, `ibergridSnapshot` and
  `errorMessage` facets.
- Always logs the complete OpenLineage v2 RunEvent payload (`openlineage_event`) via `structlog`
  so default log-only deployments preserve full dataset, snapshot, assertion and error provenance;
  optionally appends compact JSONL to `IBERGRID_LINEAGE_SINK_PATH`, and optionally POSTs to
  `IBERGRID_LINEAGE_ENDPOINT_URL/api/v1/lineage` (e.g. Marquez) with non-fatal error handling so an
  observability outage never aborts a data commit. Zero extra containers are required by default.
- Wired into `ibergrid.transform.runner.run_medallion` and `ibergrid.orchestration.maintenance`.

## Measured results (2026-11-21)

### Live 3.9-year dataset quality audit (`2023-01-01`..`2026-11-21`, RustFS + Lakekeeper)

| Table | Rows | Zero-scan metadata (ms / data bytes) | Full Soda + metadata (checks / ms) | Key domain proof |
|---|---:|---:|---:|---|
| `bronze.redata_observations` | 1,690,379 | 1,408.6 ms / **0 B** | 19 / 19 (3,275.1 ms) | 0 duplicate keys, all demands > 0 |
| `bronze.omie_day_ahead_prices` | 64,131 | 150.8 ms / **0 B** | 23 / 23 (1,126.1 ms) | All 1,421 days pass v1/v2 DST period continuity |
| `silver.demand_system_hourly` | 170,400 | 141.8 ms / **0 B** | 16 / 16 (177.8 ms) | Strict peninsular isolation; Canarias `ts_utc_verified = FALSE` |
| `silver.mibel_prices_hourly` | 34,104 | 149.8 ms / **0 B** | 22 / 22 (263.4 ms) | **3,049 real decoupled ES/PT hours** verified <= 0.02 EUR/MWh vs REData spot |
| `gold.mibel_hourly_market` | 34,104 | 172.4 ms / **0 B** | 24 / 24 (286.3 ms) | 100 % DST continuity + REE/OMIE attribution |

### Maintenance dry-run on backfilled stack

- `ibergrid lake maintain --dry-run` scanned all **13 registered tables** (**3,056 S3 objects**
  in RustFS: 2,002 in `bronze.redata_observations`, 255 in `bronze.omie_day_ahead_prices`, 85 in
  `bronze.redata_demand_stream`, 551 across 5 Silver tables, 162 across 4 Gold tables, 1 in
  `quarantine.rejected_records`) in ~3 s with **0 orphan files**.

### Reproducible Data Chaos Proof (`make chaos-data`)

Executed in a throwaway compose project (`ibergrid-chaos-data`, subnets `10.214.0.0/16`, torn down
in `finally:` without touching the backfilled `ibergrid` volumes):

| # | Injected fault scenario | Target table | Gate(s) triggered | Blocked before `wap.publish` | `main` unchanged | Audit branches left |
|---|---|---|---|---|---|---|
| 1 | Duplicate `(delivery_date, period)` key | `bronze.omie_day_ahead_prices` | `soda.no_duplicate_values`, `dst_period_count_continuity` | **Yes** | **Yes** | 0 |
| 2 | OMIE ES/PT column swap on decoupled blackout day `2025-04-28` | `silver.mibel_prices_hourly` | `cross_source_omie_redata_spot_reconciliation` | **Yes** | **Yes** | 0 |
| 3 | Canarias tagged `peninsular` + `ts_utc_verified = TRUE` | `silver.demand_system_hourly` | `peninsular_non_peninsular_isolation`, `canarias_unverified_timestamp_flag` | **Yes** | **Yes** | 0 |
| 4 | Missing DST hour (`22h` on 23h spring-forward day `2025-03-30`) | `gold.mibel_hourly_market` | `dst_hourly_continuity` | **Yes** | **Yes** | 0 |
| 5 | Out-of-range MIBEL price (`9999.0 EUR/MWh`) | `bronze.omie_day_ahead_prices` | `metadata.bounds.price_es_eur_mwh` (**0 data bytes scanned**), `soda.invalid.price_es_eur_mwh` | **Yes** | **Yes** | 0 |

Result: **5/5 (100 %) WAP blocking rate** in 35.8 s, persisting **47 sampled forensic records**
across 8 rule groups in `quarantine.rejected_records` (with `record_key` and `payload_json`).

## Consequences

- Corrupt batches (duplicates, ES/PT swaps, peninsular/Canarias leaks, DST gaps, range breaches)
  never reach `main` readers and leave an auditable SQL-queryable record in
  `quarantine.rejected_records`.
- Zero-scan Iceberg metadata checks run in ~150 ms per table with 0 data bytes scanned and feed the
  Phase 5 benchmark comparison against full SQL scans.
- `ibergrid-app:local` stays lean at 813 MB; Soda Core v4 and its contracts stay confined to
  `ibergrid-orchestration:local` (1.46 GB).
- OpenLineage v2 events are emitted natively on every transform and maintenance run without adding
  an always-on Marquez container to the single-server memory budget.
