# 0007. Medallion Silver and Gold layers with dbt Core, DuckDB and PyIceberg WAP

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

Phases 1 and 2 populated three Bronze Iceberg tables across 2023-01-01 to 2026-11-21:
`bronze.redata_observations` (1,689,319 batch rows across four REData widgets and up to six
electric systems, [ADR 0003](0003-redata-ingestion.md)), `bronze.omie_day_ahead_prices` (64,131
rows spanning OMIE's hourly v1 and quarter-hourly v2 market contracts,
[ADR 0004](0004-omie-ingestion.md)), and `bronze.redata_demand_stream` (append-only 5-minute
national real-time demand events from Redpanda, [ADR 0006](0006-streaming-lane.md)).

Bronze stores what each source delivered, with five analytical hazards that downstream consumers
must never have to re-solve:

1. **Two engines, one Write-Audit-Publish contract.** [ADR 0002](0002-iceberg-write-and-read-paths.md)
   measured that DuckDB 2.0.1 reads Iceberg via Lakekeeper but cannot write to Iceberg branches or
   guard `main` with snapshot-id assertions. How does dbt Core + `dbt-duckdb` build and test Silver
   and Gold models without bypassing WAP?
2. **Batch and stream reconciliation.** `bronze.redata_demand_stream` is append-only with revisions
   and epoch resets (`stream reset`), while `bronze.redata_observations` (`demand_realtime`) is
   authoritative for closed Madrid days. How does Silver merge them deterministically?
3. **Peninsular vs non-peninsular isolation and Canarias timestamps.** Non-peninsular systems
   (Baleares, Canarias, Ceuta, Melilla) are not part of the MIBEL peninsular day-ahead market, and
   Canarias' REData timestamps are probably Canarian wall-clock times stamped with peninsular
   offsets (ADR 0003). How do Silver and Gold enforce separation structurally?
4. **Hourly vs quarter-hourly market harmonization across DST.** OMIE switched from 60-minute (v1)
   to 15-minute (v2) periods on 2025-10-01, while REData's spot price switched on 2025-01-01. How
   does Silver build a continuous 23/24/25-hour MIBEL series across both eras without session
   time-zone bugs on DST fall-back days?
5. **Generation totals double-counting and taxonomy.** REData's `generation_daily` delivers
   `Total generation` (`series_id = '10296'`) alongside individual technologies, and `series_type`
   carries the technology display name rather than renewable vs non-renewable class.

## Decision drivers

- **Zero broken data on `main`.** A failing dbt model or data test must abort before any Silver or
  Gold Iceberg branch is published, leaving every `main` snapshot untouched.
- **Cross-table read consistency.** Every Silver/Gold model and dbt test in a run must read one
  consistent snapshot per Bronze table, even while `stream-consumer` or Bronze ingestion commits
  new snapshots concurrently.
- **Keep the app image lean.** `catalog-init`, `doctor`, `ingest` and the stream services must not
  grow by 400 MB of dbt dependencies; dbt lives alongside Dagster in the orchestration image.
- **Offline testability.** The full `dbt build` + WAP publish pipeline must run in unit tests on
  PyIceberg's SQL catalog and `FakeIcebergRest` with no Docker or live APIs.
- **Reproducible, measured numbers.** Every table size, runtime, memory peak and domain invariant
  recorded below comes from running against the real 2023-2026 backfilled stack.

## Considered options

- **Execution & publication path.**
  - (a) **Staging DuckDB + PyIceberg WAP (chosen):** `dbt-duckdb` builds and tests all models in an
    ephemeral local DuckDB file reading snapshot-pinned Bronze Iceberg views; only after 100 % of
    models and tests pass does `ibergrid.transform` export each selected table to Arrow, align it
    strictly to its registered `TableSpec` schema, and publish it into Lakekeeper through
    `ibergrid.lake.wap`.
  - (b) **Direct DuckDB Iceberg writes:** rejected because DuckDB 2.0.1 has no branch writes or
    WAP publish guards (ADR 0002) and a failing downstream dbt test would leave upstream Silver
    models already visible on `main`.
  - (c) **Pure Python / Polars transforms without dbt:** rejected; loses declarative SQL DAGs,
    built-in schema and singular tests, and standardized medallion lineage.
- **Catalog and secret wiring into `dbt-duckdb`.**
  - (a) **Custom `dbt-duckdb` plugin (`ibergrid.transform.plugin`, chosen):** reuses
    `ibergrid.lake.duck.connection_config` and `duck.configure` in memory (prepared temporary S3
    secret, baked extensions, `ACCESS_DELEGATION_MODE 'none'`), keeps `profiles.yml` free of
    credentials, sets `GLOBAL TimeZone = 'UTC'` across all child cursors, and creates
    snapshot-pinned `bronze_input.*` views (or empty typed tables for 0-snapshot Bronze tables).
  - (b) **Templated secrets and `ATTACH` in `profiles.yml`:** rejected because it duplicates
    `duck.py` configuration, cannot handle 0-snapshot Bronze tables gracefully, and risks leaking
    secrets in rendered dbt artifacts.
- **Dagster granularity.**
  - (a) **Single subsettable `@multi_asset` (`medallion_models`, chosen):** all 5 Silver and 4 Gold
    tables share one `dbt build` invocation when materialized together (pool `transform`, limit 1),
    while subset selection runs `dbt build --indirect-selection=cautious --select +<target>` and
    publishes only the selected tables.
  - (b) **Nine separate unpartitioned Dagster assets:** rejected because materializing the full
    medallion would run `dbt build` nine times in separate subprocesses.

## Measurements

Measured on 2026-11-21 inside the `transform` container (`ibergrid-orchestration:local`,
`dbt-core==1.12.7`, `dbt-duckdb==1.11.1`, `duckdb==2.0.1`, `pyiceberg==0.12.0`, `threads=1`)
against the real backfilled 2023-01-01..2026-11-21 stack (Lakekeeper 0.15.1 + RustFS 1.0.6):

### Container image sizes (`docker image inspect`)

| Image | Size | Change from phase 2 |
|---|---|---|
| `ibergrid-app:local` | 813,060,692 bytes (813 MB) | 0 bytes (dbt stays out of `app`) |
| `ibergrid-orchestration:local` | 1,438,789,329 bytes (1.44 GB) | +401 MB (`dbt-core`, `dbt-duckdb`, `dbt/`) |

### Full 3.9-year materialization (`2023-01-01..2026-11-21`)

Input Bronze snapshots: `redata_observations` (`3461553927514407468`, 1,689,319 rows),
`omie_day_ahead_prices` (`1723416405280473062`, 64,131 rows), `redata_demand_stream`
(`4092455876848438779`, 2,671 rows).

- **Cold run (empty Silver/Gold tables, append onto audit branches):** 25.85 s total
  (`dbt build`: 7.84 s for 12 models + 87 passing tests; 9 WAP Iceberg publishes: 18.0 s);
  peak RSS **703.4 MiB**.
- **Warm idempotent rerun (same process, full `AlwaysTrue()` overwrite via WAP):** 22.84 s total
  (`dbt build`: 4.52 s; 9 WAP overwrites: 18.3 s); peak RSS **917.6 MiB**; identical row counts on
  all 9 tables.

| Table | Partitioning | Rows | Parquet files | Data bytes | Publish time |
|---|---|---:|---:|---:|---:|
| `silver.demand_national_5min` | `month(ts_utc)` | 409,248 | 48 | 6,990,998 | 2.85 s |
| `silver.demand_system_hourly` | `month(ts_utc)` | 170,400 | 48 | 1,316,459 | 2.05 s |
| `silver.mibel_prices_period` | `month(delivery_date)` | 64,131 | 47 | 1,538,425 | 2.05 s |
| `silver.mibel_prices_hourly` | `month(hour_utc)` | 34,104 | 48 | 1,301,651 | 2.36 s |
| `silver.generation_daily` | `month(generation_date)` | 68,207 | 47 | 1,342,989 | 2.49 s |
| `gold.mibel_hourly_market` | `month(hour_utc)` | 34,104 | 48 | 1,673,699 | 2.43 s |
| `gold.mibel_daily_summary` | Unpartitioned | 1,421 | 1 | 134,477 | 0.75 s |
| `gold.system_demand_daily` | Unpartitioned | 7,100 | 1 | 397,768 | 0.83 s |
| `gold.optimal_consumption_windows` | Unpartitioned | 576 | 1 | 29,052 | 0.99 s |
| **Total (9 tables)** | | **789,291** | **289** | **14.73 MB** | **16.8 s** |

### Empirical domain findings verified on the backfilled dataset

- **Generation taxonomy completeness:** All 17 `series_id` values present across 1,420 days and 6
  systems in `bronze.redata_observations` (`generation_daily`) are seeded in
  `dbt/seeds/generation_technologies.csv`. Across all 8,520 `(day, system)` groups,
  `max(abs(reported_total_mwh - sum(technology_mwh))) == 0.0` (exact zero discrepancy). Coal
  (`10289`) had 10 negative net-generation days down to `-120.0 MWh` (auxiliary consumption).
- **OMIE vs REData cross-source spot reconciliation:** Across all **34,080** matched UTC hours
  (2023-01-01..2026-11-20, spanning hourly and 15-minute eras),
  `max(abs(omie_redata_diff_eur_mwh)) == 0.0` (exact zero).
- **Batch + stream reconciliation:** `silver.demand_national_5min` resolved 408,960 rows from
  `batch` (`2023-01-01..2026-11-20`, all 1,420 closed Madrid days) and 288 rows from `stream`
  (`2026-11-21`, open day).
- **DuckDB child-cursor timezone trap (discovered during testing):** Executing `SET TimeZone =
  'UTC'` on a DuckDB parent connection does **not** propagate to `conn.cursor()`, which reverts to
  the host OS time zone (`Europe/Madrid`), causing DST fall-back hour bucketing mismatches.
  `SET GLOBAL TimeZone = 'UTC'` in `configure_connection` plus `SET TimeZone = 'UTC'` in
  `configure_cursor` eliminated the drift completely.
- **Annual MIBEL evolution (`gold.mibel_daily_summary`):**

| Year | Days | ES mean (€/MWh) | ES min (€/MWh) | Zero/neg ES hours | Decoupled hours | Peninsular renewable % | Solar % |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023 | 365 | 87.10 | 0.00 | 109 | 464 | 52.90 % | 16.73 % |
| 2024 | 366 | 63.03 | -2.00 | 784 | 547 | 58.86 % | 19.33 % |
| 2025 | 365 | 65.28 | -15.00 | 797 | 938 | 57.59 % | 20.66 % |
| 2026 (to Nov 21) | 325 | 88.55 | -9.82 | 950 | 1,100 | 58.43 % | 24.74 % |

- **Load-shifting windows (`gold.optimal_consumption_windows`):** On January weekdays the four
  cheapest hours are overnight (`02:00–05:00` Madrid, €57.17–€61.84/MWh, 0 % zero/negative); on
  April weekdays they shift to solar midday (`14:00–17:00` Madrid, €9.29–€11.98/MWh, 44–64 %
  zero/negative hours).
- **Isolated systems (`gold.system_demand_daily`):** Peninsular averages 650.4 GWh/day (56.91 %
  renewable, `ts_utc_verified = TRUE`); Canarias 24.6 GWh/day (20.51 %, `ts_utc_verified = FALSE`);
  Baleares 17.2 GWh/day (13.87 %); Melilla 0.60 GWh/day (2.48 %); Ceuta 0.55 GWh/day (0.0 %). On
  the 2025-04-28 Iberian blackout, peninsular demand fell 44 % to 364.2 GWh with 3 decoupled hours
  and 8 zero/negative ES hours while non-peninsular demand (39.1 GWh) stayed strictly isolated.

## Decision outcome

### Medallion layers and contracts

All 9 Silver and Gold tables are declared as `TableSpec`s in `ibergrid.lake.registry` (created by
`ibergrid lake init`) and built by the `dbt/` project (`dbt_project.yml`, `profiles.yml`,
`macros/generate_schema_name.sql`, `seeds/generation_technologies.csv`):

- **Staging views (`staging.*`, internal to staging DuckDB):** `stg_redata_observations`,
  `stg_omie_day_ahead_prices`, and `stg_redata_demand_stream` (deduplicated per `(series_id,
  ts_utc)` by highest `(coalesce(stream_epoch, 0), kafka_offset)` per ADR 0006).
- **Silver Iceberg tables (`silver.*`):**
  1. `silver.demand_national_5min`: batch wins on closed Madrid days (all 276/288/300 Real points
     present); deduplicated stream supplies open/partial days.
  2. `silver.demand_system_hourly`: hourly demand per electric system tagged with `system_scope`
     (`peninsular` vs `non_peninsular`) and `ts_utc_verified` (`FALSE` for `canarias`).
  3. `silver.mibel_prices_period`: native OMIE v1/v2 periods with ES-PT spread, decoupling and
     zero/negative flags.
  4. `silver.mibel_prices_hourly`: UTC 1-hour bucketed MIBEL series across v1 and v2 enriched with
     REData peninsular PVPC, spot price and `omie_redata_diff_eur_mwh`.
  5. `silver.generation_daily`: daily generation per system and technology excluding `10296`
     (`Total generation`) from technology rows while carrying `reported_total_mwh`.
- **Gold Iceberg tables (`gold.*`):**
  1. `gold.mibel_hourly_market`: peninsular-only hourly market mart with 1-hour gap-guarded demand
     and price ramps, intraday price rank, evening ramp window flag (17:00–22:00 Madrid), daily
     peninsular renewable/solar/wind shares, `source_attribution` and `updated_at_utc`.
  2. `gold.mibel_daily_summary`: daily MIBEL executive summary with `peninsular_demand_mwh` and
     `non_peninsular_demand_mwh` in separate columns.
  3. `gold.system_demand_daily`: daily demand and renewable mix per electric system.
  4. `gold.optimal_consumption_windows`: `calendar_month x day_type x madrid_hour` price quantile
     and renewable profile with 4-hour optimal load-shifting rank.

### Staging DuckDB + PyIceberg WAP execution (`ibergrid.transform`)

1. `ibergrid.transform.plugin.Plugin` attaches Lakekeeper via `duck.configure`, sets
   `GLOBAL TimeZone = 'UTC'`, and exposes every Bronze table in schema `bronze_input` pinned to its
   `main` snapshot ID (or as an empty typed table from `spec.schema.as_arrow()` when `main` has no
   snapshot yet).
2. `ibergrid.transform.runner.run_medallion` invokes `dbt build` in-process via `dbtRunner` using a
   temporary working directory (`staging.duckdb`, `target/`, `logs/`) so `/opt/ibergrid/dbt` stays
   read-only in containers. Subset runs pass `--indirect-selection=cautious --select +<model>`.
3. Six singular tests (`assert_generation_technologies_mapped`,
   `assert_generation_reconciles_to_total`, `assert_peninsular_isolation`,
   `assert_mibel_hourly_dst_continuity`, `assert_mibel_redata_spot_reconciliation`,
   `assert_stream_batch_reconciliation`) plus 81 schema tests gate publication. Any failure aborts
   before touching Iceberg.
4. Each selected table is read from the staging DuckDB to Arrow, aligned strictly to
   `spec.schema.as_arrow()`, and published via `wap.start` -> `wap.write` (append on a first empty
   table, `AlwaysTrue()` overwrite thereafter) -> single-column branch row-count audit ->
   `wap.publish`, retrying `PublishConflict` up to 3 times and discarding audit branches on any
   failure. Every snapshot summary records `ibergrid.transform.bronze-snapshots` and
   `ibergrid.transform.dbt-tests-passed`.

### CLI, container and Dagster orchestration

- CLI: `ibergrid transform run [--select <layer|table> ...]` (and `make transform`, backed by the
  on-demand `transform` service with a 1,280 MiB limit using the orchestration image).
- Dagster: external `AssetSpec` `bronze/redata_demand_stream` plus `@dg.multi_asset`
  `medallion_models` (`can_subset=True`, pool `transform` with limit 1), job `medallion_transform`
  with `SCHEDULED_RETRIES`, and two daily Madrid schedules at 09:30 (after REData 09:00) and 16:30
  (after OMIE 16:00). Because every WAP branch carries `<dagster_run_id>-transform-...`, the
  existing `sweep_audit_branches` job automatically cleans up dead transform runs too.

## Consequences

- **Good:** All 87 dbt tests run on snapshot-consistent Bronze views before any Silver or Gold
  Iceberg table is written, preserving ADR 0002's WAP guarantee without JVM engines.
- **Good:** Full 3.9-year rebuilds finish in ~25 s and 14.7 MB of Parquet, making full-refresh
  medallion runs simple, deterministic and idempotent without incremental state bugs.
- **Good:** The `app` image stays at 813 MB; `dbt-core` and `dbt-duckdb` live exclusively in the
  `transform` dependency group (included in `orchestration` and `dev`).
- **Bad:** Full-refresh overwrites rewrite all partitions of Silver/Gold tables (~14.7 MB per run),
  creating historical snapshots and unreferenced files until phase 4 adds automated snapshot expiry
  and orphan-file cleanup.
- **Bad:** The orchestration image grows from 1.04 GB to 1.44 GB (+401 MB) due to dbt Core and its
  transitive parser/MetricFlow packages.
- **Neutral:** If REE introduces a new generation technology `series_id`, `dbt build` fails loudly
  on `assert_generation_technologies_mapped` until `dbt/seeds/generation_technologies.csv` is
  updated; this is intentional so technology shares never silently drift below 100 %.
