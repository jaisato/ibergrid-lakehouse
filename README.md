# IberGrid Lakehouse

An open, multi-engine lakehouse for Iberian electricity data: Red Eléctrica's REData API and OMIE
day-ahead market files, stored as Apache Iceberg tables behind a REST catalog and readable from
DuckDB, Polars and PyIceberg without a JVM.

**Status: phase 1 (batch ingestion) complete.** The stack runs end to end (object storage,
Iceberg REST catalog, broker, Dagster) with CI/CD around it; the lake library writes Iceberg
tables through Write-Audit-Publish and reads them with DuckDB; two connectors ingest Red
Eléctrica's REData API (demand, daily generation mix, prices) into `bronze.redata_observations`
and OMIE's day-ahead marginal prices for Spain and Portugal into `bronze.omie_day_ahead_prices`;
and Dagster runs them as partitioned assets, with daily schedules, backfills from 2023-01-01 and
a sweeper for the audit branches of dead runs. Streaming is next (see [Roadmap](#roadmap)).

## Why

Most of the open lakehouse stack can now run on a laptop and on a single small server:

- **Apache Iceberg** tables give ACID commits, schema evolution and time travel on plain object
  storage, and every major engine reads them.
- A **REST catalog** ([Lakekeeper](https://github.com/lakekeeper/lakekeeper)) decouples the
  engines from storage details, so DuckDB, Polars and PyIceberg can share the same tables.
- The electricity data is public, sub-terabyte and arrives both in batches (daily market files)
  and as frequent updates (system demand, generation mix), which exercises batch and streaming
  ingestion on the same tables.

The choices and the alternatives that were rejected are in
[ADR 0001](docs/adr/0001-stack-and-scope.md).

## Architecture

```mermaid
C4Context
  title IberGrid Lakehouse - system context (C4 level 1)

  Person(analyst, "Data engineer / analyst", "Queries Iceberg tables with DuckDB, Polars or PyIceberg")

  System(lakehouse, "IberGrid Lakehouse", "Ingests, stores and serves Iberian electricity data as Apache Iceberg tables")

  System_Ext(ree, "Red Electrica REData API", "Demand, generation mix, prices and balances (JSON over HTTPS)")
  System_Ext(omie, "OMIE", "Day-ahead market result files")
  System_Ext(r2, "Cloudflare R2", "S3-compatible object storage in production")
  System_Ext(github, "GitHub", "CI, image registry (GHCR) and deploys")

  Rel(analyst, lakehouse, "Reads tables through the Iceberg REST catalog")
  Rel(lakehouse, ree, "Pulls data from", "HTTPS")
  Rel(lakehouse, omie, "Downloads files from", "HTTPS")
  Rel(lakehouse, r2, "Stores table data in", "S3 API")
  Rel(github, lakehouse, "Builds, publishes and deploys", "SSH")
```

The container view (C4 level 2), networks and ports are in [docs/architecture.md](docs/architecture.md).

## Quickstart

Requirements: Docker Engine with Compose v2, GNU Make, and [uv](https://docs.astral.sh/uv/) for
the Python tooling.

```bash
make env     # .env with random local secrets (never overwrites an existing one)
make up      # build the images, start the stack, wait for health checks and one-shots
make smoke   # ibergrid doctor from inside the compose network
docker compose run --rm --no-deps doctor lake init   # create the lake's tables, once
```

The Dagster UI is at <http://127.0.0.1:28300>: pick an asset of the `bronze` group, choose
partitions (one day, a range, or all of them since 2023-01-01) and materialize them: a range is
one run, the selected REData assets in sequence and OMIE in a run of its own
([Orchestration](#orchestration)). The schedules start stopped locally, so
nothing calls REData or OMIE until you do.

The same ingestion runs from the CLI, from inside the compose network:

```bash
docker compose run --rm --no-deps ingest redata --dataset demand_hourly \
  --start 2026-11-20 --end 2026-11-20
docker compose run --rm --no-deps ingest omie --start 2026-11-20 --end 2026-11-20
```

`make smoke` prints one line per dependency and exits non-zero if any is down:

```text
ok    storage  HeadBucket http://storage:9000/ibergrid (2 ms)
ok    catalog  GET http://catalog:8181/health -> 200 (1 ms)
ok    broker   TCP connect to broker:9092
doctor: all checks passed
```

`make help` lists every target; `make down` stops the stack and `make down-volumes` also deletes
its data.

## Lake

`ibergrid.lake` is the library every pipeline writes and reads through; who does what, the
Write-Audit-Publish protocol and the measured engine capabilities are in
[ADR 0002](docs/adr/0002-iceberg-write-and-read-paths.md).

- **PyIceberg writes.** Tables are declared as `TableSpec`s in `ibergrid.lake.registry` (today
  `bronze.redata_observations` and `bronze.omie_day_ahead_prices`) and created by
  `ensure_table`, which also adds new optional columns and refuses any other change. Every write
  goes to a run's audit branch
  (`wap.start`, `wap.write`); the audit reads the snapshot `wap.head` (or `wap.read`) returns, and
  `wap.publish(table, run, expected_head)` moves `main` to exactly that snapshot in one guarded
  commit, optionally with table properties such as stream offsets, or raises `PublishConflict`
  and changes nothing.
- **DuckDB reads.** `duck.connect` attaches the catalog as `lake`; `duck.scan` (main or a
  snapshot), `duck.metadata` and `duck.describe` answer from it.
- **Maintenance.** `maintenance.expire_snapshots` (PyIceberg) and
  `maintenance.rewrite_data_files` (DuckDB compaction, which commits to `main`, so runs open
  across it have to restart).

The CLI wraps the table registry and the metadata queries, from inside the compose network:

```bash
docker compose run --rm --no-deps doctor lake init                  # create the registry's tables
docker compose run --rm --no-deps doctor lake describe <ns>.<table> # snapshot, refs, data files
```

## Ingestion

Clients use their own S3 endpoint and keys (`IBERGRID_S3_*`), so the same code runs from the host
against the published ports, as the integration tests do. Warehouses are created with remote
signing off; one created by an earlier version keeps it on until `make down-volumes && make up`
(local) or `catalog update-credential` (server, see `.env.example`) rewrites the storage-profile
fields this code owns.

### REData

`ibergrid ingest redata` pulls REData into `bronze.redata_observations`, one long table with one
row per point for every widget; [ADR 0003](docs/adr/0003-redata-ingestion.md) has the measured
API limits and the decisions.

| Dataset | REData widget | Points | Electric systems |
|---|---|---|---|
| `demand_realtime` | `demanda/demanda-tiempo-real` | 5 minutes: real, forecast, scheduled | national (the widget's own scope; above the five systems' sum) |
| `demand_hourly` | `demanda/evolucion` | hourly demand | peninsular, canarias, baleares, ceuta, melilla (Canarias' timestamps: see below) |
| `generation_daily` | `generacion/estructura-generacion` | daily generation by technology (REData has no hourly view) | national and the five systems |
| `market_prices` | `mercados/precios-mercados-tiempo-real` | spot price (15 minutes since 2025) and PVPC (hourly) | peninsular |

```bash
docker compose run --rm --no-deps ingest redata --dataset demand_realtime \
  --start 2026-10-01 --end 2026-10-31            # --system <name> (repeatable) narrows it
```

```text
redata: demand_realtime national 2026-10-01..2026-10-31: 35760 rows, 35760/35760 points (100.0 %), snapshot 435293640119925785
redata: 1 chunk(s) published
```

- **Chunks.** Local (Madrid) days, one request and one Write-Audit-Publish run per calendar month
  (per calendar year for daily generation, ending at the last full month: REData answers a long
  daily window that ends mid-month up to the month's end), at most one request a second per
  process, with bounded retries.
- **Idempotent.** Each chunk replaces exactly its dataset, system and UTC window, so a rerun or a
  backfill leaves the same rows. The first failed chunk exits 1; earlier chunks stay published.
- **Checked, not gated.** A structural problem refuses the chunk before anything is written:
  another widget scope than expected, a malformed answer, a value that is not a finite number, a
  repeated key, a point outside the window. Everything else is measured and kept, as Bronze
  should: each line reports expected against received points (DST-aware) with the incomplete
  days, the points that break the dataset's value rule (demand <= 0, prices outside
  -500..4000 EUR/MWh, a share outside 0..1) and the series that were in the window before but
  are missing now; the snapshot summary records the same (`ibergrid.redata.*`). Quality gates
  are phase 4.
- **Raw answers.** Each response body lands at
  `s3://<bucket>/landing/redata/<dataset>/<system>/<YYYY>/<MM>/<start>_<end>-<sha256[:12]>.json`,
  outside the warehouse: one object per window and answer content (the hash leaves out
  REData's cache status, which changes between identical answers). Every row carries that path
  and that hash.
- **Canarias' timestamps are as delivered.** REData stamps every system with peninsular offsets;
  Canarias' load shape suggests Canarian wall-clock times instead, which would put its `ts_utc`
  one hour early. Not verified, so joining Canarias to other systems on `ts_utc` is unsafe for
  now ([ADR 0003](docs/adr/0003-redata-ingestion.md)).
- **Library.** `ibergrid.sources.redata.ingest.ingest(settings, dataset, start, end, systems,
  run_id_prefix)` is what the CLI and the Dagster assets call.

### OMIE

`ibergrid ingest omie` downloads OMIE's daily `marginalpdbc` files (day-ahead marginal prices,
MIBEL) into `bronze.omie_day_ahead_prices`, one row per delivery period with Spain's and
Portugal's prices side by side; [ADR 0004](docs/adr/0004-omie-ingestion.md) has the measurements
and the decisions.

```bash
docker compose run --rm --no-deps ingest omie --start 2025-09-25 --end 2025-10-05
```

```text
omie: 2025-09-25..2025-09-30: 6 file(s) (v1 6), 144 rows, snapshot 5117603404312451457
omie: 2025-10-01..2025-10-05: 5 file(s) (v2 5), 480 rows, snapshot 7983525335044605240
omie: 2 chunk(s) published
```

- **Two contract versions.** Hourly periods (23, 24 or 25 a day, v1) for delivery days up to
  2025-09-30, quarter-hours (92, 96 or 100, v2) from 2025-10-01, SDAC's 15-minute market time
  unit. The delivery date picks the version and the period count must match it; the line grammar
  is strict. A file that breaks its contract refuses its chunk before anything is landed or
  written, so a backfill stops at the first such month, and resumes by rerunning.
- **File versions.** A day's file is `marginalpdbc_<YYYYMMDD>.<N>`: `.1` almost always, and a
  republication replaces it under the next number (`20230528.2`, `20251030.3`, `20251127.2` so
  far). The connector asks for `.1`, then `.2` up to `.5`, and takes the first that exists;
  `source_file` and `file_version` record which. A rerun picks up a republished or rewritten
  file; the Dagster schedules re-ingest the last 7 delivery days for that. Daily files start
  on 2023-01-01 (older years are yearly archives, not read).
- **UTC intervals.** `interval_start_utc` is the Madrid midnight of the delivery day plus
  (period - 1) times the resolution, contiguous on every DST day; `resolution_minutes` and
  `contract_version` travel with each row.
- **Chunks and idempotency.** One download per delivery day (at most one a second) and one
  Write-Audit-Publish run per calendar month, replacing exactly its delivery days; the table is
  partitioned by `month(delivery_date)`, so a rerun never touches another month: a whole-month
  rerun replaces that month's files, and a rerun of some days rewrites the files holding them,
  keeping the other days' rows unchanged. Prices outside -500..4000 EUR/MWh are counted per run,
  not refused.
- **Not published yet.** OMIE answers 404 for a file it does not have (every version of a day
  not out yet: five requests). A delivery day after today (Madrid) ends the ingestion there with
  exit 0 (`not published yet from <day>`; the library reports `not_published_from`), after
  writing the days before it, or exit 1 with `--require-published` (then that day's month chunk
  writes nothing; earlier chunks stay published); a missing day up to today is always an error. Results normally appear the afternoon before, around 13:00-14:30 Madrid time.
- **Original files.** Each lands byte for byte at
  `s3://<bucket>/landing/omie/marginalpdbc/<YYYY>/<MM>/marginalpdbc_<YYYYMMDD>-<sha256[:12]>.<N>`;
  every row carries that path and that hash.
- **Library.** `ibergrid.sources.omie.ingest.ingest(settings, start, end, run_id_prefix,
  require_published=False)`.
- **Cross-check.** REData's spot price equals OMIE's Spanish price, and differs from Portugal's
  where the zones decouple, on the recorded days that can tell them apart (2025-04-28 and
  2025-10-01); on days the zones agree it confirms only the timing. Phase 4 turns it into a gate.

## Orchestration

Dagster runs the connectors as assets; [ADR 0005](docs/adr/0005-dagster-orchestration.md) has the
choice against Airflow, the measurements behind the schedules and the decisions.

| Asset (group `bronze`) | Partitions (Madrid days) | Pool | Schedule (Madrid time) |
|---|---|---|---|
| `bronze/redata_observations/<dataset>`, one per REData dataset | 2023-01-01 to yesterday | `redata` | 09:00, yesterday; re-ingests the 3 days before |
| `bronze/omie_day_ahead_prices` | 2023-01-01 to tomorrow | `omie` | 16:00, tomorrow if out; 21:30, tomorrow required; both re-ingest the last 7 delivery days |

- **Backfills are one run per range.** A run hands its whole partition range to one connector
  call, which cuts it into its own chunks (months; years for daily generation), so a backfill of
  any length never becomes one Write-Audit-Publish run per day. Partitions are recorded as their
  chunks publish: a run that fails part way keeps what it published, and rerunning is safe.
- **Each partition records what snapshot expiry would erase**: for REData the day's expected and
  received points per system and each chunk's rule violations, dropped series, snapshot, run id
  and raw answer; for OMIE the chunk's rows, contract versions, out-of-range prices, snapshot and
  run id.
- **One writer per table, one request a second per source.** Each pool has a limit of 1, checked
  before a run is launched; REData and OMIE runs may overlap.
- **Audit branches of dead runs.** Every Write-Audit-Publish run is named after its Dagster run;
  `sweep_audit_branches` (05:30) removes the `audit-*` branches whose run has finished and keeps
  the others.
- **Schedules start stopped** unless `IBERGRID_ORCHESTRATION_SCHEDULES_RUNNING=true`, the
  production default; start them from the UI to try them locally.
- **Failures.** A scheduled run retries a failed step twice, 15 minutes apart; backfills are not
  retried (a retry redoes the whole range). A failed run shows in the UI, and the trailing windows
  refill missed days; nothing sends notifications yet (phase 6).
- **Runs** execute in `dagster-code`; their metadata, logs and schedule ticks live in
  `orchestration-db` (Postgres), and survive `make down` (not `make down-volumes`).

## Services

One container per service. Ports are published on 127.0.0.1 only, and only in local development.

| Service | Image | Role | Local port |
|---|---|---|---|
| `storage` | `rustfs/rustfs:1.0.6` | S3-compatible object storage (local profile; Cloudflare R2 in production) | [29000](http://127.0.0.1:29000) S3, [29001](http://127.0.0.1:29001/rustfs/console/) console |
| `storage-init` | `amazon/aws-cli:2.41.2` | One-shot: creates the bucket | |
| `catalog-db` | `postgres:17-alpine` | Lakekeeper's database | |
| `catalog-migrate` | `quay.io/lakekeeper/catalog:v0.15.1` | One-shot: catalog schema migrations | |
| `catalog` | `quay.io/lakekeeper/catalog:v0.15.1` | Iceberg REST catalog and management API | [28181](http://127.0.0.1:28181/health) |
| `catalog-init` | app image (`ibergrid catalog bootstrap`) | One-shot: bootstraps the catalog and creates the warehouse, idempotently | |
| `broker` | `redpandadata/redpanda:v26.2.2` | Kafka API, single node | 29092 Kafka, 29644 admin |
| `doctor` | app image (`ibergrid doctor`) | On demand: readiness report (`make smoke`); also runs `lake` commands | |
| `ingest` | app image (`ibergrid ingest`) | On demand: `ingest redata ...`, `ingest omie ...` (384 MiB limit) | |
| `orchestration-db` | `postgres:17-alpine` | Dagster's runs, event logs, schedules | |
| `orchestration-migrate` | orchestration image (`dagster instance migrate`) | One-shot: Dagster's schema | |
| `dagster-code` | orchestration image | Dagster code server; runs execute here | |
| `dagster-webserver` | orchestration image | Dagster UI and GraphQL API | [28300](http://127.0.0.1:28300) |
| `dagster-daemon` | orchestration image | Schedules, backfills, run queue and pools, run monitoring | |

Every third-party image is pinned by tag and digest in [compose.yaml](compose.yaml). The app and
orchestration images are two targets of one [Dockerfile](Dockerfile): the orchestration image is
the app plus Dagster, so the app image stays without it.

## Development

```bash
make sync               # install the locked dependencies (uv.lock)
make ci                 # lint, types, unit tests (>= 90 % branch coverage) and repository checks
make test-integration   # against the running stack (make up first); the Dagster tests that
                        # materialize assets run only on a fresh stack and skip on a backfilled one
make test-live          # opt-in: a few requests to the live REData API and omie.es (never in CI)
```

The unit tests run the lake library on PyIceberg's SQL catalog (sqlite and local files), with no
Docker, and replay recorded REData answers and OMIE files (`tests/fixtures/redata` and
`tests/fixtures/omie`, recorded by `scripts/record_redata_fixtures.py` and
`scripts/record_omie_fixtures.py`) from a local server: they never call the live sources. DuckDB downloads its `iceberg`, `httpfs` and `avro` extensions on first use outside the
app image, which bakes them in at build time.

Branches, commits and pull requests follow [CONTRIBUTING.md](CONTRIBUTING.md). Security reports go
through [SECURITY.md](SECURITY.md).

## Deployment

Pushes to `main` publish the app and orchestration images to `ghcr.io/jaisato/ibergrid-lakehouse`
and `ghcr.io/jaisato/ibergrid-lakehouse-orchestration`. Tags `v*.*.*` rerun CI, publish the
version and deploy it to a single server over SSH: the job copies [compose.yaml](compose.yaml) and
[compose.prod.yaml](compose.prod.yaml), pins both image digests in `release.env` (the secrets in
`.env` are never rewritten), creates the lake's tables (`lake init`, idempotent), runs
`docker compose up -d --wait` and then `ibergrid doctor` ([deploy/remote.sh](deploy/remote.sh)).
The Dagster UI publishes no port there: an SSH tunnel to the webserver's internal address serves
it ([deploy/deploy.env.example](deploy/deploy.env.example)). Production storage is Cloudflare R2. The deploy job stays
skipped until the server's variables exist ([deploy/deploy.env.example](deploy/deploy.env.example)).

## Data sources

Every row keeps a pointer to the raw answer or file it came from; REData's rows also record their
widget and the update date REE reports (`series_last_update`).

- **Red Eléctrica (REE), REData API** (`apidatos.ree.es`): Spanish electricity system data. REE's
  [API page](https://www.ree.es/es/datos/apidatos) says the API lets third parties "retrieve data
  from the REData widgets and use them for your own purposes"; no licence text for the API was
  found. This project's practice: every dataset and every view of it carries "Source: Red
  Eléctrica, REData" and the update date REE reports.
- **OMIE**, day-ahead market result files (`marginalpdbc`, the marginal prices for Spain and
  Portugal, downloaded from `www.omie.es`): Iberian electricity market data, credited as "Source:
  OMIE" with a link to [omie.es](https://www.omie.es). No licence terms are assumed beyond what
  OMIE publishes on its site. Every row records the file it came from, its version and its
  SHA-256.

This project is not affiliated with or endorsed by REE or OMIE.

## Roadmap

Each phase lands as reviewed pull requests; later phases may be reordered as the project evolves.

| Phase | Scope | Status |
|---|---|---|
| 0 | Foundation and risk spike: compose stack, `ibergrid` CLI, CI/CD, docs; the lake library and the measured engine capabilities ([ADR 0002](docs/adr/0002-iceberg-write-and-read-paths.md)) | Done |
| 1 | Batch ingestion: REE REData ([ADR 0003](docs/adr/0003-redata-ingestion.md)) and OMIE ([ADR 0004](docs/adr/0004-omie-ingestion.md)) connectors, orchestrated with Dagster: assets, partitions, schedules, backfills ([ADR 0005](docs/adr/0005-dagster-orchestration.md)) | Done |
| 2 | Streaming lane: Redpanda producer and consumer, dead-letter queue, deduplication, exactly-once into Bronze | Planned |
| 3 | dbt medallion: Silver and Gold models | Planned |
| 4 | Quality and maintenance: WAP quality gates, quarantine, compaction and snapshot expiry, OpenLineage, data chaos tests | Planned |
| 5 | Benchmarks | Planned |
| 6 | Serving (API and Evidence portal), production on a single server with R2 at `ibergrid-lakehouse.jasato.com`, final README | Planned |

## License

[MIT](LICENSE). The data belongs to its publishers and keeps their terms.
