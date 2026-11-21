# Architecture

The stack runs the storage, catalog and broker layers and the CLI that wires them together; the
lake library (`ibergrid.lake`) writes and reads Iceberg tables on them, and the phase-1 connectors
(`ibergrid.sources.redata` and `ibergrid.sources.omie`) ingest REData and OMIE into Bronze
through it. Dagster and streaming build on these next. The decisions behind the stack are in
[ADR 0001](adr/0001-stack-and-scope.md); who writes and who reads Iceberg, and how
Write-Audit-Publish works, in [ADR 0002](adr/0002-iceberg-write-and-read-paths.md); how REData is
ingested, in [ADR 0003](adr/0003-redata-ingestion.md), and OMIE, in
[ADR 0004](adr/0004-omie-ingestion.md).

## System context (C4 level 1)

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

## Containers (C4 level 2)

```mermaid
C4Container
  title IberGrid Lakehouse - containers (C4 level 2, phase 1)

  Person(analyst, "Data engineer / analyst")

  System_Boundary(stack, "Docker Compose project ibergrid") {
    Container(app, "ibergrid CLI", "Python 3.12, PyIceberg, DuckDB, app image", "catalog-init bootstraps the catalog; doctor checks readiness and runs lake commands; ingest pulls REData and OMIE")
    Container(catalog, "catalog", "Lakekeeper 0.15.1", "Iceberg REST catalog and management API")
    ContainerDb(catalogdb, "catalog-db", "Postgres 17", "Catalog state and encrypted storage credentials")
    Container(broker, "broker", "Redpanda 26.2", "Kafka API for streaming ingestion (topics from phase 2)")
    ContainerDb(storage, "storage", "RustFS 1.0.6", "S3-compatible bucket, local profile only")
  }

  System_Ext(r2, "Cloudflare R2", "Object storage in production")
  System_Ext(ree, "Red Electrica REData API", "Public JSON:API, no token")
  System_Ext(omie, "OMIE", "Daily marginalpdbc files, public downloads, no token")

  Rel(analyst, catalog, "Iceberg REST", "HTTP")
  Rel(app, ree, "ingest redata: at most 1 request/s", "HTTPS")
  Rel(app, omie, "ingest omie: one file per delivery day, at most 1 request/s", "HTTPS")
  Rel(app, catalog, "Bootstrap, warehouse, Iceberg REST, /health", "HTTP")
  Rel(app, storage, "Table data and landed raw answers and files, with its own keys; HeadBucket", "S3 API")
  Rel(app, broker, "TCP readiness", "Kafka port")
  Rel(catalog, catalogdb, "Reads and writes", "SQL")
  Rel(catalog, storage, "Validates and manages the warehouse", "S3 API")
  Rel(catalog, r2, "Same role in production", "S3 API")
```

## Service inventory

| Container | Image (pinned by digest in `compose.yaml`) | Purpose | Networks | Local ports (127.0.0.1) |
|---|---|---|---|---|
| `storage` | `rustfs/rustfs:1.0.6` | S3 object storage; `local-storage` profile only | storage | 29000 (S3), 29001 (console at `/rustfs/console/`) |
| `storage-init` | `amazon/aws-cli:2.41.2` | One-shot: create the bucket (an existing one is fine) | storage | |
| `catalog-db` | `postgres:17-alpine` | Lakekeeper state; encrypted storage credentials | catalog-db (internal) | |
| `catalog-migrate` | `quay.io/lakekeeper/catalog:v0.15.1` | One-shot: `lakekeeper migrate` | catalog-db | |
| `catalog` | `quay.io/lakekeeper/catalog:v0.15.1` | `lakekeeper serve`: Iceberg REST catalog and management API | catalog-db, catalog, storage | 28181 |
| `catalog-init` | `ibergrid-app` (this repository) | One-shot: `ibergrid catalog bootstrap` | catalog | |
| `broker` | `docker.redpanda.com/redpandadata/redpanda:v26.2.2` | Kafka API, single node, dev-container mode | broker | 29092 (Kafka), 29644 (admin) |
| `doctor` | `ibergrid-app` (this repository) | On demand (scale 0): `ibergrid doctor`; also runs `ibergrid lake ...` | storage, catalog, broker | |
| `ingest` | `ibergrid-app` (this repository) | On demand (scale 0): `ibergrid ingest ...`, with a 384 MiB memory limit | storage, catalog | |

Start-up order comes from `depends_on`: `storage` healthy, then `storage-init`; `catalog-db`
healthy, then `catalog-migrate`, then `catalog` healthy; then `catalog-init`. `docker compose up
--wait` returns once every long-running service is healthy and every one-shot has exited 0.

## Design notes

- **Least connectivity.** Each network groups the services that must talk to each other.
  Postgres sits on an `internal` network with no egress; only Lakekeeper reaches it. The catalog
  joins `storage` because it validates the warehouse location and manages files there.
- **Hardening.** Every container, one-shots included, runs with `no-new-privileges`, all
  capabilities dropped, a memory limit and a non-root user: RustFS as `rustfs` (10001), the AWS
  CLI as `nobody` (65534), Postgres as `postgres` (70), Lakekeeper as distroless `nonroot`
  (65532, set in compose because the image declares no user), Redpanda as `redpanda` (101) and
  the app as 10001.
- **Secrets.** Compose reads them from `.env` with `${VAR:?}` and no defaults, so a missing secret
  stops `docker compose` before anything starts. The app keeps them as `SecretStr` and its
  validation errors never echo input values.
- **Storage readiness.** `ibergrid doctor` sends a HeadBucket through pyarrow's S3 client
  instead of RustFS's anonymous `/health/ready`: the same check proves the endpoint, the keys and
  the bucket on RustFS and on Cloudflare R2, which has no health endpoint.
- **Lake access ([ADR 0002](adr/0002-iceberg-write-and-read-paths.md)).** PyIceberg writes,
  always to a run's audit branch, and publishes; DuckDB reads, queries metadata and compacts.
  Lakekeeper hands every client the warehouse's in-network endpoint (`http://storage:9000`) and
  no credentials, and the warehouse has remote signing off, so each client applies its own
  `IBERGRID_S3_*` endpoint and keys: the same code runs in containers and from the host. The app
  image bakes DuckDB's extensions at build time and never downloads them at runtime.
- **Ingestion ([ADR 0003](adr/0003-redata-ingestion.md)).** `ibergrid ingest redata` runs in
  the `ingest` one-shot, whose networks have egress to the public API. Per chunk (a dataset, a
  system and a calendar month or year of local days) it fetches, checks the structure, lands the
  raw answer under `landing/` in the bucket (outside the warehouse prefix) and publishes the rows
  to `bronze.redata_observations` through Write-Audit-Publish, replacing exactly that window. It
  has a 384 MiB memory limit, since a month of five-minute demand peaks at 238 MiB; the other app
  one-shots keep 192 MiB.
- **OMIE ([ADR 0004](adr/0004-omie-ingestion.md)).** `ibergrid ingest omie` runs in the same
  one-shot. Per chunk (a calendar month of delivery days) it downloads one file per day, checks
  each against its contract version (hourly until 2025-09-30, quarter-hourly from 2025-10-01),
  lands the original files under `landing/omie/` and publishes the rows to
  `bronze.omie_day_ahead_prices` through Write-Audit-Publish, replacing exactly those delivery
  days. It stops, with exit 0, at the first day OMIE has not published yet. A month peaks at
  114 MiB.
- **Catalog URI per client.** `LAKEKEEPER__BASE_URI` is left unset, so `/catalog/v1/config`
  advertises the address each client used: `http://127.0.0.1:28181/catalog` from the host and
  `http://catalog:8181/catalog` from containers.
- **No authentication yet (catalog and broker).** Lakekeeper runs without an identity provider, so
  its management API is open to whoever reaches port 8181. Redpanda runs in dev-container mode
  with unauthenticated Kafka and admin APIs. Locally both listen on 127.0.0.1 only; in production
  no port is published. Phase 2 hardens the broker, and authentication is a prerequisite for the
  public endpoints of phase 6.
- **One image for the CLI.** `catalog-init` and `doctor` run the same app image. In production it
  is pulled from GHCR by the digest the release job recorded: the image that job built, ran once
  (`ibergrid version`) and pushed. The server never builds anything.
- **Storage keys live in two places.** `catalog-init` stores the S3 keys in the warehouse when it
  creates it and never updates them. After rotating `IBERGRID_S3_*`, run
  `ibergrid catalog update-credential` (see `.env.example`), which also resets the storage-profile
  fields ibergrid owns and keeps the others: that is how a warehouse created before ADR 0002 gets
  remote signing turned off.

## Environments

| | Local (`make up`) | Production (`compose.prod.yaml`) |
|---|---|---|
| Object storage | RustFS container (`local-storage` profile) | Cloudflare R2, `https://<account-id>.r2.cloudflarestorage.com`, region `auto` |
| App image | Built from source (`compose.override.yaml`) | `ghcr.io/jaisato/ibergrid-lakehouse@sha256:...`, digest in `release.env` |
| Env files | `.env` | `.env` (secrets, never written by the deploy) and `release.env` |
| Host ports | 127.0.0.1, 28xxx-29xxx | None |
| Restart policy | `on-failure` | `unless-stopped` |
| Logs | Docker defaults | `json-file`, 3 x 10 MB per container |
