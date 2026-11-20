# Architecture

Phase 0 runs the storage, catalog and broker layers and the CLI that wires them together. The
pipelines (connectors, Dagster, streaming) build on these services in later phases. The decisions
behind the stack are in [ADR 0001](adr/0001-stack-and-scope.md).

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
  title IberGrid Lakehouse - containers (C4 level 2, phase 0)

  Person(analyst, "Data engineer / analyst")

  System_Boundary(stack, "Docker Compose project ibergrid") {
    Container(app, "ibergrid CLI", "Python 3.12, app image", "catalog-init bootstraps the catalog; doctor checks readiness")
    Container(catalog, "catalog", "Lakekeeper 0.15.1", "Iceberg REST catalog and management API")
    ContainerDb(catalogdb, "catalog-db", "Postgres 17", "Catalog state and encrypted storage credentials")
    Container(broker, "broker", "Redpanda 26.2", "Kafka API for streaming ingestion (topics from phase 2)")
    ContainerDb(storage, "storage", "RustFS 1.0.6", "S3-compatible bucket, local profile only")
  }

  System_Ext(r2, "Cloudflare R2", "Object storage in production")

  Rel(analyst, catalog, "Iceberg REST", "HTTP")
  Rel(app, catalog, "Bootstrap, warehouse, /health", "HTTP")
  Rel(app, storage, "Signed HeadBucket", "S3 API")
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
| `doctor` | `ibergrid-app` (this repository) | On demand (scale 0): `ibergrid doctor` | storage, catalog, broker | |

Start-up order comes from `depends_on`: `storage` healthy, then `storage-init`; `catalog-db`
healthy, then `catalog-migrate`, then `catalog` healthy; then `catalog-init`. `docker compose up
--wait` returns once every long-running service is healthy and every one-shot has exited 0.

## Design notes

- **Least connectivity.** Each network groups the services that must talk to each other.
  Postgres sits on an `internal` network with no egress; only Lakekeeper reaches it. The catalog
  joins `storage` because it validates the warehouse location and manages files there.
- **Hardening.** Every container runs with `no-new-privileges` and all capabilities dropped,
  under a non-root user (Postgres as `postgres`, the AWS CLI as `nobody`, the app as uid 10001),
  with a memory limit.
- **Secrets.** Compose reads them from `.env` with `${VAR:?}` and no defaults, so a missing secret
  stops `docker compose` before anything starts. The app keeps them as `SecretStr` and its
  validation errors never echo input values.
- **Storage readiness.** `ibergrid doctor` sends a SigV4-signed `HEAD /<bucket>` instead of
  RustFS's anonymous `/health/ready`: the same check proves the endpoint, the keys and the bucket
  on RustFS and on Cloudflare R2, which has no health endpoint.
- **Catalog without authentication (for now).** Lakekeeper runs without an identity provider, so
  its management API is open to whoever reaches port 8181. Locally that is 127.0.0.1 only; in
  production the port is not published at all. Authentication is a prerequisite for the public
  endpoints of phase 6.
- **One image for the CLI.** `catalog-init` and `doctor` run the same app image; in production it
  is pulled from GHCR by digest, so what runs is exactly what CI built.

## Environments

| | Local (`make up`) | Production (`compose.prod.yaml`) |
|---|---|---|
| Object storage | RustFS container (`local-storage` profile) | Cloudflare R2, `https://<account-id>.r2.cloudflarestorage.com`, region `auto` |
| App image | Built from source (`compose.override.yaml`) | `ghcr.io/jaisato/ibergrid-lakehouse@sha256:...` |
| Host ports | 127.0.0.1, 28xxx-29xxx | None |
| Restart policy | `on-failure` | `unless-stopped` |
| Logs | Docker defaults | `json-file`, 3 x 10 MB per container |
