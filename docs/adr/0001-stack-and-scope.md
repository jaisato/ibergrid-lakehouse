# 0001. Stack and scope

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

IberGrid Lakehouse ingests public Iberian electricity data (Red Eléctrica's REData API and OMIE
day-ahead market files) and serves it for analysis. The data is small (well under a terabyte over
many years), arrives as daily batches and as frequent updates, and has to be cheap to run: a laptop
for development and a single small server plus object storage in production.

Which table format, catalog, storage, engines, orchestrator and broker should the project use, and
what is deliberately out of scope?

## Decision drivers

- Open formats and open protocols: any engine should be able to read the tables.
- Several engines over the same tables, without a JVM in the default path.
- Runs locally with `docker compose` and on one server; cost scales with data, not with clusters.
- Batch and streaming ingestion into the same tables.
- Every component maintained and pinnable by version and digest.
- Different from the owner's other project, streamhouse, which already covers Spark, Delta Lake
  and Airflow.

## Considered options

- Apache Iceberg + Lakekeeper (REST catalog) + RustFS/R2 + DuckDB, Polars and PyIceberg +
  Dagster + Redpanda
- Apache Spark + Delta Lake + Airflow
- Apache Iceberg with Apache Polaris as the REST catalog
- MinIO as the local object storage
- Plain Parquet files with a naming convention, without a table format

## Decision outcome

Chosen option: **Apache Iceberg + Lakekeeper + RustFS/R2 + DuckDB, Polars and PyIceberg +
Dagster + Redpanda**, because it is the smallest stack that keeps the tables open to every engine
and runs without a JVM.

- **Apache Iceberg** tables: ACID commits, schema and partition evolution, time travel and
  snapshot-based maintenance on object storage, with readers in DuckDB, Polars, PyIceberg, Trino,
  Spark and more.
- **Lakekeeper** as the Iceberg REST catalog: a single Rust binary on Postgres, with a management
  API for warehouses and storage credentials, and S3-compatible storage support (including
  Cloudflare R2).
- **RustFS** for local S3-compatible storage and **Cloudflare R2** in production: the same S3 API
  in both, and R2 has no egress fees for readers.
- **DuckDB, Polars and PyIceberg** as engines: in-process and fast on sub-terabyte data. PyIceberg
  talks to the REST catalog, Polars scans Iceberg tables through it, and DuckDB has its own Iceberg
  extension with REST catalog support.
- **Dagster** (from phase 3) for asset-based orchestration with partitions, backfills and checks.
- **Redpanda** as the Kafka-compatible broker: one binary, no JVM, no ZooKeeper, small enough for
  a single node.

### Consequences

- Good: the tables are portable; adding an engine (Trino, Spark) later needs no data migration.
- Good: the phase 0 stack idles at about 320 MB of memory (limits add up to 3.2 GB, mostly
  Redpanda's 1 GB reservation) and starts in under a minute.
- Good: production storage is pay-per-use object storage instead of a cluster.
- Bad: single-node engines cap the data size and concurrency; fine for this data, not for large
  multi-tenant workloads.
- Bad: Lakekeeper is younger than Hive Metastore or Polaris; versions are pinned and upgrades
  need their migration step (`catalog-migrate`).
- Bad: without an identity provider the catalog API is unauthenticated; it stays off the public
  network until authentication lands (phase 6).
- Neutral: the CLI talks to Lakekeeper's management API, so a catalog change would touch
  `ibergrid.catalog_admin` and the compose files, not the tables.

## Pros and cons of the other options

### Spark + Delta Lake + Airflow

- Good: mature, the industry default for large lakehouses.
- Bad: JVM services and a much larger footprint for sub-terabyte data.
- Bad: the owner's streamhouse project already demonstrates this stack; repeating it adds little.

### Apache Polaris as the REST catalog

- Good: an Apache project, implements the same Iceberg REST specification.
- Bad: a heavier JVM service with more moving parts to run on a small server.

### MinIO for local storage

- Bad: discontinued as an open-source distribution; its images stopped being published (they
  return 404 since 2026-11-02), so it cannot be pinned and updated. RustFS offers the same S3 API.

### Plain Parquet files

- Good: simplest possible storage, every engine reads it.
- Bad: no atomic commits, schema evolution, snapshots or concurrent writers; each consumer would
  reimplement table semantics with file naming conventions.
