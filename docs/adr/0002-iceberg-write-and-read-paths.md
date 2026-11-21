# 0002. Iceberg write and read paths

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

[ADR 0001](0001-stack-and-scope.md) chose Apache Iceberg behind Lakekeeper, with PyIceberg and
DuckDB as the JVM-free engines. Before any connector writes data, three questions need answers
measured on this stack rather than taken from documentation:

1. Which engine writes Iceberg tables and which reads them?
2. How does a run's output get checked before readers see it (Write-Audit-Publish, WAP), given
   what each engine can do with Iceberg branches?
3. How do clients reach object storage both inside the compose network and from the host (tests,
   developers), when the warehouse's storage endpoint, `http://storage:9000`, resolves only
   inside the network?

## Decision drivers

- One write path with a precise concurrency story: optimistic commits and explicit requirements.
- Readers never see a run that has not been checked.
- The same client configuration in containers, on the host and in production (Cloudflare R2).
- Every capability claim measured, and pinned by a test, so an upgrade that changes one shows up.
- No JVM and no new services.

## Considered options

1. PyIceberg writes and runs WAP; DuckDB reads, answers metadata queries and compacts.
2. DuckDB for everything, writes included.
3. PyIceberg for everything, reads included.
4. Storage access delegated by Lakekeeper (vended credentials or remote signing) instead of each
   client using its own keys.

## Measurements

Lakekeeper 0.15.1 on RustFS 1.0.6, PyIceberg 0.12.0 (with pyarrow 26.0.1 and pyiceberg-core
0.10.1), DuckDB 2.0.1 with its `iceberg`, `httpfs` and `avro` extensions. Measured on 2026-11-21
from the host (published ports) and from the app image inside the compose network. Each row is
pinned by `tests/integration/test_capabilities.py`, `tests/integration/test_lake.py` or a unit
test.

| Capability | PyIceberg 0.12.0 | DuckDB 2.0.1, REST catalog attached |
|---|---|---|
| Create a namespace and a table | Yes | Yes (`CREATE TABLE ... AS`, format v2); `CREATE OR REPLACE` is not implemented |
| Append | Yes; temporal partition transforms need the `pyiceberg-core` extra | Yes (`INSERT`) |
| Overwrite with a filter | Yes, one commit (`overwrite(df, overwrite_filter=...)`) | `DELETE`, `UPDATE`, `INSERT`: one commit each |
| Write to a branch | Yes (`branch=`), except on a table without snapshots: "can only be written to the main BRANCH" | No: `INSERT INTO t AT (BRANCH => ...)` is a parser error |
| Read a branch | Yes (`scan(snapshot_id=<head>)`) | Only through the head's snapshot id: `AT (VERSION => 'name')` fails to cast the name, and there is no `BRANCH` keyword |
| Read an older snapshot | Yes | Yes (`AT (VERSION => <id>)`, `iceberg_scan(..., snapshot_from_id => <id>)`) |
| Fast-forward publish with guards | Yes: one `commit_table` with `assert-ref-snapshot-id` on `main` and on the branch | No ref management (only `iceberg_rollback_to_snapshot`) |
| Expire snapshots | Yes (`by_ids`, `older_than`); branch and tag heads are protected; works with open branches | Only on tables whose sole ref is `main`: "supports only the main branch; found unsupported snapshot reference" |
| Delete files that expiry or compaction unreferenced | No | No: after 5 appends, a compaction and an expiry, all 6 data files are still in the bucket and 1 is live |
| Compact data files | Not implemented | Yes (`iceberg_rewrite_data_files`): 12 files of a month-partitioned, PyIceberg-written table became 2, with an audit branch open and intact |
| Metadata-only queries | `inspect.*` | `iceberg_snapshots`, `refs`, `files`, `manifests`, `column_stats`, `partition_stats`, `history`, by catalog name or `metadata.json` path |
| From the host, default configuration | Fails: the table config's `s3.endpoint` (`http://storage:9000/`) overrides the client's | Fails: the default delegation mode uses the table config's endpoint ("Could not resolve hostname ... storage:9000") |
| From the host, configuration below | Works | Works |
| In the network, app image | Works | Works, extensions loaded from the image without downloads |

What Lakekeeper 0.15.1 hands a client in `loadTable`, whatever `X-Iceberg-Access-Delegation`
says (`vended-credentials`, `remote-signing`, empty or absent):

- always `s3.endpoint` (the warehouse's endpoint), `s3.region` and `s3.path-style-access`, and no
  `storage-credentials`, because the warehouse has STS off;
- on a warehouse with remote signing on, Lakekeeper's default and what the foundation created,
  also `s3.remote-signing-enabled=true`, the signer endpoint and
  `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`, which makes a default PyIceberg fail without
  `s3fs` (`ModuleNotFoundError`). With `remote-signing-enabled: false` those keys disappear.

One more result was measured during the spike and is not pinned, because pinning it would send
test traffic to a third party: DuckDB attached with delegation off but without an S3 secret
sends its requests to AWS's public endpoint (`NoSuchBucket`).

Two interoperability traps, pinned by tests:

- An append of zero rows to a table without snapshots makes PyIceberg write a snapshot with an
  empty manifest list, and DuckDB cannot read that table any more ("Invalid Avro file block count
  or size").
- PyIceberg refuses branch writes on a table without snapshots, and an Iceberg ref needs a
  snapshot to point at.

Cost: the app image is 789 MB (the virtual environment is 378 MB, of which pyarrow 170, DuckDB 66
and pyiceberg-core 38; the DuckDB extensions add 86.5). A WAP cycle with DuckDB reads in the app
container peaked at 99 MiB of cgroup memory against its 192 MiB limit; `doctor` at 64 MiB.

## Decision outcome

Chosen option: **1, PyIceberg writes and runs WAP; DuckDB reads, answers metadata queries and
compacts; every client uses its own S3 endpoint and keys.** It is the only split in which every
step is supported: DuckDB cannot write branches or move refs, and PyIceberg cannot compact.

### Who does what

| Job | Engine | Module |
|---|---|---|
| Table creation and additive schema evolution | PyIceberg | `ibergrid.lake.tables` (`TableSpec`, `ensure_table`), `ibergrid.lake.registry` |
| Every data write (append, overwrite with a filter) | PyIceberg, on audit branches only | `ibergrid.lake.wap` |
| Publish and discard | PyIceberg (`commit_table` with explicit requirements) | `ibergrid.lake.wap` |
| Analytical reads (main, a snapshot, a branch head) | DuckDB | `ibergrid.lake.duck` |
| Metadata queries (snapshots, refs, files, stats) | DuckDB | `ibergrid.lake.duck` |
| Snapshot expiry | PyIceberg, because DuckDB refuses tables with an open branch | `ibergrid.lake.maintenance` |
| Compaction | DuckDB (`iceberg_rewrite_data_files`) | `ibergrid.lake.maintenance` |

DuckDB can write `main`, and the probes keep that measured, but nothing in the project does:
writes that bypass the audit branch would bypass the audit.

### Storage access: each client brings its own

- The warehouse is created with `sts-enabled: false` and `remote-signing-enabled: false`: the
  catalog neither vends credentials nor signs requests. Lakekeeper has no authentication yet
  (ADR 0001), so a signing endpoint would let anyone who reaches the catalog read and write the
  bucket's table data.
- PyIceberg: `ibergrid.lake.catalog.LakeCatalog` applies the client's FileIO properties
  (`PyArrowFileIO`, `IBERGRID_S3_ENDPOINT_URL`, the keys, the region, path-style addressing and
  remote signing off) after the table config. That overrides a private PyIceberg hook
  (`_load_file_io`); a unit test pins it against a server that answers like Lakekeeper.
- DuckDB: `ibergrid.lake.duck.connect` creates a temporary S3 secret scoped to `s3://<bucket>`
  and attaches the catalog with `ACCESS_DELEGATION_MODE 'none'`. The app image bakes the
  extensions at build time (`IBERGRID_DUCKDB_EXTENSION_DIRECTORY`, autoinstall off), so a
  container never downloads code; elsewhere DuckDB installs them into its default directory on
  first use.
- Integration tests therefore run on the host, with the published ports and the keys from
  `.env`, and one of them runs the same library from the app image inside the network.

### The Write-Audit-Publish protocol

```mermaid
sequenceDiagram
  participant Run as Run (PyIceberg)
  participant Cat as Lakekeeper
  participant Duck as Reader (DuckDB)
  Run->>Cat: start: create branch audit-<run> at main's head (base)
  Run->>Cat: write: append / overwrite on audit-<run>, snapshot property ibergrid.run-id
  Duck->>Cat: main: unchanged; audit-<run>: read through its head snapshot
  Note over Run,Duck: audit: checks on the branch (phase 5)
  Run->>Cat: publish: one commit, requires main == base and branch == head;<br/>sets main to head and removes the branch
  Cat-->>Run: rejected if main or the branch moved: PublishConflict, nothing changed
  Duck->>Cat: main: the run's rows
```

1. `wap.start(table, run_id)` records `main`'s head as the base and creates `audit-<run_id>` there
   (the commit requires that the branch does not exist). On a table without snapshots the base is
   `None` and the first write stages a snapshot that no ref points at, then branches from it.
2. `wap.write(table, audit, data, overwrite_filter=None)` appends, or replaces the rows matching
   the filter, on the branch only. A write of zero rows commits nothing (the empty-manifest trap).
3. The audit reads the branch: `wap.read` (PyIceberg) or `duck.scan_ref` (DuckDB).
4. `wap.publish(table, audit)` refreshes the table and raises `PublishConflict` unless `main` is
   still at the base, then sends one `commit_table` with the requirements table UUID, `main` at
   the base and the branch at the audited head, and the updates "set `main` to the head" and
   "remove the branch". PyIceberg's `manage_snapshots()` is not used for this: its transaction
   keeps a single requirement per type, so chaining both ref updates would drop the branch guard.
5. `wap.discard(table, audit)` removes the branch; snapshot expiry later drops its snapshots.

Invariants, each pinned by a unit test on a real catalog (`tests/unit/test_wap.py`) and the main
ones again on the stack (`tests/integration/test_lake.py`):

- A write never modifies `main`.
- Publishing is atomic: `main` moves to the audited head and the branch disappears in one catalog
  commit, or nothing happens.
- A conflict never loses data: `PublishConflict` leaves `main` and the branch as they were,
  whether `main` moved before the publish or during it, and whether the branch moved during it.
- A run belongs to one table: its handle carries the table UUID and is refused elsewhere.

## Consequences

- Good: readers only ever see published runs; a lost race is an explicit `PublishConflict`, not a
  merged or half-written table.
- Good: one client configuration works in containers, on the host and against R2, and no
  credential is stored anywhere but `.env` and the warehouse.
- Good: the capability probes turn an engine upgrade into a failing test that points here.
- Bad: two engines to keep in step; DuckDB reads branches only through snapshot ids.
- Bad: `LakeCatalog` depends on a private PyIceberg method; every PyIceberg upgrade must re-run
  the unit test that pins it.
- Bad: neither engine deletes files: expired snapshots and compacted files stay in the bucket
  until an orphan-file cleanup exists.
- Bad: the app image grows to 789 MB, and the DuckDB extensions come from DuckDB's repository at
  build time (signed, built for the locked version, not pinned by digest).
- Neutral: unit tests and host clients download the DuckDB extensions once (about 83 MB on disk).

### For the next phases (numbered as in the README roadmap)

- **Phase 2, connectors.** Each connector run is one WAP run: `ensure_table` from the registry
  (the Bronze specs land in `ibergrid.lake.registry`), `start`, `write`, `publish`. On
  `PublishConflict` the run is redone from the new `main`, never forced. A day without rows commits
  nothing.
- **Phase 3, Dagster.** The run id is the Dagster run id (plus the partition key), so a branch
  names its run; `AuditBranch` is plain data that ops can pass along. Asset checks read the branch
  with DuckDB before the publish step. `lake init` runs before the first materialization.
- **Phase 4, streaming.** Micro-batches take the same path. Concurrent runs on one table conflict
  instead of interleaving, so the stream publishes small batches and retries on conflict; commits
  retry eight times on fresh metadata (`commit.retry.num-retries`).
- **Phase 5, quality and maintenance.** Quality gates sit between write and publish. Compaction is
  `maintenance.rewrite_data_files` (DuckDB), expiry `maintenance.expire_snapshots` (PyIceberg).
  An orphan-file cleanup has to be built, and DuckDB's own expiry stays unusable while branches
  are open.
- **Phase 6, production.** The same configuration with the R2 endpoint over HTTPS; not measured
  against R2 yet.

## Pros and cons of the other options

### DuckDB for everything

- Good: one engine, fast SQL writes, and it compacts.
- Bad: no branch writes and no ref management, so no WAP; no way to require "`main` has not
  moved" on a commit.

### PyIceberg for everything

- Good: one engine with full branch and ref support.
- Bad: no compaction; analytical SQL would need another engine anyway.

### Storage access delegated by Lakekeeper

- Good: clients would need no storage keys at all.
- Bad: the warehouse runs without STS (Lakekeeper's own Cloudflare R2 example turns it off), so
  delegation means remote signing, which DuckDB does not support and for which Lakekeeper selects
  PyIceberg's fsspec FileIO, an `s3fs` dependency.
- Bad: on an unauthenticated catalog, a signing endpoint hands storage access to anyone who
  reaches it.
- Bad: Lakekeeper would still push the in-network endpoint, so host clients would fail anyway.
