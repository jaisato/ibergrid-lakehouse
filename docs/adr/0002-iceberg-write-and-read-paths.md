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
- Readers never see a run that has not been checked, and never more than what was checked.
- The same client configuration in containers and on the host, and the same settings shape for
  production (Cloudflare R2).
- Every capability claim measured, and pinned by a test where that is possible, so an upgrade
  that changes one shows up.
- No JVM and no new services.

## Considered options

1. PyIceberg writes and runs WAP; DuckDB reads, answers metadata queries and compacts.
2. DuckDB for everything, writes included.
3. PyIceberg for everything, reads included.
4. Storage access delegated by Lakekeeper (vended credentials or remote signing) instead of each
   client using its own keys.

## Measurements

Lakekeeper 0.15.1 on RustFS 1.0.6; PyIceberg 0.12.0 with pyarrow 26.0.1 and pyiceberg-core
0.10.1; DuckDB 2.0.1 with the extension builds `iceberg` f979b5b28a, `httpfs` f71cfdb4bf and
`avro` 4176191c96 (`duckdb_extensions()`). Measured on 2026-11-21 from the host (published
ports) and from the app image inside the compose network. Unless marked †, a cell is pinned by
`tests/integration/test_capabilities.py`, `tests/integration/test_lake.py` or a unit test; †
marks what was measured during the spike and is not pinned.

| Capability | PyIceberg 0.12.0 | DuckDB 2.0.1, REST catalog attached |
|---|---|---|
| Create a namespace and a table | Yes | Yes (`CREATE SCHEMA`, `CREATE TABLE ... AS`, format v2); `CREATE OR REPLACE` is not implemented |
| Append | Yes; tables partitioned by a temporal transform need the `pyiceberg-core` extra (†: `NotInstalledError` without it) | Yes (`INSERT`) |
| Overwrite with a filter | Yes: one commit, two snapshots (`overwrite`, then `append`) | `DELETE`, `UPDATE` and `INSERT` work; each statement commits on its own (†) |
| Write to a branch | Yes (`branch=`), except on a table without snapshots: "can only be written to the main BRANCH" | No: `INSERT INTO t AT (BRANCH => ...)` is a parser error |
| Read a branch | Yes (`scan(snapshot_id=<head>)`) | Only through the head's snapshot id: `AT (VERSION => 'name')` fails to cast the name, and there is no `BRANCH` keyword |
| Read an older snapshot | Yes | Yes (`AT (VERSION => <id>)`, `iceberg_scan(..., snapshot_from_id => <id>)`) |
| Move refs with guards | Yes: one `commit_table` with `assert-ref-snapshot-id` requirements on several refs | No: apart from `iceberg_rollback_to_snapshot` (†: it worked in the spike), no function creates, moves or removes a ref |
| Expire snapshots | Yes (`by_ids`, `older_than`); branch and tag heads are protected; works with open branches | Only on tables whose sole ref is `main`: "supports only the main branch; found unsupported snapshot reference" |
| Delete the files that expiry or compaction stop referencing | No | No: after 5 appends, a compaction and both engines' expiry, every data file is still in the bucket |
| Compact data files | Not implemented (†) | Yes (`iceberg_rewrite_data_files`): 12 files of a month-partitioned, PyIceberg-written table became 2. It commits a `replace` snapshot to `main` |
| How DuckDB commits | | `POST /v1/{prefix}/transactions/commit` requiring `assert-table-uuid`, `assert-current-schema-id` and `assert-ref-snapshot-id` on `main`; a PyIceberg append that lands between its plan and its commit wins (409, `duckdb.TransactionException`) |
| Metadata-only queries | `inspect.*` | `iceberg_snapshots`, `refs`, `files`, `manifests`, `column_stats`, `partition_stats`, `history`, by catalog name or `metadata.json` path |
| From the host, default configuration | Fails: the table config's `s3.endpoint` (`http://storage:9000/`) overrides the client's | Fails: the default delegation mode uses the table config's endpoint ("Could not resolve hostname ... storage:9000") |
| From the host, the configuration below | Works | Works |
| In the network, app image | Works | Works, extensions loaded from the image; an empty extension directory fails and downloads nothing |
| Wrong secret key | `OSError` (`SIGNATURE_DOES_NOT_MATCH`), nothing committed, no key in the message | HTTP 403, no key in the message |

What Lakekeeper 0.15.1 hands a client in `loadTable`, whatever `X-Iceberg-Access-Delegation`
says (`vended-credentials`, `remote-signing`, empty or absent, all four pinned): `s3.endpoint`
(the warehouse's endpoint), `s3.region` and `s3.path-style-access`, and no `storage-credentials`,
because the warehouse has STS off. On a warehouse with remote signing on (Lakekeeper's default,
and what the foundation created), it also handed out the signer endpoint and
`py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`, which makes a default PyIceberg fail without
`s3fs` (`ModuleNotFoundError`) (†: measured on that warehouse; the unit test only replays that
table config from a local server). A profile update (`POST /management/v1/warehouse/{id}/storage`)
needs the credential in the same request: the profile alone failed storage validation with 400 (†).

Also measured during the spike and not pinned, because pinning it would send test traffic to a
third party: DuckDB attached with delegation off but without an S3 secret sends its requests to
AWS's public endpoint (`NoSuchBucket`).

Three interoperability traps, pinned by tests:

- An append of zero rows to a table without snapshots makes PyIceberg write a snapshot with an
  empty manifest list, and DuckDB cannot read that snapshot ("Invalid Avro file block count or
  size").
- PyIceberg refuses branch writes on a table without snapshots, and an Iceberg ref needs a
  snapshot to point at.
- PyIceberg retries a data commit that lost a race (four retries by default), on fresh metadata.
  If the run's branch was removed in between, the retry re-creates it on a snapshot without a
  parent: the branch then holds only the latest write, and a naive publish would replace `main`
  with it (reproduced on Lakekeeper and on the SQL catalog).

Cost: the app image is 789 MB (the virtual environment is 378 MB, of which pyarrow 170, DuckDB 66
and pyiceberg-core 38; the DuckDB extensions add 86.5). A WAP cycle with DuckDB reads in the app
container peaked at 99 MiB of cgroup memory against its 192 MiB limit; `doctor` at 64 MiB (†).

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
| Analytical reads (main, a snapshot such as an audited head) | DuckDB | `ibergrid.lake.duck` |
| Metadata queries (snapshots, refs, files, stats) | DuckDB | `ibergrid.lake.duck` |
| Snapshot expiry | PyIceberg, because DuckDB refuses tables with an open branch | `ibergrid.lake.maintenance` |
| Compaction | DuckDB (`iceberg_rewrite_data_files`) | `ibergrid.lake.maintenance` |

DuckDB can write `main`, and the probes keep that measured, but no data write goes through it: a
write outside an audit branch skips the audit. Its one commit in this project is compaction,
which replaces files without changing rows and commits a `replace` snapshot to `main`. The catalog
serialises it against everything else: a write that lands first makes the compaction fail
(`MaintenanceConflict`, nothing changed), and a run open across a compaction can no longer publish
(`PublishConflict`, its branch intact). Compaction therefore runs when no run is open, or the open
runs restart afterwards.

### Storage access: each client brings its own

- The warehouse is created with `sts-enabled: false` and `remote-signing-enabled: false`: the
  catalog neither vends credentials nor signs requests. Lakekeeper has no authentication yet
  (ADR 0001), so a signing endpoint would let anyone who reaches the catalog read and write the
  bucket's table data. A warehouse created earlier keeps remote signing until
  `ibergrid catalog update-credential` rewrites its profile (or, locally, until
  `make down-volumes`).
- PyIceberg: `ibergrid.lake.catalog.LakeCatalog` drops any vended credential from the table
  config and applies the client's FileIO properties (`PyArrowFileIO`, `IBERGRID_S3_ENDPOINT_URL`,
  the keys, the region, path-style addressing and remote signing off) on top. That overrides a
  private PyIceberg hook (`_load_file_io`, marked `@override` so mypy fails if it disappears); a
  unit test pins it for `loadTable` and `createTable` against a server that answers like
  Lakekeeper.
- DuckDB: `ibergrid.lake.duck.connect` creates a temporary S3 secret, with the keys passed as
  prepared parameters and scoped to `s3://<bucket>/`, and attaches the catalog with
  `ACCESS_DELEGATION_MODE 'none'`. The app image bakes the extensions at build time
  (`IBERGRID_DUCKDB_EXTENSION_DIRECTORY`, autoinstall off), so a container never downloads code;
  elsewhere DuckDB installs them into its default directory on first use.
- Integration tests therefore run on the host, with the published ports and the keys from
  `.env`, and one of them runs the same library from the app image inside the network.

### The Write-Audit-Publish protocol

```mermaid
sequenceDiagram
  participant Run as Run (PyIceberg)
  participant Cat as Lakekeeper
  participant Audit as Audit (DuckDB or PyIceberg)
  Run->>Cat: start: create branch audit-<run> at main's head (the base)
  Run->>Cat: write: append / overwrite on audit-<run>, snapshot property ibergrid.run-id
  Run->>Audit: head: snapshot S, checked to descend from the base
  Audit->>Cat: read snapshot S, run the checks (phase 4)
  Run->>Cat: publish(expected_head=S): one commit requiring table UUID, main == base,<br/>branch == S; sets main to S, removes the branch, sets checkpoint properties
  Cat-->>Run: rejected if main or the branch moved: PublishConflict, nothing changed
```

1. `wap.start(table, run_id)` records `main`'s head as the base and creates `audit-<run_id>`
   there (the commit requires that the branch does not exist). On a table without snapshots the
   base is `None` and the branch starts at an empty snapshot staged for it, which no ref but the
   branch points at and which is never published.
2. `wap.write(table, run, data, overwrite_filter=None)` appends, or replaces the rows matching
   the filter, on the branch only. An append of zero rows commits nothing. A write to a branch
   that no longer exists (published or discarded) raises instead of starting a new one.
3. The audit reads exactly one snapshot: `wap.head(table, run)` returns it (or `wap.read`, with
   the rows), and DuckDB reads it with `duck.scan(con, table, snapshot_id=...)`. Both refuse a
   branch that no longer descends from the snapshot the run started from (the re-created branch
   of the trap above).
4. `wap.publish(table, run, expected_head, set_properties=None)` refreshes, refuses a re-created
   branch, raises `PublishConflict` unless `main` is still at the base and the branch is still at
   `expected_head`, then sends one `commit_table` with the requirements table UUID, `main` at the
   base and the branch at `expected_head`, and the updates "set `main` to `expected_head`",
   "remove the branch" and, if given, "set these table properties". PyIceberg's
   `manage_snapshots()` is not used for this: its transaction keeps a single requirement per type,
   so chaining both ref updates would drop the branch guard.
5. Publishing again is safe: when the branch is gone and `expected_head` is in `main`'s history
   since the base, `publish` returns it. The same check runs after a commit whose answer was lost
   (`CommitStateUnknownException`, a 5xx): a landed commit is reported as published, one that did
   not land raises `WapError` so the caller publishes again.
6. `wap.discard(table, run)` removes the branch; snapshot expiry later drops its snapshots and
   keeps the whole lineage of every open branch back to its fork from `main`.

Invariants, each pinned by a unit test on a real catalog (`tests/unit/test_wap.py`) and the
races again on Lakekeeper (`tests/integration/test_lake.py`):

- A write never modifies `main`.
- Publishing is atomic: `main` moves to the audited snapshot, the branch disappears and the
  properties change in one catalog commit, or nothing happens.
- Only what was audited is published: a branch written after its audit, before or during the
  publish commit, is refused.
- Only the run's own lineage is published: a branch removed and re-created during a write is
  refused by `head`, `read` and `publish`.
- A conflict never loses data: `PublishConflict` leaves `main`, the branch and the properties as
  they were.
- A run belongs to one table: its handle carries the table UUID and is refused elsewhere.

### Checkpoints travel with the data

Run and stream checkpoints, such as Kafka offsets per partition, are table properties committed
in the same catalog commit as the data they describe: `publish(..., set_properties=...)` for a
WAP run, and one PyIceberg transaction (`tx.set_properties(...)` and `tx.append(...)`) for a
direct write; both are one metadata version, pinned by unit tests. Snapshot summaries are not a
durable place for them: compaction makes `main`'s head a `replace` snapshot without the run's
properties, and expiry drops the snapshots that had them.

## Consequences

- Good: readers only ever see published runs, and only the snapshot that was audited; a lost race
  is an explicit `PublishConflict`, not a merged or half-written table.
- Good: one client configuration works in containers and on the host; production takes the same
  settings with the R2 endpoint over HTTPS, which has not been measured yet.
- Good: the capability probes turn an engine upgrade into a failing test that points here.
- Bad: two engines to keep in step; DuckDB reads branches only through snapshot ids.
- Bad: `LakeCatalog` depends on a private PyIceberg method; every PyIceberg upgrade must re-run
  the unit test that pins it.
- Bad: compaction and open runs exclude each other: a run open across a compaction restarts.
- Bad: neither engine deletes files: expired snapshots and compacted files stay in the bucket
  until an orphan-file cleanup exists.
- Bad: the app image grows to 789 MB, and the DuckDB extensions come from DuckDB's repository at
  build time (signed, built for the locked version, not pinned by digest).
- Neutral: unit tests and host clients download the DuckDB extensions once (about 83 MB on disk).

### For the next phases

- **Phase 1, batch ingestion (REData and OMIE connectors, Dagster).** Each connector run is one
  WAP run: `ensure_table` from the registry (the Bronze specs land in `ibergrid.lake.registry`),
  `start`, `write`, `head`, checks, `publish(expected_head)`. The run id is the Dagster run id
  plus the partition key, so a branch names its run, and `AuditBranch` is plain data that ops
  can pass along. On `PublishConflict` the run is redone from the new `main`, never forced; a
  partition without rows commits nothing. `lake init` runs before the first materialization.
- **Phase 2, streaming lane.** Each micro-batch is a WAP run (or a direct transaction) whose
  publish sets the consumer offsets as table properties in the same commit; a restarted consumer
  resumes from the offsets on `main`: the basis for exactly-once writes into Bronze.
  Concurrent runs on one table conflict instead of interleaving, so one writer owns a stream's
  table and retries a conflict from the new `main`.
- **Phase 3, dbt medallion.** Open question: dbt-duckdb writing Iceberg through DuckDB's attached
  catalog would write `main` directly (DuckDB cannot write branches), outside WAP. The leading
  option is to let dbt-duckdb build the Silver and Gold models in DuckDB and hand them to the WAP
  path above for the Iceberg write; the decision lands with phase 3, in its own ADR.
- **Phase 4, quality and maintenance.** Quality gates and quarantine sit between `head` and
  `publish`. Compaction (`maintenance.rewrite_data_files`, DuckDB) is scheduled when no run is
  open; expiry is `maintenance.expire_snapshots` (PyIceberg). An orphan-file cleanup has to be
  built, and DuckDB's own expiry stays unusable while branches are open. OpenLineage and the data
  chaos tests can drive the races pinned here.
- **Phase 5, benchmarks.** The memory measured here is one small run; real volumes decide the
  containers' limits.
- **Phase 6, serving and production.** The same configuration with the R2 endpoint over HTTPS,
  not measured against R2 yet; delegated access becomes worth revisiting once the catalog has
  authentication (see the next section).

## Pros and cons of the other options

### DuckDB for everything

- Good: one engine, fast SQL writes, and it compacts.
- Bad: no branch writes and no ref management, so no WAP; no way to require "`main` has not
  moved" on a commit.

### PyIceberg for everything

- Good: one engine with full branch and ref support.
- Bad: no compaction; analytical SQL would need another engine anyway.

### Storage access delegated by Lakekeeper

- Good: clients would need no long-lived storage keys at all.
- Bad: locally there is no STS for this warehouse, so delegation means remote signing, which
  DuckDB does not support and for which Lakekeeper selects PyIceberg's fsspec FileIO, an `s3fs`
  dependency.
- Bad: on an unauthenticated catalog, a signing or vending endpoint hands storage access to
  anyone who reaches it.
- Bad: Lakekeeper still pushes the warehouse's endpoint, which host clients cannot resolve
  locally.
- Neutral: Lakekeeper's current S3 storage documentation describes vended credentials for R2
  through a `cloudflare-r2` credential type
  ([docs.lakekeeper.io, Cloudflare R2](https://docs.lakekeeper.io/docs/latest/storage-s3/#cloudflare-r2));
  not evaluated here, on 0.15.1 or otherwise. With authentication in phase 6 it could replace
  static keys in production.
