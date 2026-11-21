# 0005. Dagster asset-oriented orchestration of the batch lane

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

The REData and OMIE connectors ([ADR 0003](0003-redata-ingestion.md),
[ADR 0004](0004-omie-ingestion.md)) are libraries: `ingest(...)` calls that cut a window into
chunks and run one Write-Audit-Publish cycle per chunk ([ADR 0002](0002-iceberg-write-and-read-paths.md)).
Phase 1 ends when something runs them every day, backfills history from 2023-01-01, keeps a
durable record of what each run wrote, and cleans up after runs that die. Both connector records
left requirements for this pull request: one writer per table, one request a second per source,
backfills that hand whole ranges to one call, measurements kept beyond snapshot expiry, an OMIE
schedule in two calls with a cut-off for tomorrow's file, and a sweeper for stale audit branches.

[ADR 0001](0001-stack-and-scope.md) named Dagster as the orchestrator. The project brief asks for
a record of that choice against Apache Airflow, judged on traceability and local testing, and of
how the orchestration is laid out:

1. Dagster or Airflow, and why, for this lane?
2. What is an asset, and how are its partitions and backfills cut?
3. How are the per-table and per-source limits enforced?
4. When do the schedules run, and what does each run ask for?
5. How are the audit branches of dead runs found and removed?
6. Which services, which run launcher, and where does run metadata live?

## Decision drivers

- Traceability: for every partition of every table, which run wrote it, when, from which raw
  answer or file, and how complete it was, kept as long as the orchestrator's database.
- Local testing: the orchestration code runs in-process in unit tests against the same local fakes
  and SQL catalog as the connectors, and end to end on the compose stack, without the live sources.
- The connectors' guarantees unchanged: their chunking, idempotency and pacing are not
  re-implemented around them.
- Least privilege and one container per service, as for the rest of the stack (no Docker socket).
- Polite to two free public sources, also during a full backfill.

## Considered options

- **Orchestrator.** (a) Dagster 1.13, asset-oriented; (b) Apache Airflow 3.2, task-oriented
  with assets and, since 3.2, asset partitioning (AIP-76); (c) cron and the CLI.
- **Assets.** (a) one per REData dataset and one for OMIE; (b) one multi-asset per source.
- **Backfill policy.** (a) a single run per backfilled range; (b) Dagster's bounded multi-run
  (N partitions per run); (c) one run per partition.
- **Run launcher.** (a) the default launcher, runs as subprocesses of the code server;
  (b) `DockerRunLauncher`, one container per run.
- **Pool enforcement.** (a) per run, before it is launched; (b) per op, inside launched runs.
- **Stale branches.** (a) by ownership: the Dagster run named in the branch is over; (b) by age.

## Measurements

Measured on 2026-11-21 (Madrid times).

**REData, when D-1 is complete** (5 requests at 08:50 and 5 at 09:46, through the connector's own
client and completeness measure; the probe script is not committed): for 2026-11-20, `demand_realtime`
national 1,152/1,152 points, `demand_hourly` peninsular and Canarias 24/24, `generation_daily`
national 15/15, `market_prices` 120/120. REData's `last-update` was 06:47 that morning for hourly
demand and daily generation (08:47 for the five-minute series, which REData keeps updating; the
prices for D-1 were last updated on D-2). The same morning **today's** answers were not empty:
`demand_hourly` already had 24 points for 2026-11-21 (forecast-shaped values for the hours still
to come) and `generation_daily` one point per technology. A point count therefore cannot tell
provisional from final data for the current day, and ADR 0003's "today answers 502" held only
before that morning's update. A second probe at 09:46 found the same counts and the same 06:47
`last-update` for hourly demand and daily generation: REData updated them once that morning, not
hourly.

**OMIE, when tomorrow's file appears** (its listing, fetched once, 559 KB, 1,421 daily files from
2023-01-01 to 2026-11-21; a file's last modification is an upper bound of its first
publication). Of the 400 files of the 15-minute era (delivery from 2025-10-01) last modified on
D-1, by Madrid time:

| Last modified on D-1 at or after | 15:45 | 16:00 | 21:00 | 21:30 | 22:00 | 23:00 |
|---|---|---|---|---|---|---|
| Files | 37 (9.2 %) | 37 (9.2 %) | 15 (3.8 %) | 12 (3.0 %) | 11 (2.8 %) | 8 (2.0 %) |

Earliest 13:14, median 14:06, p90 15:34, p95 19:16, p99 23:41, latest 23:59; 17 more files were
last modified on D or later (in-place rewrites). Over all 1,421 files: median 13:26, p95 17:12.

**Dagster's partitions** (pinned by unit tests): with `DailyPartitionsDefinition(start_date=
"2023-01-01", timezone="Europe/Madrid")` the last partition at 2026-11-21 09:00 is 2026-11-20 with
`end_offset=0`, 2026-11-21 with 1 and 2026-11-22 (tomorrow) with 2; 2026-03-29 and 2026-10-25 are
one partition each, of 23 and 25 hours.

**Images** (`docker image inspect`): the app image 790,305,126 bytes (790 MB, as before Dagster),
the orchestration image 1,037,329,584 bytes (1.04 GB): Dagster and its dependencies add 247 MB.

**The full backfill** is in its own section below.

## Decision outcome

### Dagster, because the unit of work here is a partition of a table

Chosen: **Dagster 1.13.31** (dagster, dagster-webserver, dagster-graphql; dagster-postgres
0.29.31), as ADR 0001 planned.

- **Traceability.** Each partition of each asset carries its own materialization events with
  metadata (below) and the run that wrote it; the UI and GraphQL answer "which days of daily
  generation are incomplete" or "which run last wrote 2025-10-30" directly, and the lineage of
  later layers (phase 3) hangs off the same asset keys. Airflow 3.2 records task instances and
  asset events, and added asset partitioning in 3.2 (AIP-76,
  [release notes](https://airflow.apache.org/blog/airflow-3.2.0/)); its unit of record remains the
  task run, so per-day completeness would live in task logs, XComs or asset-event extras.
- **Local testing.** `dagster.materialize(...)` runs an asset in-process with its resource,
  against the connectors' fakes and the SQL catalog (`tests/unit/test_orchestration_runs.py`), in
  seconds, and its partition status and metadata are read back from an ephemeral instance;
  schedules are evaluated as plain functions of their tick time, DST days included; and
  `dagster definitions validate` checks the code location offline (the release smoke test).
  Airflow's `dag.test()` also runs a DAG in one process; the lane was not prototyped in Airflow, so
  this comparison rests on both projects' documentation and Dagster's measured tests.
- **Footprint.** Comparable: Dagster needs a webserver, a daemon and a code server on Postgres;
  Airflow 3 an API server, a scheduler and a DAG processor (and a triggerer for deferrable tasks)
  on its metadata database (documentation, not measured here).
- **Cron and the CLI** would schedule the commands but keep no record of partitions, no backfill
  status and no concurrency limits; each would have to be built.

### Assets, partitions and the backfill policy

- **One asset per REData dataset** (`bronze/redata_observations/<dataset>`) and one for OMIE
  (`bronze/omie_day_ahead_prices`). Each REData dataset has its own systems, chunk size and
  completeness, and `ingest()` takes one dataset; separate assets give separate partition status.
- **Daily partitions by Madrid day from 2023-01-01** (OMIE's first daily file), REData's ending
  yesterday (`end_offset=0`: the last complete day), OMIE's ending tomorrow (`end_offset=2`,
  measured above: 1 ends today), because OMIE publishes delivery day D on D-1.
- **A single run per backfilled range** (`BackfillPolicy.single_run()`, a Dagster beta parameter):
  the run hands its whole partition range to one `ingest(settings, dataset, start, end, ...)` or
  `ingest(settings, start, end, ...)` call, and the connector cuts it into its own chunks (calendar
  months; calendar years ending at the last full month for daily generation). Dagster's multi-run
  policy splits a range by a number of partitions, not at month boundaries, so it would split
  chunks and turn each year of daily generation into many requests; one run per partition would
  cost a request, a WAP cycle and two snapshots per day (ADR 0003).
- **Partitions are recorded as their chunks publish.** The asset passes `on_chunk` and logs one
  materialization per day of each published chunk; for REData a day is recorded once every
  system's chunk of its window has published. REData's plan now goes window by window, every
  system's chunk of a month (or year) before the next one; it went system by system, so a
  five-system backfill recorded no partition until its last system's pass, and one failing in its
  third system recorded none of what the first two published. The asset yields no output of its
  own. So a long backfill that fails part way keeps, in Dagster as in the lake, the
  partitions it published; rerunning the range redoes all of it, idempotently, and a narrower
  range resumes where it stopped.
- **The metadata is what snapshot expiry would erase.** REData, per day: `complete`,
  `expected_points`, `received_points`, `points` (per system, that day) and `chunks` (per system:
  rows, rule violations, dropped series, snapshot id, run id, landing path) plus the chunk's
  `window`. OMIE, per day: the chunk's `window`, `rows`, `contract_versions`, `rule_violations`,
  `snapshot_id` and `run_id`. Snapshot ids travel as text: 63-bit integers lose digits as JSON
  numbers in the UI.
- **Run ids.** The asset passes the Dagster run id as the connectors' prefix, so every WAP run is
  `<Dagster run id>-<chunk slug>-a<attempt>-<4 hex>` and its branch `audit-<that>`: the dataset or
  source and the window are already in the slug, and the prefix stays within the connectors' 40
  characters. This refines ADR 0002's "run id plus partition key and attempt": the random suffix
  per attempt (ADR 0003) already keeps retries apart.

### Pools: one writer per table, one request a second per source

Every REData asset is in the pool `redata` and the OMIE asset in `omie`; `dagster.yaml` gives every
pool a default limit of 1 and enforces it **per run** (`granularity: run`): the queued run
coordinator does not launch a run until its pools have a free slot, so a waiting run holds no
process or memory. All runs use the in-process executor, so the assets of one run execute one
after another, never side by side. REData and OMIE write different tables and use different
sources, so one run of each may run at once (`max_concurrent_runs: 3`, leaving room for the
sweeper). The limit of 1 keeps "main at the run's base" from turning concurrent REData chunks into
publish conflicts and keeps each source at one request a second, since pacing is per process.

### Schedules, in Madrid time

None runs between 02:00 and 03:00, the hour DST skips or repeats, so each fires once a day; the
partition each asks for comes from Dagster's partition definition at tick time, pinned around
2026-03-29 and 2026-10-25. Schedules start **stopped** unless
`IBERGRID_ORCHESTRATION_SCHEDULES_RUNNING=true` (production's default), so a local stack never
calls the sources on its own.

| Schedule | Madrid time | Asks for | Why |
|---|---|---|---|
| `redata_daily_schedule` | 09:00 | yesterday and the 3 days before, every REData dataset | D-1 complete at 08:50 and 09:46 on 2026-11-21 (one morning's evidence), after REData's 06:47 update; never today, which answers 502 or provisional points; the trailing days pick up later corrections |
| `omie_afternoon_schedule` | 16:00 | tomorrow, tolerating "not published yet"; the last 7 delivery days again | 91 % of the 15-minute era's files were final by 16:00 |
| `omie_evening_schedule` | 21:30 | tomorrow, **required**; the last 7 delivery days again | 97 % final by 21:30; a later file fails the run (after its retries at 21:45 and 22:00) instead of passing as "not yet" |
| `sweep_audit_branches_schedule` | 05:30 | the sweeper | before the day's runs |

**The OMIE runs make two connector calls**, as ADR 0004 requires: first the delivery days up to
today, from 7 days before the partition (the trailing window: every rewrite and republication in
the listing happened by D+2; content-hashed landing makes the rerun cheap) without
`require_published`, since a missing day up to today is an error anyway; then tomorrow alone, with
`require_published` false in the afternoon and true in the evening. `require_published` discards
the validated days of its own month chunk, so it never shares a call with the trailing window. In
the afternoon, a tomorrow not yet out is a successful run that does not record tomorrow's
partition; in the evening it fails the run. A file that appears after 21:30 is still ingested by
the next day's trailing window (then it is a day up to today, which must exist).

**REData's scheduled runs also re-ingest the 3 days before yesterday** (`trailing_days: 3`): one
request per system, as for yesterday alone, and two across a month boundary. They pick up late
points and any revision REE makes within those days (how often and how far back REE revises is not
measured), and they fill a day whose run failed, up to three days later. Older revisions reach
Bronze only when a backfill reruns the window. Both trailing windows stop at 2023-01-01.

**Retries and failures.** A scheduled run whose step fails runs it again twice, 15 minutes apart
(`op_retry_policy` on the three scheduled jobs: a source not ready yet, a transient outage). A run
that fails without a failed step (one that never started, for example) is queued again up to twice
(`run_retries`, which has no delay of its own). Backfills are not retried, since a retry redoes the
whole range. What is left is a failed run, visible in the UI and in its run status, and the next
days' trailing windows; nothing sends a notification yet (phase 6, with the public endpoints).

### The sweeper: ownership, not age

`sweep_audit_branches` (daily, 05:30) lists the `audit-*` refs of every registry table
(`wap.audit_branches`), parses the Dagster run id at the start of each name, and removes the branch
(`wap.discard_branch`) only when this instance knows that run and it has finished: success,
failure or cancellation. A branch of a run that is queued, starting, started or canceling, of a run
this instance does not know (another instance, a deleted run), or of no Dagster run at all (the
CLI's timestamped prefixes) is kept and logged; each removal is logged with the table and the run's
status. Age cannot be used: refs carry no creation time and an idle run's head is its base
(ADR 0002).

A stop of `dagster-code` (a deploy recreates it) waits for its runs: the code server does not exit
while a run is in flight, so compose gives it `stop_grace_period: 5m`. With Docker's default 10 s,
the review measured a restart in 10.9 s that killed the run worker, left the run "started" and kept
a second run of its pool queued; with the grace period, a stop during a 31-day OMIE run took 29.8 s
and the run ended in success (measured on a fresh stack, 2026-11-21). A run whose worker is killed
anyway (a stop beyond 5 minutes, an out-of-memory kill) never reports its end, and the default
launcher cannot check a run worker's health: run monitoring cancels it past
`max_runtime_seconds`, 2 hours (4 times the full backfill's 30 minutes; a longer run sets the
`dagster/max_runtime_seconds` tag). Until then the run holds its pool (Dagster counts starting,
started and canceling runs against run-granularity pools), so that source's runs queue for up to 2
hours; the next sweep frees its branches.

### Services and run launcher

| Service | Role | Networks |
|---|---|---|
| `orchestration-db` | Postgres 17: runs, event logs (materializations), schedules and ticks; own volume | orchestration-db (internal) |
| `orchestration-migrate` | One-shot `dagster instance migrate`: the schema exists before the services start, so they never race to create it, and upgrades migrate explicitly | orchestration-db |
| `dagster-code` | gRPC code server with `ibergrid.orchestration.definitions`; **runs execute here** | orchestration, orchestration-db, storage, catalog |
| `dagster-webserver` | UI and GraphQL; 127.0.0.1:28300 locally, no port in production | orchestration, orchestration-db (+ orchestration-ui locally, to publish the port) |
| `dagster-daemon` | schedules, backfills, the run queue and its pools, run monitoring | orchestration, orchestration-db |

The **default run launcher** runs each run as a subprocess of the code server that holds the code.
`DockerRunLauncher` would isolate runs in containers, but the daemon would need the Docker socket,
which is root on the host; and the subprocess model keeps the image, the code and the limits in one
place. The costs: `dagster-code` is the only Dagster service with the lake's keys and egress, its
memory limit covers the runs, and a code server restart kills the runs in flight (above). Compute
logs are written by the runs into a volume that the webserver mounts read-only. `dagster.yaml`
and `workspace.yaml` are baked into the orchestration image (`config/dagster/`), which is a separate
Dockerfile target (`orchestration`, the app plus the `orchestration` dependency group), so the app
image used by `ingest` and `doctor` stays without Dagster; production pulls it by digest like the
app image.

Each healthcheck starts a Python process (about 1 s of CPU, measured in review: with 10 and 15 s
intervals they took some 17 % of a core), so they run every 60 s, and every 5 s during a service's
first 2 minutes so that `up --wait` still returns quickly.

The deploy (`deploy/remote.sh`) creates the lake's tables before it starts the stack:
`docker compose run --rm doctor lake init`, idempotent, which first starts the catalog and its
one-shots. On an empty project that command started storage, the catalog and their one-shots,
created both tables and started no Dagster service (measured locally with the local-storage
profile), so the schedules never run against missing tables. In production the webserver
publishes no port, but the server reaches it at its address on the internal `orchestration`
network (measured: HTTP 200 from the host), so an SSH tunnel to that address serves the UI
(`deploy/deploy.env.example`).

## The full backfill

On 2026-11-21, on the local stack (`make up`, then `lake init`), two backfills launched through the
webserver's GraphQL API at 09:12 Madrid, 2023-01-01 to 2026-11-20 (1,420 partitions), against the
live REData API and omie.es (with REData's plan still going system by system, so demand_hourly's
partitions were recorded during its last system's pass):

| | REData (4 assets) | OMIE |
|---|---|---|
| Runs | 1 (the four assets in sequence) | 1 |
| Duration | 30.3 min (demand_hourly 13.3, demand_realtime 8.9, generation_daily 4.6, market_prices 3.4); the run waited 1 min 47 s to start while Dagster wrote its 5,680 "planned" events | 28.3 min |
| Chunks (one WAP run each) | 359: 47 months for each sub-daily system, 5 per daily-generation system (2023, 2024, 2025, 2026-01..10, 2026-11-01..20) | 47 months |
| HTTP requests | 360: one per chunk and one retry (a 503 at 08:32 UTC) | 1,425: 1,420 days, 4 more for the republished `20230528.2`, `20251030.3` and `20251127.2`, one retry (a read timeout on 2025-08-10); 0.84 a second |
| Rows | 1,689,319: demand_realtime 1,324,424; demand_hourly 5 × 34,080; generation_daily 76,727; market_prices 117,768 | 64,035 (24,095 hourly v1, 39,940 quarter-hourly v2) |
| Partitions recorded | 1,420 per asset, 0 failed | 1,420, 0 failed |
| Publish conflicts, dropped series, rule violations | 0, 0, 0 | 0, -, 0 |

Both backfills ended `COMPLETED_SUCCESS`, both tables kept only `main` (no branch left behind), and
nothing failed; the two transient errors were retried by the clients. `dagster-code` peaked at
1,260 MiB of cgroup memory with both runs and the code server (its limit is 1,536 MiB): the REData
run worker held 464 MiB during the five-minute demand, OMIE's 300 MiB, the code server 310 MiB.
Afterwards Dagster's database was 37 MB (15,373 events, 7,627 materializations once the runs below
had also run), the bucket 231 MB.

Completeness (from each published run's snapshot summary): demand_hourly and market_prices 100 %;
demand_realtime 99.65 % (21 incomplete days, such as 2023-04-12 with 432 of 864 points);
generation_daily 56 to 96 % by system (Ceuta 56 %, national 94 %), because a year chunk counts every
technology that appears in it on every day, and some appear on few days (ADR 0003's shortcut until
phase 4 declares the expected series).

**Idempotency.** October 2025 (the first 15-minute month for OMIE, with a 25-hour day and the
republished `marginalpdbc_20251030.3`) was hashed (SHA-256 over every row ordered by key, without
`fetched_at` and `run_id`), backfilled again through Dagster (REData 13 requests in 74 s, OMIE 33 in
39 s) and hashed again: demand_realtime (26,820 rows), demand_hourly (3,725), market_prices (3,725)
and OMIE (2,980) were identical, REData's `series_last_update`, `landing_path` and `payload_sha256`
included, and both tables kept their totals. Daily generation (1,674 rows) was not: the October
window is not the backfill's (the year 2025), and REData's sums differ in the last bit between the
two (58 values and 77 shares, at most 4.8e-16 relative). Rerunning the backfill's own window (2025,
6 requests, 70 s) restored the exact October hash. A rerun of the same window is identical; another
window of daily generation can differ by rounding, which the daily schedule (one day at a time)
will show against backfilled years.

**The two-call schedule, live.** The `omie_afternoon_schedule` tick for 2026-11-21 16:00 was
evaluated in the code server (the UI's "test schedule") and its run request launched: partition
2026-11-22 with the job's config. It re-ingested 2026-11-15..21 in one chunk (672 rows, 7
partitions recorded again), found no file for 2026-11-22 (5 requests) and succeeded without
recording that partition. A run launched by hand without the job's config gets the asset's
defaults (no trailing window), which a unit test now pins against the schedule's request.

## Consequences

- Good: every partition of every Bronze asset says which run wrote it, from which raw answer, and
  how complete it was, for as long as the orchestrator's database lives, also after snapshot
  expiry; a partial backfill leaves accurate partition status.
- Good: a backfill of any range is one connector call per asset with the connectors' own chunks,
  never one WAP run per day; reruns are idempotent, so retrying a failed backfill is safe.
- Good: the orchestration is unit-tested in-process against the connectors' fakes, and end to end
  through the real services in the integration tests, without the live sources: the health and
  sweeper tests everywhere, the materializing ones (partitions and metadata, a range as one
  request, pool exclusion) only on an instance without real materializations, since they record
  materializations of the real asset keys and deleting their runs does not undo that (CI's fresh
  stack runs them; a backfilled stack skips them).
- Good: the per-table and per-source limits are configuration (`pools.default_limit: 1`), checked
  before launch.
- Bad: a single run per range means a failure late in a long backfill costs a rerun of the whole
  range (idempotent, but the requests are spent again); narrow the range to resume.
- Bad: `backfill_policy` is a Dagster beta parameter: a minor release may change it.
- Bad: runs live and die with `dagster-code`: a stop waits up to 5 minutes for them, and a run
  killed anyway holds its pool until the 2-hour runtime limit cancels it; the sweeper frees its
  branches the next day.
- Bad: the orchestration image adds 247 MB over the app image, and the stack four long-running
  services and a one-shot.
- Bad: OMIE's evening run fails, after its retries, on the few days (3 % of the 15-minute era, an
  upper bound) whose file appears after 22:00; until notifications exist (phase 6), a failed run
  in the UI is the only signal, and the next day's trailing window ingests the file.
- Bad: the lock moves python-dateutil from 4.0.0 to 2.9.0.post0 (which adds six) for both images:
  graphene, through dagster-graphql, caps it below 3, and pyiceberg (through strictyaml) is the app
  image's user of it.
- Neutral: REData revisions older than the 3-day trailing window reach Bronze only through a
  backfill.
- Neutral: asset checks and quality gates are phase 4; lineage to Silver comes with phase 3.
