# 0004. OMIE day-ahead prices into Bronze

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

The second phase-1 connector brings OMIE's day-ahead market results: the marginal price of every
delivery period for Spain and Portugal (MIBEL), published as one `marginalpdbc` text file per
delivery day on `www.omie.es`. It writes through the Write-Audit-Publish library of
[ADR 0002](0002-iceberg-write-and-read-paths.md) with the guarantees of the REData connector
([ADR 0003](0003-redata-ingestion.md)): idempotent reruns, structural problems refused, everything
else measured and recorded. What OMIE adds:

1. The file format changed on 2025-10-01, when the single day-ahead coupling (SDAC) moved to a
   15-minute market time unit: how is a file recognised, and how does a backfill that crosses the
   change (or meets a future one) behave?
2. How do OMIE's numbered periods become UTC intervals, on 23-, 24- and 25-hour days?
3. One row per period with both zones, or one row per zone?
4. How is a three-year table of 24 to 100 rows a day partitioned?
5. What does a run do with a delivery day whose results are not out yet?

## Decision drivers

- A rerun of any window leaves exactly the rows a single run leaves, so backfills need no cleanup.
- Bronze keeps what OMIE published; only what would corrupt the table or misread the file is
  refused. A file read with the wrong contract is the worst outcome: plausible, wrong rows.
- No new dependency and no new service; polite to a free public site.
- Every claim measured, with the request that showed it.

## Considered options

- **Contract.** (a) the delivery date picks the version, the period count confirms it; (b) the
  period count alone picks it; (c) one lenient parser for every shape.
- **Rows.** (a) one row per period, Spain and Portugal side by side; (b) one row per zone and
  period.
- **Partitioning.** (a) `month(delivery_date)`; (b) `month(interval_start_utc)`; (c) none; (d)
  `delivery_date` by day.
- **Chunks.** (a) a calendar month of delivery days per run; (b) one day per run; (c) a year.
- **Days not published yet.** (a) end the ingestion at the first one after today, writing the
  days before it; (b) fail; (c) skip it and go on.

## Measurements

Measured on 2026-11-21 between 05:43 and 06:10 UTC, at most one request a second: 2 probes for
days without a file, 16 downloads (curl, then the connector's own client for the 8 recorded days),
the journeys below inside the stack (89 requests) and `make test-live` (2). The files the tests
replay are in `tests/fixtures/omie`, byte for byte (recorded by `scripts/record_omie_fixtures.py`).

The download, `GET /es/file-download?parents=marginalpdbc&filename=marginalpdbc_YYYYMMDD.1`:

| Request | Answer |
|---|---|
| A published day (each of the 8 recorded, 2026-11-20, the journeys' days) | 200 at once, no redirect (curl `num_redirects=0`; the brief for this work saw a 302 first, not reproduced), `application/octet-stream`, `Content-Disposition: attachment; filename=...`; 0.44 to 0.53 s |
| 2026-11-22 at 06:43 Madrid time (results due that afternoon) | 404, `text/html`, OMIE's 26,832-byte "not found" page, 0.35 s |
| 2027-06-01 | the same 404 page |

So "not published" is a 404, the same for tomorrow before publication and for a day years ahead;
what a 404 means depends on the day (below). The publication hour itself was not measured.

The file (the 8 recorded days; the 46 distinct files of the journeys parsed under the same rules):

- Latin-1 text, CRLF line ends: `MARGINALPDBC;`, then `YYYY;MM;DD;period;price_PT;price_ES;`, then
  `*`. 535 to 691 bytes for an hourly day, 2.2 to 2.7 KB for a quarter-hourly one.
- **Column 5 is Portugal, column 6 Spain.** On 2025-04-28 (the Iberian blackout, zones decoupled)
  period 8 has 40.7 and 47.94, and REData's Spanish spot price for 07:00 local is 47.94; periods
  15 and 16 have -1.51/-3 and -1.51/-1.54, REData -3 and -1.54 (REData's values were measured for
  this work's brief and not repeated here; the recorded days below confirm the match).
- Prices are written with a dot and zero to two decimals in the 8 recorded files (`18`, `16.3`,
  `24.63`), negative ones with a minus (down to -3 EUR/MWh on 2025-04-28). The zones differed in
  202 of October 2026's 2,980 quarter-hours.
- Periods per day, against the length of the Madrid day:

| Delivery day | Periods | Contract |
|---|---|---|
| 2024-03-31 (spring forward) | 23 hourly | v1 |
| 2024-10-27 (fall back) | 25 hourly | v1 |
| 2025-04-28, 2025-09-30 | 24 hourly | v1 |
| 2025-10-01 (first 15-minute day), 2026-11-18 | 96 quarter-hours | v2 |
| 2025-10-26 (fall back) | 100 | v2 |
| 2026-03-29 (spring forward) | 92 | v2 |

- REData's spot price (`market_prices`, series 600) equals OMIE's Spanish price on every one of
  the 92 and 96 quarter-hours of 2026-03-29 and 2026-11-18 (pinned by a unit test on both
  sources' recorded answers).

The journeys, `docker compose run --rm --no-deps ingest omie ...` from the app image against
omie.es (memory: the container's cgroup `memory.peak`, under its 384 MiB limit):

| Window | Result | Time | Peak memory |
|---|---|---|---|
| 2026-10-01..31 | 1 chunk: 31 files (v2), 2,980 rows | 42.4 s (rerun 39.4 s) | 113 MiB (114) |
| 2025-09-25..10-05 | 2 chunks: 6 files (v1) and 144 rows, 5 files (v2) and 480 rows | 13.6 s (14.6 s) | 99 MiB (102) |
| 2025-10-26 | 1 file (v2), 100 rows | 2.6 s | 95 MiB |
| 2026-11-19..30 | 3 files, 288 rows; "not published yet from 2026-11-22", exit 0 | 7.2 s | 96 MiB |

After the reruns, main's 3,704 rows of the first three windows were identical to the first runs',
every column but `fetched_at` and `run_id` included (one SHA-256 over DuckDB's ordered read), the
46 landed files had not been duplicated, and the only ref was `main`. A month of quarter-hours is
one 70 KB Parquet file.

Partitioning, measured on a SQL catalog with those rows (2025-09-25..30 written by one run and
2025-10-01..05 by another, then October rerun), counting the files the rerun's overwrite touches:

| Spec | October's run writes | The rerun | The rerun after compacting each partition into one file |
|---|---|---|---|
| `month(delivery_date)` | 1 file, 1 partition | deletes 1 file (480 rows), adds 1 | the same |
| `month(interval_start_utc)` | 2 files, 2 partitions (the first two hours of a Madrid month are in the previous UTC month) | deletes 2 files, adds 2 | rewrites September's file: deletes 152 rows (144 of September), re-adds September's 144 |

## Decision outcome

### Two contract versions, picked by the date and confirmed by the count

`ibergrid.sources.omie.marginalpdbc`: v1 for delivery days up to 2025-09-30 (60-minute periods),
v2 from 2025-10-01 (15 minutes). The expected count is the length of the Madrid day divided by
the resolution: 23/24/25 or 92/96/100. Before anything is landed or written, a file is refused
(`InvalidFile`, which names the file, the line and the problem, the whole chunk with it) for: a
first line other than the header (an HTML page, a BOM), no trailer, a line that is not exactly
six `;`-terminated fields of four-digit year, two-digit month and day, period and two decimal
prices (`-?\d+(\.\d+)?`: no comma, no exponent, no empty field), a date other than the requested
day, periods other than 1, 2, 3 and so on, or a count that is not the expected one. A count that
fits the other version is reported as such ("96 periods make a v2 file ..., but 2025-09-30 is a
v1 day"). Picking the version from the count alone would read a quarter-hourly file stamped with
an old date as valid; a lenient parser would turn a changed column into wrong prices.

### Periods to UTC

`interval_start_utc = UTC(Madrid midnight of D) + (period - 1) * resolution`, `interval_end_utc =
start + resolution`. The intervals of a day are contiguous and end at the next Madrid midnight on
every DST case recorded (pinned per fixture), and the last hour of 2025-09-30 ends at 22:00 UTC
where the first quarter-hour of 2025-10-01 starts (pinned on the stack too).

### One row per period, Spain and Portugal side by side

`bronze.omie_day_ahead_prices` (declared in `ibergrid.lake.registry`, created by `ibergrid lake
init`): `delivery_date`, `period`, `resolution_minutes`, `interval_start_utc`, `interval_end_utc`,
`price_es_eur_mwh`, `price_pt_eur_mwh`, `contract_version`, `source_file`, `content_sha256`,
`fetched_at`, `run_id`, `landing_path`; key (`delivery_date`, `period`). It mirrors the file (one
line, one row), names each zone's column after the zone rather than its position, and keeps the
two zones comparable in one row (decoupling is `price_es <> price_pt`). Both resolutions share the
table: `resolution_minutes` and `contract_version` say which a row is, and Silver (phase 3)
resamples. A row per zone would double the rows and push the comparison into a self-join.

### Partitioned by `month(delivery_date)`

The column every run replaces by is the partition column, so a run's overwrite deletes whole
files of one partition and never rewrites a neighbour's rows, before or after compaction (the
measurement above); `month(interval_start_utc)` splits every month's run across two partitions,
and once compacted a rerun rewrites part of the previous month. A month is one 70 KB file per run:
twelve partitions a year, which the phase-4 compaction keeps at one file each once daily runs
(PR 5) add a file a day. Unpartitioned, compaction would merge everything into files that every
rerun rewrites; by day, a three-year table is over a thousand partitions of 5 KB. Queries by
`interval_start_utc` still prune by the files' column statistics, and the table is small.

### Chunks of a calendar month

A chunk is the delivery days of one calendar month: one download per day (at most one a second),
then one Write-Audit-Publish run replacing exactly those days (`delivery_date` between the first
and the last), about 40 s for a full month. A run per day would cost a WAP cycle, two snapshots and
a small file per day of a backfill; a year would download 365 files before writing any and lose
them all to one bad file. The month is also the partition.

### Not published yet

A day OMIE has no file for (404) after today (Madrid) ends the ingestion there: the chunk writes
the days before it, no later day is asked for, the CLI says "not published yet from <day>" and
exits 0, and the snapshot records `ibergrid.omie.not-published-from`. A missing day up to today is
an error: its chunk writes nothing and the run exits 1, since results for day D come out on D-1.
The rule does not depend on the hour, so tomorrow is never an error, before or after its
publication; a schedule that runs too early publishes up to today and says so.

### Landing

Each file, after its chunk passed the checks and before the run, goes to
`s3://<bucket>/landing/omie/marginalpdbc/<YYYY>/<MM>/marginalpdbc_<YYYYMMDD>-<sha256[:12]>.1`,
byte for byte, outside the warehouse prefix (`ibergrid.storage.land`, now shared with REData).
OMIE's files carry no volatile metadata, so the hash is over the raw bytes: a rerun lands the same
key, a file OMIE corrected gets its own, and each row's `landing_path` and `content_sha256` name
the bytes it came from.

### HTTP client

`ibergrid._http.PoliteClient`, REData's request pace and retry loop moved out of its connector
unchanged (at most one request a second per process, four attempts on timeouts, broken responses,
429 and 5xx, exponential backoff with jitter or `Retry-After` up to 60 s), with
`IBERGRID_OMIE_*` settings (`MAX_RPS` capped at 1, a 30 s timeout). OMIE's client turns a 404 into
`None` and any other 4xx into a permanent error; redirects are followed (`urllib`'s default,
pinned by a unit test with a 302).

### Write-Audit-Publish per chunk

As for REData: run id `<prefix>-omie-<first>-<last>-a<attempt>-<4 random hex>`; `wap.write(...,
overwrite_filter=<the days written>, snapshot_properties=<measurements>)`; the audit reads the
branch head and requires, inside the window, as many rows and distinct keys as the batch and,
outside it, as many rows as on the run's base; a `PublishConflict` restarts the chunk on the new
`main` (three attempts) and any other failure discards the branch. The snapshot summary records
`ibergrid.omie.window`, `files`, `periods`, `contract-versions` (files per version),
`rule-violations` (prices outside the harmonised -500..4000 EUR/MWh per column: counted, never
refused) and, when it applies, `not-published-from`. Completeness needs no measure of its own: a
day is either complete or refused.

### A format change during a historical backfill

How a change of the file's columns, during a backfill, is handled:

1. **Caught before writing.** The date says which contract a file must follow and the grammar is
   strict, so an added or removed field, a comma decimal, a new header or another period count is
   refused before anything of its chunk is landed or written, with the file and line in the error.
2. **Contained.** A backfill is a sequence of month chunks, in order. The refusing chunk writes
   nothing, the published ones stay, the CLI exits 1: no month holds a mix of readings.
3. **Fixed as a new version, not a looser parser.** The change becomes a contract v3 with the
   first delivery day it applies to and its own line rule, mapped onto the same columns (new data
   arrives as optional columns through `ensure_table`'s additive evolution), and `contract_version`
   on each row tells Silver which rule produced it. A v3-shaped file stamped with an older date
   stays refused.
4. **Resumed by rerunning.** Chunks are idempotent, so the backfill restarts at the failed month,
   or over the whole range at the cost of the downloads.
5. **What the grammar cannot see** is a change that keeps the shape: Spain and Portugal swapped, or
   a unit changed. That is the cross-source check's job (next).

### Cross-source reconciliation (phase 4)

REData's spot price (`market_prices`, series 600) is OMIE's Spanish price: equal on every
quarter-hour of the two days both sources were recorded for. Phase 4 makes it a gate between the
audit and the publish: for each interval, REData's spot equals `price_es_eur_mwh`, joining on UTC
time (REData's `ts_utc` and OMIE's `interval_start_utc`). From 2025-01-01 to 2025-09-30 REData
reports the spot at 15-minute points while OMIE's periods are hourly: each OMIE hour must equal
the four REData points it covers (ADR 0003: REData repeats each hourly value four times then);
before 2025 both are hourly. Until then the check is a unit test on the recorded answers.

### For the Dagster pull request

- One table and "main at the run's base" serialise OMIE's chunks, like REData's: a concurrency
  limit of 1 for the OMIE assets keeps both the publish order and the request pace (REData and
  OMIE write different tables and do not conflict with each other).
- Daily partitions by delivery day suit the schedule: tomorrow's file after its publication. A run
  before it publishes nothing and succeeds, so the asset should report it as skipped, not as an
  empty success. Backfills hand a range of partitions to one `ingest(settings, start, end, ...)`
  call, which cuts it into months.
- The snapshot measurements become materialization metadata, since snapshot expiry removes them.

## Consequences

- Good: any window can be rerun or backfilled without cleanup, across the 2025-10-01 change and
  every DST day; both resolutions sit in one table with contiguous UTC intervals.
- Good: a file that does not follow its day's contract is refused with the file and line named,
  before anything is written; a backfill stops at the first such month and resumes by rerunning.
- Good: the original files are kept byte for byte and every row points at its own.
- Bad: a 404 cannot tell "not published yet" from a moved download URL for days after today; for
  earlier days it is an error, and the live test downloads a recent day to notice a move.
- Bad: tomorrow is never an error, so a schedule that runs before publication succeeds with
  nothing new (it says "not published yet").
- Bad: the per-chunk Write-Audit-Publish loop is now in two connectors with small differences
  (REData's dropped series); a third connector should extract it.
- Neutral: a column swap with the same format passes the grammar until the phase-4
  reconciliation with REData runs.
