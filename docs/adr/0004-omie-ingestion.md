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
2. A file's name carries a version, and OMIE republishes some days under a new one: which file is
   a day's, and what happens to rows already written from a file OMIE replaced?
3. How do OMIE's numbered periods become UTC intervals, on 23-, 24- and 25-hour days?
4. One row per period with both zones, or one row per zone?
5. How is a three-year table of 24 to 100 rows a day partitioned?
6. What does a run do with a delivery day whose results are not out yet?

## Decision drivers

- A rerun of any window leaves exactly the rows a single run leaves, so backfills need no cleanup.
- Bronze keeps what OMIE published; only what would corrupt the table or misread the file is
  refused. A file read with the wrong contract is the worst outcome: plausible, wrong rows.
- No new dependency and no new service; polite to a free public site.
- Every claim measured, with the request that showed it.

## Considered options

- **Contract.** (a) the delivery date picks the version, the period count confirms it; (b) the
  period count alone picks it; (c) one lenient parser for every shape.
- **File version.** (a) ask for `.1`, then `.2` and up, and take the first that exists; (b) read
  OMIE's file listing; (c) always `.1`.
- **Rows.** (a) one row per period, Spain and Portugal side by side; (b) one row per zone and
  period.
- **Partitioning.** (a) `month(delivery_date)`; (b) `month(interval_start_utc)`; (c) none; (d)
  `delivery_date` by day.
- **Chunks.** (a) a calendar month of delivery days per run; (b) one day per run; (c) a year.
- **Days not published yet.** (a) end the ingestion at the first one after today, writing the
  days before it, and let the caller require publication; (b) always fail; (c) skip it and go on.

## Measurements

Measured on 2026-11-21 between 05:43 and 07:08 UTC, at most one request a second: probes for days
without a file, 16 downloads of the first fixtures (curl, then the connector's own client), the
journeys below inside the stack, and `make test-live`. OMIE's file listing
(`/es/file-access-list?...realdir=marginalpdbc`, 548 KB, with each file's size and last
modification time) was fetched once by this pull request's reviewer the same morning; the figures
below that come from it say so. The files the tests replay are in `tests/fixtures/omie`, byte for
byte and under OMIE's names (recorded by `scripts/record_omie_fixtures.py`, SHA-256 pinned by a
test).

### The download

`GET /es/file-download?parents=marginalpdbc&filename=marginalpdbc_<YYYYMMDD>.<version>`:

| Request | Answer |
|---|---|
| A published day under its name | 200 at once, no redirect (curl `num_redirects=0`; the brief for this work saw a 302 first, not reproduced), `application/octet-stream`, `Content-Disposition: attachment; filename=...`; 0.44 to 0.53 s |
| A name OMIE does not have: 2026-11-22 at 06:43 Madrid time (results due that afternoon), 2027-06-01, `marginalpdbc_20230528.1`, 2022-06-15 | 404, `text/html`, OMIE's 26,832-byte "not found" page, 0.35 to 0.54 s |

### File versions (listing)

The listing holds daily files from 2023-01-01 to 2026-11-21, **one per day**: 1,418 named `.1`
and three republished ones, `marginalpdbc_20230528.2` (last modified 2023-05-27 16:05),
`marginalpdbc_20251030.3` (2025-10-29 13:31) and `marginalpdbc_20251127.2` (2025-11-26 14:59);
for these `.1` answers 404, so a republication replaces the file rather than adding one beside
it. The connector's client found `marginalpdbc_20230528.2` on its second request and
`marginalpdbc_20251030.3` on its third (5 requests in 6.2 s); both meet their contracts (24 and 96
periods). Years up to 2022 are listed only as yearly archives (`marginalpdbc_2018.zip` to
`marginalpdbc_2022.zip`), and a 2022 day answers 404: daily files start on 2023-01-01.

52 of the 1,421 files were last modified on their delivery day or later, up to two days after it
(17 of them since 2025-10-01; `marginalpdbc_20251102.1`, for example, at 01:38 on 2025-11-02): a
file can also be rewritten in place under the same name.

### Publication time (listing)

A file's last modification time is an upper bound of its first publication. 1,369 of the 1,421
files were last modified the day before their delivery day, Madrid time: earliest 12:57, median
13:26, 75 % by 14:06, 95 % by 17:10. Since the 15-minute market (400 files): median 14:06, 75 % by
14:26, 90 % by 15:34, 95 % by 19:16; 37 after 16:00, 27 after 18:00. The publication of the
2026-11-22 file was not watched live.

### The file

The 10 recorded files, and the files of the journeys parsed under the same rules:

- Latin-1 text, CRLF line ends: `MARGINALPDBC;`, then `YYYY;MM;DD;period;price_PT;price_ES;`, then
  `*`. 535 to 691 bytes for an hourly day, 2.2 to 2.7 KB for a quarter-hourly one.
- **Column 5 is Portugal, column 6 Spain.** REData's spot price (`market_prices`, series 600,
  recorded for the same days) equals column 6 and differs from column 5 wherever the zones
  differ: on 2025-04-28 (the Iberian blackout) in periods 8 (Portugal 40.7, Spain and REData
  47.94), 15 (-1.51; -3) and 16 (-1.51; -1.54), REData repeating each hour at its four
  quarter-hours; on 2025-10-01 in quarter-hours 40 and 73. On 2026-03-29 and 2026-11-18 the zones
  never differ, so those days confirm the UTC alignment (92 and 96 quarter-hours equal instant for
  instant) but not the column order. All of it is pinned by unit tests on the recorded answers.
- Prices are written with a dot and zero to two decimals in the recorded files (`18`, `16.3`,
  `24.63`), negative ones with a minus (down to -3 EUR/MWh on 2025-04-28). The zones differed in
  202 of October 2026's 2,980 quarter-hours.
- Periods per day, against the length of the Madrid day:

| Delivery day | Periods | Contract |
|---|---|---|
| 2024-03-31 (spring forward) | 23 hourly | v1 |
| 2024-10-27 (fall back) | 25 hourly | v1 |
| 2023-05-28 (`.2`), 2025-04-28, 2025-09-30 | 24 hourly | v1 |
| 2025-10-01 (first 15-minute day), 2025-10-30 (`.3`), 2026-11-18 | 96 quarter-hours | v2 |
| 2025-10-26 (fall back) | 100 | v2 |
| 2026-03-29 (spring forward) | 92 | v2 |

### Journeys in the stack

`docker compose run --rm --no-deps ingest omie ...` from the app image against omie.es (memory:
the container's cgroup `memory.peak`, under its 384 MiB limit). Before the version fix:

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

After the version fix, on a fresh stack:

| Window | Result | Time | Peak memory |
|---|---|---|---|
| 2023-05-28 | `marginalpdbc_20230528.2` (v1), 24 rows | 4.0 s (rerun 5.6 s) | 92 MiB (94) |
| 2025-10-25..11-05 | 2 chunks: 7 files (v2, `marginalpdbc_20251030.3` among them) and 676 rows, 5 files and 480 rows | 19.7 s (17.6 s) | 101 MiB (101) |
| 2025-11-27 | `marginalpdbc_20251127.2` (v2), 96 rows | 4.0 s (4.1 s) | 95 MiB (94) |
| 2026-11-22, `--require-published` (at 08:06 Madrid time) | "OMIE has not published marginalpdbc_20261122.1 to .5 yet ...", exit 1 | 6.0 s (5 requests) | 73 MiB |

The rerun left main's 1,276 rows identical (every column but `fetched_at` and `run_id`), with
`source_file`, `file_version` and the landing key naming `.2`, `.3` and `.2` for the republished
days, 14 landed files for 14 days, and only `main` as ref.

### Partitioning

Measured on a SQL catalog with those rows (2025-09-25..30 written by one run and 2025-10-01..05 by
another, then October rerun), counting the files the rerun's overwrite touches:

| Spec | October's run writes | The rerun | The rerun after compacting each partition into one file |
|---|---|---|---|
| `month(delivery_date)` | 1 file, 1 partition | deletes 1 file (480 rows), adds 1 | the same |
| `month(interval_start_utc)` | 2 files, 2 partitions (the first two hours of a Madrid month are in the previous UTC month) | deletes 2 files, adds 2 | rewrites September's file: deletes 152 rows (144 of September), re-adds September's 144 |

A rerun of part of what one file holds rewrites that file, whatever the spec: with 2025-10-25 and
26 written by one run, a rerun of the 26th alone deleted that file (196 rows) and wrote the 25th's
96 rows back unchanged, run id included (pinned by a unit test).

## Decision outcome

### Two contract versions, picked by the date and confirmed by the count

`ibergrid.sources.omie.marginalpdbc`: v1 for delivery days up to 2025-09-30 (60-minute periods),
v2 from 2025-10-01 (15 minutes). The expected count is the length of the Madrid day divided by
the resolution: 23/24/25 or 92/96/100. Before anything is landed or written, a file is refused
(`InvalidFile`, which names the file under its version, the line and the problem, the whole chunk
with it) for: a first line other than the header (an HTML page, a BOM), no trailer, a line that is
not exactly six `;`-terminated fields of four-digit year, two-digit month and day, a period
without leading zeros (1 to 999) and two decimal prices (`-?\d+(\.\d+)?`: no comma, no exponent,
no empty field), a date other than the requested day, periods other than 1, 2, 3 and so on, or a
count that is not the expected one. A count that fits the other version is reported as such ("96
periods make a v2 file ..., but 2025-09-30 is a v1 day"). Picking the version from the count
alone would read a quarter-hourly file stamped with an old date as valid; a lenient parser would
turn a changed column into wrong prices.

The contract version (v1, v2: the file's format) is not the file version (`.1`, `.2`: OMIE's
publication of that day); each row records both.

### The file version: the first that exists, from `.1`

`OmieClient.fetch(day)` asks for `.1`, then `.2` and so on up to `.5` (the highest seen is 3) and
takes the first that answers 200; a 404 for every version means OMIE has no file for the day. The
listing shows one file per day and republications that replace the earlier version, so the first
that exists is the current one; were two versions ever to coexist, this would take the lower, and
asking for the next version of every day to rule that out would double the requests. Reading the
listing instead would download 548 KB per run, and assuming `.1` is the bug this replaced: the
three republished days could never be ingested, a backfill stopped at 2023-05, and a correction
published as `.2` left the superseded prices in Bronze.

The cost: a normal day is one request, a republished day one more per version, and a day with no
file at all five (about 5 s at one a second), so a run that asks for tomorrow before its
publication spends five requests to learn it. The name travels with the data: `source_file`
(`marginalpdbc_20251030.3`), `file_version` (3), the landing key's extension and every message.

**Corrections.** A day republished under a new version, or rewritten in place (52 files in the
listing), changes Bronze only when that day is ingested again: the rerun finds the current file,
lands it under its own key (its hash and version differ) and replaces the day's rows, whose
`file_version`, `content_sha256` and `landing_path` show which publication they came from. Every
rewrite in the listing happened by the second day after delivery, so the scheduled ingestion
(PR 5) re-ingests a trailing window of the last 7 delivery days on each run.

### Periods to UTC

`interval_start_utc = UTC(Madrid midnight of D) + (period - 1) * resolution`, `interval_end_utc =
start + resolution`. The intervals of a day are contiguous and end at the next Madrid midnight on
every DST case recorded (pinned per fixture), and the last hour of 2025-09-30 ends at 22:00 UTC
where the first quarter-hour of 2025-10-01 starts (pinned on the stack too).

### One row per period, Spain and Portugal side by side

`bronze.omie_day_ahead_prices` (declared in `ibergrid.lake.registry`, created by `ibergrid lake
init`): `delivery_date`, `period`, `resolution_minutes`, `interval_start_utc`, `interval_end_utc`,
`price_es_eur_mwh`, `price_pt_eur_mwh`, `contract_version`, `source_file`, `file_version`,
`content_sha256`, `fetched_at`, `run_id`, `landing_path`; key (`delivery_date`, `period`). It
mirrors the file (one line, one row), names each zone's column after the zone rather than its
position, and keeps the two zones comparable in one row (decoupling is `price_es <> price_pt`).
Both resolutions share the table: `resolution_minutes` and `contract_version` say which a row is,
and Silver (phase 3) resamples. A row per zone would double the rows and push the comparison into
a self-join.

### Partitioned by `month(delivery_date)`

The column every run replaces by is the partition column, so a run never touches another month's
partition, before or after compaction: a rerun of a whole month deletes that month's files and
writes one, and a rerun of some of its days (a daily run, a correction) rewrites only the files
that hold those days, copying the other days' rows into a new file, unchanged. With
`month(interval_start_utc)` every month's run spreads over two partitions, and once compacted a
rerun rewrites part of the previous month (the measurement above). A month is one 70 KB file per
run: twelve partitions a year, which the phase-4 compaction keeps at one file each once daily runs
(PR 5) add a file a day; after that compaction a daily rerun rewrites its month's file, about
3,000 rows. Unpartitioned, compaction would merge everything into files that every rerun rewrites;
by day, a three-year table is over a thousand partitions of 5 KB. Queries by `interval_start_utc`
still prune by the files' column statistics, and the table is small.

### Chunks of a calendar month

A chunk is the delivery days of one calendar month: one download per day (at most one a second),
then one Write-Audit-Publish run replacing exactly those days (`delivery_date` between the first
and the last), about 40 s for a full month. A run per day would cost a WAP cycle, two snapshots and
a small file per day of a backfill; a year would download 365 files before writing any and lose
them all to one bad file. The month is also the partition.

### Not published yet

A day for which OMIE has no file in any version after today (Madrid) has not been published yet.
By default it ends the ingestion there: the chunk writes the days before it, no later day is
asked for, the CLI says "not published yet from <day>" and exits 0, the snapshot records
`ibergrid.omie.not-published-from`, and the last report's `not_published_from` gives the day to a
library caller (None when every day was out). With `require_published` (CLI
`--require-published`) the same day is an error: the chunks before its chunk stay published, its
own chunk writes nothing, and the run exits 1. A missing day up to today is always an error, since
results for day D come out on D-1. The default does not depend on the hour, so a run that asks for
tomorrow before its publication succeeds with nothing new; the scheduled run (PR 5) passes
`require_published` once past a cut-off chosen from the publication times above (95 % of the
15-minute era's files last modified on D-1 were final by 19:16; 91 % of all that era's files), so
a late file fails loudly instead of passing as "not yet". Because the flag also discards the
validated days of its own month chunk, PR 5 asks for the trailing re-ingest window without the
flag and for tomorrow alone with it, in two calls.

Today is read once when `ingest` starts (`madrid_today()`, with an injectable clock for tests): a
run that crosses Madrid's midnight keeps judging by the day it started on, which errs towards "not
published yet" for the new day.

### Landing

Each file, after its chunk passed the checks and before the run, goes to
`s3://<bucket>/landing/omie/marginalpdbc/<YYYY>/<MM>/marginalpdbc_<YYYYMMDD>-<sha256[:12]>.<N>`
(`<N>` is the file version), byte for byte, outside the warehouse prefix
(`ibergrid.storage.land`, now shared with REData). OMIE's files carry no volatile metadata, so the
hash is over the raw bytes: a rerun lands the same key, and a file OMIE republished or rewrote
gets its own, so each row's `landing_path` and `content_sha256` name the bytes it came from.

### HTTP client

`ibergrid._http.PoliteClient`, REData's request pace and retry loop moved out of its connector
with its behaviour, messages and retry log fields unchanged (at most one request a second per
process, four attempts on timeouts, broken responses, 429 and 5xx, exponential backoff with jitter
or `Retry-After` up to 60 s; each source names its log fields, `widget` for REData and `file` for
OMIE), with `IBERGRID_OMIE_*` settings (`MAX_RPS` capped at 1, a 30 s timeout). OMIE's client
turns a 404 into the next version, and 404 for every version into `None`; any other 4xx is a
permanent error; redirects are followed (`urllib`'s default, pinned by a unit test with a 302).

### Write-Audit-Publish per chunk

As for REData: run id `<prefix>-omie-<first>-<last>-a<attempt>-<4 random hex>`; `wap.write(...,
overwrite_filter=<the days written>, snapshot_properties=<measurements>)`; the audit reads the
branch head and requires, inside the window, as many rows and distinct keys as the batch and,
outside it, as many rows as on the run's base; a `PublishConflict` restarts the chunk on the new
`main` (three attempts) and any other failure discards the branch. The snapshot summary records
`ibergrid.omie.window`, `files`, `periods`, `contract-versions` (files per contract version),
`rule-violations` (prices outside the harmonised -500..4000 EUR/MWh per column: counted, never
refused) and, when it applies, `not-published-from`. Completeness needs no measure of its own: a
day is either complete or refused.

### A format change during a historical backfill

How a change of the file's columns, during a backfill, is handled:

1. **Caught before writing.** The date says which contract a file must follow and the grammar is
   strict, so an added or removed field, a comma decimal, a new header or another period count is
   refused before anything of its chunk is landed or written, with the file, its version and the
   line in the error.
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

A republished day (`.2`) is not a format change: it is found and read like any other.

### Cross-source reconciliation (phase 4)

REData's spot price is OMIE's Spanish price. The unit tests check it on the recorded days where it
can tell the columns apart (2025-04-28 and 2025-10-01: equal to Spain everywhere, different from
Portugal wherever the zones differ; swapping the columns fails them), and the UTC alignment on a
92-quarter-hour day and a recent one. Phase 4 makes it a gate between the audit and the publish:
for each REData spot point, the OMIE period holding it has that Spanish price, joining on UTC time
(REData's `ts_utc` within OMIE's `interval_start_utc`..`interval_end_utc`). From 2025-01-01 to
2025-09-30 REData reports the spot at 15-minute points while OMIE's periods are hourly, so each
OMIE hour holds four equal REData points (ADR 0003); before 2025 both are hourly. A swap only
shows on days the zones decouple: the gate counts how many such periods each run checked.

### For the Dagster pull request

- One table and "main at the run's base" serialise OMIE's chunks, like REData's: a concurrency
  limit of 1 for the OMIE assets keeps both the publish order and the request pace (REData and
  OMIE write different tables and do not conflict with each other).
- Daily partitions by delivery day. The schedule asks for tomorrow after its usual publication and
  passes `require_published` past the cut-off (above), so a missing file is a failed run, not an
  empty success; each run also re-ingests the last 7 delivery days to pick up republications and
  in-place rewrites. Backfills hand a range of partitions to one `ingest(settings, start, end,
  ...)` call, which cuts it into months; daily files start on 2023-01-01.
- The snapshot measurements become materialization metadata, since snapshot expiry removes them.

## Consequences

- Good: any window from 2023-01-01 can be rerun or backfilled without cleanup, across the
  2025-10-01 change, every DST day and every republished day; both resolutions sit in one table
  with contiguous UTC intervals.
- Good: a file that does not follow its day's contract is refused with the file and line named,
  before anything is written; a backfill stops at the first such month and resumes by rerunning.
- Good: the original files are kept byte for byte and every row points at its own, version
  included.
- Bad: a 404 for every version cannot tell "not published yet" from a moved download URL for
  days after today; for earlier days it is an error, and the live test downloads a recent day and
  a republished one to notice a move. A day with no file costs five requests.
- Bad: by default tomorrow is never an error, so a caller that runs before publication succeeds
  with nothing new (it says "not published yet"); callers that need the file pass
  `require_published`.
- Bad: a correction reaches Bronze only when its day is ingested again; the trailing window of
  PR 5 bounds the delay.
- Bad: the per-chunk Write-Audit-Publish loop is now in two connectors with small differences
  (REData's dropped series); a third connector should extract it.
- Neutral: years before 2023 are only in OMIE's yearly archives, which this connector does not
  read.
- Neutral: a column swap with the same format passes the grammar until the phase-4
  reconciliation with REData runs.
