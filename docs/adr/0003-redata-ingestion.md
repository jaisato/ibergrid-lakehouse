# 0003. REData ingestion into Bronze

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

Phase 1 starts with Red Eléctrica's REData API (`https://apidatos.ree.es`, public, no token): the
first connector that writes real data through the Write-Audit-Publish library of
[ADR 0002](0002-iceberg-write-and-read-paths.md). It has to be callable from the CLI now and from
Dagster in a later pull request, and rerunning any window has to be safe. REData's own
documentation does not say how large a request may be, which combinations of widget, time
granularity and electric system exist, or how the time zones work, so this record answers those
questions from measurements and then decides:

1. One Bronze table for every widget, or one per widget?
2. How are requests cut into chunks, and which time zone defines a window?
3. What is checked before writing, and what happens to gaps in the source?
4. Where do the raw answers go?
5. Generation by technology exists only per day: what would hourly generation take?

## Decision drivers

- A rerun of a window leaves exactly the rows a single run leaves (idempotency), so backfills and
  retries need no cleanup.
- Bronze stores what the source gave; only what would corrupt the table is refused (ADR 0001's
  medallion: hard quality gates are phase 4).
- No new HTTP dependency and no new service; polite to a free public API.
- Every limit claimed here measured, with the request that showed it.

## Considered options

- **Table layout.** (a) one long table `bronze.redata_observations`, one row per point, keyed by
  (dataset, electric system, series id, UTC timestamp); (b) one table per widget; (c) one column
  per series.
- **Completeness.** (a) measured and recorded; (b) enforced, refusing a chunk with missing points.
- **Chunks.** (a) one request per calendar month (sub-daily widgets) or calendar year (daily);
  (b) a fixed number of days; (c) one request per day.
- **Windows.** (a) local Spanish days, stored in UTC; (b) UTC days.
- **Landing.** (a) one object per chunk at a deterministic key, overwritten by a rerun; (b) one
  immutable object per fetch.

## Measurements

Measured on 2026-11-21 against the live API, `/en/datos/` (English titles; series ids do not
depend on the language), one request a second. The probes' summaries are not committed; the
answers the tests replay are (`tests/fixtures/redata`, recorded by
`scripts/record_redata_fixtures.py`, which keeps only some series of the large five-minute
answers).

Widgets and parameters:

| Dataset | Widget | `time_trunc` | Points | Systems |
|---|---|---|---|---|
| `demand_realtime` | `demanda/demanda-tiempo-real` | `hour` | 5 minutes: 288 a day, 276 on 2026-03-29, 300 on 2026-10-25 | peninsular only; geo parameters answer 400 or 502 |
| `demand_hourly` | `demanda/evolucion` | `hour` | 24 a day (23, 25) | `geo_trunc=electric_system`, `geo_limit` and `geo_ids`: peninsular 8741, canarias 8742, baleares 8743, ceuta 8744, melilla 8745; every system's series has id 10297; without geo parameters: 400 |
| `generation_daily` | `generacion/estructura-generacion` | `day` | one per technology and day | without geo parameters (national: the sum of the systems, 724,850 MWh on 2026-11-20 against 687,088 peninsular) and each of the five systems, each with its own technologies |
| `market_prices` | `mercados/precios-mercados-tiempo-real` | `hour` | PVPC 24 a day; spot 24 a day until 2024-12-31, 96 from 2025-01-01 (each hourly value four times until the 15-minute market, distinct by 2026-11) | without geo parameters; with them only PVPC comes back |

Largest request windows (whole days, `T00:00` to `T23:59`):

| Widget | Accepted | Refused (400) |
|---|---|---|
| `demanda-tiempo-real` | 2026-10-01..31 (8,940 points per series, 3.3 MB, 5.3 s); 2026-10-15..11-14 (10.0 s) | 2026-10-01..11-01; 2026-02-15..03-16 (30 days); 2026-09-01..10-31; a quarter |
| `evolucion` (canarias) | 2026-10-01..31 (745 points, 1.2 s); 2026-10-31..11-01 | 2026-10-01..11-01; a quarter; a year |
| `precios-mercados-tiempo-real` | 2026-10-01..31 (PVPC 745, spot 2,980; 2.0 s) | 2026-10-01..11-01; a quarter |
| `estructura-generacion` | 2025 (canarias 6.5 s; national 18.9 s, 560 KB); 2024 (366 days); 2024-07-01..2025-06-30; 2024-01-01..2025-01-01 | 2024-01-01..2025-03-31; 2024..2025 |

The rule is not a number of days (30 days across February and March fail, 31 days from the 15th
work); a calendar month and a calendar year are always accepted.

Other answers:

- Windows without data answer **502** with the same body as a failure ("The requested data is
  currently unavailable. Please try again later."): 2026-12-05 for all four widgets, tomorrow
  for `precios`, today for `evolucion` and `estructura-generacion`, and 2014-01-01 for
  `demanda-tiempo-real`. Today's
  five-minute demand answers 200 with a partial Real series (53 of 288 points at 04:33).
- Datetimes carry their offset, and every system is delivered in peninsular time (Canarias
  included: `2026-11-18T00:00:00.000+01:00`). The 25-hour day has both `02:00+02:00` and
  `02:00+01:00`.
- Negative values are real: the spot price reached -6.01 EUR/MWh (2025-04), PVPC -13.14 (2026-10)
  and coal's daily net generation -120 MWh (2026). Percentages stayed within 0..1 everywhere,
  also in the months with negative prices (the spot's share bottoms out at 0).
- "Total scheduled" (id 2054) exists in recent five-minute data and not in 2023 (June 2023 has
  Real, Forecasted and Scheduled).
- Generation by technology has no hourly view: `estructura-generacion`, `estructura-renovables`
  and `evolucion-renovable-no-renovable` answer 400 with `time_trunc=hour`. The one generation
  widget that answers hourly, `evolucion-estructura-generacion-emisiones-asociadas`, gives two
  aggregates (with and without CO2 emissions) with 25 and 26 points for 2026-11-19, a 24-hour day,
  the first stamped `+02:00` in November.
- Memory: a month of five-minute demand (35,760 rows) in the app container peaked at 195 MiB of
  cgroup memory (268 MiB resident, shared libraries included); under the previous 192 MiB limit
  the same load reached the limit (44 `max` events in one run) and survived only by swapping.

## Decision outcome

### One long Bronze table

`bronze.redata_observations` (declared in `ibergrid.lake.registry`, created by `ibergrid lake
init`): `dataset`, `widget`, `electric_system`, `series_id`, `series_type`, `series_title`,
`granularity`, `ts_utc`, `ts_local` (the datetime string as delivered), `value`, `percentage`,
`series_last_update`, `fetched_at`, `run_id`, `landing_path`, `payload_sha256`. Partitioned by
`dataset` (identity) and `month(ts_utc)`, which needs PyIceberg's `pyiceberg-core` extra (locked
since ADR 0002).

Every widget answers in the same JSON:API shape, so one normaliser and one table cover them all,
a new widget is a registry entry and not a migration, and schema drift (Total scheduled
appearing) arrives as rows, not columns. One table per widget would repeat the same schema four
times; a column per series breaks on every drift. Silver (phase 3) pivots what it needs.

### Chunks, windows and idempotency

A chunk is one dataset, one system and whole local days inside one calendar month (sub-daily
widgets) or one calendar year (daily generation): the measured limits, and the smallest number of
requests. It is one request, one landing object and one Write-Audit-Publish run.

Local days are Madrid days (`Europe/Madrid`, from the system zoneinfo or the `tzdata` wheel)
because REData's days are, for every system. A chunk replaces exactly the rows of its dataset and
system with `ts_utc` in [UTC of the first local midnight, UTC of the midnight after the last day).
A UTC day would cut REData's days in two and could not be requested.

So a rerun of the same window publishes the same rows, apart from `fetched_at` and `run_id`
(pinned by a unit test on the SQL catalog and an integration test on the stack). A rerun replaces
the window with what REData answers now: if a series disappears from an answer, its rows in that
window go too. Chunks run in order and stop at the first failure; the published ones stay.

### Validation before writing; completeness measured

Before anything is landed or written, the whole chunk is refused (`InvalidResponse`) on: a shape
or type problem; a value that is not a finite number; a datetime without an offset; a point outside
the window; a repeated (series, timestamp); a value outside the dataset's rule (demand > 0;
prices within the harmonised limits -500..4000 EUR/MWh; generation any finite value, since net
generation can be negative); a percentage outside 0..1; or no point at all, so that an empty
answer never empties a window.

Completeness is measured, not enforced: for every local day, the points expected from each series'
granularity (276/288/300 five-minute points, 92/96/100 quarter-hours, 23/24/25 hours, one daily
point) against those received. Granularity is measured per series and chunk from the delivered
spacing, because the spot price changed from hourly to quarter-hourly; a series with a single point
takes the dataset's nominal one. The CLI prints the totals and the incomplete days; the snapshot
summary records `ibergrid.redata.expected-points`, `received-points`, `incomplete-days` (only the
days that differ, as JSON), `window`, `dataset`, `system` and `payload-sha256`. A series missing
from the whole answer is not counted (nothing says it should be there), and a series that loses
points can be measured at a coarser spacing (shortcut: measured, not declared, until phase 4
declares expected series and spacings).

### Landing

The raw body goes, after validation and before the run, to
`s3://<bucket>/landing/redata/<dataset>/<system>/<YYYY>/<MM>/<start>_<end>.json`, outside the
warehouse prefix, through pyarrow's S3 client (`ibergrid.storage.s3_filesystem`, shared with
`doctor`). A rerun overwrites it, so landing stays one object per window. Each row carries the
object's path and the body's SHA-256: if a run fails after landing, the object is newer than the
table, and the hash says so.

### HTTP client

`ibergrid._http.exchange` (the standard-library helper, now returning headers for
`Retry-After`): at most `IBERGRID_REDATA_MAX_RPS` requests a second (default and maximum 1), four
attempts on timeouts, 429 and 5xx with exponential backoff (1, 2, 4 s, times a random 0.5..1) or
`Retry-After` up to 60 s; any other status is permanent and names the widget and the parameters.
A window without data therefore fails after 3.5 to 7 s of waiting, with REData's message.

### Write-Audit-Publish per chunk

Run id `<prefix>-<dataset>-<system>-<start>-<end>-a<attempt>` (the prefix is the caller's, such
as an orchestrator's run id, or a UTC timestamp); `wap.write(..., overwrite_filter=<window>,
snapshot_properties=<completeness>)`; the audit reads the branch head with PyIceberg inside the
window and requires as many rows, and as many distinct keys, as the validated batch; `wap.publish`
moves `main`. Any failure discards the branch; a `PublishConflict` (another writer moved `main`)
restarts the chunk on the new `main`, up to three attempts.

### Hourly generation is not in REData

REData offers generation by technology per day only (measured above). Hourly or quarter-hourly
generation by technology needs another source, and both candidates need a personal token
(neither was probed for this record): ENTSO-E's Transparency Platform (actual generation per
production type, bidding zone ES) or REE's ESIOS API (generation indicators per technology). Which one, and who holds the token, is the owner's
decision for phase 3; until then Bronze holds daily generation.

## Consequences

- Good: any window can be rerun or backfilled without cleanup; the Dagster assets of a later pull
  request call `ingest(settings, dataset, start, end, systems, run_id_prefix)` per partition.
- Good: gaps are visible per day in the CLI and in the snapshot summary without blocking a
  partial source (today's demand, a series that started late).
- Good: four widgets, one table, one normaliser; a widget with the same shape is a registry
  entry.
- Bad: the long table needs a pivot for most analyses (phase 3's Silver models).
- Bad: a window without data and a failing API look the same (502), so the former costs four
  requests and fails; a schedule must not ask for days REData has not published.
- Bad: the app one-shots' memory limit goes from 192 MiB to 384 MiB for a month of five-minute
  data.
- Neutral: landing objects are overwritten, so only the latest answer per window is kept; the
  table's history (snapshots) keeps the earlier rows until expiry.
- Neutral: `balance/balance-electrico` (nested `content[]`) is not ingested yet.
