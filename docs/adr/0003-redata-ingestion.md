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
3. What is refused before writing, and what is only measured?
4. Where do the raw answers go?
5. Generation by technology exists only per day: what would hourly generation take?

## Decision drivers

- A rerun of a window leaves exactly the rows a single run leaves (idempotency), so backfills and
  retries need no cleanup.
- Bronze stores what the source gave; only what would corrupt the table is refused (ADR 0001's
  medallion: hard quality gates are phase 4).
- No new HTTP dependency and no new long-running service; polite to a free public API.
- Every limit claimed here measured, with the request that showed it.

## Considered options

- **Table layout.** (a) one long table `bronze.redata_observations`, one row per point, keyed by
  (dataset, electric system, series id, UTC timestamp); (b) one table per widget; (c) one column
  per series.
- **Checks.** (a) refuse structural problems, measure everything else (completeness, value-rule
  breaches, series that disappear); (b) also refuse missing points or implausible values.
- **Chunks.** (a) one request per calendar month (sub-daily widgets) or calendar year (daily);
  (b) a fixed number of days; (c) one request per day.
- **Windows.** (a) local Spanish days, stored in UTC; (b) UTC days.
- **Landing.** (a) one object per window at a fixed key, overwritten by a rerun; (b) one object
  per window and answer, named by the answer's hash; (c) one object per fetch.

## Measurements

Measured on 2026-11-21 against the live API, `/en/datos/` (English titles; series ids do not
depend on the language), one request a second; the items marked "review" were measured by the
pull request's reviewer the same day and are not repeated here. The probes' summaries are not
committed; the answers the tests replay are (`tests/fixtures/redata`, recorded by
`scripts/record_redata_fixtures.py`, which keeps only some series of the large answers).

Widgets and parameters (`data.type` is the answer's own name for what it returns, the same on
every date probed):

| Dataset | Widget and `data.type` | `time_trunc` | Points | Systems |
|---|---|---|---|---|
| `demand_realtime` | `demanda/demanda-tiempo-real`, "Demanda nacional en tiempo real" | `hour` | 5 minutes: 288 a day, 276 on 2026-03-29, 300 on 2026-10-25 | national (see below); geo parameters answer 400 or 502 |
| `demand_hourly` | `demanda/evolucion`, "Evolución de la demanda" | `hour` | 24 a day (23, 25) | `geo_trunc=electric_system`, `geo_limit` and `geo_ids`: peninsular 8741, canarias 8742, baleares 8743, ceuta 8744, melilla 8745; every system's series has id 10297; without geo parameters: 400 |
| `generation_daily` | `generacion/estructura-generacion`, "Generation structure by technology" | `day` | one per technology and day | without geo parameters (national: the sum of the systems, 724,850 MWh on 2026-11-20 against 687,088 peninsular) and each of the five systems, each with its own technologies |
| `market_prices` | `mercados/precios-mercados-tiempo-real`, "Precios mercado peninsular en tiempo real" | `hour` | PVPC 24 a day; spot 24 a day until 2024-12-31, 96 from 2025-01-01 (each hourly value four times until the 15-minute market, distinct by 2026-11) | without geo parameters; with them only PVPC comes back |

`demand_realtime` is national, as its `data.type` says, not peninsular: on 2026-11-20 its daily
Real is 756,672 MWh against 714,959 for the five systems' hourly demand and 675,062 for the
peninsula alone. It also runs above the five systems' sum, by 1.3 % to 11.8 % per hour (most at
11:00, Total scheduled alike); in review, 2026-11-18 showed the same and 2023-06-15 did not.
Measured, unexplained (self-consumption now counted is a guess).

Largest request windows (whole days, `T00:00` to `T23:59`):

| Widget | Accepted | Refused (400) |
|---|---|---|
| `demanda-tiempo-real` | 2026-10-01..31 (8,940 points per series, 3.3 MB, 5.3 s); 2026-10-15..11-14 (10.0 s) | 2026-10-01..11-01; 2026-02-15..03-16 (30 days); 2026-09-01..10-31; a quarter |
| `evolucion` (canarias) | 2026-10-01..31 (745 points, 1.2 s); 2026-10-31..11-01 | 2026-10-01..11-01; a quarter; a year |
| `precios-mercados-tiempo-real` | 2026-10-01..31 (PVPC 745, spot 2,980; 2.0 s) | 2026-10-01..11-01; a quarter |
| `estructura-generacion` | 2025 (canarias 6.5 s; national 18.9 s, 560 KB); 2024 (366 days); 2024-07-01..2025-06-30; 2024-01-01..2025-01-01 | 2024-01-01..2025-03-31; 2024..2025 |

For the sub-daily widgets the rule is not a number of days (30 days across February and March
fail, 31 days from the 15th work); a calendar month is always accepted.

A long daily window is accepted but **not answered exactly when it ends inside a month**: some
series come back up to that month's end (or the last day with data). National 2026-01-01..11-19
returned 12 of 17 series with a point for 2026-11-20; Canarias 2025-02-10..10-20 and
2025-03-01..10-20 returned 3 of 9 series up to 2025-10-31. Ending at a month's end is exact
(national 2026-01-01..10-31, Canarias 2025-02-10..09-30, whose start inside a month is fine), and
so are short windows (Canarias 2025-01-01..03-02, 61 days; review: the threshold lies between 61
and 253 days).

Other answers:

- Windows without data answer **502** with the same body as a failure ("The requested data is
  currently unavailable. Please try again later."): 2026-12-05 for all four widgets, tomorrow
  for `precios`, today for `evolucion` and `estructura-generacion`, and 2014-01-01 for
  `demanda-tiempo-real`. Today's five-minute demand answers 200 with a partial Real series (53 of
  288 points at 04:33).
- Datetimes carry their offset, and every system is stamped with peninsular offsets (Canarias
  included: `2026-11-18T00:00:00.000+01:00`). The 25-hour day has both `02:00+02:00` and
  `02:00+01:00`.
- Canarias' clock is in doubt. Its hourly demand on 2026-11-20 has its minimum at 03:00 and its
  maximum at 19:00, like Baleares (peninsular time) and one hour before the peninsula (04:00 and
  20:00), and correlates best with Baleares at lag 0 (0.972, against 0.893 and 0.916 at -1 and +1
  hours); the review found the same on 2026-11-18 (0.989) and 2023-06-15. Were these peninsular
  instants, Canarian routines would show one hour later than Baleares'. So REData probably stamps
  Canarian wall-clock times with peninsular offsets, which puts Canarias' `ts_utc` one hour early.
  REE's own demand viewer (`demanda.ree.es`) was tried with a handful of requests: its peninsula
  service answers, but its Canarias curve names could not be found, so there was no
  independent, explicitly zoned source to confirm it.
- Negative values are real: the spot price reached -6.01 EUR/MWh (2025-04), PVPC -13.14 (2026-10)
  and coal's daily net generation -120 MWh (2026). Percentages stayed within 0..1 everywhere,
  also in the months with negative prices (the spot's share bottoms out at 0).
- Series vary between answers: "Total scheduled" (id 2054) exists in recent five-minute data and
  not in 2023 (June 2023 has Real, Forecasted and Scheduled); "Fuel + Gas" appears in a year of
  national generation (one point in 2025, two in 2026) but not in the single-day answers.
- Generation by technology has no hourly view: `estructura-generacion`, `estructura-renovables`
  and `evolucion-renovable-no-renovable` answer 400 with `time_trunc=hour`. The one generation
  widget that answers hourly, `evolucion-estructura-generacion-emisiones-asociadas`, gives two
  aggregates (with and without CO2 emissions) with 25 and 26 points for 2026-11-19, a 24-hour day,
  the first stamped `+02:00` in November.
- Memory: a month of five-minute demand (35,760 rows) in the app image peaks at 238 MiB of cgroup
  memory with the audit described below (195 MiB before it also counted the rows outside the
  window; 268 MiB resident, shared libraries included). Under the 192 MiB limit of the other
  one-shots that earlier code already reached the limit (44 `max` events in one run) and survived
  only by swapping.

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
requests. A year chunk that would end inside a month ends at the month before, and the rest of
that month is a chunk of its own, because a long daily window ending mid-month comes back with
points after its end. A chunk is one request, one landing object and one Write-Audit-Publish run.

Local days are Madrid days (`Europe/Madrid`, from the system zoneinfo or the `tzdata` wheel, now
a direct dependency) because REData's days are, for every system. A chunk replaces exactly the
rows of its dataset and system with `ts_utc` in [UTC of the first local midnight, UTC of the
midnight after the last day). A UTC day would cut REData's days in two and could not be requested.

So a rerun of the same window publishes the same rows, apart from `fetched_at` and `run_id`
(pinned by unit tests on the SQL catalog, also for the days around a rerun day, and by an
integration test on the stack). A rerun replaces the window with what REData answers now: if a
series disappears from an answer, its rows in that window go too, and the run records it (below).
Chunks run in order and stop at the first failure; the published ones stay.

### Canarias' timestamps: kept as delivered, an owner decision

`ts_utc` is the delivered datetime converted with its own offset, for every system. For
Canarias that is probably one hour early (measured above, not verified), so joins of Canarias
with other systems on `ts_utc` are unsafe until it is settled. Converting Canarias as Canarian
wall-clock time (`Atlantic/Canary`) is a one-line change in the normaliser plus a migration of the
rows already stored; it needs an authoritative source with an explicit time zone first (REE's
`demanda.ree.es` Canarias service once its curve names are known, or ESIOS with a token). The
owner decides which, and when.

### Refuse what corrupts, measure the rest

Before anything is landed or written, the whole chunk is refused (`InvalidResponse`) on:
another `data.type` than the dataset expects (the widget changed scope, as a peninsular reading
of the national widget would have hidden); a shape or type problem; a number that is not finite;
a datetime without an offset; a point outside the window; a repeated (series, timestamp); or no
point at all, so that an empty answer never empties a window.

Everything else is measured and kept, and recorded both in the run's report (the CLI line) and in
the snapshot summary (`ibergrid.redata.*`):

- Completeness: for every local day, the points expected from each series' granularity
  (276/288/300 five-minute points, 92/96/100 quarter-hours, 23/24/25 hours, one daily point)
  against those received (`expected-points`, `received-points`, `incomplete-days` with only the
  days that differ, as JSON). Granularity is measured per series and chunk from the delivered
  spacing, because the spot price changed from hourly to quarter-hourly; a series with a single
  point takes the dataset's nominal one. A series missing from the whole answer is not counted,
  and a series that loses points can be measured at a coarser spacing (shortcut: measured, not
  declared, until phase 4 declares expected series and spacings).
- Value-rule breaches per series (`rule-violations`): demand <= 0, prices outside the harmonised
  limits -500..4000 EUR/MWh, a percentage outside 0..1. Generation has no rule, since net
  generation can be negative.
- Dropped series (`dropped-series`): series that main held inside the window when the run started
  and the new answer lacks, also logged as a warning.

### Landing

The raw body goes, after the structural checks and before the run, to
`s3://<bucket>/landing/redata/<dataset>/<system>/<YYYY>/<MM>/<start>_<end>-<sha256[:12]>.json`,
outside the warehouse prefix, through pyarrow's S3 client (`ibergrid.storage.s3_filesystem`,
shared with `doctor`). The hash in the key makes the object immutable per answer: a rerun with
the same answer writes the same object again, a different answer gets its own, so the
`landing_path` of the rows on main always names the body they came from, also when a later run
landed and then failed. Each row also carries the body's SHA-256.

### HTTP client

`ibergrid._http.exchange` (the standard-library helper, now returning headers for `Retry-After`
and treating a response cut short or not HTTP as no answer): at most `IBERGRID_REDATA_MAX_RPS`
requests a second per process (default and maximum 1), four attempts on timeouts, broken
responses, 429 and 5xx, with exponential backoff (1, 2, 4 s, times a random 0.5..1) or
`Retry-After` up to 60 s; any other status is permanent and names the widget and the parameters.
A window without data therefore fails after 3.5 to 7 s of waiting, with REData's message.

### Write-Audit-Publish per chunk

Run id `<prefix>-<dataset>-<system>-<start>-<end>-a<attempt>-<4 random hex>` (the prefix is the
caller's, such as an orchestrator's run id, or a UTC timestamp); the random part makes every
attempt's branch new, so a retried task with the same prefix never meets a branch an earlier
attempt left behind or already published. `wap.write(..., overwrite_filter=<window>,
snapshot_properties=<measurements>)`; the audit reads the branch head with PyIceberg and requires,
inside the window, as many rows and as many distinct keys as the validated batch and, outside it,
as many rows of the dataset and system as on the run's base; `wap.publish` moves `main`. Any
failure discards the branch; a `PublishConflict` (another writer moved `main`) restarts the chunk
on the new `main`, up to three attempts. A process killed between start and discard leaves its
`audit-*` branch behind: harmless to readers, but a sweeper of stale audit branches is needed
(the Dagster pull request or phase 4).

### For the Dagster pull request

- One table and "main at the run's base" serialise every REData chunk table-wide: two chunks
  publishing at once conflict, and the loser restarts (three attempts, immediately). REData assets
  need a concurrency limit of 1 (a Dagster pool), not just per-partition isolation.
- The request pace is per process: N workers send up to N requests a second. The same limit of 1
  keeps the pace.
- Backfills must use the month and year chunks of `plan()`, not one run per day and dataset: a
  day's run costs a request, a WAP cycle and two snapshots.

### Hourly generation is not in REData

REData offers generation by technology per day only (measured above). Hourly or quarter-hourly
generation by technology needs another source, and both candidates need a personal token
(neither was probed for this record): ENTSO-E's Transparency Platform (actual generation per
production type, bidding zone ES) or REE's ESIOS API (generation indicators per technology).
Which one, and who holds the token, is the owner's decision for phase 3; until then Bronze holds
daily generation.

## Consequences

- Good: any window can be rerun or backfilled without cleanup; the Dagster assets of a later pull
  request call `ingest(settings, dataset, start, end, systems, run_id_prefix)` per partition.
- Good: gaps, implausible values and vanished series are visible per run in the CLI and in the
  snapshot summary without blocking a partial source (today's demand, a series that started late).
- Good: four widgets, one table, one normaliser; a widget with the same shape is a registry
  entry, and a widget that changes scope is refused.
- Bad: the long table needs a pivot for most analyses (phase 3's Silver models).
- Bad: a window without data and a failing API look the same (502), so the former costs four
  requests and fails; a schedule must not ask for days REData has not published.
- Bad: Canarias' `ts_utc` is probably one hour early until the owner settles its clock.
- Bad: the `ingest` one-shot needs a 384 MiB memory limit for a month of five-minute data (238 MiB
  measured).
- Neutral: landing keeps one object per window and distinct answer, so reruns with changed
  answers accumulate objects until a retention policy exists.
- Neutral: `balance/balance-electrico` (nested `content[]`) is not ingested yet.
