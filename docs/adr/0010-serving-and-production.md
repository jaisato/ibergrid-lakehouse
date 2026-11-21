# ADR 0010: Real-time serving API, static analytical portal and production edge boundary

- **Status:** Accepted
- **Date:** 2026-11-21
- **Deciders:** IberGrid Lakehouse maintainers
- **Related:** [ADR 0001](0001-stack-and-scope.md), [ADR 0002](0002-iceberg-write-and-read-paths.md), [ADR 0003](0003-redata-ingestion.md), [ADR 0007](0007-dbt-medallion.md), [ADR 0008](0008-quality-maintenance-lineage.md), [ADR 0009](0009-duckdb-polars-vs-spark.md)

## Context

Phases 0–5 established a full end-to-end Apache Iceberg lakehouse (Bronze batch & streaming
ingestion, dbt Core Silver/Gold medallion, Soda v4 + zero-scan metadata WAP gates, automated S3
orphan cleanup, OpenLineage v2 observability and reproducible DuckDB vs Polars benchmarks).
Phase 6 closes the architecture with public consumption and production hardening:

1. **Real-time serving API:** external clients and dashboards need a read-only REST API with
   OpenAPI docs over Gold and Silver Iceberg tables without standing up a separate OLAP serving
   database (ClickHouse, Druid, StarRocks) that would blow the single-server European RAM and
   cost budget.
2. **Interactive analytical portal:** reviewers and analysts need a fast, zero-friction visual
   interface exploring the 3.9-year Iberian electricity dataset (Solar Duck Curve & 12x24 load
   shifting, <= €0/MWh explosion, ES-PT decoupling & 2025-04-28 Iberian blackout spotlight,
   separated Spanish electric systems with Canarias clock transparency, and multi-engine
   benchmark / chaos proofs) — deployable both statically on Cloudflare R2/Pages (zero egress)
   and self-hosted from the API container at `/portal`.
3. **Production security boundary:** Lakekeeper (`catalog`), Redpanda (`broker`), Dagster
   (`dagster-webserver`) and Postgres run without public OIDC/TLS layers. Exposing public
   endpoints requires strict Docker network isolation, automatic TLS, rate limiting, CORS
   allowlists, security headers and explicit route blocking so internal control planes can never
   be reached from the internet.
4. **Lean CLI image preservation:** adding FastAPI and Uvicorn must not bloat the lean
   `ibergrid-app` image (~813 MB) used by `catalog-init`, `doctor`, `ingest` and the streaming
   containers.

## Decision

### 1. Read-only FastAPI + DuckDB serving layer (`ibergrid.api`)

- **Dedicated `api` dependency group & container stage:** `fastapi==0.165.0` and
  `uvicorn==0.54.0` live in `[dependency-groups] api` in `pyproject.toml`, built into a third
  stage (`--target api`, published as `ghcr.io/jaisato/ibergrid-lakehouse-api`) in the existing
  multi-stage [Dockerfile](../../Dockerfile). `src/ibergrid/api/__init__.py` intentionally does
  not import `ibergrid.api.app`, so `ibergrid.portal` and `ibergrid.cli` stay importable inside
  the lean `ibergrid-app` image without FastAPI installed.
- **Direct DuckDB reads over Iceberg REST catalog:** `LakeQueryStore` reuses
  `ibergrid.lake.duck.connect` with `SET GLOBAL TimeZone = 'UTC'`, serializes cursor access
  through a process-level lock, caches query responses in memory with a short configurable TTL
  (`IBERGRID_API_CACHE_TTL_SECONDS=60`, bounded to 256 entries), reconnects cleanly after
  transient `duckdb.Error` failures (returning HTTP `503`), and gracefully returns empty `200 OK`
  responses when a table has not been initialized or populated yet.
- **Seven focused endpoints with attribution envelope:**
  - `GET /health` — DuckDB + Lakekeeper `/health` readiness check (`200 ok` / `503 degraded`,
    works even before `lake init`).
  - `GET /v1/market/hourly` — peninsular hourly MIBEL market (`gold.mibel_hourly_market`, bounded
    to `max_hourly_range_days=90`).
  - `GET /v1/market/daily` — daily MIBEL summary (`gold.mibel_daily_summary`, bounded to
    `max_daily_range_days=1825`).
  - `GET /v1/systems/daily` — all 5 Spanish electric systems (`gold.system_demand_daily`),
    strictly preserving `system_scope` (`peninsular` vs `non_peninsular`) and Canarias
    `ts_utc_verified=False` ([ADR 0003](0003-redata-ingestion.md)).
  - `GET /v1/windows/optimal` — `month x day_type x madrid_hour` load-shifting profiles
    (`gold.optimal_consumption_windows`).
  - `GET /v1/demand/realtime` — reconciled trailing `1..168` hours of 5-minute national demand
    (`silver.demand_national_5min`) with explicit `batch_points` and `stream_points` provenance.
  - `GET /v1/metadata/tables` — zero-scan Iceberg manifest inventory across all 13 registered
    tables (`total_data_bytes_scanned == 0`).
  - Every `/v1/*` response embeds `source_attribution`, `updated_at_utc` and
    `non_commercial_notice` complying with REE and OMIE attribution practices.
- **Abuse protection & headers:** an in-process thread-safe `SlidingWindowRateLimiter` (bounded
  client cardinality `MAX_TRACKED_CLIENTS=10_000` with stale/oldest eviction) guards `/v1/*`,
  emitting `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `429` + `Retry-After` on overflow.
  Client IP extraction uses `request.client.host` by default and trusts the rightmost
  `X-Forwarded-For` hop only when `IBERGRID_API_TRUST_PROXY_HEADERS=true` (set in
  `compose.prod.yaml` behind Caddy). CORS uses an explicit origin allowlist (never `*` with
  credentials); every response sets `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
  and `Referrer-Policy: strict-origin-when-cross-origin`.

### 2. Static-first interactive analytical portal (`portal/` & `ibergrid portal export`)

- **Zero Node/bundler runtime:** rather than adding a heavy Node.js / Evidence.dev build
  toolchain (which would double CI complexity, expand npm supply-chain surface and break the
  Python-only container build), the portal is pure semantic HTML5, CSS and vanilla ES2022 JS
  using Chart.js 4.6.1 pinned with a Subresource Integrity (`sha384-...`) digest.
- **Dual deployment (static R2/Pages + mounted `/portal`):**
  - `ibergrid portal export` (`make portal-export` / `scripts/export_portal.py`) materializes
    precalculated Gold & Silver JSON/CSV bundles (`summary.json`, `optimal_windows.{json,csv}`,
    `daily_market.{json,csv}`, `blackout_2025_04_28.json`, `systems_daily.json`,
    `benchmarks.json`) to local `portal/data/` and/or uploads the complete static site + data
    directly to `s3://<bucket>/<prefix>/` (Cloudflare R2 or RustFS) via PyArrow S3.
  - The FastAPI app mounts `portal/` at `/portal` (with `GET /` redirecting to `/portal/`) and
    automatically enriches the live status badge and trailing 5-minute demand view from `/health`
    and `/v1/demand/realtime` when backed by the live API, while degrading seamlessly to static
    JSON bundles when hosted on Cloudflare R2 / Pages with zero compute cost.

### 3. Production edge security boundary (`deploy/Caddyfile`, `compose.prod.yaml`)

- **Network isolation (`public-edge`):** `compose.yaml` defines a dedicated `public-edge`
  network. `api` joins `[storage, catalog, public-edge]` (512 MiB limit, UID `10001`,
  `no-new-privileges`, `cap_drop: [ALL]`). No internal control plane (`catalog`, `broker`,
  `catalog-db`, `orchestration-db`, `dagster-*`, `storage`) joins `public-edge`.
- **Optional Caddy TLS reverse proxy (`profiles: [edge]`):** `compose.prod.yaml` adds a pinned
  `caddy:2.10.2-alpine` `proxy` container (`read_only: true`, `cap_drop: [ALL]`,
  `cap_add: [NET_BIND_SERVICE]`, `mem_limit: 128m`) on `networks: [public-edge]` **only**,
  publishing ports `80/443`. Keeping `proxy` behind `profiles: [edge]` allows deploying safely on
  servers without public DNS yet (zero host ports open) and turning on automatic Let's Encrypt
  HTTPS for `IBERGRID_DOMAIN` (`ibergrid-lakehouse.jasato.com` by default) simply by setting
  `COMPOSE_PROFILES=edge` in `.env`.
- **Defense in depth in [deploy/Caddyfile](../../deploy/Caddyfile):** sets HSTS, CSP (allowing
  only `'self'` and `https://cdn.jsdelivr.net`), `X-Frame-Options: DENY`, strips `Server`, and
  explicitly responds `403 Forbidden` to `/catalog/*`, `/management/*` and `/graphql*`.
- **Three-image immutable release pipeline:** `.github/workflows/release.yml` builds,
  smoke-tests, pushes and pins all three images (`app`, `orchestration`, `api`) by immutable
  `sha256` digest in `release.env` via `deploy/remote.sh`, and copies `deploy/Caddyfile`
  alongside `compose.yaml` and `compose.prod.yaml`.

## Consequences

### Positive

- **Zero extra database or cloud cost:** DuckDB reads Iceberg directly from R2/RustFS with
  sub-50 ms cached/warm latency inside a 512 MiB container; static portal bundles fit inside
  Cloudflare R2's free tier (`< 1 MiB`) with `$0.00` egress.
- **Strict least-privilege network blast radius:** even a hypothetical compromise of Caddy can
  reach only the read-only FastAPI endpoints on `api:8000`, never Lakekeeper's unauthenticated
  management API, Redpanda, Postgres or Dagster.
- **Lean `ibergrid-app` image preserved:** CLI, ingestion and streaming containers pay zero
  dependency or memory penalty for FastAPI/Uvicorn.
- **Out-of-the-box local experience:** `make up` brings up both Dagster UI (`:28300`) and the
  live API + interactive portal (`:28000/portal/` and `:28000/docs`) bound exclusively to
  `127.0.0.1`.

### Negative & Trade-offs

- **In-memory rate limiter & cache are per-process:** sufficient for the single-server European
  deployment target (`<= 2` Uvicorn workers or 1 worker with 512 MiB limit); horizontal scaling
  across multiple nodes would move rate-limit counters and response cache to Redis or Cloudflare
  Rate Limiting at the edge.
- **Chart.js loaded from jsDelivr CDN:** mitigated with exact version pinning (`4.6.1`) and
  cryptographic `integrity="sha384-..."` + `crossorigin="anonymous"` + strict CSP
  `script-src 'self' https://cdn.jsdelivr.net`. Air-gapped deployments can vendor the 204 KB UMD
  bundle into `portal/` and drop the CDN origin from the CSP.
- **Precalculated portal bundles refresh on demand:** `make portal-export` / `ibergrid portal
  export` updates static JSON/CSV files (wiring it as a post-Gold Dagster asset or cron hook is
  trivial once Cloudflare R2 public bucket hosting is configured).

## Alternatives considered

1. **ClickHouse / StarRocks serving tier:** rejected — adds >= 2–4 GiB baseline RAM, duplicates
   storage already in Iceberg Parquet, and breaks the ~€3.79–€6.49/mo single-server budget
   established in [ADR 0009](0009-duckdb-polars-vs-spark.md).
2. **Evidence.dev (SvelteKit + Node.js) static portal:** considered in the initial roadmap
   sketch, rejected after evaluating operational overhead — requires a Node.js + npm toolchain
   in CI and Docker, duplicates DuckDB WASM/native extensions and adds dozens of transitive JS
   dependencies to audit, whereas vanilla HTML/CSS/JS + SRI-pinned Chart.js achieves identical
   static R2/Pages portability with zero build step and zero Node runtime.
3. **Exposing Lakekeeper behind OIDC proxy for direct public SQL access:** rejected — public
   internet callers do not need raw Iceberg catalog mutations or unbounded scan budgets; exposing
   a bounded, read-only FastAPI contract gives predictable memory, latency and rate limits.
4. **Adding FastAPI to `ibergrid-app`:** rejected — keeps `catalog-init`, `doctor`, `ingest` and
   `stream-*` minimal and isolates web dependencies to the `api` image.

## Verification

- **Unit tests (`tests/unit/test_api.py`, `tests/unit/test_portal.py`, `tests/unit/test_remote_sh.py`):**
  exercise all 7 FastAPI endpoints against local Iceberg tables, empty-table fallback, date-range
  validation (`422`), Canarias `ts_utc_verified=False`, sliding-window rate limiting (`429` +
  `Retry-After` + client eviction + `X-Forwarded-For` trust), CORS, security headers, `/portal`
  static mount, degraded health (`503`), portal bundle export (local + S3 prefix + asset upload)
  and 3-digest `deploy/remote.sh` pinning/rollback.
- **Integration tests (`tests/integration/test_api_and_portal.py`):** verify the live `api`
  container on `127.0.0.1:28000` (`/health`, all `/v1/*` endpoints, `/portal/` HTML + JSON data,
  rate-limit headers), verify the lean `ibergrid-app:local` image starts without FastAPI, and
  verify `ibergrid portal export --s3-prefix` round-trips objects to RustFS cleanly.
- **Repository & config gates (`make ci`):** `ruff`, strict `mypy`, `>= 90 %` branch coverage,
  `compose-config` (local, prod, and prod `--profile edge`), `hadolint`, `shellcheck`,
  `actionlint`, `gitleaks` and `pip-audit`.
