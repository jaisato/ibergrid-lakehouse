# Two images from one file, both on the same locked environment:
#   app (the default, last stage): the `ibergrid` CLI (catalog-init, doctor, ingest in compose);
#   orchestration (--target orchestration): the same plus Dagster's `orchestration` dependency
#   group and its instance configuration, for the dagster-code, -webserver and -daemon services.
# `uv` pins the uv binary by digest; `deps` installs the locked runtime dependencies into a
# virtual environment and downloads DuckDB's extensions; each builder adds the project (and, for
# orchestration, Dagster); the final stages copy only the environment and the extensions: no uv,
# no compilers, no cache, and no extension download at runtime.

FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv

FROM python:3.12-slim-trixie@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS deps

COPY --from=uv /uv /bin/uv

# Bytecode compiled at build time and real copies instead of hard links (the uv cache stays here).
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, with the DuckDB extensions the lake loads (ibergrid.lake.duck.EXTENSIONS; a
# unit test keeps the two lists equal), signed by DuckDB and built for the locked DuckDB version.
# This layer is reused until uv.lock changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project --no-editable \
 && .venv/bin/python -c "import duckdb; c = duckdb.connect(config={'extension_directory': '/app/duckdb-extensions'}); [c.install_extension(e) for e in ('httpfs', 'avro', 'iceberg')]"


# The project itself, installed into the environment (not editable, so src/ is not needed).
FROM deps AS builder
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable


# The same with Dagster, its dependencies in a layer of their own.
FROM deps AS builder-orchestration
RUN uv sync --locked --no-dev --group orchestration --no-install-project --no-editable
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --no-dev --group orchestration --no-editable


FROM python:3.12-slim-trixie@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS runtime

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.source="https://github.com/jaisato/ibergrid-lakehouse" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="MIT"

# System user and group without a shell or home; the fixed ids (10001:10001) let compose pin them.
RUN groupadd --system --gid 10001 ibergrid \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin ibergrid

WORKDIR /app
COPY --from=deps /app/duckdb-extensions /app/duckdb-extensions

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    IBERGRID_LOG_FORMAT=json \
    IBERGRID_DUCKDB_EXTENSION_DIRECTORY=/app/duckdb-extensions


FROM runtime AS orchestration

LABEL org.opencontainers.image.title="ibergrid-lakehouse-orchestration" \
      org.opencontainers.image.description="IberGrid Lakehouse orchestration: Dagster code server, webserver, daemon and dbt medallion transforms"

COPY --from=builder-orchestration /app/.venv /app/.venv
# dbt medallion project (ADR 0007): kept read-only; runs write target/ and logs/ to /tmp.
COPY dbt /opt/ibergrid/dbt
# dagster.yaml and workspace.yaml (ADR 0005). The home is writable for Dagster's own files; the
# compute-log directory is a volume mount point, owned by the user so a new volume inherits it.
COPY config/dagster/ /opt/dagster/home/
RUN mkdir -p /opt/dagster/compute-logs \
 && chown 10001:10001 /opt/dagster/home /opt/dagster/compute-logs

ENV DAGSTER_HOME=/opt/dagster/home \
    IBERGRID_TRANSFORM_DBT_PROJECT_DIR=/opt/ibergrid/dbt

USER 10001

WORKDIR /opt/dagster/home
CMD ["dagster", "--version"]


FROM runtime AS app

LABEL org.opencontainers.image.title="ibergrid-lakehouse" \
      org.opencontainers.image.description="IberGrid Lakehouse CLI: catalog bootstrap, lake tables, ingestion and readiness checks"

COPY --from=builder /app/.venv /app/.venv

USER 10001

ENTRYPOINT ["ibergrid"]
CMD ["--help"]
