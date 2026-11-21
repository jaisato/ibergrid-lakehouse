# App image: the `ibergrid` CLI (catalog-init and doctor in compose, later the pipelines).
# Three stages: `uv` pins the uv binary by digest, `builder` installs the locked dependencies into a
# virtual environment and downloads DuckDB's extensions, and the final stage copies only those two
# directories: no uv, no compilers, no cache, and no extension download at runtime.

FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv

FROM python:3.12-slim-trixie@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS builder

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

# Then the project itself, installed into the environment (not editable, so src/ is not needed).
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable


FROM python:3.12-slim-trixie@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ARG VERSION=dev
ARG REVISION=unknown

LABEL org.opencontainers.image.source="https://github.com/jaisato/ibergrid-lakehouse" \
      org.opencontainers.image.title="ibergrid-lakehouse" \
      org.opencontainers.image.description="IberGrid Lakehouse CLI: catalog bootstrap, lake tables and readiness checks" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="MIT"

# System user without a shell or home; the fixed uid lets orchestrators pin it.
RUN useradd --system --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin ibergrid

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/duckdb-extensions /app/duckdb-extensions

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    IBERGRID_LOG_FORMAT=json \
    IBERGRID_DUCKDB_EXTENSION_DIRECTORY=/app/duckdb-extensions

USER 10001

ENTRYPOINT ["ibergrid"]
CMD ["--help"]
