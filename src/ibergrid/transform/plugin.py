"""dbt-duckdb plugin that wires Lakekeeper Bronze snapshots into the staging DuckDB (ADR 0007).

Instead of putting S3 credentials or endpoints into ``profiles.yml``, ``dbt-duckdb`` calls this
plugin when it opens its staging database:

1. ``update_connection_config`` applies ``ibergrid.lake.duck.connection_config`` (baked extensions
   in Docker, auto-install off when ``IBERGRID_DUCKDB_EXTENSION_DIRECTORY`` is set).
2. ``configure_connection`` calls ``ibergrid.lake.duck.configure`` (extensions, prepared temporary
   S3 secret, Iceberg REST catalog ``lake``), forces UTC session time zone, and exposes every
   Bronze table in schema ``bronze_input`` pinned to its current ``main`` snapshot — or as an empty
   typed table when the table has no snapshot yet (e.g. ``bronze.redata_demand_stream`` on a fresh
   stack). Every model and test in a ``dbt build`` therefore reads a single consistent point in
   time across all Bronze tables.
"""

import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import duckdb
from dbt.adapters.duckdb.plugins import BasePlugin

from ibergrid.lake import duck, registry
from ibergrid.settings import Settings

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCK = threading.Lock()
_ACTIVE: "TransformContext | None" = None


@dataclass
class TransformContext:
    """In-memory settings and snapshot pins shared between ``runner`` and the dbt-duckdb plugin."""

    settings: Settings
    bronze_namespace: str = "bronze"
    bronze_snapshots: dict[str, int | None] = field(default_factory=dict)


@contextmanager
def use_context(context: TransformContext) -> Iterator[TransformContext]:
    """Serialize in-process dbt runs and bind ``context`` for the plugin across dbt threads."""
    global _ACTIVE
    with _LOCK:
        _ACTIVE = context
        try:
            yield context
        finally:
            _ACTIVE = None


def current_context() -> TransformContext:
    """Active runner context, or a fresh context from ``IBERGRID_*`` env vars for standalone dbt."""
    return _ACTIVE if _ACTIVE is not None else TransformContext(Settings())


def _validate_identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"invalid SQL identifier {name!r}")
    return name


class Plugin(BasePlugin):
    """Configures dbt-duckdb's connection with Lakekeeper Bronze views and UTC semantics."""

    def update_connection_config(self, creds: Any, config: dict[str, Any]) -> None:
        del creds
        ctx = current_context()
        config.update(duck.connection_config(ctx.settings))

    def configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        ctx = current_context()
        namespace = _validate_identifier(ctx.bronze_namespace)
        duck.configure(conn, ctx.settings)
        # All TIMESTAMPTZ expressions in Silver/Gold SQL assume UTC session semantics; Madrid local
        # dates and hours are always converted explicitly with AT TIME ZONE 'Europe/Madrid'.
        # GLOBAL ensures every child cursor dbt-duckdb opens inherits UTC even on non-UTC hosts.
        conn.execute("SET GLOBAL TimeZone = 'UTC'")
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze_input")
        for spec in registry.BRONZE_TABLES:
            table_name = _validate_identifier(spec.name)
            lake_ref = f"{duck.CATALOG}.{namespace}.{table_name}"
            try:
                snapshot_id = duck.ref_head(conn, lake_ref, "main")
            except duckdb.Error as exc:
                raise RuntimeError(
                    f"{namespace}.{table_name} is not available in catalog "
                    f"({exc}); run `ibergrid lake init` first"
                ) from exc
            ctx.bronze_snapshots[spec.name] = snapshot_id
            if snapshot_id is None:
                # Empty table with no snapshot yet: expose an empty typed table matching the
                # Iceberg schema so dbt models and tests run cleanly (e.g. stream table before the
                # first event arrives).
                temp_name = f"_ibergrid_empty_{table_name}"
                conn.register(temp_name, spec.schema.as_arrow().empty_table())
                try:
                    conn.execute(
                        f"CREATE OR REPLACE TABLE bronze_input.{table_name} AS "  # noqa: S608
                        f"SELECT * FROM {temp_name}"
                    )
                finally:
                    conn.unregister(temp_name)
            else:
                quoted = "'" + lake_ref.replace("'", "''") + "'"
                conn.execute(
                    f"CREATE OR REPLACE VIEW bronze_input.{table_name} AS "  # noqa: S608
                    f"SELECT * FROM iceberg_scan({quoted}, snapshot_from_id => {snapshot_id})"
                )

    def configure_cursor(self, cursor: Any) -> None:
        cursor.execute("SET TimeZone = 'UTC'")
