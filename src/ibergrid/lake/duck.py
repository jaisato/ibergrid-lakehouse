"""DuckDB over the lake: analytical reads and metadata-only queries.

``connect`` attaches the REST catalog as ``lake`` with access delegation off and the client's own
S3 secret (ADR 0002): Lakekeeper's table config names the in-network endpoint, which the host
cannot resolve. Every helper takes a table as DuckDB's table functions do: a catalog name such as
``lake.ops.smoke`` or the path of a ``metadata.json`` file.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, get_args
from urllib.parse import urlsplit

import duckdb

from ibergrid.settings import Settings, base_url

CATALOG = "lake"
EXTENSIONS = ("httpfs", "avro", "iceberg")
Metadata = Literal[
    "snapshots", "refs", "files", "manifests", "column_stats", "partition_stats", "history"
]


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with the extensions loaded, the S3 secret and the catalog attached."""
    directory = settings.duckdb.extension_directory
    # With a baked directory nothing is ever downloaded: a missing extension fails loudly.
    config = {} if directory is None else {"extension_directory": directory}
    con = duckdb.connect(config={**config, "autoinstall_known_extensions": directory is None})
    for extension in EXTENSIONS:
        if directory is None:
            con.install_extension(extension)
        con.load_extension(extension)

    storage = settings.storage
    endpoint = urlsplit(base_url(storage.endpoint_url))
    # Values as prepared parameters: a DuckDB parser error quotes the statement, never these.
    # The trailing slash keeps the scope from matching sibling buckets (s3://<bucket>-other).
    con.execute(
        "CREATE TEMPORARY SECRET lake_s3 (TYPE s3, URL_STYLE 'path', KEY_ID $key, SECRET $secret, "
        "REGION $region, ENDPOINT $endpoint, USE_SSL $ssl, SCOPE $scope)",
        {
            "key": storage.access_key.get_secret_value(),
            "secret": storage.secret_key.get_secret_value(),
            "region": storage.region,
            "endpoint": endpoint.netloc,
            "ssl": endpoint.scheme == "https",
            "scope": f"s3://{storage.bucket}/",
        },
    )
    con.execute(
        f"ATTACH {_quote(settings.catalog.warehouse)} AS {CATALOG} (TYPE iceberg, "
        f"ENDPOINT {_quote(base_url(settings.catalog.url) + '/catalog')}, "
        "AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def table_name(identifier: str) -> str:
    """``ops.smoke`` -> ``lake.ops.smoke``, the name the attached catalog knows it by."""
    namespace, _, name = identifier.partition(".")
    if not (namespace and name) or "." in name:
        raise ValueError(f"expected <namespace>.<table>, got {identifier!r}")
    return f"{CATALOG}.{namespace}.{name}"


def scan(
    con: duckdb.DuckDBPyConnection, table: str, *, snapshot_id: int | None = None
) -> duckdb.DuckDBPyRelation:
    """The table's rows on ``main``, or as of ``snapshot_id`` (an older snapshot or a ref head)."""
    if snapshot_id is None:
        return con.sql("FROM iceberg_scan($table)", params={"table": table})
    return con.sql(
        "FROM iceberg_scan($table, snapshot_from_id => $id)",
        params={"table": table, "id": snapshot_id},
    )


def ref_head(con: duckdb.DuckDBPyConnection, table: str, ref: str) -> int | None:
    """The snapshot a branch or tag points at, or None if the table has no such ref."""
    rows = con.execute(
        "SELECT snapshot_id FROM iceberg_refs($table) WHERE name = $ref",
        {"table": table, "ref": ref},
    ).fetchall()
    return int(rows[0][0]) if rows else None


def scan_ref(con: duckdb.DuckDBPyConnection, table: str, ref: str) -> duckdb.DuckDBPyRelation:
    """A branch's rows. DuckDB 2.0.1 has no branch syntax (``AT (VERSION => ...)`` takes only a
    snapshot id), so this reads the snapshot the branch points at now. An audit reads
    ``wap.head``'s snapshot with ``scan`` instead, so that ``wap.publish`` can require it."""
    head = ref_head(con, table, ref)
    if head is None:
        raise ValueError(f"{table} has no ref {ref!r}")
    return scan(con, table, snapshot_id=head)


def metadata(con: duckdb.DuckDBPyConnection, table: str, kind: Metadata) -> duckdb.DuckDBPyRelation:
    """One of the extension's metadata tables; reads metadata and manifests, never data files."""
    if kind not in get_args(Metadata):
        raise ValueError(f"unknown metadata table {kind!r}")
    return con.sql(f"FROM iceberg_{kind}($table)", params={"table": table})


@dataclass(frozen=True)
class Description:
    snapshot: tuple[int, str, datetime] | None  # main's head: id, operation, commit time (UTC)
    refs: list[tuple[str, str, int]]  # name, type, snapshot id; main first
    data_files: int
    data_bytes: int


def describe(con: duckdb.DuckDBPyConnection, table: str) -> Description:
    """Main's head, every ref and the size of main's data files, from metadata alone."""
    params = {"table": table}
    refs = con.execute(
        "SELECT name, type, snapshot_id FROM iceberg_refs($table) ORDER BY name <> 'main', name",
        params,
    ).fetchall()
    snapshot = con.execute(
        "SELECT snapshot_id, operation, timestamp_ms FROM iceberg_snapshots($table) "
        "WHERE snapshot_id = (SELECT snapshot_id FROM iceberg_refs($table) WHERE name = 'main')",
        params,
    ).fetchall()
    # content 0: data files (1 and 2 are position and equality delete files).
    [(files, size)] = con.execute(
        "SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM iceberg_files($table) "
        "WHERE content = 0",
        params,
    ).fetchall()
    return Description(
        snapshot=(int(snapshot[0][0]), snapshot[0][1], snapshot[0][2]) if snapshot else None,
        refs=[(name, kind, int(snapshot_id)) for name, kind, snapshot_id in refs],
        data_files=int(files),
        data_bytes=int(size),
    )


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
