"""DuckDB over Iceberg tables of a real catalog (sqlite + local files), read by metadata path."""

import re
from collections.abc import Callable, Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from pydantic import HttpUrl, SecretStr
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField

from ibergrid.lake import duck
from ibergrid.settings import CatalogSettings, DuckDBSettings, Settings, StorageSettings

from .conftest import FakeIcebergRest, LocalServer, Responder

Serve = Callable[[Responder], LocalServer]


def settings_for(catalog_url: str, extension_directory: str | None = None) -> Settings:
    return Settings(
        storage=StorageSettings(
            endpoint_url=HttpUrl("https://acct.r2.cloudflarestorage.com"),
            region="auto",
            access_key=SecretStr("test-access-key"),
            secret_key=SecretStr("test-secret-key-do-not-log"),
        ),
        catalog=CatalogSettings(url=HttpUrl(catalog_url)),
        duckdb=DuckDBSettings(extension_directory=extension_directory),
    )


@pytest.fixture
def con(serve: Serve, catalog: SqlCatalog) -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duck.connect(settings_for(serve(FakeIcebergRest(catalog)).url))
    yield connection
    connection.close()


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace("ns")
    table = catalog.create_table(("ns", "t"), Schema(NestedField(1, "x", LongType(), False)))
    for values in ([1, 2], [3]):
        table.append(pa.table({"x": pa.array(values, pa.int64())}))
    return table


def test_connect_attaches_the_catalog_with_a_scoped_redacted_secret(
    serve: Serve, con: duckdb.DuckDBPyConnection
) -> None:
    databases = {
        row[0] for row in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    }
    [(name, scope, secret)] = con.execute(
        "SELECT name, scope, secret_string FROM duckdb_secrets()"
    ).fetchall()

    assert duck.CATALOG in databases
    # The trailing slash: a sibling bucket such as s3://ibergrid-other is out of scope.
    assert (name, scope) == ("lake_s3", ["s3://ibergrid/"])
    assert "endpoint=acct.r2.cloudflarestorage.com" in secret
    assert "key_id=test-access-key" in secret
    assert "region=auto" in secret
    assert "use_ssl=true" in secret
    assert "test-secret-key-do-not-log" not in secret
    assert con.execute(
        "SELECT name FROM which_secret('s3://ibergrid/x/y.parquet', 's3')"
    ).fetchall() == [("lake_s3",)]
    assert (
        con.execute("SELECT name FROM which_secret('s3://ibergrid-other/x', 's3')").fetchall() == []
    )


def test_a_baked_extension_directory_is_used_without_downloading(
    serve: Serve, catalog: SqlCatalog
) -> None:
    directory = str(Path.home() / ".duckdb" / "extensions")  # DuckDB's default directory
    for extension in duck.EXTENSIONS:  # already there from the other tests: no download
        duckdb.connect(config={"extension_directory": directory}).install_extension(extension)

    con = duck.connect(
        settings_for(serve(FakeIcebergRest(catalog)).url, extension_directory=directory)
    )

    [(setting, autoinstall)] = con.execute(
        "SELECT current_setting('extension_directory'), "
        "current_setting('autoinstall_known_extensions')"
    ).fetchall()
    assert (setting, autoinstall) == (directory, False)
    loaded = {
        row[0]
        for row in con.execute(
            "SELECT extension_name FROM duckdb_extensions() WHERE loaded"
        ).fetchall()
    }
    assert set(duck.EXTENSIONS) <= loaded


def test_a_baked_extension_directory_never_downloads(tmp_path: Path) -> None:
    empty = tmp_path / "extensions"
    empty.mkdir()

    # Loading fails before the catalog is contacted, so no server is needed.
    with pytest.raises(duckdb.IOException, match="not found"):
        duck.connect(settings_for("http://127.0.0.1:9", extension_directory=str(empty)))

    assert list(empty.rglob("*")) == []


def test_duckdb_has_no_way_to_move_iceberg_refs() -> None:
    """Pins ADR 0002: besides rollback, no DuckDB function creates, moves or removes a ref."""
    con = duckdb.connect()
    con.load_extension("iceberg")

    names = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name ILIKE "
            "'%iceberg%' AND (function_name ILIKE '%branch%' OR function_name ILIKE '%tag%' "
            "OR function_name ILIKE '%ref%' OR function_name ILIKE '%snapshot%')"
        ).fetchall()
    }

    assert names == {
        "iceberg_refs",
        "iceberg_snapshots",
        "iceberg_expire_snapshots",
        "iceberg_rollback_to_snapshot",
    }


def test_the_app_image_bakes_exactly_these_extensions() -> None:
    dockerfile = (Path(__file__).resolve().parents[2] / "Dockerfile").read_text()

    baked = re.search(r"install_extension\(e\) for e in \(([^)]*)\)", dockerfile)

    assert baked is not None
    assert tuple(name.strip(" '") for name in baked.group(1).split(",")) == duck.EXTENSIONS


def test_scan_reads_main_or_any_snapshot(con: duckdb.DuckDBPyConnection, table: Table) -> None:
    first, second = (s.snapshot_id for s in table.metadata.snapshots)

    assert sorted(duck.scan(con, table.metadata_location).fetchall()) == [(1,), (2,), (3,)]
    assert sorted(duck.scan(con, table.metadata_location, snapshot_id=first).fetchall()) == [
        (1,),
        (2,),
    ]
    assert len(duck.scan(con, table.metadata_location, snapshot_id=second).fetchall()) == 3


def test_scan_ref_reads_a_branch_by_its_head(con: duckdb.DuckDBPyConnection, table: Table) -> None:
    table.manage_snapshots().create_branch(
        table.metadata.snapshots[0].snapshot_id, "audit-x"
    ).commit()
    table.append(pa.table({"x": pa.array([10], pa.int64())}), branch="audit-x")

    assert (
        duck.ref_head(con, table.metadata_location, "audit-x")
        == table.metadata.refs["audit-x"].snapshot_id
    )
    assert sorted(duck.scan_ref(con, table.metadata_location, "audit-x").fetchall()) == [
        (1,),
        (2,),
        (10,),
    ]
    assert duck.ref_head(con, table.metadata_location, "nope") is None
    with pytest.raises(ValueError, match="no ref 'nope'"):
        duck.scan_ref(con, table.metadata_location, "nope")


@pytest.mark.parametrize("kind", ["snapshots", "refs", "files", "manifests", "history"])
def test_metadata_tables_answer_from_metadata(
    con: duckdb.DuckDBPyConnection, table: Table, kind: duck.Metadata
) -> None:
    assert duck.metadata(con, table.metadata_location, kind).fetchall()


def test_file_counts_and_sizes_come_from_the_manifests(
    con: duckdb.DuckDBPyConnection, table: Table
) -> None:
    files = duck.metadata(con, table.metadata_location, "files")

    [(count, size)] = files.aggregate("count(*), sum(file_size_in_bytes)").fetchall()

    expected = table.inspect.files()
    assert (count, size) == (expected.num_rows, sum(expected["file_size_in_bytes"].to_pylist()))
    assert duck.metadata(con, table.metadata_location, "column_stats").fetchall()


def test_unknown_metadata_tables_are_rejected(con: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(ValueError, match="unknown metadata table"):
        duck.metadata(con, "x", "files; DROP TABLE y")  # type: ignore[arg-type]


@pytest.mark.parametrize("identifier", ["smoke", "ops.", ".smoke", "a.b.c"])
def test_table_names_need_a_namespace_and_a_table(identifier: str) -> None:
    with pytest.raises(ValueError, match=r"expected <namespace>\.<table>"):
        duck.table_name(identifier)


def test_table_name_points_into_the_attached_catalog() -> None:
    assert duck.table_name("ops.smoke") == "lake.ops.smoke"


def test_describe_summarises_main_refs_and_data_files(
    con: duckdb.DuckDBPyConnection, table: Table
) -> None:
    first = table.metadata.snapshots[0].snapshot_id
    table.manage_snapshots().create_branch(first, "audit-x").commit()

    description = duck.describe(con, table.metadata_location)

    assert description.snapshot is not None
    snapshot_id, operation, _ = description.snapshot
    assert (snapshot_id, operation) == (table.metadata.current_snapshot_id, "append")
    assert description.refs == [
        ("main", "BRANCH", table.metadata.current_snapshot_id),
        ("audit-x", "BRANCH", first),
    ]
    files = table.inspect.files()
    assert (description.data_files, description.data_bytes) == (
        files.num_rows,
        sum(files["file_size_in_bytes"].to_pylist()),
    )


def test_describe_an_empty_table(con: duckdb.DuckDBPyConnection, catalog: SqlCatalog) -> None:
    catalog.create_namespace("empty")
    table = catalog.create_table(("empty", "t"), Schema(NestedField(1, "x", LongType(), False)))

    assert duck.describe(con, table.metadata_location) == duck.Description(None, [], 0, 0)


def test_the_attached_catalog_serves_tables_by_name(
    con: duckdb.DuckDBPyConnection, table: Table
) -> None:
    assert sorted(con.sql("FROM lake.ns.t").fetchall()) == [(1,), (2,), (3,)]
    assert duck.describe(con, "lake.ns.t").data_files == 2
