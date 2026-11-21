"""What Lakekeeper 0.15.1, PyIceberg 0.12.0 and DuckDB 2.0.1 do on this stack (ADR 0002).

Each probe asserts the behaviour measured when the ADR was written, failures included, so an
upgrade that changes one of them fails here and sends the reader back to the ADR.
"""

import json

import duckdb
import pyarrow as pa
import pytest
from pyarrow import fs
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField

from ibergrid import _http
from ibergrid.lake import duck
from ibergrid.lake.catalog import LakeCatalog, s3_properties
from ibergrid.settings import Settings, base_url

from .conftest import CATALOG_URL

pytestmark = pytest.mark.integration

SCHEMA = Schema(NestedField(1, "x", LongType(), required=False))


def numbers(*values: int) -> pa.Table:
    return pa.table({"x": pa.array(values, pa.int64())})


@pytest.fixture
def table(catalog: LakeCatalog, namespace: str) -> Table:
    table: Table = catalog.create_table((namespace, "probe"), SCHEMA)
    table.append(numbers(1, 2))
    return table


def main_head(table: Table) -> int:
    head = table.metadata.current_snapshot_id
    assert head is not None
    return head


@pytest.fixture
def name(namespace: str) -> str:
    return f"{duck.CATALOG}.{namespace}.probe"


def raw_duckdb(settings: Settings, delegation: str | None) -> duckdb.DuckDBPyConnection:
    """``duck.connect`` with the ATTACH option under test instead of ours."""
    con = duck.connect(settings)
    con.execute(f"DETACH {duck.CATALOG}")
    option = "" if delegation is None else f", ACCESS_DELEGATION_MODE '{delegation}'"
    con.execute(
        f"ATTACH 'lakehouse' AS {duck.CATALOG} (TYPE iceberg, "
        f"ENDPOINT '{CATALOG_URL}/catalog', AUTHORIZATION_TYPE 'none'{option})"
    )
    return con


# --- Lakekeeper: what a loadTable hands the client ----------------------------------------------


def test_lakekeeper_pushes_the_in_network_endpoint_and_no_credentials(
    catalog: LakeCatalog, namespace: str, table: Table
) -> None:
    prefix = catalog.properties["prefix"]
    url = f"{CATALOG_URL}/catalog/v1/{prefix}/namespaces/{namespace}/tables/probe"

    for delegation in ("vended-credentials", "remote-signing"):
        status, body = _http.send(
            "GET", url, timeout=10, headers={"X-Iceberg-Access-Delegation": delegation}
        )
        response = json.loads(body)
        assert status == 200
        assert response["config"]["s3.endpoint"] == "http://storage:9000/"
        assert not any("signer" in key or "remote-signing" in key for key in response["config"])
        assert "s3.access-key-id" not in response["config"]
        assert response.get("storage-credentials", []) == []


# --- PyIceberg ----------------------------------------------------------------------------------


def test_pyiceberg_without_the_client_override_cannot_write_from_the_host(
    settings: Settings, namespace: str, table: Table
) -> None:
    plain = RestCatalog(
        "plain",
        uri=f"{CATALOG_URL}/catalog",
        warehouse="lakehouse",
        **s3_properties(settings.storage),
    )

    with pytest.raises(OSError, match="storage"):
        plain.load_table((namespace, "probe")).append(numbers(3))


def test_pyiceberg_cannot_write_a_branch_of_a_table_without_snapshots(
    catalog: LakeCatalog, namespace: str
) -> None:
    empty = catalog.create_table((namespace, "empty"), SCHEMA)

    with pytest.raises(Exception, match="can only be written to the main BRANCH"):
        empty.append(numbers(1), branch="audit-x")


def test_duckdb_cannot_read_pyiceberg_empty_first_snapshot(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    empty = catalog.create_table((namespace, "empty"), SCHEMA)
    empty.append(numbers())  # an append of no rows: a snapshot with an empty manifest list

    with pytest.raises(duckdb.InvalidInputException, match="Invalid Avro file block count"):
        duck.scan(con, f"{duck.CATALOG}.{namespace}.empty").fetchall()


# --- DuckDB against the REST catalog ------------------------------------------------------------


def test_duckdb_default_delegation_follows_the_in_network_endpoint(
    settings: Settings, table: Table, name: str
) -> None:
    con = raw_duckdb(settings, delegation=None)

    with pytest.raises(duckdb.IOException, match="storage:9000"):
        con.sql(f"FROM {name}").fetchall()


def test_duckdb_reads_main_and_snapshots(
    table: Table, name: str, con: duckdb.DuckDBPyConnection
) -> None:
    first = table.metadata.current_snapshot_id
    table.append(numbers(3))

    assert sorted(con.sql(f"SELECT x FROM {name}").fetchall()) == [(1,), (2,), (3,)]
    assert sorted(con.sql(f"SELECT x FROM {name} AT (VERSION => {first})").fetchall()) == [
        (1,),
        (2,),
    ]


def test_duckdb_reads_a_branch_only_through_its_head_snapshot(
    table: Table, name: str, con: duckdb.DuckDBPyConnection
) -> None:
    table.manage_snapshots().create_branch(main_head(table), "audit-x").commit()
    table.append(numbers(10), branch="audit-x")
    head = table.metadata.refs["audit-x"].snapshot_id

    with pytest.raises(duckdb.InvalidInputException, match="Could not convert string 'audit-x'"):
        con.sql(f"FROM {name} AT (VERSION => 'audit-x')").fetchall()
    with pytest.raises(duckdb.ParserException):
        con.sql(f"FROM {name} AT (BRANCH => 'audit-x')").fetchall()
    assert sorted(con.sql(f"SELECT x FROM {name} AT (VERSION => {head})").fetchall()) == [
        (1,),
        (2,),
        (10,),
    ]


def test_duckdb_writes_main_but_not_a_branch(
    table: Table, name: str, con: duckdb.DuckDBPyConnection
) -> None:
    con.execute(f"INSERT INTO {name} VALUES (3)")
    con.execute(f"DELETE FROM {name} WHERE x = 1")
    con.execute(f"UPDATE {name} SET x = 20 WHERE x = 2")

    with pytest.raises(duckdb.ParserException):
        con.execute(f"INSERT INTO {name} AT (BRANCH => 'audit-x') VALUES (4)")
    table.refresh()
    assert sorted(table.scan().to_arrow()["x"].to_pylist()) == [3, 20]


def test_duckdb_creates_tables_but_not_create_or_replace(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    created = f"{duck.CATALOG}.{namespace}.by_duckdb"
    con.execute(f"CREATE TABLE {created} AS SELECT range AS x FROM range(3)")

    with pytest.raises(duckdb.NotImplementedException, match="CREATE OR REPLACE"):
        con.execute(f"CREATE OR REPLACE TABLE {created} AS SELECT 1 AS x")
    by_duckdb = catalog.load_table((namespace, "by_duckdb"))
    assert by_duckdb.format_version == 2
    assert by_duckdb.scan().to_arrow().num_rows == 3


@pytest.mark.parametrize(
    "kind",
    ["snapshots", "refs", "files", "manifests", "column_stats", "partition_stats", "history"],
)
def test_duckdb_metadata_tables_work_through_the_catalog(
    table: Table, name: str, con: duckdb.DuckDBPyConnection, kind: duck.Metadata
) -> None:
    duck.metadata(con, name, kind).fetchall()


def test_duckdb_expiry_refuses_tables_with_a_branch(
    table: Table, name: str, con: duckdb.DuckDBPyConnection
) -> None:
    table.append(numbers(3))
    expire = (
        "FROM iceberg_expire_snapshots($table, older_than => now()::TIMESTAMP, retain_last => 1)"
    )
    table.manage_snapshots().create_branch(main_head(table), "audit-x").commit()

    with pytest.raises(duckdb.NotImplementedException, match="only the main branch"):
        con.execute(expire, {"table": name}).fetchall()
    table.manage_snapshots().remove_branch("audit-x").commit()
    assert con.execute(expire, {"table": name}).fetchall() == [(1, 1)]


def test_neither_expiry_nor_compaction_deletes_files(
    settings: Settings, table: Table, name: str, con: duckdb.DuckDBPyConnection
) -> None:
    storage = settings.storage
    s3 = fs.S3FileSystem(
        access_key=storage.access_key.get_secret_value(),
        secret_key=storage.secret_key.get_secret_value(),
        region=storage.region,
        endpoint_override=base_url(storage.endpoint_url),
        force_virtual_addressing=False,
    )
    location = table.location().removeprefix("s3://")

    def data_files() -> int:
        infos = s3.get_file_info(fs.FileSelector(f"{location}/data", recursive=True))
        return sum(info.type == fs.FileType.File for info in infos)

    for value in range(4):
        table.append(numbers(value))
    con.execute("FROM iceberg_rewrite_data_files($table)", {"table": name}).fetchall()
    con.execute(
        "FROM iceberg_expire_snapshots($table, older_than => now()::TIMESTAMP, retain_last => 1)",
        {"table": name},
    ).fetchall()

    assert duck.describe(con, name).data_files == 1
    assert data_files() == 6  # 5 appends and 1 compacted file: nothing was deleted
