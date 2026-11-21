"""Snapshot expiry on a real Iceberg catalog (sqlite + local files)."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField

from ibergrid.lake.maintenance import expire_snapshots

# Snapshot timestamps come from the wall clock (which can step back on WSL): every cutoff is
# far from them, never "now".
LATER = datetime.now(UTC) + timedelta(days=1)
EARLIER = datetime.now(UTC) - timedelta(days=1)


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    catalog.create_namespace("ns")
    table = catalog.create_table(("ns", "t"), Schema(NestedField(1, "x", LongType(), False)))
    for value in range(5):
        table.append(pa.table({"x": pa.array([value], pa.int64())}))
    return table


def ids(table: Table) -> list[int]:
    return [s.snapshot_id for s in table.metadata.snapshots]


def test_keeps_the_last_snapshots_of_main_and_the_data(table: Table) -> None:
    history = ids(table)

    expired = expire_snapshots(table, older_than=LATER, retain_last=2)

    assert sorted(expired) == sorted(history[:3])
    assert ids(table) == history[3:]
    assert sorted(table.scan().to_arrow()["x"].to_pylist()) == [0, 1, 2, 3, 4]


def test_nothing_newer_than_the_cutoff_is_expired(table: Table) -> None:
    assert expire_snapshots(table, older_than=EARLIER) == []
    assert len(ids(table)) == 5


def test_branch_heads_survive(table: Table) -> None:
    second = ids(table)[1]
    table.manage_snapshots().create_branch(second, "audit-run").commit()

    expire_snapshots(table, older_than=LATER, retain_last=1)

    assert ids(table) == [second, table.metadata.current_snapshot_id]
    assert table.scan(snapshot_id=second).to_arrow().num_rows == 2


def test_an_empty_table_has_nothing_to_expire(catalog: SqlCatalog) -> None:
    catalog.create_namespace("empty")
    table = catalog.create_table(("empty", "t"), Schema(NestedField(1, "x", LongType(), False)))

    assert expire_snapshots(table, older_than=LATER) == []


def test_main_head_can_never_be_expired(table: Table) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        expire_snapshots(table, older_than=LATER, retain_last=0)
