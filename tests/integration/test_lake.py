"""The lake library on the real stack: PyIceberg writes and publishes, DuckDB reads."""

import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import DateType, LongType, NestedField

from ibergrid.lake import duck, maintenance, wap
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.tables import TableSpec, ensure_table

from .conftest import compose_run

pytestmark = pytest.mark.integration

SCHEMA = Schema(
    NestedField(1, "day", DateType(), required=False),
    NestedField(2, "value", LongType(), required=False),
)
MONTHLY = PartitionSpec(PartitionField(1, 1000, MonthTransform(), "day_month"))


def rows(*pairs: tuple[date, int]) -> pa.Table:
    return pa.table(
        {"day": [d for d, _ in pairs], "value": pa.array([v for _, v in pairs], pa.int64())}
    )


def make_table(catalog: LakeCatalog, namespace: str, name: str = "readings") -> Table:
    ensure_table(catalog, TableSpec(namespace, name, SCHEMA, partition_spec=MONTHLY))
    table: Table = catalog.load_table((namespace, name))
    return table


def values(con: duckdb.DuckDBPyConnection, relation: duckdb.DuckDBPyRelation) -> list[int]:
    return sorted(row[1] for row in relation.fetchall())


JAN, FEB = date(2026, 1, 15), date(2026, 2, 15)


def test_duckdb_sees_a_run_on_main_only_after_publish(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"

    first = wap.start(table, "first-load")
    wap.write(table, first, rows((JAN, 1), (FEB, 2)))
    assert values(con, duck.scan(con, name)) == []
    assert values(con, duck.scan_ref(con, name, first.name)) == [1, 2]
    wap.publish(table, first)
    assert values(con, duck.scan(con, name)) == [1, 2]

    second = wap.start(table, "correction")
    wap.write(table, second, rows((FEB, 20)), overwrite_filter="day >= '2026-02-01'")
    assert values(con, duck.scan(con, name)) == [1, 2]
    assert values(con, duck.scan_ref(con, name, second.name)) == [1, 20]
    head = wap.publish(table, second)

    assert values(con, duck.scan(con, name)) == [1, 20]
    assert duck.describe(con, name).refs == [("main", "BRANCH", head)]
    assert head is not None
    snapshot = table.snapshot_by_id(head)
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary[wap.RUN_ID_PROPERTY] == "correction"


def test_a_publish_after_main_moved_conflicts_and_loses_nothing(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    seed = wap.start(table, "seed")
    wap.write(table, seed, rows((JAN, 1)))
    wap.publish(table, seed)
    early, late = wap.start(table, "early"), wap.start(table, "late")
    wap.write(table, early, rows((JAN, 2)))
    wap.write(table, late, rows((JAN, 3)))
    main = wap.publish(table, early)

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(table, late)

    assert duck.ref_head(con, name, "main") == main
    assert values(con, duck.scan(con, name)) == [1, 2]
    assert values(con, duck.scan_ref(con, name, late.name)) == [1, 3]
    wap.discard(table, late)
    assert [ref for ref, _, _ in duck.describe(con, name).refs] == ["main"]


def test_lakekeeper_rejects_a_publish_racing_a_write_to_main(
    catalog: LakeCatalog, namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = make_table(catalog, namespace)
    audit = wap.start(table, "racing")
    head = wap.write(table, audit, rows((JAN, 1)))
    commit = table.catalog.commit_table

    def other_writer_first(*args: Any) -> Any:
        # Another process appends to main after publish's checks, before its commit arrives.
        monkeypatch.setattr(table.catalog, "commit_table", commit)
        catalog.load_table((namespace, "readings")).append(rows((FEB, 2)))
        return commit(*args)

    monkeypatch.setattr(table.catalog, "commit_table", other_writer_first)

    with pytest.raises(wap.PublishConflict, match="nothing was published"):
        wap.publish(table, audit)

    table.refresh()
    assert [r["value"] for r in table.scan().to_arrow().to_pylist()] == [2]
    assert table.metadata.refs[audit.name].snapshot_id == head


def test_expiry_keeps_main_and_open_branches_readable(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    for value in range(4):
        table.append(rows((JAN, value)))
    audit = wap.start(table, "open-run")
    wap.write(table, audit, rows((FEB, 9)))

    expired = maintenance.expire_snapshots(
        table, older_than=datetime.now(UTC) + timedelta(days=1), retain_last=1
    )

    assert len(expired) == 3
    assert values(con, duck.scan(con, name)) == [0, 1, 2, 3]
    assert values(con, duck.scan_ref(con, name, audit.name)) == [0, 1, 2, 3, 9]


def test_duckdb_compacts_a_partitioned_table_with_an_open_branch(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    for value in range(6):
        table.append(rows((JAN, value), (FEB, value + 100)))
    audit = wap.start(table, "open-run")
    wap.write(table, audit, rows((FEB, 999)))

    result = maintenance.rewrite_data_files(con, name)

    assert result == maintenance.Compaction(
        rewritten_files=12, added_files=2, rewritten_bytes=result.rewritten_bytes, failed_files=0
    )
    assert duck.describe(con, name).data_files == 2  # one file per month
    assert values(con, duck.scan(con, name)) == [*range(6), *range(100, 106)]
    assert 999 in values(con, duck.scan_ref(con, name, audit.name))
    table.refresh()
    assert table.current_snapshot().summary.operation.value == "replace"  # type: ignore[union-attr]


# The same library inside the compose network: the app image, its baked DuckDB extensions and the
# in-network endpoints (http://storage:9000, http://catalog:8181).
IN_NETWORK_RUN = """
import json, sys
from datetime import UTC, datetime
import pyarrow as pa
from ibergrid.lake import duck, wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import SMOKE
from ibergrid.settings import Settings

settings = Settings()
table = rest_catalog(settings).load_table(SMOKE.identifier)
audit = wap.start(table, sys.argv[1])
now = pa.array([datetime.now(UTC)], pa.timestamp("us", "UTC"))
wap.write(table, audit, pa.table({"run_id": [audit.run_id], "written_at": now}))
con = duck.connect(settings)
name = duck.table_name("ops.smoke")
mine = f"run_id = '{audit.run_id}'"
before = duck.scan(con, name).filter(mine).fetchall()
branch = duck.scan_ref(con, name, audit.name).filter(mine).fetchall()
wap.publish(table, audit)
after = duck.scan(con, name).filter(mine).fetchall()
print(json.dumps({"before": len(before), "branch": len(branch), "after": len(after)}))
"""


def test_the_app_image_runs_init_wap_and_describe_in_the_network() -> None:
    for expected in ("created|unchanged", "unchanged"):
        result = compose_run("doctor", "lake", "init")
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.strip().rpartition(" ")[2] in expected.split("|")

    run_id = f"it-{datetime.now(UTC):%Y%m%d%H%M%S%f}"
    result = compose_run("doctor", "-c", IN_NETWORK_RUN, run_id, entrypoint="python")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == {"before": 0, "branch": 1, "after": 1}

    result = compose_run("doctor", "lake", "describe", "ops.smoke")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refs:       main -> " in result.stdout
    assert "audit-" not in result.stdout
