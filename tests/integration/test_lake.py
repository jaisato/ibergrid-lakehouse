"""The lake library on the real stack: PyIceberg writes and publishes, DuckDB reads."""

import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import duckdb
import pyarrow as pa
import pytest
from pydantic import HttpUrl, SecretStr
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import DateType, LongType, NestedField

from ibergrid.lake import duck, maintenance, registry, wap
from ibergrid.lake.catalog import LakeCatalog, rest_catalog
from ibergrid.lake.tables import TableSpec, ensure_table
from ibergrid.settings import Settings

from .conftest import CatalogProxy, compose_run

pytestmark = pytest.mark.integration

SCHEMA = Schema(
    NestedField(1, "day", DateType(), required=False),
    NestedField(2, "value", LongType(), required=False),
)
MONTHLY = PartitionSpec(PartitionField(1, 1000, MonthTransform(), "day_month"))
JAN, FEB = date(2026, 1, 15), date(2026, 2, 15)


def rows(*pairs: tuple[date, int]) -> pa.Table:
    return pa.table(
        {"day": [d for d, _ in pairs], "value": pa.array([v for _, v in pairs], pa.int64())}
    )


def make_table(catalog: LakeCatalog, namespace: str, name: str = "readings") -> Table:
    ensure_table(catalog, TableSpec(namespace, name, SCHEMA, partition_spec=MONTHLY))
    table: Table = catalog.load_table((namespace, name))
    return table


def values(relation: duckdb.DuckDBPyRelation) -> list[int]:
    return sorted(row[1] for row in relation.fetchall())


def interleave(table: Table, writer: Callable[[], object], monkeypatch: pytest.MonkeyPatch) -> None:
    """Run ``writer`` (another process) just before the next PyIceberg commit leaves."""
    commit = table.catalog.commit_table

    def other_writer_first(*args: Any) -> Any:
        monkeypatch.setattr(table.catalog, "commit_table", commit)
        writer()
        return commit(*args)

    monkeypatch.setattr(table.catalog, "commit_table", other_writer_first)


def test_duckdb_sees_a_run_on_main_only_after_publish(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"

    first = wap.start(table, "first-load")
    audited = wap.write(table, first, rows((JAN, 1), (FEB, 2)))
    assert values(duck.scan(con, name)) == []
    assert values(duck.scan(con, name, snapshot_id=audited)) == [1, 2]  # the audit's read
    wap.publish(table, first, audited)
    assert values(duck.scan(con, name)) == [1, 2]

    second = wap.start(table, "correction")
    wap.write(table, second, rows((FEB, 20)), overwrite_filter="day >= '2026-02-01'")
    audited = wap.head(table, second)
    assert values(duck.scan(con, name)) == [1, 2]
    assert values(duck.scan(con, name, snapshot_id=audited)) == [1, 20]
    published = wap.publish(table, second, audited, set_properties={"ibergrid.checkpoint": "c2"})

    assert values(duck.scan(con, name)) == [1, 20]
    assert duck.describe(con, name).refs == [("main", "BRANCH", published)]
    table.refresh()
    assert table.properties["ibergrid.checkpoint"] == "c2"


def test_a_publish_after_main_moved_conflicts_and_loses_nothing(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    table.append(rows((JAN, 1)))
    early, late = wap.start(table, "early"), wap.start(table, "late")
    main = wap.publish(table, early, wap.write(table, early, rows((JAN, 2))))
    late_head = wap.write(table, late, rows((JAN, 3)))

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(table, late, late_head)

    assert duck.ref_head(con, name, "main") == main
    assert values(duck.scan(con, name)) == [1, 2]
    assert values(duck.scan_ref(con, name, late.name)) == [1, 3]
    wap.discard(table, late)
    assert [ref for ref, _, _ in duck.describe(con, name).refs] == ["main"]


def test_lakekeeper_rejects_a_publish_racing_a_write_to_main(
    catalog: LakeCatalog, namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = make_table(catalog, namespace)
    table.append(rows((JAN, 1)))
    audit = wap.start(table, "racing")
    audited = wap.write(table, audit, rows((JAN, 2)))
    elsewhere = catalog.load_table((namespace, "readings"))
    interleave(table, lambda: elsewhere.append(rows((FEB, 3))), monkeypatch)

    with pytest.raises(wap.PublishConflict, match="nothing was published"):
        wap.publish(table, audit, audited, set_properties={"ibergrid.checkpoint": "lost"})

    table.refresh()
    assert sorted(r["value"] for r in table.scan().to_arrow().to_pylist()) == [1, 3]
    assert table.metadata.refs[audit.name].snapshot_id == audited
    assert "ibergrid.checkpoint" not in table.properties


def test_lakekeeper_rejects_publishing_a_branch_written_after_its_audit(
    catalog: LakeCatalog, namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = make_table(catalog, namespace)
    table.append(rows((JAN, 1)))
    audit = wap.start(table, "audited")
    audited = wap.write(table, audit, rows((JAN, 2)))
    elsewhere = catalog.load_table((namespace, "readings"))

    # After the audit and before publish: refused before any commit.
    wap.write(elsewhere, audit, rows((FEB, 8)))
    with pytest.raises(wap.PublishConflict, match="since its audit"):
        wap.publish(table, audit, audited)

    # During publish's commit: Lakekeeper refuses it (branch no longer at the audited head).
    audited = wap.head(table, audit)
    interleave(table, lambda: wap.write(elsewhere, audit, rows((FEB, 9))), monkeypatch)
    with pytest.raises(wap.PublishConflict, match="nothing was published"):
        wap.publish(table, audit, audited)

    table.refresh()
    assert [r["value"] for r in table.scan().to_arrow().to_pylist()] == [1]
    assert 9 in wap.read(table, audit).rows["value"].to_pylist()


def test_a_run_that_deletes_every_row_leaves_main_readable(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    table.append(rows((JAN, 1), (FEB, 2)))
    audit = wap.start(table, "wipe")

    audited = wap.write(table, audit, rows(), overwrite_filter="value >= 0")
    wap.publish(table, audit, audited)

    assert values(duck.scan(con, name)) == []
    assert duck.describe(con, name).data_files == 0


def test_lakekeeper_orders_checkpoints_of_runs_without_data(
    catalog: LakeCatalog, namespace: str
) -> None:
    table = make_table(catalog, namespace)
    table.append(rows((JAN, 1)))
    elsewhere = catalog.load_table((namespace, "readings"))
    late, early = wap.start(table, "late"), wap.start(elsewhere, "early")
    wap.publish(elsewhere, early, wap.head(elsewhere, early), set_properties={"offsets": "p0:20"})

    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(table, late, wap.head(table, late), set_properties={"offsets": "p0:12"})

    table.refresh()
    assert table.properties["offsets"] == "p0:20"


def test_a_checkpoint_needs_live_data_in_main(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    table.append(rows((JAN, 1)))
    wipe = wap.start(table, "wipe")
    wap.publish(table, wipe, wap.write(table, wipe, rows(), overwrite_filter="value >= 0"))
    idle = wap.start(table, "idle")

    with pytest.raises(wap.WapError, match="a checkpoint needs data"):
        wap.publish(table, idle, wap.head(table, idle), set_properties={"offsets": "p0:5"})

    assert values(duck.scan(con, name)) == []
    assert duck.describe(con, name).data_files == 0


def test_a_checkpoint_whose_publish_answer_was_lost_resumes(
    catalog: LakeCatalog,
    namespace: str,
    settings: Settings,
    proxy: CatalogProxy,
    con: duckdb.DuckDBPyConnection,
) -> None:
    make_table(catalog, namespace).append(rows((JAN, 1)))
    via_proxy = settings.catalog.model_copy(update={"url": HttpUrl(proxy.url)})
    table = rest_catalog(settings.model_copy(update={"catalog": via_proxy})).load_table(
        (namespace, "readings")
    )
    audit = wap.start(table, "idle")
    audited = wap.head(table, audit)
    proxy.fail_commits = [None, 502]  # the checkpoint append lands, the publish gets a 502

    with pytest.raises(wap.WapError, match="publish again with the same arguments"):
        wap.publish(table, audit, audited, set_properties={"offsets": "p0:5"})
    checkpoint = wap.publish(table, audit, audited, set_properties={"offsets": "p0:5"})

    table.refresh()
    assert table.metadata.current_snapshot_id == checkpoint != audited
    assert table.properties["offsets"] == "p0:5"
    assert values(duck.scan(con, f"{duck.CATALOG}.{namespace}.readings")) == [1]


def test_a_discard_racing_a_write_cannot_publish_a_partial_run(
    catalog: LakeCatalog, namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reviewer's repro: PyIceberg's retry re-created the branch without the run's base."""
    table = make_table(catalog, namespace)
    table.append(rows((JAN, 1)))
    audit = wap.start(table, "discarded")
    wap.write(table, audit, rows((JAN, 2)))
    elsewhere = catalog.load_table((namespace, "readings"))
    interleave(table, lambda: wap.discard(elsewhere, audit), monkeypatch)

    with pytest.raises(wap.WapError, match="no longer descends from snapshot"):
        wap.write(table, audit, rows((FEB, 3)))

    recreated = table.metadata.refs[audit.name].snapshot_id
    with pytest.raises(wap.WapError, match="no longer descends from snapshot"):
        wap.publish(table, audit, recreated)
    table.refresh()
    assert [r["value"] for r in table.scan().to_arrow().to_pylist()] == [1]


def test_expiry_keeps_main_and_open_runs_readable(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    for value in range(4):
        table.append(rows((JAN, value)))
    audit = wap.start(table, "open-run")
    wap.write(table, audit, rows((FEB, 9)))
    audited = wap.write(table, audit, rows((FEB, 10)))

    expired = maintenance.expire_snapshots(
        table, older_than=datetime.now(UTC) + timedelta(days=1), retain_last=1
    )

    assert len(expired) == 3  # main's older snapshots; the run's two and the fork stay
    assert values(duck.scan(con, name)) == [0, 1, 2, 3]
    assert values(duck.scan(con, name, snapshot_id=audited)) == [0, 1, 2, 3, 9, 10]
    assert wap.publish(table, audit, audited) == audited


def test_compaction_commits_to_main_so_an_open_run_must_restart(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection
) -> None:
    table = make_table(catalog, namespace)
    name = f"{duck.CATALOG}.{namespace}.readings"
    for value in range(6):
        table.append(rows((JAN, value), (FEB, value + 100)))
    audit = wap.start(table, "open-run")
    audited = wap.write(table, audit, rows((FEB, 999)))

    result = maintenance.rewrite_data_files(con, name)

    assert result == maintenance.Compaction(
        rewritten_files=12, added_files=2, rewritten_bytes=result.rewritten_bytes, failed_files=0
    )
    assert duck.describe(con, name).data_files == 2  # one file per month
    assert values(duck.scan(con, name)) == [*range(6), *range(100, 106)]
    table.refresh()
    assert table.current_snapshot().summary.operation.value == "replace"  # type: ignore[union-attr]
    with pytest.raises(wap.PublishConflict, match="main moved"):
        wap.publish(table, audit, audited)
    assert 999 in values(duck.scan(con, name, snapshot_id=audited))  # the run's data is intact


def test_a_write_during_compaction_wins_and_the_compaction_says_so(
    catalog: LakeCatalog, namespace: str, settings: Settings, proxy: CatalogProxy
) -> None:
    table = make_table(catalog, namespace)
    for value in range(6):
        table.append(rows((JAN, value)))
    via_proxy = settings.catalog.model_copy(update={"url": HttpUrl(proxy.url)})
    con = duck.connect(settings.model_copy(update={"catalog": via_proxy}))
    name = f"{duck.CATALOG}.{namespace}.readings"
    elsewhere = catalog.load_table((namespace, "readings"))
    proxy.before_next_commit = lambda: elsewhere.append(rows((FEB, 777)))

    with pytest.raises(maintenance.MaintenanceConflict, match="changed during the compaction"):
        maintenance.rewrite_data_files(con, name)

    [commit] = proxy.commits
    assert commit["status"] == 409
    [change] = commit["body"]["table-changes"]
    assert sorted(r["type"] for r in change["requirements"]) == [
        "assert-current-schema-id",
        "assert-ref-snapshot-id",
        "assert-table-uuid",
    ]
    [ref] = [r for r in change["requirements"] if r["type"] == "assert-ref-snapshot-id"]
    assert ref["ref"] == "main"
    table.refresh()
    assert 777 in [r["value"] for r in table.scan().to_arrow().to_pylist()]
    assert table.current_snapshot().summary.operation.value == "append"  # type: ignore[union-attr]


def test_a_compaction_whose_commit_answer_is_unknown_is_not_called_a_conflict(
    catalog: LakeCatalog, namespace: str, settings: Settings, proxy: CatalogProxy
) -> None:
    table = make_table(catalog, namespace)
    for value in range(6):
        table.append(rows((JAN, value)))
    via_proxy = settings.catalog.model_copy(update={"url": HttpUrl(proxy.url)})
    con = duck.connect(settings.model_copy(update={"catalog": via_proxy}))
    proxy.fail_commits = [502]

    with pytest.raises(duckdb.TransactionException) as failure:
        maintenance.rewrite_data_files(con, f"{duck.CATALOG}.{namespace}.readings")

    assert not isinstance(failure.value, maintenance.MaintenanceConflict)
    assert "Conflict_409" not in str(failure.value)


def test_wrong_storage_keys_fail_without_leaking_or_committing(
    catalog: LakeCatalog, namespace: str, settings: Settings
) -> None:
    table = make_table(catalog, namespace)
    table.append(rows((JAN, 1)))
    wrong_secret = "wrong-" + "0" * 20
    storage = settings.storage.model_copy(update={"secret_key": SecretStr(wrong_secret)})
    wrong = settings.model_copy(update={"storage": storage})
    keys = (settings.storage.access_key.get_secret_value(), wrong_secret)

    bad = rest_catalog(wrong).load_table((namespace, "readings"))
    with pytest.raises(OSError, match="SIGNATURE_DOES_NOT_MATCH") as pyiceberg_error:
        bad.append(rows((FEB, 2)))
    with pytest.raises(duckdb.HTTPException, match="403") as duckdb_error:
        duck.scan(duck.connect(wrong), f"{duck.CATALOG}.{namespace}.readings").fetchall()

    for message in (str(pyiceberg_error.value), str(duckdb_error.value)):
        assert not any(key in message for key in keys)
    table.refresh()
    assert len(table.metadata.snapshots) == 1


# The same library inside the compose network: the app image, its baked DuckDB extensions and the
# in-network endpoints (http://storage:9000, http://catalog:8181).
IN_NETWORK_RUN = """
import json, sys
from datetime import UTC, datetime
import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType
from ibergrid.lake import duck, wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.tables import TableSpec, ensure_table
from ibergrid.settings import Settings

namespace, run_id = sys.argv[1:]
settings = Settings()
catalog = rest_catalog(settings)
schema = Schema(NestedField(1, "run_id", StringType()), NestedField(2, "at", TimestamptzType()))
created = ensure_table(catalog, TableSpec(namespace, "smoke", schema)).created
table = catalog.load_table((namespace, "smoke"))
audit = wap.start(table, run_id)
now = pa.array([datetime.now(UTC)], pa.timestamp("us", "UTC"))
audited = wap.write(table, audit, pa.table({"run_id": [run_id], "at": now}))
con = duck.connect(settings)
name = duck.table_name(f"{namespace}.smoke")
before = duck.scan(con, name).fetchall()
branch = duck.scan(con, name, snapshot_id=audited).fetchall()
wap.publish(table, audit, audited)
after = duck.scan(con, name).fetchall()
counts = {"before": len(before), "branch": len(branch), "after": len(after)}
print(json.dumps({"created": created, **counts}))
"""


def test_the_app_image_runs_init_wap_and_describe_in_the_network(namespace: str) -> None:
    result = compose_run("doctor", "lake", "init")
    assert result.returncode == 0, result.stdout + result.stderr
    # Every registered Bronze, Silver and Gold table: created once, unchanged afterwards.
    expected = "".join(
        rf"lake: {re.escape(spec.namespace)}\.{re.escape(spec.name)} (created|unchanged)\n"
        for spec in registry.TABLES
    )
    assert re.fullmatch(expected, result.stdout), result.stdout

    run_id = f"it-{datetime.now(UTC):%Y%m%d%H%M%S%f}"
    result = compose_run("doctor", "-c", IN_NETWORK_RUN, namespace, run_id, entrypoint="python")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "created": True,
        "before": 0,
        "branch": 1,
        "after": 1,
    }

    result = compose_run("doctor", "lake", "describe", f"{namespace}.smoke")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "refs:       main -> " in result.stdout
    assert "audit-" not in result.stdout
