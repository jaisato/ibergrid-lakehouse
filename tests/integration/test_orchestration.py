"""Dagster on the running stack: the services are healthy, runs launched through the webserver
materialize partitions with the connectors' measurements, hand a range to one connector call and
never overlap within a pool, and the sweeper removes a finished run's audit branch.

Runs execute in dagster-code, so the recorded sources are served from the host on the gateway of
a network that container is on (never the live APIs), into a throwaway namespace and landing
prefix. The materializing tests record materializations of the real asset keys, and deleting
their runs afterwards does not restore those assets' latest records: they run only on an instance
without real materializations (a fresh stack, as in CI) and are skipped on a backfilled one.
"""

import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, suppress
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

import pytest
from pyarrow import fs
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES, REDATA_OBSERVATIONS, TABLES
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings
from ibergrid.storage import s3_filesystem

from ..unit.conftest import FakeOmie, FakeRedata, LocalServer, restamped
from .conftest import REPO

pytestmark = pytest.mark.integration

DAGSTER_URL = os.environ.get("IBERGRID_TEST_DAGSTER_URL", "http://127.0.0.1:28300")
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "ibergrid")
DAY = "2026-11-18"  # recorded for REData's five-minute demand and for OMIE
REALTIME = ["bronze", "redata_observations", "demand_realtime"]
OMIE = ["bronze", "omie_day_ahead_prices"]
LAUNCH = """
mutation($params: ExecutionParams!) {
  launchRun(executionParams: $params) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { errors { message } }
  }
}"""


def graphql(query: str, **variables: Any) -> dict[str, Any]:
    request = urllib.request.Request(  # noqa: S310 (fixed http URL)
        f"{DAGSTER_URL}/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        answer: dict[str, Any] = json.load(response)
    assert "errors" not in answer, answer["errors"]
    data: dict[str, Any] = answer["data"]
    return data


@pytest.fixture(scope="module")
def fresh() -> None:
    """Skip on an instance that holds materializations of the real asset keys."""
    nodes = graphql("{ assetNodes { partitionStats { numMaterialized } } }")["assetNodes"]
    found = sum((node["partitionStats"] or {}).get("numMaterialized", 0) for node in nodes)
    if found and os.environ.get("CI"):
        pytest.fail(f"CI's stack must be fresh, but the instance holds {found} materializations")
    if found:
        pytest.skip(
            f"the instance holds {found} materialized partitions of the real assets: these tests "
            "would leave their latest records pointing at deleted test runs (fresh stack only)"
        )


@dataclass(frozen=True)
class Sandbox:
    namespace: str
    run_config: dict[str, Any]
    redata: FakeRedata
    redata_server: LocalServer
    runs: list[str]  # deleted at teardown


@pytest.fixture(scope="module")
def sandbox(catalog: LakeCatalog, settings: Settings) -> Iterator[Sandbox]:
    """Fake sources on the gateway, a throwaway namespace and landing prefix; each is cleaned up
    on its own, whatever happened to the others."""
    gateway = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            f"{PROJECT}_catalog",
            "-f",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    namespace = f"it_{uuid.uuid4().hex[:12]}"
    landing = f"tests/{namespace}"
    with ExitStack() as cleanup:
        fake_redata = FakeRedata()
        redata = LocalServer(fake_redata, host=gateway)
        cleanup.callback(redata.close)
        omie = LocalServer(FakeOmie(), host=gateway)
        cleanup.callback(omie.close)
        filesystem = s3_filesystem(settings.storage, timeout=30)
        cleanup.callback(remove_prefix, filesystem, f"{settings.storage.bucket}/{landing}")
        catalog.create_namespace(namespace)
        cleanup.callback(catalog.drop_namespace, namespace)
        for spec in TABLES:
            ensure_table(catalog, replace(spec, namespace=namespace))
            cleanup.callback(catalog.purge_table, (namespace, spec.name))
        lake = {
            "redata_base_url": redata.url,
            "omie_base_url": omie.url,
            "namespace": namespace,
            "landing_prefix": landing,
        }
        box = Sandbox(namespace, {"resources": {"lake": {"config": lake}}}, fake_redata, redata, [])
        cleanup.callback(delete_runs, box.runs)
        yield box


def remove_prefix(filesystem: fs.FileSystem, path: str) -> None:
    with suppress(FileNotFoundError):  # nothing landed there (the materializing tests skipped)
        filesystem.delete_dir(path)


def delete_runs(runs: list[str]) -> None:
    with ExitStack() as each:
        for run_id in runs:
            each.callback(
                graphql,
                "mutation($id: String!) { deleteRun(runId: $id) { __typename } }",
                id=run_id,
            )


def launch(box: Sandbox, job: str, assets: list[list[str]] | None = None, **tags: str) -> str:
    selector: dict[str, Any] = {
        "repositoryLocationName": "ibergrid",
        "repositoryName": "__repository__",
        "jobName": job,
    }
    if assets is not None:
        selector["assetSelection"] = [{"path": path} for path in assets]
    params = {
        "selector": selector,
        "runConfigData": box.run_config,
        "executionMetadata": {"tags": [{"key": k, "value": v} for k, v in tags.items()]},
    }
    launched = graphql(LAUNCH, params=params)["launchRun"]
    assert launched["__typename"] == "LaunchRunSuccess", launched
    run_id: str = launched["run"]["runId"]
    box.runs.append(run_id)
    return run_id


def finished(run_id: str, timeout: float = 180) -> dict[str, Any]:
    query = "query($id: ID!) { runOrError(runId: $id) { ... on Run { status startTime endTime } } }"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run: dict[str, Any] = graphql(query, id=run_id)["runOrError"]
        if run["status"] in {"SUCCESS", "FAILURE", "CANCELED"}:
            return run
        time.sleep(1)
    raise AssertionError(f"run {run_id} still running after {timeout} s")


def materialization(path: list[str], partition: str, run_id: str) -> dict[str, Any]:
    query = """
    query($key: AssetKeyInput!, $partition: String!) {
      assetOrError(assetKey: $key) { ... on Asset {
        assetMaterializations(partitions: [$partition], limit: 1) {
          runId
          metadataEntries {
            label
            ... on BoolMetadataEntry { boolValue }
            ... on IntMetadataEntry { intValue }
            ... on TextMetadataEntry { text }
            ... on JsonMetadataEntry { jsonString }
          }
        }
      } }
    }"""
    asset = graphql(query, key={"path": path}, partition=partition)["assetOrError"]
    [found] = asset["assetMaterializations"]
    assert found["runId"] == run_id
    values = {}
    for entry in found["metadataEntries"]:
        value = next(v for k, v in entry.items() if k != "label")
        values[entry["label"]] = json.loads(value) if "jsonString" in entry else value
    return values


def test_the_dagster_services_are_healthy() -> None:
    ps = subprocess.run(
        [
            "docker",
            "compose",
            "ps",
            "--format",
            "json",
            "orchestration-db",
            "dagster-code",
            "dagster-webserver",
            "dagster-daemon",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    health = {c["Service"]: c["Health"] for c in map(json.loads, ps.splitlines())}
    assert health == dict.fromkeys(
        ["orchestration-db", "dagster-code", "dagster-webserver", "dagster-daemon"], "healthy"
    )
    instance = graphql(
        "{ instance { daemonHealth { allDaemonStatuses { daemonType healthy } }"
        " poolConfig { poolGranularity defaultPoolLimit } } }"
    )["instance"]
    statuses = instance["daemonHealth"]["allDaemonStatuses"]
    assert {s["daemonType"] for s in statuses} >= {
        "SCHEDULER",
        "BACKFILL",
        "QUEUED_RUN_COORDINATOR",
    }
    assert all(s["healthy"] for s in statuses), statuses
    assert instance["poolConfig"] == {"poolGranularity": "run", "defaultPoolLimit": 1}


def test_a_stopping_code_server_lets_its_runs_finish_and_killed_runs_are_bounded() -> None:
    """The stop grace period, the runtime limit and the run retries (ADR 0005), as the running
    services have them."""
    code = compose("ps", "-q", "dagster-code")
    assert run(["docker", "inspect", "-f", "{{.Config.StopTimeout}}", code]) == "300"
    loaded = compose(
        "exec",
        "-T",
        "dagster-code",
        "python",
        "-c",
        "from dagster import DagsterInstance; i = DagsterInstance.get(); print("
        "i.run_monitoring_max_runtime_seconds, i.run_retries_max_retries, "
        "i.run_retries_retry_on_asset_or_op_failure)",
    )
    assert loaded.split() == ["7200", "2", "False"]


def run(command: list[str]) -> str:
    return subprocess.run(
        command, cwd=REPO, capture_output=True, text=True, check=True, timeout=120
    ).stdout.strip()


def compose(*args: str) -> str:
    return run(["docker", "compose", *args])


def test_a_run_through_the_webserver_materializes_partitions_with_their_measurements(
    fresh: None, sandbox: Sandbox, catalog: LakeCatalog
) -> None:
    redata = launch(sandbox, "__ASSET_JOB", [REALTIME], **{"dagster/partition": DAY})
    omie = launch(sandbox, "__ASSET_JOB", [OMIE], **{"dagster/partition": DAY})

    assert (finished(redata)["status"], finished(omie)["status"]) == ("SUCCESS", "SUCCESS")
    realtime = materialization(REALTIME, DAY, redata)
    assert (realtime["complete"], realtime["received_points"]) == (True, 1152)
    [chunk] = realtime["chunks"]
    assert chunk["run_id"].startswith(f"{redata}-demand_realtime-national-20261118-20261118-a1-")
    assert chunk["landing_path"].startswith(f"s3://ibergrid/tests/{sandbox.namespace}/landing/")
    assert materialization(OMIE, DAY, omie)["rows"] == 96
    tables = {spec.name: catalog.load_table((sandbox.namespace, spec.name)) for spec in TABLES}
    assert len(tables[REDATA_OBSERVATIONS.name].scan().to_arrow()) == 1152
    assert len(tables[OMIE_DAY_AHEAD_PRICES.name].scan().to_arrow()) == 96
    assert all(set(table.metadata.refs) == {"main"} for table in tables.values())


def test_a_partition_range_is_one_run_and_one_request_per_chunk(
    fresh: None, sandbox: Sandbox
) -> None:
    """A backfill's single run gets the range as these tags; the connector asks for it at once."""
    recorded = "demand_realtime-national-2026-11-18_2026-11-18.json"
    window = "demand_realtime-national-2026-11-16_2026-11-17.json"
    sandbox.redata.payloads[window] = restamped(recorded, date(2026, 11, 16), date(2026, 11, 17))
    asked = len(sandbox.redata_server.requests)

    run = launch(
        sandbox,
        "__ASSET_JOB",
        [REALTIME],
        **{
            "dagster/asset_partition_range_start": "2026-11-16",
            "dagster/asset_partition_range_end": "2026-11-17",
        },
    )

    assert finished(run)["status"] == "SUCCESS"
    requests = sandbox.redata_server.requests[asked:]
    windows = [
        (params["start_date"], params["end_date"])
        for params in (dict(urllib.parse.parse_qsl(r.path.partition("?")[2])) for r in requests)
    ]
    assert windows == [("2026-11-16T00:00", "2026-11-17T23:59")]
    for day in ("2026-11-16", "2026-11-17"):
        assert materialization(REALTIME, day, run)["window"] == "2026-11-16/2026-11-17"


def test_runs_of_one_pool_never_overlap(fresh: None, sandbox: Sandbox) -> None:
    first = launch(sandbox, "__ASSET_JOB", [REALTIME], **{"dagster/partition": DAY})
    second = launch(sandbox, "__ASSET_JOB", [REALTIME], **{"dagster/partition": DAY})

    runs = sorted((finished(first), finished(second)), key=lambda run: run["startTime"])

    assert [run["status"] for run in runs] == ["SUCCESS", "SUCCESS"]
    assert runs[0]["endTime"] <= runs[1]["startTime"]  # the pool (limit 1) held the second back


def test_the_sweeper_removes_the_branch_of_a_finished_run_and_keeps_the_others(
    sandbox: Sandbox, catalog: LakeCatalog
) -> None:
    """No materialization: the finished run is a first sweep, which finds nothing to remove."""
    over = launch(sandbox, "sweep_audit_branches")
    assert finished(over)["status"] == "SUCCESS"
    table: Table = catalog.load_table((sandbox.namespace, OMIE_DAY_AHEAD_PRICES.name))
    left = wap.start(table, f"{over}-omie-20261118-20261118-a1-dead")  # as a killed run leaves it
    stranger = wap.start(table, f"{uuid.uuid4()}-omie-20261118-20261118-a1-dead")

    assert finished(launch(sandbox, "sweep_audit_branches"))["status"] == "SUCCESS"

    assert wap.audit_branches(table) == [stranger.name]
    assert left.name not in table.metadata.refs
    wap.discard(table, stranger)
