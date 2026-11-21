"""Dagster on the running stack: the services are healthy, a run launched through the webserver
materializes partitions with the connectors' measurements, and the sweeper removes a finished run's
audit branch.

Runs execute in dagster-code, so the recorded sources are served from the host on the gateway of
a network that container is on (never the live APIs), into a throwaway namespace and landing
prefix; the test's runs are deleted afterwards, with the materializations they recorded.
"""

import json
import os
import subprocess
import time
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from typing import Any

import pytest
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES, REDATA_OBSERVATIONS, TABLES
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings
from ibergrid.storage import s3_filesystem

from ..unit.conftest import FakeOmie, FakeRedata, LocalServer
from .conftest import REPO

pytestmark = pytest.mark.integration

DAGSTER_URL = os.environ.get("IBERGRID_TEST_DAGSTER_URL", "http://127.0.0.1:28300")
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


@dataclass(frozen=True)
class Sandbox:
    namespace: str
    run_config: dict[str, Any]
    runs: list[str]  # deleted at teardown


@pytest.fixture(scope="module")
def sandbox(catalog: LakeCatalog, settings: Settings) -> Iterator[Sandbox]:
    gateway = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            "ibergrid_catalog",
            "-f",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    redata, omie = LocalServer(FakeRedata(), host=gateway), LocalServer(FakeOmie(), host=gateway)
    namespace = f"it_{uuid.uuid4().hex[:12]}"
    catalog.create_namespace(namespace)
    for spec in TABLES:
        ensure_table(catalog, replace(spec, namespace=namespace))
    landing = f"tests/{namespace}"
    lake = {
        "redata_base_url": redata.url,
        "omie_base_url": omie.url,
        "namespace": namespace,
        "landing_prefix": landing,
    }
    box = Sandbox(namespace, {"resources": {"lake": {"config": lake}}}, [])
    yield box
    for run_id in box.runs:  # and with them, the materializations of the real asset keys
        graphql("mutation($id: String!) { deleteRun(runId: $id) { __typename } }", id=run_id)
    for identifier in catalog.list_tables(namespace):
        catalog.purge_table(identifier)
    catalog.drop_namespace(namespace)
    s3_filesystem(settings.storage, timeout=30).delete_dir(f"{settings.storage.bucket}/{landing}")
    redata.close()
    omie.close()


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


def finished(run_id: str, timeout: float = 180) -> str:
    query = "query($id: ID!) { runOrError(runId: $id) { ... on Run { status } } }"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status: str = graphql(query, id=run_id)["runOrError"]["status"]
        if status in {"SUCCESS", "FAILURE", "CANCELED"}:
            return status
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


def test_a_run_through_the_webserver_materializes_partitions_with_their_measurements(
    sandbox: Sandbox, catalog: LakeCatalog
) -> None:
    redata = launch(sandbox, "__ASSET_JOB", [REALTIME], **{"dagster/partition": DAY})
    omie = launch(sandbox, "__ASSET_JOB", [OMIE], **{"dagster/partition": DAY})

    assert (finished(redata), finished(omie)) == ("SUCCESS", "SUCCESS")
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


def test_the_sweeper_removes_the_branch_of_a_finished_run_and_keeps_the_others(
    sandbox: Sandbox, catalog: LakeCatalog
) -> None:
    finished_run = launch(sandbox, "__ASSET_JOB", [REALTIME], **{"dagster/partition": DAY})
    assert finished(finished_run) == "SUCCESS"
    table: Table = catalog.load_table((sandbox.namespace, OMIE_DAY_AHEAD_PRICES.name))
    left = wap.start(table, f"{finished_run}-omie-20261118-20261118-a1-dead")  # a killed run's
    stranger = wap.start(table, f"{uuid.uuid4()}-omie-20261118-20261118-a1-dead")

    assert finished(launch(sandbox, "sweep_audit_branches")) == "SUCCESS"

    assert wap.audit_branches(table) == [stranger.name]
    assert left.name not in table.metadata.refs
    wap.discard(table, stranger)
