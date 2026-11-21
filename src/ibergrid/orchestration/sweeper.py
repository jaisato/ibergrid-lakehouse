"""Remove the audit branches of Dagster runs that are over (ADR 0005).

A run killed between ``wap.start`` and its publish or discard leaves ``audit-<run id>`` behind:
harmless to readers, but it pins snapshots that expiry would drop. Iceberg refs carry no creation
time and an idle run's head is its base (ADR 0002), so a branch's age says nothing; its owner
does. The assets name every run after the Dagster run that writes it, so a branch whose Dagster
run has finished (success, failure, cancellation) is removed. A branch of a run in progress, of a
run this instance does not know, or of no Dagster run at all (the CLI's timestamped runs) is kept.
"""

import re

import dagster as dg
from pyiceberg.exceptions import NoSuchTableError

from ibergrid.lake import wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import TABLES
from ibergrid.orchestration.resources import Lake

# audit-<Dagster run id>-<connector slug>-a<attempt>-<hex>, the run id being the assets' prefix.
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_RUN_BRANCH = re.compile(re.escape(wap.BRANCH_PREFIX) + f"({_UUID})-")
FINISHED = frozenset(
    {dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED}
)


@dg.op(description="Remove the audit branches whose Dagster run has finished; log each one.")
def discard_finished_runs_branches(context: dg.OpExecutionContext, lake: Lake) -> None:
    catalog = rest_catalog(lake.settings())
    for spec in TABLES:
        identifier = lake.table(spec)
        name = ".".join(identifier)
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            context.log.info(f"{name}: no such table yet")
            continue
        for branch in wap.audit_branches(table):
            match = _RUN_BRANCH.match(branch)
            run = context.instance.get_run_by_id(match[1]) if match else None
            if run is None:
                context.log.info(f"{name}: kept {branch} (no Dagster run of this instance)")
            elif run.status not in FINISHED:
                context.log.info(f"{name}: kept {branch} (run {run.status.value})")
            elif wap.discard_branch(table, branch):
                context.log.info(f"{name}: removed {branch} (run {run.status.value})")


@dg.job(description="Daily: remove the audit branches of Dagster runs that are over (ADR 0005).")
def sweep_audit_branches() -> None:
    discard_finished_runs_branches()
