"""Write-Audit-Publish on Iceberg branches.

A run writes to its own audit branch, its output is checked there, and only then does ``main``
(what every reader sees) move to it. The invariants, each pinned by a test:

1. A write never modifies ``main``: every write in this module targets the run's branch.
2. Publishing is one catalog commit that fast-forwards ``main`` to the branch head and removes
   the branch, and the catalog applies it only if ``main`` is still the snapshot the branch started
   from and the branch is still at the head that was audited.
3. A conflict never loses data: ``PublishConflict`` leaves ``main`` and the branch as they were,
   so the run can be inspected, rebased by rerunning it, or discarded.
"""

import re
from dataclasses import dataclass
from uuid import UUID

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import Table
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    AssertTableUUID,
    RemoveSnapshotRefUpdate,
    SetSnapshotRefUpdate,
)

BRANCH_PREFIX = "audit-"
RUN_ID_PROPERTY = "ibergrid.run-id"
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}")


class WapError(RuntimeError):
    """The audit branch is not in the state the operation needs."""


class PublishConflict(WapError):
    """``main`` or the branch moved since the run started: nothing was published."""


@dataclass(frozen=True)
class AuditBranch:
    """One run's branch on one table, and the ``main`` snapshot it started from."""

    table_uuid: UUID
    run_id: str
    base_snapshot_id: int | None  # None: the table had no snapshot yet

    @property
    def name(self) -> str:
        return BRANCH_PREFIX + self.run_id


def start(table: Table, run_id: str) -> AuditBranch:
    """Branch the run off the current ``main``.

    Iceberg refs point at snapshots, so on a table without one the run's first write creates the
    branch instead.
    """
    if not _RUN_ID.fullmatch(run_id):
        raise ValueError(f"run id {run_id!r} must match {_RUN_ID.pattern}")
    table.refresh()
    audit = AuditBranch(table.metadata.table_uuid, run_id, _head(table, MAIN_BRANCH))
    if audit.name in table.metadata.refs:
        raise WapError(f"branch {audit.name!r} already exists: publish or discard it first")
    if audit.base_snapshot_id is not None:
        table.manage_snapshots().create_branch(audit.base_snapshot_id, audit.name).commit()
    return audit


def write(
    table: Table,
    audit: AuditBranch,
    data: pa.Table,
    *,
    overwrite_filter: BooleanExpression | str | None = None,
) -> int | None:
    """Append ``data`` to the branch, or replace the branch rows matching ``overwrite_filter``.

    Returns the branch head. Each snapshot records the run id under ``ibergrid.run-id``. Writing
    no rows commits nothing (except an overwrite's deletes): DuckDB 2.0.1 cannot read a snapshot
    whose manifest list is empty, which is what an empty append to an empty table produces.
    """
    table.refresh()
    _check_table(table, audit)
    head = _head(table, audit.name)
    if data.num_rows == 0 and (overwrite_filter is None or head is None):
        return head
    properties = {RUN_ID_PROPERTY: audit.run_id}
    if head is None:
        # No snapshot to branch from, and PyIceberg 0.12 writes only `main` on such a table:
        # stage the snapshot (no ref moves), then branch from it.
        before = {snapshot.snapshot_id for snapshot in table.metadata.snapshots}
        table.append(data, snapshot_properties=properties, branch=None)
        [staged] = [
            snapshot.snapshot_id
            for snapshot in table.metadata.snapshots
            if snapshot.snapshot_id not in before
            and snapshot.summary is not None
            and snapshot.summary[RUN_ID_PROPERTY] == audit.run_id
        ]
        table.manage_snapshots().create_branch(staged, audit.name).commit()
    elif overwrite_filter is None:
        table.append(data, snapshot_properties=properties, branch=audit.name)
    else:
        table.overwrite(
            data,
            overwrite_filter=overwrite_filter,
            snapshot_properties=properties,
            branch=audit.name,
        )
    return _head(table, audit.name)


def read(table: Table, audit: AuditBranch) -> pa.Table:
    """Everything the branch holds now: ``main`` as of the start plus the run's writes."""
    table.refresh()
    _check_table(table, audit)
    head = _head(table, audit.name)
    if head is None:  # nothing written yet on a table that had no snapshot
        return table.schema().as_arrow().empty_table()
    return table.scan(snapshot_id=head).to_arrow()


def publish(table: Table, audit: AuditBranch) -> int | None:
    """Fast-forward ``main`` to the branch head and remove the branch, in one commit.

    Returns the snapshot ``main`` points at afterwards. Raises ``PublishConflict`` when ``main``
    is no longer at the run's base snapshot; ``main`` and the branch are then left untouched.
    """
    table.refresh()
    _check_table(table, audit)
    main = _head(table, MAIN_BRANCH)
    if main != audit.base_snapshot_id:
        raise PublishConflict(
            f"main moved from {audit.base_snapshot_id} to {main} since run {audit.run_id!r} "
            f"started; branch {audit.name!r} is kept"
        )
    head = _head(table, audit.name)
    if head is None and main is not None:
        raise WapError(f"branch {audit.name!r} does not exist: the run was discarded")
    if head is None:  # nothing was written on a table that had no snapshot
        return main
    requirements = (
        AssertTableUUID(uuid=audit.table_uuid),
        AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=main),
        AssertRefSnapshotId(ref=audit.name, snapshot_id=head),
    )
    updates = (
        SetSnapshotRefUpdate(
            ref_name=MAIN_BRANCH,
            type=SnapshotRefType.BRANCH,
            snapshot_id=head,
            max_ref_age_ms=None,
            max_snapshot_age_ms=None,
            min_snapshots_to_keep=None,
        ),
        RemoveSnapshotRefUpdate(ref_name=audit.name),
    )
    try:
        table.catalog.commit_table(table, requirements, updates)
    except CommitFailedException as exc:
        raise PublishConflict(
            f"main or branch {audit.name!r} moved while publishing run {audit.run_id!r}; "
            "nothing was published"
        ) from exc
    table.refresh()
    return head


def discard(table: Table, audit: AuditBranch) -> None:
    """Remove the branch; its snapshots stay until snapshot expiry removes them."""
    table.refresh()
    _check_table(table, audit)
    if audit.name in table.metadata.refs:
        table.manage_snapshots().remove_branch(audit.name).commit()


def _head(table: Table, ref: str) -> int | None:
    snapshot_ref = table.metadata.refs.get(ref)
    return None if snapshot_ref is None else snapshot_ref.snapshot_id


def _check_table(table: Table, audit: AuditBranch) -> None:
    if table.metadata.table_uuid != audit.table_uuid:
        raise WapError(f"run {audit.run_id!r} belongs to table {audit.table_uuid}, not this one")
