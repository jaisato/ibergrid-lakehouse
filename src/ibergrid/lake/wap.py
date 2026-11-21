"""Write-Audit-Publish on Iceberg branches.

A run writes to its own audit branch, its output is checked there, and only then does ``main``
(what every reader sees) move to it::

    audit = wap.start(table, run_id)              # branch audit-<run_id> at main's head
    wap.write(table, audit, rows)                 # appends or overwrites, on the branch only
    audited = wap.head(table, audit)              # the snapshot the checks read (or wap.read)
    wap.publish(table, audit, expected_head=audited)

The invariants, each pinned by a test:

1. A write never modifies ``main``: every write in this module targets the run's branch.
2. Publishing is one catalog commit that moves ``main`` to the audited snapshot and removes the
   branch; the catalog applies it only if ``main`` is still the run's base and the branch is
   still at the audited snapshot.
3. Only the run's own lineage is published: a branch that no longer descends from the snapshot
   the run started from (removed and re-created by a concurrent writer) is refused.
4. A conflict never loses data: ``PublishConflict`` leaves ``main`` and the branch as they were.
5. Publishing twice is safe: once the audited snapshot is in ``main``'s history, publishing the
   same run again returns it, also after a commit whose response was lost.
6. Checkpoints (``publish(..., set_properties=...)``) always move ``main``, so the guard on
   ``main`` orders them: a stale checkpoint conflicts instead of overwriting a newer one.
"""

import re
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from uuid import UUID

import pyarrow as pa
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    ValidationException,
)
from pyiceberg.expressions import BooleanExpression
from pyiceberg.table import Table
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRefType
from pyiceberg.table.snapshots import ancestors_of
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    AssertTableUUID,
    RemovePropertiesUpdate,
    RemoveSnapshotRefUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    TableUpdate,
)

BRANCH_PREFIX = "audit-"
RUN_ID_PROPERTY = "ibergrid.run-id"
CHECKPOINT_PROPERTY = "ibergrid.checkpoint"  # marks a run's checkpoint snapshot (no data change)
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}")


class WapError(RuntimeError):
    """The audit branch is not in the state the operation needs."""


class PublishConflict(WapError):
    """``main`` moved since the run started, or the branch since its audit: nothing changed."""


@dataclass(frozen=True)
class AuditBranch:
    """One run's branch on one table: plain data, safe to pass between processes."""

    table_uuid: UUID
    run_id: str
    base_snapshot_id: int | None  # main's head when the run started; None: no snapshot yet
    root_snapshot_id: int  # where the branch starts: the base, or an empty staged snapshot

    @property
    def name(self) -> str:
        return BRANCH_PREFIX + self.run_id


@dataclass(frozen=True)
class BranchRead:
    snapshot_id: int | None  # pass it to publish as expected_head
    rows: pa.Table


def start(table: Table, run_id: str) -> AuditBranch:
    """Branch the run off the current ``main``.

    A ref needs a snapshot, and PyIceberg writes only ``main`` on a table without one, so there
    the branch starts at an empty snapshot staged for it (no ref moves; never published).
    """
    if not _RUN_ID.fullmatch(run_id):
        raise ValueError(f"run id {run_id!r} must match {_RUN_ID.pattern}")
    table.refresh()
    name = BRANCH_PREFIX + run_id
    if name in table.metadata.refs:
        raise WapError(f"branch {name!r} already exists: publish or discard it first")
    base = _ref(table, MAIN_BRANCH)
    with _concurrently(f"starting run {run_id!r}"):
        if base is None:
            before = {snapshot.snapshot_id for snapshot in table.metadata.snapshots}
            table.append(
                table.schema().as_arrow().empty_table(),
                snapshot_properties={RUN_ID_PROPERTY: run_id},
                branch=None,
            )
            staged = [
                snapshot.snapshot_id
                for snapshot in table.metadata.snapshots
                if snapshot.snapshot_id not in before
                and snapshot.summary is not None
                and snapshot.summary[RUN_ID_PROPERTY] == run_id
            ]
            if len(staged) != 1:
                raise WapError(
                    f"starting run {run_id!r}: another start of the same run id raced this one"
                )
            [root] = staged
        else:
            root = base
        table.manage_snapshots().create_branch(root, name).commit()
    return AuditBranch(table.metadata.table_uuid, run_id, base, root)


def write(
    table: Table,
    audit: AuditBranch,
    data: pa.Table,
    *,
    overwrite_filter: BooleanExpression | str | None = None,
    snapshot_properties: Mapping[str, str] | None = None,
) -> int | None:
    """Append ``data`` to the branch, or replace the branch rows matching ``overwrite_filter``.

    Returns ``head(table, audit)`` afterwards. Each snapshot records the run id under
    ``ibergrid.run-id``, next to ``snapshot_properties`` (a run's measurements, for example),
    which may not set that key or the checkpoint marker. An append of no rows commits nothing; an
    overwrite of no rows deletes.
    """
    reserved = {RUN_ID_PROPERTY, CHECKPOINT_PROPERTY} & set(snapshot_properties or {})
    if reserved:
        raise ValueError(f"snapshot properties {sorted(reserved)} are set by the WAP protocol")
    _lineage_head(table, audit)
    if data.num_rows or overwrite_filter is not None:
        properties = {**(snapshot_properties or {}), RUN_ID_PROPERTY: audit.run_id}
        with _concurrently(f"writing run {audit.run_id!r}"):
            if overwrite_filter is None:
                table.append(data, snapshot_properties=properties, branch=audit.name)
            elif not data.num_rows:
                # A delete, not an overwrite: an overwrite with no rows that removes every row
                # leaves an append snapshot with an empty manifest list, unreadable for DuckDB.
                table.delete(overwrite_filter, snapshot_properties=properties, branch=audit.name)
            else:
                table.overwrite(
                    data,
                    overwrite_filter=overwrite_filter,
                    snapshot_properties=properties,
                    branch=audit.name,
                )
    # PyIceberg retries a lost commit race on fresh metadata: if the branch was removed in the
    # meantime, the retry re-creates it from nothing, which the lineage check refuses.
    return head(table, audit)


def head(table: Table, audit: AuditBranch) -> int | None:
    """The snapshot an audit reads and ``publish`` then requires (``expected_head``).

    None when the run has written nothing on a table that had no snapshot: there is nothing to
    read (DuckDB cannot read the empty root) and nothing to publish.
    """
    snapshot_id = _lineage_head(table, audit)
    if audit.base_snapshot_id is None and not _has_data(table, snapshot_id):
        return None
    return snapshot_id


def read(table: Table, audit: AuditBranch) -> BranchRead:
    """The branch's rows (``main`` as of the start plus the run's writes) and their snapshot."""
    snapshot_id = head(table, audit)
    if snapshot_id is None:
        return BranchRead(None, table.schema().as_arrow().empty_table())
    return BranchRead(snapshot_id, table.scan(snapshot_id=snapshot_id).to_arrow())


def publish(
    table: Table,
    audit: AuditBranch,
    expected_head: int | None,
    *,
    set_properties: Mapping[str, str] | None = None,
    remove_properties: Collection[str] = (),
) -> int | None:
    """Move ``main`` to the audited snapshot and remove the branch, in one catalog commit.

    ``expected_head`` is what the audit read (``head`` or ``read``). ``set_properties`` (a
    stream's Kafka offsets, for example) are set in the same commit, so they change together with
    ``main`` or not at all, and only if ``main`` is still at the run's base; so are
    ``remove_properties`` removed. A run that wrote
    nothing still has to move ``main`` for that guard to order it against other publishes, so it
    gets a checkpoint snapshot of its own: an empty append (no data change) on the branch, which
    is then published. When ``main`` holds no live data file (no snapshot yet, or everything
    deleted), that append would have an empty manifest list, which DuckDB cannot read, so there a
    checkpoint needs data. If the publish commit fails after the checkpoint append (lost answer,
    crash), publishing again with the same arguments resumes from that checkpoint.

    Returns the snapshot ``main`` moved to (None: nothing was written on an empty table).
    Publishing again after success returns it too. An empty run without properties changes
    nothing, so publishing it after a discard also succeeds: that carries no proof.

    Raises ``PublishConflict`` when ``main`` is no longer at the run's base or the branch is no
    longer at ``expected_head``; ``WapError`` when the run was discarded or its branch re-created.
    """
    table.refresh()
    _check_table(table, audit)
    # Removing a property (a checkpoint nobody writes any more) is fenced like setting one.
    changes_properties = bool(set_properties) or bool(remove_properties)
    checkpoint = changes_properties and expected_head == audit.base_snapshot_id
    if checkpoint and (expected_head is None or not _has_data(table, expected_head)):
        raise WapError(
            f"run {audit.run_id!r} wrote nothing and main holds no data file: a checkpoint "
            "needs data there"
        )
    branch = _ref(table, audit.name)
    if branch is None:
        found, published = _published(table, audit, expected_head, by_run_id=checkpoint)
        if found:
            return published
        raise WapError(f"branch {audit.name!r} does not exist: the run was discarded")
    main = _ref(table, MAIN_BRANCH)
    if main != audit.base_snapshot_id:
        raise PublishConflict(
            f"main moved from {audit.base_snapshot_id} to {main} since run {audit.run_id!r} "
            f"started; branch {audit.name!r} is kept"
        )
    audited = head(table, audit)
    # A checkpoint run whose earlier attempt appended its checkpoint and then failed (lost
    # answer, crash) resumes from it; anything else on the branch since the audit conflicts.
    resumed = checkpoint and _own_checkpoint(table, audit, audited, expected_head)
    if audited != expected_head and not resumed:
        raise PublishConflict(
            f"branch {audit.name!r} moved from {expected_head} to {audited} since its audit; "
            "audit it again"
        )
    target = audited
    if checkpoint and not resumed:
        with _concurrently(f"checkpointing run {audit.run_id!r}"):
            table.append(
                table.schema().as_arrow().empty_table(),
                snapshot_properties={RUN_ID_PROPERTY: audit.run_id, CHECKPOINT_PROPERTY: "true"},
                branch=audit.name,
            )
        target = branch = table.metadata.refs[audit.name].snapshot_id
        if not _own_checkpoint(table, audit, target, expected_head):
            raise PublishConflict(f"branch {audit.name!r} moved since its audit; audit it again")
    requirements = (
        AssertTableUUID(uuid=audit.table_uuid),
        AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=main),
        AssertRefSnapshotId(ref=audit.name, snapshot_id=branch),
    )
    updates: tuple[TableUpdate, ...] = (RemoveSnapshotRefUpdate(ref_name=audit.name),)
    if target is not None:
        move = SetSnapshotRefUpdate(
            ref_name=MAIN_BRANCH,
            type=SnapshotRefType.BRANCH,
            snapshot_id=target,
            max_ref_age_ms=None,
            max_snapshot_age_ms=None,
            min_snapshots_to_keep=None,
        )
        updates = (move, *updates)
    if set_properties:
        updates = (*updates, SetPropertiesUpdate(updates=dict(set_properties)))
    if remove_properties:
        updates = (*updates, RemovePropertiesUpdate(removals=list(remove_properties)))
    try:
        table.catalog.commit_table(table, requirements, updates)
    except CommitFailedException as exc:
        raise PublishConflict(
            f"main or branch {audit.name!r} moved while publishing run {audit.run_id!r}; "
            "nothing was published"
        ) from exc
    except CommitStateUnknownException as exc:
        # The catalog may have applied the commit and lost the answer: look before reporting.
        table.refresh()
        if _ref(table, audit.name) is None and _published(table, audit, target)[0]:
            return target
        raise WapError(
            f"the catalog did not confirm publishing run {audit.run_id!r} and it has not landed; "
            "publish again with the same arguments"
        ) from exc
    table.refresh()
    return target


def discard(table: Table, audit: AuditBranch) -> None:
    """Remove the branch; its snapshots stay until snapshot expiry removes them."""
    table.refresh()
    _check_table(table, audit)
    if audit.name in table.metadata.refs:
        with _concurrently(f"discarding run {audit.run_id!r}"):
            table.manage_snapshots().remove_branch(audit.name).commit()


def audit_branches(table: Table) -> list[str]:
    """The names of the run branches on ``table``: published and discarded runs leave none."""
    table.refresh()
    return sorted(name for name in table.metadata.refs if name.startswith(BRANCH_PREFIX))


def discard_branch(table: Table, name: str) -> bool:
    """Remove a run's branch by name, for a run whose handle is lost (its process was killed).

    The caller must know that the run is over: refs carry no owner or creation time (ADR 0002).
    False if the branch was already gone.
    """
    if not name.startswith(BRANCH_PREFIX):
        raise ValueError(f"{name!r} is not a run branch ({BRANCH_PREFIX}*)")
    table.refresh()
    if name not in table.metadata.refs:
        return False
    with _concurrently(f"discarding branch {name!r}"):
        table.manage_snapshots().remove_branch(name).commit()
    return True


def _lineage_head(table: Table, audit: AuditBranch) -> int:
    """The branch head after a refresh, once it is known to descend from the run's root."""
    table.refresh()
    _check_table(table, audit)
    branch = _ref(table, audit.name)
    if branch is None:
        raise WapError(f"branch {audit.name!r} does not exist: the run was published or discarded")
    lineage = ancestors_of(table.snapshot_by_id(branch), table.metadata)
    if audit.root_snapshot_id not in {snapshot.snapshot_id for snapshot in lineage}:
        raise WapError(
            f"branch {audit.name!r} no longer descends from snapshot {audit.root_snapshot_id}: it "
            "was removed and re-created while the run wrote; discard the run"
        )
    return branch


def _published(
    table: Table, audit: AuditBranch, expected_head: int | None, *, by_run_id: bool = False
) -> tuple[bool, int | None]:
    """Whether the run is in ``main``'s history since its base, and the snapshot that proves it.

    A run that wrote data is found by its audited snapshot; an empty run that checkpointed, by
    its checkpoint snapshot (``by_run_id``). An empty run without properties left no trace.
    """
    if expected_head == audit.base_snapshot_id and not by_run_id:
        return True, expected_head
    for snapshot in ancestors_of(table.current_snapshot(), table.metadata):
        # The base first: with a reused run id, it may be the previous run's checkpoint.
        if snapshot.snapshot_id == audit.base_snapshot_id:
            return False, None
        if by_run_id:
            found = (
                snapshot.summary is not None and snapshot.summary[RUN_ID_PROPERTY] == audit.run_id
            )
        else:
            found = snapshot.snapshot_id == expected_head
        if found:
            return True, snapshot.snapshot_id
    return False, None


def _has_data(table: Table, snapshot_id: int) -> bool:
    snapshot = table.snapshot_by_id(snapshot_id)
    summary = None if snapshot is None else snapshot.summary
    return summary is None or summary["total-data-files"] != "0"


def _own_checkpoint(
    table: Table, audit: AuditBranch, snapshot_id: int | None, parent: int | None
) -> bool:
    """Whether ``snapshot_id`` is this run's checkpoint snapshot on top of ``parent``."""
    snapshot = None if snapshot_id is None else table.snapshot_by_id(snapshot_id)
    summary = None if snapshot is None else snapshot.summary
    return (
        snapshot is not None
        and summary is not None
        and snapshot.parent_snapshot_id == parent
        and summary[RUN_ID_PROPERTY] == audit.run_id
        and summary[CHECKPOINT_PROPERTY] == "true"
        # The marker is only a claim: a checkpoint changes no file, so anything that does was
        # never audited and must not be resumed.
        and all(summary[key] in (None, "0") for key in _FILE_CHANGES)
    )


_FILE_CHANGES = (
    "added-data-files",
    "deleted-data-files",
    "added-delete-files",
    "removed-delete-files",
)


def _ref(table: Table, ref: str) -> int | None:
    snapshot_ref = table.metadata.refs.get(ref)
    return None if snapshot_ref is None else snapshot_ref.snapshot_id


def _check_table(table: Table, audit: AuditBranch) -> None:
    if table.metadata.table_uuid != audit.table_uuid:
        raise WapError(f"run {audit.run_id!r} belongs to table {audit.table_uuid}, not this one")


@contextmanager
def _concurrently(what: str) -> Iterator[None]:
    try:
        yield
    except (CommitFailedException, ValidationException) as exc:
        # ValidationException: an overwrite whose files a concurrent commit already removed.
        raise WapError(f"{what}: the table changed concurrently ({exc})") from exc
