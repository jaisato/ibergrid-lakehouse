"""REData into ``bronze.redata_observations``: one Write-Audit-Publish run per chunk (ADR 0003).

Per chunk: fetch, check the structure (nothing is written on failure), land the raw body at a key
named by window and content, write the rows to an audit branch replacing exactly the chunk's
dataset, system and UTC window, read the branch back, publish. The audit requires the batch inside
the window (row count and unique keys) and, outside it, as many rows of that dataset and system as
on the run's base. A rerun of the same window therefore leaves the same rows; chunks stop at the
first failure, and the ones already published stay published.
"""

import json
import re
import secrets
import warnings
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime

import structlog
from pyarrow import fs
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import REDATA_OBSERVATIONS
from ibergrid.settings import Settings
from ibergrid.sources.redata.client import RedataClient, RedataError
from ibergrid.sources.redata.datasets import Chunk, dataset_named, plan
from ibergrid.sources.redata.normalize import (
    Completeness,
    completeness,
    content_sha256,
    granularities,
    parse,
    to_arrow,
    violations,
)
from ibergrid.storage import land, s3_filesystem

log = structlog.get_logger(__name__)

KEY = ("dataset", "electric_system", "series_id", "ts_utc")
PUBLISH_ATTEMPTS = 3  # a run that loses the race to another writer restarts on the new main
LANDING_PREFIX = "landing/redata"
_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,39}")


class AuditFailed(RedataError):
    """The audit branch does not hold exactly what the run meant to write: nothing was published."""


@dataclass(frozen=True)
class ChunkReport:
    chunk: Chunk
    rows: int
    completeness: Completeness
    violations: dict[str, int]  # series id -> points breaking the value rule or the 0..1 share
    dropped_series: tuple[str, ...]  # in the window before the run, absent from this answer
    snapshot_id: int | None  # main's head after the publish
    run_id: str
    landing_path: str


def ingest(
    settings: Settings,
    dataset: str,
    start: date,
    end: date,
    systems: Sequence[str] = (),
    run_id_prefix: str | None = None,
    on_chunk: Callable[[ChunkReport], None] | None = None,
    *,
    table: tuple[str, str] = REDATA_OBSERVATIONS.identifier,
    landing_prefix: str = LANDING_PREFIX,
) -> list[ChunkReport]:
    """Ingest local days [start, end] of ``dataset`` for ``systems`` (all when empty).

    ``run_id_prefix`` names the runs (an orchestrator's run id, for example); by default a UTC
    timestamp and a random suffix. ``on_chunk`` sees each chunk's report once it is published.
    ``table`` and ``landing_prefix`` exist for tests that must not touch the real ones.
    """
    chunks = plan(dataset_named(dataset), start, end, tuple(systems))
    prefix = run_id_prefix or f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(2)}"
    if not _PREFIX.fullmatch(prefix):
        raise ValueError(f"run id prefix {prefix!r} must match {_PREFIX.pattern}")
    client = RedataClient.from_settings(settings.redata)
    filesystem = s3_filesystem(settings.storage, timeout=30)
    try:
        target = rest_catalog(settings).load_table(table)
    except NoSuchTableError as exc:
        name = ".".join(table)
        raise RedataError(f"table {name} does not exist: run `ibergrid lake init` first") from exc
    reports = []
    for chunk in chunks:
        report = ingest_chunk(
            target, client, filesystem, settings.storage.bucket, chunk, prefix, landing_prefix
        )
        if on_chunk is not None:
            on_chunk(report)
        reports.append(report)
    return reports


def ingest_chunk(
    table: Table,
    client: RedataClient,
    filesystem: fs.FileSystem,
    bucket: str,
    chunk: Chunk,
    run_id_prefix: str,
    landing_prefix: str = LANDING_PREFIX,
) -> ChunkReport:
    payload = client.fetch(chunk)
    fetched_at = datetime.now(UTC)
    points = parse(payload, chunk)
    grains = granularities(points, chunk)
    measured = completeness(points, grains, chunk)
    broken = violations(points, chunk)
    series = {point.series_id for point in points}
    sha256 = content_sha256(
        payload
    )  # the raw body is landed whole; its cache metadata is not hashed
    key = chunk.landing_key(landing_prefix, sha256)
    landing_path = land(filesystem, bucket, key, payload, content_type="application/json")
    window = chunk.row_filter()
    attempt = 0
    while True:
        attempt += 1
        # A random part per attempt: a retried task with the same prefix never meets an earlier
        # attempt's branch (one a failed discard left behind, or one already published).
        run_id = f"{run_id_prefix}-{chunk.slug}-a{attempt}-{secrets.token_hex(2)}"
        data = to_arrow(
            points,
            grains,
            chunk,
            fetched_at=fetched_at,
            run_id=run_id,
            landing_path=landing_path,
            payload_sha256=sha256,
        )
        audit = wap.start(table, run_id)
        try:
            base = audit.base_snapshot_id
            dropped = tuple(sorted(_series(table, base, window) - series))
            outside = _count(table, base, chunk.outside_filter())
            properties = _properties(chunk, measured, broken, dropped, sha256)
            with warnings.catch_warnings():
                # A window's first load replaces nothing, and PyIceberg warns about exactly that.
                warnings.filterwarnings("ignore", "Delete operation did not match any records")
                head = wap.write(
                    table, audit, data, overwrite_filter=window, snapshot_properties=properties
                )
            _audit(table, head, data.num_rows, chunk, outside)
            published = wap.publish(table, audit, head)
        except wap.PublishConflict as exc:
            _discard(table, audit)
            if attempt == PUBLISH_ATTEMPTS:
                raise
            log.warning("redata.publish_conflict", run_id=run_id, error=str(exc))
            continue
        except BaseException:
            _discard(table, audit)
            raise
        if dropped:
            log.warning("redata.dropped_series", run_id=run_id, series=list(dropped))
        log.info(
            "redata.published",
            run_id=run_id,
            rows=data.num_rows,
            expected=measured.expected,
            received=measured.received,
            violations=broken,
            snapshot_id=published,
        )
        return ChunkReport(
            chunk, data.num_rows, measured, broken, dropped, published, run_id, landing_path
        )


def _properties(
    chunk: Chunk,
    measured: Completeness,
    broken: dict[str, int],
    dropped: tuple[str, ...],
    sha256: str,
) -> dict[str, str]:
    return {
        "ibergrid.redata.dataset": chunk.dataset.name,
        "ibergrid.redata.system": chunk.system.name,
        "ibergrid.redata.window": f"{chunk.start}/{chunk.end}",
        "ibergrid.redata.expected-points": str(measured.expected),
        "ibergrid.redata.received-points": str(measured.received),
        # Only the days that differ: complete days follow from the window and the totals.
        "ibergrid.redata.incomplete-days": json.dumps(
            {str(day): [expected, received] for day, expected, received in measured.incomplete}
        ),
        "ibergrid.redata.rule-violations": json.dumps(broken),
        "ibergrid.redata.dropped-series": json.dumps(list(dropped)),
        "ibergrid.redata.payload-sha256": sha256,
    }


def _audit(table: Table, head: int | None, rows: int, chunk: Chunk, outside: int) -> None:
    """The branch head holds the validated rows inside the window, each key once, and as many rows
    of the dataset and system outside it as the run's base (``outside``)."""
    if head is None:
        raise AuditFailed("the audit branch has no snapshot to read")
    written = table.scan(
        snapshot_id=head, row_filter=chunk.row_filter(), selected_fields=KEY
    ).to_arrow()
    keys = written.group_by(list(KEY)).aggregate([]).num_rows
    if written.num_rows != rows or keys != rows:
        raise AuditFailed(
            f"snapshot {head} holds {written.num_rows} rows ({keys} distinct keys) in the window, "
            f"expected {rows}"
        )
    kept = _count(table, head, chunk.outside_filter())
    if kept != outside:
        raise AuditFailed(
            f"snapshot {head} holds {kept} rows of {chunk.dataset.name} {chunk.system.name} "
            f"outside the window, {outside} before the run"
        )


def _count(table: Table, snapshot_id: int | None, row_filter: str) -> int:
    if snapshot_id is None:
        return 0
    # One narrow column: the filter's columns are read anyway, the other 15 need not be.
    scan = table.scan(
        snapshot_id=snapshot_id, row_filter=row_filter, selected_fields=("series_id",)
    )
    return scan.count()


def _series(table: Table, snapshot_id: int | None, row_filter: str) -> set[str]:
    if snapshot_id is None:
        return set()
    scan = table.scan(
        snapshot_id=snapshot_id, row_filter=row_filter, selected_fields=("series_id",)
    )
    return set(scan.to_arrow()["series_id"].to_pylist())


def _discard(table: Table, audit: wap.AuditBranch) -> None:
    try:
        wap.discard(table, audit)
    except Exception as exc:  # the original failure matters more; a leftover branch reads nothing
        log.warning("redata.discard_failed", run_id=audit.run_id, branch=audit.name, error=str(exc))
