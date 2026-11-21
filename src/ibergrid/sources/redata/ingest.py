"""REData into ``bronze.redata_observations``: one Write-Audit-Publish run per chunk (ADR 0003).

Per chunk: fetch, validate (nothing is written on failure), land the raw body at a deterministic
key, write the rows to an audit branch replacing exactly the chunk's dataset, system and UTC
window, read the branch back (row count and key uniqueness inside the window), publish. A rerun of
the same window therefore leaves the same rows; chunks stop at the first failure, and the ones
already published stay published.
"""

import hashlib
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
    granularities,
    parse,
    to_arrow,
)
from ibergrid.storage import s3_filesystem

log = structlog.get_logger(__name__)

KEY = ("dataset", "electric_system", "series_id", "ts_utc")
PUBLISH_ATTEMPTS = 3  # a run that loses the race to another writer restarts on the new main
_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,39}")


class AuditFailed(RedataError):
    """The audit branch does not hold exactly the validated rows: nothing was published."""


@dataclass(frozen=True)
class ChunkReport:
    chunk: Chunk
    rows: int
    completeness: Completeness
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
) -> list[ChunkReport]:
    """Ingest local days [start, end] of ``dataset`` for ``systems`` (all when empty).

    ``run_id_prefix`` names the runs (an orchestrator's run id, for example); by default a UTC
    timestamp and a random suffix. ``on_chunk`` sees each chunk's report once it is published.
    """
    chunks = plan(dataset_named(dataset), start, end, tuple(systems))
    prefix = run_id_prefix or f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(2)}"
    if not _PREFIX.fullmatch(prefix):
        raise ValueError(f"run id prefix {prefix!r} must match {_PREFIX.pattern}")
    client = RedataClient.from_settings(settings.redata)
    filesystem = s3_filesystem(settings.storage, timeout=30)
    try:
        table = rest_catalog(settings).load_table(REDATA_OBSERVATIONS.identifier)
    except NoSuchTableError as exc:
        name = ".".join(REDATA_OBSERVATIONS.identifier)
        raise RedataError(f"table {name} does not exist: run `ibergrid lake init` first") from exc
    reports = []
    for chunk in chunks:
        report = ingest_chunk(table, client, filesystem, settings.storage.bucket, chunk, prefix)
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
) -> ChunkReport:
    payload = client.fetch(chunk)
    fetched_at = datetime.now(UTC)
    points = parse(payload, chunk)
    grains = granularities(points, chunk)
    measured = completeness(points, grains, chunk)
    sha256 = hashlib.sha256(payload).hexdigest()
    landing_path = land(filesystem, bucket, chunk.landing_key, payload)
    window = chunk.row_filter()
    properties = {
        "ibergrid.redata.dataset": chunk.dataset.name,
        "ibergrid.redata.system": chunk.system.name,
        "ibergrid.redata.window": f"{chunk.start}/{chunk.end}",
        "ibergrid.redata.expected-points": str(measured.expected),
        "ibergrid.redata.received-points": str(measured.received),
        # Only the days that differ: complete days follow from the window and the totals.
        "ibergrid.redata.incomplete-days": json.dumps(
            {str(day): [expected, received] for day, expected, received in measured.incomplete}
        ),
        "ibergrid.redata.payload-sha256": sha256,
    }
    attempt = 0
    while True:
        attempt += 1
        run_id = f"{run_id_prefix}-{chunk.slug}-a{attempt}"
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
            with warnings.catch_warnings():
                # A window's first load replaces nothing, and PyIceberg warns about exactly that.
                warnings.filterwarnings("ignore", "Delete operation did not match any records")
                head = wap.write(
                    table, audit, data, overwrite_filter=window, snapshot_properties=properties
                )
            _audit(table, head, data.num_rows, window)
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
        log.info(
            "redata.published",
            run_id=run_id,
            rows=data.num_rows,
            expected=measured.expected,
            received=measured.received,
            snapshot_id=published,
        )
        return ChunkReport(chunk, data.num_rows, measured, published, run_id, landing_path)


def land(filesystem: fs.FileSystem, bucket: str, key: str, payload: bytes) -> str:
    """Write the raw body at ``<bucket>/<key>``, replacing any earlier one; returns its URI."""
    path = f"{bucket}/{key}"
    if filesystem.type_name != "s3":  # a local disk (tests, a laptop) needs the directories
        filesystem.create_dir(path.rpartition("/")[0], recursive=True)
    with filesystem.open_output_stream(path, metadata={"Content-Type": "application/json"}) as out:
        out.write(payload)
    return f"s3://{path}"


def _audit(table: Table, head: int | None, rows: int, window: str) -> None:
    """The branch head holds exactly the validated rows inside the window, each key once."""
    if head is None:
        raise AuditFailed("the audit branch has no snapshot to read")
    written = table.scan(snapshot_id=head, row_filter=window, selected_fields=KEY).to_arrow()
    keys = written.group_by(list(KEY)).aggregate([]).num_rows
    if written.num_rows != rows or keys != rows:
        raise AuditFailed(
            f"snapshot {head} holds {written.num_rows} rows ({keys} distinct keys) in the window, "
            f"expected {rows}"
        )


def _discard(table: Table, audit: wap.AuditBranch) -> None:
    try:
        wap.discard(table, audit)
    except Exception as exc:  # the original failure matters more; a leftover branch reads nothing
        log.warning("redata.discard_failed", run_id=audit.run_id, branch=audit.name, error=str(exc))
