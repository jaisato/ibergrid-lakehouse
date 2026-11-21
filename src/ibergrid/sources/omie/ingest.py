"""OMIE into ``bronze.omie_day_ahead_prices``: one Write-Audit-Publish run per chunk (ADR 0004).

A chunk is the delivery days of one calendar month: one download per day, then one run. Every
file is checked against its day's contract before anything is landed or written; then each file
lands at a key named by its day and content, and the rows go to an audit branch replacing exactly
the chunk's delivery days, the branch is read back and published. The audit requires the batch
inside the window (row count and unique keys) and, outside it, as many rows as on the run's base.

A day OMIE has no file for (no version ``.1`` to ``.5``) ends the ingestion where it is: a
delivery day after today (Madrid) has not been published yet, so the days before it are written
and nothing after it is asked for, unless the caller requires publication, which makes it an
error; a missing day up to today is always an error, and the chunk writes nothing. Chunks run in
order and stop at the first failure; the ones already published stay published.
"""

import hashlib
import json
import re
import secrets
import warnings
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import structlog
from pyarrow import fs
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import OMIE_DAY_AHEAD_PRICES
from ibergrid.settings import Settings
from ibergrid.sources.omie.client import OmieClient, OmieError, all_versions
from ibergrid.sources.omie.marginalpdbc import MADRID, DayFile, outside_limits, parse, to_rows
from ibergrid.storage import land, s3_filesystem

log = structlog.get_logger(__name__)

KEY = ("delivery_date", "period")
PUBLISH_ATTEMPTS = 3  # a run that loses the race to another writer restarts on the new main
LANDING_PREFIX = "landing/omie/marginalpdbc"
ARROW_SCHEMA = OMIE_DAY_AHEAD_PRICES.schema.as_arrow()
_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,39}")


class AuditFailed(OmieError):
    """The audit branch does not hold exactly what the run meant to write: nothing was published."""


@dataclass(frozen=True)
class Chunk:
    """Delivery days [start, end], inclusive: one download each, one Write-Audit-Publish run."""

    start: date
    end: date

    @property
    def days(self) -> list[date]:
        return [self.start + timedelta(days=n) for n in range((self.end - self.start).days + 1)]

    @property
    def slug(self) -> str:
        return f"omie-{self.start:%Y%m%d}-{self.end:%Y%m%d}"

    # PyIceberg filter syntax over dates this module formats, so nothing needs escaping.
    def row_filter(self) -> str:
        """Exactly the rows this chunk replaces."""
        return f"delivery_date >= '{self.start}' AND delivery_date <= '{self.end}'"

    def outside_filter(self) -> str:
        """The rows this chunk must leave alone."""
        return f"delivery_date < '{self.start}' OR delivery_date > '{self.end}'"


@dataclass(frozen=True)
class ChunkReport:
    chunk: Chunk  # as planned
    written: Chunk | None  # the days published: the chunk or its start; None if none was out yet
    rows: int
    versions: dict[int, int]  # contract version -> files
    violations: dict[str, int]  # price column -> periods outside the harmonised limits
    not_published_from: date | None  # the first delivery day after today OMIE had no file for
    snapshot_id: int | None  # main's head after the publish
    run_id: str | None


def plan(start: date, end: date) -> list[Chunk]:
    """The chunks for delivery days [start, end]: one per calendar month."""
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    chunks = []
    first = start
    while first <= end:
        month_end = (first.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        chunks.append(Chunk(first, min(end, month_end)))
        first = chunks[-1].end + timedelta(days=1)
    return chunks


def landing_key(prefix: str, day: date, version: int, content_sha256: str) -> str:
    """Outside the warehouse prefix, named by day, content and OMIE's version (the extension): a
    rerun that downloads the same bytes overwrites the same object, a file OMIE changed, in place
    or under a new version, gets its own."""
    stem = f"marginalpdbc_{day:%Y%m%d}-{content_sha256[:12]}"
    return f"{prefix}/{day:%Y}/{day:%m}/{stem}.{version}"


def madrid_today(now: datetime | None = None) -> date:
    """The date in Madrid at ``now`` (default: the system clock)."""
    return (now or datetime.now(UTC)).astimezone(MADRID).date()


def ingest(
    settings: Settings,
    start: date,
    end: date,
    run_id_prefix: str | None = None,
    on_chunk: Callable[[ChunkReport], None] | None = None,
    *,
    table: tuple[str, str] = OMIE_DAY_AHEAD_PRICES.identifier,
    landing_prefix: str = LANDING_PREFIX,
    require_published: bool = False,
) -> list[ChunkReport]:
    """Ingest delivery days [start, end], up to the last one OMIE has published.

    The last report's ``not_published_from`` is the first delivery day OMIE had no file for yet
    (None when every day was out); with ``require_published`` such a day raises ``OmieError``
    instead, after the chunks before it are published. Today (Madrid) is read once, when the call
    starts: a run that crosses Madrid's midnight still judges by the day it started on.

    ``run_id_prefix`` names the runs (an orchestrator's run id, for example); by default a UTC
    timestamp and a random suffix. ``on_chunk`` sees each chunk's report once it is done.
    ``table`` and ``landing_prefix`` exist for tests that must not touch the real ones.
    """
    chunks = plan(start, end)
    prefix = run_id_prefix or f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(2)}"
    if not _PREFIX.fullmatch(prefix):
        raise ValueError(f"run id prefix {prefix!r} must match {_PREFIX.pattern}")
    client = OmieClient.from_settings(settings.omie)
    filesystem = s3_filesystem(settings.storage, timeout=30)
    try:
        target = rest_catalog(settings).load_table(table)
    except NoSuchTableError as exc:
        name = ".".join(table)
        raise OmieError(f"table {name} does not exist: run `ibergrid lake init` first") from exc
    today = madrid_today()
    reports = []
    for chunk in chunks:
        report = ingest_chunk(
            target,
            client,
            filesystem,
            settings.storage.bucket,
            chunk,
            prefix,
            landing_prefix,
            today=today,
            require_published=require_published,
        )
        if on_chunk is not None:
            on_chunk(report)
        reports.append(report)
        if report.not_published_from is not None:
            break  # every later day is later still
    return reports


def ingest_chunk(
    table: Table,
    client: OmieClient,
    filesystem: fs.FileSystem,
    bucket: str,
    chunk: Chunk,
    run_id_prefix: str,
    landing_prefix: str = LANDING_PREFIX,
    *,
    today: date,
    require_published: bool = False,
) -> ChunkReport:
    downloads: list[tuple[DayFile, bytes, datetime]] = []
    not_published = None
    for day in chunk.days:
        found = client.fetch(day)
        if found is None:
            if day <= today:
                raise OmieError(
                    f"OMIE has no {all_versions(day)}: delivery day {day} is not in the future "
                    f"(today is {today} in Madrid), so its results should be out"
                )
            if require_published:
                raise OmieError(
                    f"OMIE has not published {all_versions(day)} yet (delivery day {day}, today "
                    f"is {today} in Madrid) and publication was required"
                )
            not_published = day
            break
        parsed = parse(found.payload, day, found.version)
        downloads.append((parsed, found.payload, datetime.now(UTC)))
    if not downloads:
        log.info(
            "omie.not_published", window=f"{chunk.start}/{chunk.end}", since=str(not_published)
        )
        return ChunkReport(chunk, None, 0, {}, {}, not_published, None, None)
    written = Chunk(chunk.start, downloads[-1][0].day)
    files = [day_file for day_file, _, _ in downloads]
    landed = []
    for day_file, payload, fetched_at in downloads:
        sha256 = hashlib.sha256(payload).hexdigest()
        key = landing_key(landing_prefix, day_file.day, day_file.version, sha256)
        path = land(filesystem, bucket, key, payload, content_type="text/plain; charset=ISO-8859-1")
        landed.append(
            (day_file, {"fetched_at": fetched_at, "content_sha256": sha256, "landing_path": path})
        )
    versions = dict(sorted(Counter(day_file.contract.version for day_file in files).items()))
    broken = outside_limits(files)
    properties = _properties(written, files, versions, broken, not_published)
    attempt = 0
    while True:
        attempt += 1
        # A random part per attempt: a retried task with the same prefix never meets an earlier
        # attempt's branch (one a failed discard left behind, or one already published).
        run_id = f"{run_id_prefix}-{written.slug}-a{attempt}-{secrets.token_hex(2)}"
        batch = pa.Table.from_pylist(
            [row for day_file, meta in landed for row in to_rows(day_file, run_id=run_id, **meta)],
            schema=ARROW_SCHEMA,
        )
        audit = wap.start(table, run_id)
        try:
            outside = _count(table, audit.base_snapshot_id, written.outside_filter())
            with warnings.catch_warnings():
                # A window's first load replaces nothing, and PyIceberg warns about exactly that.
                warnings.filterwarnings("ignore", "Delete operation did not match any records")
                head = wap.write(
                    table,
                    audit,
                    batch,
                    overwrite_filter=written.row_filter(),
                    snapshot_properties=properties,
                )
            _audit(table, head, batch.num_rows, written, outside)
            published = wap.publish(table, audit, head)
        except wap.PublishConflict as exc:
            _discard(table, audit)
            if attempt == PUBLISH_ATTEMPTS:
                raise
            log.warning("omie.publish_conflict", run_id=run_id, error=str(exc))
            continue
        except BaseException:
            _discard(table, audit)
            raise
        log.info(
            "omie.published",
            run_id=run_id,
            window=f"{written.start}/{written.end}",
            rows=batch.num_rows,
            violations=broken,
            not_published_from=str(not_published) if not_published else None,
            snapshot_id=published,
        )
        return ChunkReport(
            chunk, written, batch.num_rows, versions, broken, not_published, published, run_id
        )


def _properties(
    written: Chunk,
    files: list[DayFile],
    versions: dict[int, int],
    broken: dict[str, int],
    not_published: date | None,
) -> dict[str, str]:
    properties = {
        "ibergrid.omie.window": f"{written.start}/{written.end}",
        "ibergrid.omie.files": str(len(files)),
        "ibergrid.omie.periods": str(sum(len(day_file.periods) for day_file in files)),
        "ibergrid.omie.contract-versions": json.dumps({str(v): n for v, n in versions.items()}),
        "ibergrid.omie.rule-violations": json.dumps(broken),
    }
    if not_published is not None:
        properties["ibergrid.omie.not-published-from"] = str(not_published)
    return properties


def _audit(table: Table, head: int | None, rows: int, window: Chunk, outside: int) -> None:
    """The branch head holds the validated rows inside the window, each key once, and as many
    rows outside it as the run's base (``outside``)."""
    if head is None:
        raise AuditFailed("the audit branch has no snapshot to read")
    written = table.scan(
        snapshot_id=head, row_filter=window.row_filter(), selected_fields=KEY
    ).to_arrow()
    keys = written.group_by(list(KEY)).aggregate([]).num_rows
    if written.num_rows != rows or keys != rows:
        raise AuditFailed(
            f"snapshot {head} holds {written.num_rows} rows ({keys} distinct keys) in the window, "
            f"expected {rows}"
        )
    kept = _count(table, head, window.outside_filter())
    if kept != outside:
        raise AuditFailed(
            f"snapshot {head} holds {kept} rows outside {window.start}..{window.end}, "
            f"{outside} before the run"
        )


def _count(table: Table, snapshot_id: int | None, row_filter: str) -> int:
    if snapshot_id is None:
        return 0
    # One narrow column: the filter's column is read anyway, the other 12 need not be.
    return table.scan(
        snapshot_id=snapshot_id, row_filter=row_filter, selected_fields=("period",)
    ).count()


def _discard(table: Table, audit: wap.AuditBranch) -> None:
    try:
        wap.discard(table, audit)
    except Exception as exc:  # the original failure matters more; a leftover branch reads nothing
        log.warning("omie.discard_failed", run_id=audit.run_id, branch=audit.name, error=str(exc))
