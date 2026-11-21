"""``redata.demand.realtime.v1`` into ``bronze.redata_demand_stream``, exactly once and without
Kafka transactions (ADR 0006).

Every message is validated against the contract; one that fails goes to the dead-letter topic with
its reason in the headers. An exact duplicate (the key and content hash of the latest accepted
version) is dropped; a revision (same key, another hash) is kept. Accepted events are buffered and
flushed as one Write-Audit-Publish run whose publish also sets the next offset of every partition
in a table property: the rows and the offsets that produced them land in one fenced catalog commit
or not at all (ADR 0002). On start the consumer reads that property, not the consumer group, and
seeks there; the group receives the offsets afterwards for lag visibility only.

So a crash anywhere replays from the last published offsets: before the publish nothing of the
batch is in ``main``; after it, the offsets already point past the batch. Dead letters are flushed
before the publish, so they are at least once. Any failure discards the open run and starts again
from the table's checkpoint, which covers a lost commit race (another writer moved ``main``), a
compaction, an outage of the catalog and a commit whose answer was lost.
"""

import json
import re
import secrets
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import pyarrow as pa
import structlog
from confluent_kafka import (
    OFFSET_BEGINNING,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicPartition,
)
from pydantic import ValidationError
from pyiceberg.table import Table

from ibergrid.lake import wap
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.settings import StreamSettings
from ibergrid.stream.contract import DemandEvent, describe
from ibergrid.stream.health import beat

log = structlog.get_logger(__name__)

ARROW_SCHEMA = REDATA_DEMAND_STREAM.schema.as_arrow()
RUN_PREFIX = "stream-"
# audit-stream-<UTC start, to the microsecond>Z-<hex>: the start time is what tells a dead run's
# branch from a live one, because Iceberg refs carry no creation time (ADR 0002).
_RUN_BRANCH = re.compile(re.escape(wap.BRANCH_PREFIX + RUN_PREFIX) + r"(\d{8}T\d{12})Z-[0-9a-f]+")
PUBLISH_ATTEMPTS = 3
BATCH = 500  # messages per consume call
MAX_BACKOFF = 60.0
BEAT_EVERY = 5.0  # seconds between heartbeats
_BATCH_COUNTS = ("duplicates", "revisions", "dead_letters")


class ConsumerError(RuntimeError):
    """The topic or the table is not in the state the consumer needs."""


class AuditFailed(ConsumerError):
    """The audit branch does not hold exactly the batch: nothing was published."""


class DeadLetterFailed(ConsumerError):
    """The broker did not acknowledge every dead letter: the batch is not published."""


class _Rejected(Exception):
    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Buffered:
    event: DemandEvent
    partition: int
    offset: int
    timestamp: datetime | None


@dataclass(frozen=True)
class FlushReport:
    run_id: str
    rows: int
    duplicates: int
    revisions: int
    dead_letters: int
    offsets: dict[int, int]  # the checkpoint published: next offset per partition
    snapshot_id: int | None
    lag: dict[int, int]
    seconds: float


def offsets_property(group_id: str, topic: str) -> str:
    return f"ibergrid.stream.{group_id}.{topic}.offsets"


def read_checkpoint(table: Table, group_id: str, topic: str) -> dict[int, int]:
    """The next offset per partition that the last publish recorded (empty before the first)."""
    name = offsets_property(group_id, topic)
    raw = table.properties.get(name)
    if raw is None:
        return {}
    try:
        offsets = {int(partition): int(offset) for partition, offset in json.loads(raw).items()}
    except (ValueError, TypeError, AttributeError) as exc:
        raise ConsumerError(
            f"table property {name} is not a JSON object of offsets: {exc}"
        ) from exc
    if any(partition < 0 or offset < 0 for partition, offset in offsets.items()):
        raise ConsumerError(f"table property {name} holds a negative partition or offset: {raw}")
    return offsets


def topic_partitions(kafka: Consumer, topic: str) -> list[int]:
    metadata = kafka.list_topics(topic, timeout=30).topics.get(topic)
    if metadata is None or metadata.error is not None or not metadata.partitions:
        error = None if metadata is None else metadata.error
        raise ConsumerError(f"topic {topic} is not available ({error}): broker-init creates it")
    return sorted(metadata.partitions)


def describe_status(table: Table, kafka: Consumer, settings: StreamSettings) -> list[str]:
    """``ibergrid stream status``: the table, its checkpoint, the lag per partition (high
    watermark minus the checkpoint) and the dead letters the DLQ topic still retains."""
    table.refresh()
    snapshot = table.current_snapshot()
    summary = None if snapshot is None else snapshot.summary
    rows = (summary["total-records"] if summary is not None else None) or "0"
    checkpoint = read_checkpoint(table, settings.group_id, settings.topic)
    name = offsets_property(settings.group_id, settings.topic)
    lines = [
        f"table:        {'.'.join(table.name())}: {rows} rows, snapshot "
        f"{None if snapshot is None else snapshot.snapshot_id}",
        f"checkpoint:   {name} = {table.properties.get(name, 'none')}",
    ]
    total = 0
    for partition in topic_partitions(kafka, settings.topic):
        low, high = kafka.get_watermark_offsets(
            TopicPartition(settings.topic, partition), timeout=10
        )
        position = checkpoint.get(partition)
        lag = max(0, high - (low if position is None else position))
        total += lag
        lines.append(
            f"partition {partition}:  checkpoint {'none' if position is None else position}, "
            f"end {high}, lag {lag}"
        )
    dead = 0
    for partition in topic_partitions(kafka, settings.dlq_topic):
        low, high = kafka.get_watermark_offsets(
            TopicPartition(settings.dlq_topic, partition), timeout=10
        )
        dead += high - low
    lines.append(f"lag:          {total} message(s) behind {settings.topic}")
    lines.append(f"dead letters: {dead} retained in {settings.dlq_topic}")
    return lines


class StreamConsumer:
    def __init__(
        self,
        table: Table,
        kafka: Consumer,
        dlq: Producer,
        settings: StreamSettings,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
        start_offsets: Mapping[int, int] | None = None,
    ) -> None:
        self.table = table
        self.kafka = kafka
        self.dlq = dlq
        self.settings = settings
        self.clock = clock
        self.sleep = sleep
        # Where a partition without a checkpoint starts: the beginning, unless a test says.
        self.start_offsets = dict(start_offsets or {})
        self.property = offsets_property(settings.group_id, settings.topic)
        self.buffer: list[Buffered] = []
        self.latest: dict[tuple[str, datetime], str] = {}  # key -> latest accepted hash
        self.next_offsets: dict[int, int] = {}
        self.checkpointed: dict[int, int] = {}
        self.batch: Counter[str] = Counter()  # since the last flush
        self.totals: Counter[str] = Counter()
        self.first_buffered: float | None = None
        self.last_checkpoint = time.monotonic()
        self.dlq_failures: list[KafkaError] = []

    # --- Start ---------------------------------------------------------------------------------

    def start(self) -> None:
        """Remove dead runs' branches, read the checkpoint, rebuild the duplicate check, seek."""
        self.sweep_stale_branches()
        self.table.refresh()
        self.checkpointed = read_checkpoint(self.table, self.settings.group_id, self.settings.topic)
        self.next_offsets = dict(self.checkpointed)
        self.latest = self._recent_hashes()
        self.buffer.clear()
        self.batch.clear()
        self.first_buffered = None
        partitions = topic_partitions(self.kafka, self.settings.topic)
        start = {
            partition: self.checkpointed.get(
                partition, self.start_offsets.get(partition, OFFSET_BEGINNING)
            )
            for partition in partitions
        }
        self._check_retention(start)
        self.kafka.assign(
            [
                TopicPartition(self.settings.topic, partition, offset)
                for partition, offset in start.items()
            ]
        )
        log.info(
            "stream.consumer.started",
            topic=self.settings.topic,
            checkpoint=self.checkpointed,
            partitions=partitions,
            known_keys=len(self.latest),
        )

    def sweep_stale_branches(self) -> list[str]:
        """Discard this lane's run branches that started longer ago than ``stale_branch_age``:
        a process killed between ``wap.start`` and its publish or discard left them."""
        now = self.clock()
        removed = []
        for name in wap.audit_branches(self.table):
            match = _RUN_BRANCH.fullmatch(name)
            if match is None:
                continue
            started = datetime.strptime(match[1], "%Y%m%dT%H%M%S%f").replace(tzinfo=UTC)
            if (now - started).total_seconds() <= self.settings.stale_branch_age:
                continue
            try:
                gone = wap.discard_branch(self.table, name)
            except wap.WapError as exc:  # one branch that cannot go must not stop the consumer
                log.warning("stream.consumer.sweep_failed", branch=name, error=str(exc))
                continue
            if gone:
                removed.append(name)
                log.info("stream.consumer.swept_branch", branch=name, started=started.isoformat())
        return removed

    def _recent_hashes(self) -> dict[tuple[str, datetime], str]:
        """The latest accepted hash of every key of the last ``dedup_days`` days."""
        if self.table.current_snapshot() is None:
            return {}
        since = self.clock() - timedelta(days=self.settings.dedup_days)
        rows = (
            self.table.scan(
                row_filter=f"ts_utc >= '{since.isoformat()}'",
                selected_fields=(
                    "series_id",
                    "ts_utc",
                    "content_sha256",
                    "ingested_at",
                    "kafka_offset",
                ),
            )
            .to_arrow()
            .sort_by([("ingested_at", "ascending"), ("kafka_offset", "ascending")])
        )
        keys = zip(rows["series_id"].to_pylist(), rows["ts_utc"].to_pylist(), strict=True)
        return dict(zip(keys, rows["content_sha256"].to_pylist(), strict=True))

    def _check_retention(self, start: Mapping[int, int]) -> None:
        for partition, offset in start.items():
            if partition not in self.checkpointed:
                continue
            low, high = self.kafka.get_watermark_offsets(
                TopicPartition(self.settings.topic, partition), timeout=10
            )
            if offset < low:
                log.warning(
                    "stream.consumer.retention_gap",
                    partition=partition,
                    checkpoint=offset,
                    earliest=low,
                    lost=low - offset,
                )
            elif offset > high:
                log.warning(
                    "stream.consumer.checkpoint_ahead",
                    partition=partition,
                    checkpoint=offset,
                    end=high,
                )

    # --- Messages ------------------------------------------------------------------------------

    def handle(self, message: Message) -> None:
        error = message.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            log.warning("stream.consumer.kafka_error", error=str(error))
            return
        partition, offset = cast(int, message.partition()), cast(int, message.offset())
        self.next_offsets[partition] = offset + 1
        self.totals["consumed"] += 1
        try:
            event = self._validate(message)
        except _Rejected as rejected:
            self._dead_letter(message, rejected.reason, rejected.detail)
            return
        key = (event.series_id, event.ts_utc)
        previous = self.latest.get(key)
        if previous == event.content_sha256:
            self.batch["duplicates"] += 1
            return
        if previous is not None:
            self.batch["revisions"] += 1
        self.latest[key] = event.content_sha256
        self.buffer.append(Buffered(event, partition, offset, _timestamp(message)))
        if self.first_buffered is None:
            self.first_buffered = time.monotonic()

    def _validate(self, message: Message) -> DemandEvent:
        value, key = message.value(), message.key()
        if value is None:
            raise _Rejected("no-value", "a message without a value")
        try:
            event = DemandEvent.model_validate_json(value)
        except ValidationError as exc:
            raise _Rejected("contract", describe(exc)) from exc
        if key != event.key.encode():
            raise _Rejected("key", f"the key is {key!r}, the event's is {event.key!r}")
        return event

    def _dead_letter(self, message: Message, reason: str, detail: str) -> None:
        source = f"{message.topic()}/{message.partition()}/{message.offset()}"
        headers: list[tuple[str, str | bytes | None]] = [
            ("ibergrid.dlq.reason", reason),
            ("ibergrid.dlq.detail", detail[:1000]),
            ("ibergrid.dlq.source", source),
            ("ibergrid.dlq.group", self.settings.group_id),
            ("ibergrid.dlq.failed-at", self.clock().isoformat()),
        ]
        self.dlq.produce(
            self.settings.dlq_topic,
            value=message.value(),
            key=message.key(),
            headers=headers,
            on_delivery=self._dead_letter_delivered,
        )
        self.dlq.poll(0)
        self.batch["dead_letters"] += 1
        log.warning(
            "stream.consumer.dead_letter", reason=reason, source=source, detail=detail[:300]
        )

    def _dead_letter_delivered(self, error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            self.dlq_failures.append(error)

    # --- Flushes -------------------------------------------------------------------------------

    def maybe_flush(self, *, final: bool = False) -> FlushReport | None:
        """Flush the rows once the buffer is full or old enough; checkpoint progress without rows
        (duplicates, dead letters) at most every ``checkpoint_interval``, because each checkpoint
        costs three catalog commits and a snapshot (ADR 0002). ``final``: before stopping."""
        now = time.monotonic()
        if self.buffer:
            age = now - cast(float, self.first_buffered)
            if (
                final
                or len(self.buffer) >= self.settings.flush_records
                or age >= self.settings.flush_age
            ):
                return self.flush()
            return None
        if self.next_offsets == self.checkpointed:
            return None
        if not final and now - self.last_checkpoint < self.settings.checkpoint_interval:
            return None
        if not self._main_has_data():
            # A checkpoint needs live data in main (ADR 0002): the first flush carries rows.
            self.last_checkpoint = now
            log.info("stream.consumer.checkpoint_deferred", reason="main holds no data yet")
            return None
        return self.flush()

    def flush(self) -> FlushReport:
        started = time.monotonic()
        now = self.clock()
        offsets = dict(self.next_offsets)
        self._drain_dead_letters()
        run_id = f"{RUN_PREFIX}{now:%Y%m%dT%H%M%S%f}Z-{secrets.token_hex(3)}"
        rows = self._arrow(run_id, now)
        checkpoint = json.dumps({str(p): o for p, o in sorted(offsets.items())})
        audit = wap.start(self.table, run_id)
        try:
            # The fence: publish requires main at the run's base, and here the base must still
            # hold the offsets this consumer resumed from. Otherwise another writer published
            # since, and this batch may hold its messages: start again from its checkpoint.
            current = read_checkpoint(self.table, self.settings.group_id, self.settings.topic)
            if current != self.checkpointed:
                raise wap.PublishConflict(
                    f"the checkpoint moved from {self.checkpointed} to {current} since this "
                    "consumer read it: another writer owns the table"
                )
            if rows.num_rows:
                head = wap.write(
                    self.table, audit, rows, snapshot_properties=self._metrics(rows, checkpoint)
                )
                self._hold("after-write", run_id)
                self._audit(head, run_id, rows.num_rows)
            else:
                head = audit.base_snapshot_id
            published = self._publish(audit, head, {self.property: checkpoint})
        except BaseException:
            self._discard(audit)
            raise
        self._hold("after-publish", run_id)
        counts = {"rows": rows.num_rows, **{k: self.batch[k] for k in _BATCH_COUNTS}}
        self.totals.update(flushes=1, **counts)
        self.batch.clear()
        self.checkpointed = offsets
        self.last_checkpoint = time.monotonic()
        self.buffer.clear()
        self.first_buffered = None
        self._commit_group(offsets)
        self._forget_before(now - timedelta(days=self.settings.dedup_days))
        report = FlushReport(
            run_id=run_id,
            **counts,
            offsets=offsets,
            snapshot_id=published,
            lag=self.lag(offsets),
            seconds=round(time.monotonic() - started, 3),
        )
        log.info(
            "stream.consumer.flushed",
            **{k: v for k, v in report.__dict__.items()},
            total_lag=sum(report.lag.values()),
        )
        return report

    def _drain_dead_letters(self) -> None:
        undelivered = self.dlq.flush(30)
        failures, self.dlq_failures = self.dlq_failures, []
        if undelivered or failures:
            raise DeadLetterFailed(
                f"{undelivered} dead letter(s) undelivered and {len(failures)} refused: "
                + "; ".join(str(error) for error in failures[:3])
            )

    def _arrow(self, run_id: str, ingested_at: datetime) -> pa.Table:
        return pa.Table.from_pylist(
            [
                {
                    "series_id": item.event.series_id,
                    "series_title": item.event.series_title,
                    "electric_system": item.event.electric_system,
                    "ts_utc": item.event.ts_utc,
                    "ts_local": item.event.ts_local,
                    "value": item.event.value,
                    "series_last_update": item.event.series_last_update,
                    "content_sha256": item.event.content_sha256,
                    "producer_instance": item.event.producer.instance,
                    "producer_version": item.event.producer.version,
                    "emitted_at": item.event.producer.emitted_at,
                    "kafka_topic": self.settings.topic,
                    "kafka_partition": item.partition,
                    "kafka_offset": item.offset,
                    "kafka_timestamp": item.timestamp,
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                }
                for item in self.buffer
            ],
            schema=ARROW_SCHEMA,
        )

    def _metrics(self, rows: pa.Table, checkpoint: str) -> dict[str, str]:
        return {
            "ibergrid.stream.rows": str(rows.num_rows),
            "ibergrid.stream.duplicates": str(self.batch["duplicates"]),
            "ibergrid.stream.revisions": str(self.batch["revisions"]),
            "ibergrid.stream.dead-letters": str(self.batch["dead_letters"]),
            "ibergrid.stream.offsets": checkpoint,
        }

    def _audit(self, head: int | None, run_id: str, expected: int) -> None:
        """The branch head holds the batch: one row per message, all of this run."""
        if head is None:
            raise AuditFailed("the audit branch has no snapshot to read")
        written = self.table.scan(
            snapshot_id=head,
            row_filter=f"run_id == '{run_id}'",
            selected_fields=("kafka_partition", "kafka_offset"),
        ).to_arrow()
        distinct = written.group_by(["kafka_partition", "kafka_offset"]).aggregate([]).num_rows
        if written.num_rows != expected or distinct != expected:
            raise AuditFailed(
                f"snapshot {head} holds {written.num_rows} rows ({distinct} distinct messages) of "
                f"run {run_id}, expected {expected}"
            )

    def _publish(
        self, audit: wap.AuditBranch, head: int | None, properties: dict[str, str]
    ) -> int | None:
        for attempt in range(1, PUBLISH_ATTEMPTS):
            try:
                return wap.publish(self.table, audit, head, set_properties=properties)
            except wap.PublishConflict:
                raise
            except wap.WapError as exc:
                # The catalog may have applied the commit and lost the answer, or the checkpoint
                # append may be on the branch: publishing again with the same arguments completes
                # the run or resumes from its own checkpoint snapshot (ADR 0002).
                log.warning("stream.consumer.publish_retry", run_id=audit.run_id, error=str(exc))
                self.sleep(float(attempt))
        return wap.publish(self.table, audit, head, set_properties=properties)

    def _discard(self, audit: wap.AuditBranch) -> None:
        try:
            wap.discard(self.table, audit)
        except Exception as exc:  # the original failure matters more; the sweep removes it later
            log.warning("stream.consumer.discard_failed", run_id=audit.run_id, error=str(exc))

    def _hold(self, point: str, run_id: str) -> None:
        if self.settings.hold_at != point:
            return
        log.warning("stream.consumer.hold", point=point, run_id=run_id)
        while True:
            self.sleep(3600)

    def _commit_group(self, offsets: Mapping[int, int]) -> None:
        if not offsets:
            return
        partitions = [TopicPartition(self.settings.topic, p, o) for p, o in offsets.items()]
        try:
            self.kafka.commit(offsets=partitions, asynchronous=False)
        except KafkaException as exc:  # lag visibility only: the checkpoint is in the table
            log.warning("stream.consumer.group_commit_failed", error=str(exc))

    def lag(self, offsets: Mapping[int, int]) -> dict[int, int]:
        """Messages behind the end of each assigned partition (high watermark minus next offset)."""
        lag = {}
        for partition in sorted(self.next_offsets.keys() | offsets.keys()):
            try:
                low, high = self.kafka.get_watermark_offsets(
                    TopicPartition(self.settings.topic, partition), timeout=5, cached=False
                )
            except KafkaException as exc:
                log.warning("stream.consumer.lag_unknown", partition=partition, error=str(exc))
                continue
            lag[partition] = max(0, high - offsets.get(partition, low))
        return lag

    def _forget_before(self, since: datetime) -> None:
        self.latest = {key: digest for key, digest in self.latest.items() if key[1] >= since}

    def _main_has_data(self) -> bool:
        snapshot = self.table.current_snapshot()
        return snapshot is not None and (
            snapshot.summary is None or snapshot.summary["total-data-files"] != "0"
        )

    # --- Loop ----------------------------------------------------------------------------------

    def run(self, stop: threading.Event) -> None:
        """Consume until ``stop`` is set, then flush what is buffered. A failure discards the open
        run and starts again from the table's checkpoint, with backoff."""
        backoff, started, last_beat = 1.0, False, 0.0
        while not stop.is_set():
            try:
                if not started:
                    self.start()
                    started, backoff = True, 1.0
                for message in self.kafka.consume(BATCH, 1.0):
                    self.handle(message)
                self.maybe_flush()
            except KafkaException as exc:
                if exc.args and isinstance(exc.args[0], KafkaError) and exc.args[0].fatal():
                    raise
                started, backoff = self._failed(exc, stop, backoff)
            except Exception as exc:
                started, backoff = self._failed(exc, stop, backoff)
            if time.monotonic() - last_beat >= BEAT_EVERY:
                beat(self.settings.heartbeat_path, role="consumer", **self.totals)
                last_beat = time.monotonic()
        if started:
            try:
                self.maybe_flush(final=True)
            except Exception as exc:
                log.error("stream.consumer.final_flush_failed", error=str(exc))

    def _failed(self, exc: Exception, stop: threading.Event, backoff: float) -> tuple[bool, float]:
        log.error("stream.consumer.failed", error=f"{type(exc).__name__}: {exc}", retry_in=backoff)
        stop.wait(backoff)
        return False, min(backoff * 2, MAX_BACKOFF)


def _timestamp(message: Message) -> datetime | None:
    kind, milliseconds = message.timestamp()
    if kind == TIMESTAMP_NOT_AVAILABLE:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, UTC)
