"""``redata.demand.realtime.v1`` into ``bronze.redata_demand_stream``, exactly once and without
Kafka transactions (ADR 0006).

Every message is validated against the contract; one that fails goes to the dead-letter topic with
its reason in the headers. An exact duplicate (the key and content hash of the latest accepted
version) is dropped; a revision (same key, another hash) is kept. Accepted events are buffered and
flushed as one Write-Audit-Publish run whose publish also sets the checkpoint in a table property:
the topic's identity and the next offset of every partition. The rows and the offsets that
produced them land in one fenced catalog commit or not at all (ADR 0002).

On start the consumer reads that property, not the consumer group, and seeks there; the group
receives the offsets afterwards for lag visibility only. It refuses to start on a topic that
changed under the checkpoint (recreated: another topic id; or a log shorter than the checkpoint),
because those offsets would name other messages; ``reset_checkpoint`` is the deliberate way out.
A checkpoint older than the topic's retention starts at the earliest message left, with a warning:
that gap is lost to the stream (the batch lane still has the days).

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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, cast

import pyarrow as pa
import pyarrow.compute as pc
import structlog
from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from pydantic import ValidationError
from pyiceberg.table import Table

from ibergrid.lake import maintenance, wap
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.settings import StreamSettings
from ibergrid.stream.contract import DemandEvent, describe
from ibergrid.stream.health import Watchdog, beat

log = structlog.get_logger(__name__)

ARROW_SCHEMA = REDATA_DEMAND_STREAM.schema.as_arrow()
CHECKPOINT_PREFIX = "ibergrid.stream.checkpoint."
RUN_PREFIX = "stream-"
# audit-stream-<UTC start, to the microsecond>Z-<hex>: the start time is what tells a dead run's
# branch from a live one, because Iceberg refs carry no creation time (ADR 0002).
_RUN_BRANCH = re.compile(re.escape(wap.BRANCH_PREFIX + RUN_PREFIX) + r"(\d{8}T\d{12})Z-[0-9a-f]+")
PUBLISH_ATTEMPTS = 3
BATCH = 500  # messages per consume call
MAX_BACKOFF = 60.0
BEAT_EVERY = 5.0  # seconds between heartbeats
EXPIRE_EVERY = 86_400.0  # seconds between snapshot expiries
PROBE_EVERY = 60.0  # seconds between checks that the broker answers and the topic is the same
VERIFY_TIMEOUT = 30.0  # seconds to read back the message before each checkpointed offset
RETAIN_SNAPSHOTS = 50  # main's latest snapshots that expiry always keeps
HOLD_POINTS = ("after-write", "after-publish")
_BATCH_COUNTS = ("duplicates", "revisions", "dead_letters")
# Errors after which reading on would be wrong or pointless: start again from the table, whose
# checks refuse a topic that changed under the checkpoint and fail while access is denied.
_RESTART_ON = frozenset(
    {
        KafkaError._AUTO_OFFSET_RESET,
        KafkaError._UNKNOWN_PARTITION,
        KafkaError._UNKNOWN_TOPIC,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
        KafkaError.SASL_AUTHENTICATION_FAILED,
    }
)


class ConsumerError(RuntimeError):
    """The topic or the table is not in the state the consumer needs."""


class Refused(ConsumerError):
    """The table's checkpoint does not fit the topic: starting would duplicate or skip messages.
    Only an operator's ``ibergrid stream reset`` (or ``stream drop-property``) moves on."""


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
class Checkpoint:
    """What a publish records: the topic incarnation it read, the reset epoch (each ``stream
    reset`` adds one, so rows order by (epoch, offset) even when a reset reuses offsets), the next
    offset per partition and the timestamp of the message before it, which the next start reads
    back to prove the log was not rewritten under the same topic id."""

    topic: str
    topic_id: str
    offsets: dict[int, int]
    epoch: int = 0
    timestamps: dict[int, int] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "topic": self.topic,
                "topic_id": self.topic_id,
                "epoch": self.epoch,
                "offsets": {str(p): o for p, o in sorted(self.offsets.items())},
                "timestamps": {str(p): t for p, t in sorted(self.timestamps.items())},
            }
        )


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


def checkpoint_property(group_id: str) -> str:
    return CHECKPOINT_PREFIX + group_id


def read_checkpoint(table: Table, group_id: str) -> Checkpoint | None:
    """The checkpoint the last publish of ``group_id`` recorded (None before the first)."""
    name = checkpoint_property(group_id)
    raw = table.properties.get(name)
    if raw is None:
        return None
    try:
        value = json.loads(raw)
        offsets = {int(p): int(o) for p, o in value["offsets"].items()}
        timestamps = {int(p): int(t) for p, t in value.get("timestamps", {}).items()}
        checkpoint = Checkpoint(
            str(value["topic"]),
            str(value["topic_id"]),
            offsets,
            int(value.get("epoch", 0)),
            timestamps,
        )
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ConsumerError(
            f"table property {name} is not a checkpoint {{topic, topic_id, offsets}}: {exc}"
        ) from exc
    if any(partition < 0 or offset < 0 for partition, offset in offsets.items()):
        raise ConsumerError(f"table property {name} holds a negative partition or offset: {raw}")
    return checkpoint


def topic_identity(admin: AdminClient, topic: str) -> str:
    """The topic's id, which a deleted and recreated topic of the same name does not share."""
    try:
        found = admin.describe_topics(TopicCollection([topic]), request_timeout=30)[topic]
        return str(found.result(timeout=30).topic_id)
    except (KafkaException, TimeoutError) as exc:
        detail = str(exc) or type(exc).__name__
        raise ConsumerError(
            f"topic {topic} cannot be described ({detail}): broker-init creates it"
        ) from exc


def _highest_epoch(table: Table) -> int | None:
    """The highest stream epoch in the table's checkpoints or rows (None when empty and none
    recorded; rows written before epochs existed count as 0)."""
    highest: int | None = None
    for key, raw in table.properties.items():
        if not key.startswith(CHECKPOINT_PREFIX):
            continue
        try:
            epoch = int(json.loads(raw).get("epoch", 0))
        except (ValueError, TypeError, AttributeError):
            continue
        highest = epoch if highest is None else max(highest, epoch)
    if table.current_snapshot() is None:
        return highest
    scan = table.scan(selected_fields=("stream_epoch",))
    for batch in scan.to_arrow_batch_reader():
        if not batch.num_rows:
            continue
        top = int(pc.max(pc.fill_null(batch.column("stream_epoch"), 0)).as_py())
        highest = top if highest is None else max(highest, top)
    return highest


def topic_partitions(kafka: Consumer, topic: str) -> list[int]:
    metadata = kafka.list_topics(topic, timeout=30).topics.get(topic)
    if metadata is None or metadata.error is not None or not metadata.partitions:
        error = None if metadata is None else metadata.error
        raise ConsumerError(f"topic {topic} is not available ({error}): broker-init creates it")
    return sorted(metadata.partitions)


def new_run_id(now: datetime) -> str:
    return f"{RUN_PREFIX}{now.astimezone(UTC):%Y%m%dT%H%M%S%f}Z-{secrets.token_hex(3)}"


def reset_checkpoint(
    table: Table,
    kafka: Consumer,
    admin: AdminClient,
    settings: StreamSettings,
    position: Literal["earliest", "latest"],
    *,
    force: bool = False,
) -> Checkpoint:
    """Point the checkpoint at the topic as it is now (``ibergrid stream reset``), deliberately:
    after the topic was recreated or lost its tail, when the consumer refuses to start. From the
    earliest message left (what the topic retains is written again) or from the end (what is in
    the topic is skipped). The epoch goes up by one, so the rows written afterwards order after
    every earlier one, also where offsets repeat. Published like any checkpoint, so a running
    consumer conflicts instead of interleaving: stop it first. A checkpoint that still fits the
    topic (same id, offsets inside the log) is refused unless ``force``.
    """
    table.refresh()
    topic = settings.topic
    before = read_checkpoint(table, settings.group_id)
    topic_id = topic_identity(admin, topic)
    offsets, fits = {}, before is not None and (before.topic, before.topic_id) == (topic, topic_id)
    for partition in topic_partitions(kafka, topic):
        low, high = kafka.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        offsets[partition] = low if position == "earliest" else high
        if before is not None and not low <= before.offsets.get(partition, low) <= high:
            fits = False
    if fits and not force:
        raise ConsumerError(
            f"the checkpoint {cast(Checkpoint, before).to_json()} fits {topic} as it is: "
            "nothing to reset (--force to reset anyway)"
        )
    highest = _highest_epoch(table)
    epoch = (0 if highest is None else highest) + 1
    checkpoint = Checkpoint(topic, topic_id, offsets, epoch)
    _publish_properties(
        table, set_properties={checkpoint_property(settings.group_id): checkpoint.to_json()}
    )
    log.warning("stream.consumer.checkpoint_reset", position=position, checkpoint=checkpoint)
    return checkpoint


def drop_property(
    table: Table, settings: StreamSettings, name: str, *, force: bool = False
) -> None:
    """Remove a stream property from the table (``ibergrid stream drop-property``) through the
    fenced publish: another group's checkpoint after a deliberate migration, or a property an
    earlier version left. This consumer's own checkpoint only with ``force``."""
    if not name.startswith("ibergrid.stream."):
        raise ConsumerError(f"{name} is not a stream property (ibergrid.stream.*)")
    if name == checkpoint_property(settings.group_id) and not force:
        raise ConsumerError(f"{name} is this consumer's checkpoint (--force to drop it anyway)")
    table.refresh()
    if name not in table.properties:
        raise ConsumerError(f"{'.'.join(table.name())} has no property {name}")
    _publish_properties(table, remove_properties=[name])
    log.warning("stream.consumer.property_dropped", property=name)


def _publish_properties(
    table: Table,
    *,
    set_properties: dict[str, str] | None = None,
    remove_properties: tuple[str, ...] | list[str] = (),
) -> None:
    audit = wap.start(table, new_run_id(datetime.now(UTC)))
    try:
        wap.publish(
            table,
            audit,
            audit.base_snapshot_id,
            set_properties=set_properties,
            remove_properties=remove_properties,
        )
    except BaseException:
        wap.discard(table, audit)
        raise


def describe_status(
    table: Table, kafka: Consumer, admin: AdminClient, settings: StreamSettings
) -> list[str]:
    """``ibergrid stream status``: the table, its checkpoint and whether it is the topic's, the lag
    per partition (high watermark minus the checkpoint) and the dead letters retained."""
    table.refresh()
    snapshot = table.current_snapshot()
    summary = None if snapshot is None else snapshot.summary
    rows = (summary["total-records"] if summary is not None else None) or "0"
    checkpoint = read_checkpoint(table, settings.group_id)
    name = checkpoint_property(settings.group_id)
    topic_id = topic_identity(admin, settings.topic)
    mismatch = checkpoint is not None and checkpoint.topic_id != topic_id
    lines = [
        f"table:        {'.'.join(table.name())}: {rows} rows, snapshot "
        f"{None if snapshot is None else snapshot.snapshot_id}",
        f"checkpoint:   {name} = {table.properties.get(name, 'none')}",
        f"topic:        {settings.topic} {topic_id}"
        + (" (NOT the checkpoint's: the consumer refuses to start)" if mismatch else ""),
    ]
    total = 0
    offsets = {} if checkpoint is None or mismatch else checkpoint.offsets
    for partition in topic_partitions(kafka, settings.topic):
        low, high = kafka.get_watermark_offsets(
            TopicPartition(settings.topic, partition), timeout=10
        )
        position = offsets.get(partition)
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
        admin: AdminClient,
        settings: StreamSettings,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
        allow_other_checkpoints: bool = False,
        hold_at: str | None = None,
    ) -> None:
        if hold_at not in (None, *HOLD_POINTS):
            raise ValueError(f"hold_at must be one of {HOLD_POINTS}, not {hold_at!r}")
        self.table = table
        self.kafka = kafka
        self.dlq = dlq
        self.admin = admin
        self.settings = settings
        self.clock = clock
        self.sleep = sleep
        # A deliberate migration from another consumer group's checkpoint on the same table.
        self.allow_other_checkpoints = allow_other_checkpoints
        # Tests only (make chaos-stream): stop for good at this point of a flush, for a SIGKILL.
        self.hold_at = hold_at
        self.property = checkpoint_property(settings.group_id)
        self.topic_id: str | None = None
        self.epoch = 0
        self.checkpointed: Checkpoint | None = None
        self.next_timestamps: dict[int, int] = {}  # of the message before each next offset
        self.buffer: list[Buffered] = []
        self.latest: dict[tuple[str, datetime], str] = {}  # key -> latest accepted hash
        self.assigned: dict[int, int] = {}  # current fetch position per partition
        self.next_offsets: dict[int, int] = {}
        self.batch: Counter[str] = Counter()  # since the last flush
        self.totals: Counter[str] = Counter()
        self.first_buffered: float | None = None
        self.last_checkpoint = time.monotonic()
        self.last_sweep = clock()
        self.next_expiry = 0.0  # monotonic: the first loop expires
        self.failures = 0  # consecutive failed iterations
        self.failing_since: float | None = None  # epoch seconds of the first of them
        self.last_flush_at: float | None = None
        self.refused: str | None = None  # why the last start refused, until one succeeds
        self.last_probe = time.monotonic()
        self.dlq_failures: list[KafkaError] = []

    # --- Start ---------------------------------------------------------------------------------

    def start(self) -> None:
        """Remove dead runs' branches, check and read the checkpoint, rebuild the duplicate check,
        seek. ``Refused`` when the checkpoint does not fit the topic."""
        self.sweep_stale_branches()
        self.table.refresh()
        topic = self.settings.topic
        others = sorted(
            key
            for key in self.table.properties
            if key.startswith(CHECKPOINT_PREFIX) and key != self.property
        )
        if others and not self.allow_other_checkpoints:
            raise Refused(
                f"{'.'.join(self.table.name())} also holds {', '.join(others)}: another consumer "
                "group writes this table, and one table has one writer (for a deliberate "
                "migration: ibergrid stream consume --allow-other-checkpoints, then "
                "ibergrid stream drop-property)"
            )
        checkpoint = read_checkpoint(self.table, self.settings.group_id)
        topic_id = topic_identity(self.admin, topic)
        if checkpoint is not None and checkpoint.topic != topic:
            raise Refused(
                f"the checkpoint is for topic {checkpoint.topic}, and this consumer is configured "
                f"for {topic} (IBERGRID_STREAM_TOPIC). Reset deliberately: ibergrid stream reset"
            )
        if checkpoint is not None and checkpoint.topic_id != topic_id:
            raise Refused(
                f"the checkpoint was taken on {topic} {checkpoint.topic_id}, the topic is now "
                f"{topic_id}: it was deleted and recreated, so the offsets name other messages. "
                "Reset deliberately: ibergrid stream reset --to earliest|latest"
            )
        offsets = {} if checkpoint is None else checkpoint.offsets
        timestamps = {} if checkpoint is None else dict(checkpoint.timestamps)
        start, lows = {}, {}
        for partition in topic_partitions(self.kafka, topic):
            low, high = self.kafka.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            lows[partition] = low
            position = offsets.get(partition)
            if position is None:
                start[partition] = low
            elif position > high:
                raise Refused(
                    f"the checkpoint of {topic} partition {partition} is at {position}, past its "
                    f"end ({high}): messages it covers are gone (a broker that lost the tail of "
                    "its log). Reset deliberately: ibergrid stream reset --to earliest|latest"
                )
            elif position < low:
                log.warning(
                    "stream.consumer.retention_gap",
                    partition=partition,
                    checkpoint=position,
                    earliest=low,
                    lost=low - position,
                )
                start[partition] = low
                timestamps.pop(partition, None)
            else:
                start[partition] = position
        self._verify_positions(offsets, timestamps, lows)
        self.checkpointed, self.topic_id = checkpoint, topic_id
        if checkpoint is not None:
            self.epoch = checkpoint.epoch
        else:
            highest = _highest_epoch(self.table)
            self.epoch = 0 if highest is None else highest + 1
        # Past a retention gap the next checkpoint records the jump.
        self.assigned = dict(start)
        self.next_offsets = {p: start[p] for p in offsets if p in start}
        self.next_timestamps = {p: t for p, t in timestamps.items() if p in self.next_offsets}
        self.latest = self._recent_hashes()
        self.buffer.clear()
        self.batch.clear()
        self.first_buffered = None
        self.kafka.assign([TopicPartition(topic, p, offset) for p, offset in start.items()])
        self.refused = None
        self.last_probe = time.monotonic()
        log.info(
            "stream.consumer.started",
            topic=topic,
            topic_id=topic_id,
            epoch=self.epoch,
            checkpoint=offsets,
            start=start,
            known_keys=len(self.latest),
        )

    def _verify_positions(
        self, offsets: dict[int, int], timestamps: dict[int, int], lows: dict[int, int]
    ) -> None:
        """Read back the message before each checkpointed offset: its timestamp must be the one
        the checkpoint recorded. Otherwise the log was rewritten under the same topic id (a lost
        tail that grew back past the checkpoint), and the offsets name other messages."""
        expected = {
            p: t for p, t in timestamps.items() if p in offsets and offsets[p] - 1 >= lows.get(p, 0)
        }
        if not expected:
            return
        topic = self.settings.topic
        remaining = dict(expected)
        self.kafka.assign([TopicPartition(topic, p, offsets[p] - 1) for p in remaining])
        found: dict[int, tuple[int, int]] = {}
        deadline = time.monotonic() + VERIFY_TIMEOUT
        while remaining and time.monotonic() < deadline:
            got_any = False
            for message in self.kafka.consume(len(remaining), 1.0):
                partition = message.partition()
                if message.error() is None and partition in remaining:
                    found[partition] = (cast(int, message.offset()), message.timestamp()[1])
                    remaining.pop(partition)
                    got_any = True
            if got_any and remaining:
                self.kafka.assign([TopicPartition(topic, p, offsets[p] - 1) for p in remaining])
        missing = sorted(remaining)
        if missing:
            raise ConsumerError(f"could not read back the checkpoint of partitions {missing}")
        for partition, recorded in expected.items():
            offset, timestamp = found[partition]
            want = offsets[partition] - 1
            if offset != want:
                log.warning(
                    "stream.consumer.verify_offset_skipped",
                    partition=partition,
                    expected_offset=want,
                    actual_offset=offset,
                )
                continue
            if timestamp != recorded:
                raise Refused(
                    f"{topic} partition {partition}: the message before the checkpoint (offset "
                    f"{want}) has timestamp {timestamp}, the checkpoint "
                    f"recorded {recorded}: the log was rewritten under the same topic id. Reset "
                    "deliberately: ibergrid stream reset --to earliest|latest --force"
                )

    def _verify_previous_checkpoint(self) -> None:
        """Before advancing the checkpoint, verify the previous one's log fingerprint: a tail
        lost and refilled past this consumer's position while it runs keeps the topic id and
        gives no out-of-range error, so without this read-back the publish would move past the
        rewritten messages."""
        if self.checkpointed is None or not self.checkpointed.offsets:
            return
        topic = self.settings.topic
        lows: dict[int, int] = {}
        for partition, position in self.checkpointed.offsets.items():
            low, high = self.kafka.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            if position > high:
                raise Refused(
                    f"the checkpoint of {topic} partition {partition} is at {position}, past its "
                    f"end ({high}): messages it covers are gone (a broker that lost the tail of "
                    "its log). Reset deliberately: ibergrid stream reset --to earliest|latest"
                )
            lows[partition] = low
        expected = {
            p: t
            for p, t in self.checkpointed.timestamps.items()
            if p in lows and self.checkpointed.offsets[p] - 1 >= lows[p]
        }
        if not expected:
            return
        try:
            self._verify_positions(self.checkpointed.offsets, expected, lows)
        finally:
            self.kafka.assign([TopicPartition(topic, p, o) for p, o in self.assigned.items()])

    def sweep_stale_branches(self) -> list[str]:
        """Discard this lane's run branches that started longer ago than ``stale_branch_age``:
        a process killed between ``wap.start`` and its publish or discard left them."""
        now = self.last_sweep = self.clock()
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
        """The latest accepted hash of every key of the last ``dedup_days`` days: the highest
        (epoch, offset), since every version of a key is in the same partition within an epoch and
        a reset starts a later epoch (rows before epochs existed count as epoch 0). Read batch by
        batch and reduced to one row per key in Arrow, so memory follows the number of keys (a
        few thousand), not of rows (revisions: tens of thousands a day)."""
        if self.table.current_snapshot() is None:
            return {}
        since = self.clock() - timedelta(days=self.settings.dedup_days)
        scan = self.table.scan(
            row_filter=f"ts_utc >= '{since.isoformat()}'",
            selected_fields=(
                "series_id",
                "ts_utc",
                "stream_epoch",
                "kafka_offset",
                "content_sha256",
            ),
        )
        latest: dict[tuple[str, datetime], tuple[tuple[int, int], str]] = {}
        for batch in scan.to_arrow_batch_reader():
            if not batch.num_rows:
                continue
            rows = pa.Table.from_batches([batch])
            rows = rows.set_column(
                rows.schema.get_field_index("stream_epoch"),
                "stream_epoch",
                pc.fill_null(rows["stream_epoch"], 0),
            )
            reduced = (
                rows.sort_by([("stream_epoch", "descending"), ("kafka_offset", "descending")])
                .group_by(["series_id", "ts_utc"], use_threads=False)
                .aggregate(
                    [
                        ("stream_epoch", "first"),
                        ("kafka_offset", "first"),
                        ("content_sha256", "first"),
                    ]
                )
            )
            columns = (
                "series_id",
                "ts_utc",
                "stream_epoch_first",
                "kafka_offset_first",
                "content_sha256_first",
            )
            for series, ts, epoch, offset, digest in zip(
                *(reduced[column].to_pylist() for column in columns), strict=True
            ):
                order = (epoch, offset)
                if (series, ts) not in latest or latest[(series, ts)][0] < order:
                    latest[(series, ts)] = (order, digest)
        return {key: digest for key, (_, digest) in latest.items()}

    # --- Messages ------------------------------------------------------------------------------

    def handle(self, message: Message) -> None:
        error = message.error()
        if error is not None:
            if error.fatal():
                raise KafkaException(error)
            if error.code() in _RESTART_ON:
                raise ConsumerError(f"{error}: starting again from the table's checkpoint")
            log.warning("stream.consumer.kafka_error", error=str(error))
            return
        partition, offset = cast(int, message.partition()), cast(int, message.offset())
        self.assigned[partition] = offset + 1
        self.next_offsets[partition] = offset + 1
        self.next_timestamps[partition] = message.timestamp()[1]
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
        (duplicates, dead letters, a retention gap) at most every ``checkpoint_interval``, because
        each checkpoint costs three catalog commits and a snapshot (ADR 0002). ``final``: before
        stopping."""
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
        checkpointed = {} if self.checkpointed is None else self.checkpointed.offsets
        if self.next_offsets == checkpointed:
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
        self._drain_dead_letters()
        run_id = new_run_id(now)
        rows = self._arrow(run_id, now)
        checkpoint = Checkpoint(
            self.settings.topic,
            cast(str, self.topic_id),
            dict(self.next_offsets),
            self.epoch,
            dict(self.next_timestamps),
        )
        audit = wap.start(self.table, run_id)
        try:
            # The fence: publish requires main at the run's base, and here the base must still
            # hold the checkpoint this consumer resumed from. Otherwise another writer published
            # since, and this batch may hold its messages: start again from its checkpoint.
            current = read_checkpoint(self.table, self.settings.group_id)
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
                self._audit(head, run_id, rows)
            else:
                head = audit.base_snapshot_id
            # A topic recreated or a log rewritten and refilled past this consumer's position
            # gives no out-of-range error: its messages are read as if the old log went on.
            self._check_topic()
            self._verify_previous_checkpoint()
            published = self._publish(audit, head, {self.property: checkpoint.to_json()})
        except BaseException:
            self._discard(audit)
            raise
        self._hold("after-publish", run_id)
        counts = {"rows": rows.num_rows, **{k: self.batch[k] for k in _BATCH_COUNTS}}
        self.totals.update(flushes=1, **counts)
        self.batch.clear()
        self.checkpointed = checkpoint
        self.last_checkpoint = time.monotonic()
        self.last_flush_at = time.time()
        self.buffer.clear()
        self.first_buffered = None
        self._commit_group(checkpoint.offsets)
        self._forget_before(now - timedelta(days=self.settings.dedup_days))
        report = FlushReport(
            run_id=run_id,
            **counts,
            offsets=checkpoint.offsets,
            snapshot_id=published,
            lag=self.lag(checkpoint.offsets),
            seconds=round(time.monotonic() - started, 3),
        )
        log.info("stream.consumer.flushed", **report.__dict__, total_lag=sum(report.lag.values()))
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
                    "kafka_topic_id": self.topic_id,
                    "stream_epoch": self.epoch,
                }
                for item in self.buffer
            ],
            schema=ARROW_SCHEMA,
        )

    def _metrics(self, rows: pa.Table, checkpoint: Checkpoint) -> dict[str, str]:
        return {
            "ibergrid.stream.rows": str(rows.num_rows),
            "ibergrid.stream.duplicates": str(self.batch["duplicates"]),
            "ibergrid.stream.revisions": str(self.batch["revisions"]),
            "ibergrid.stream.dead-letters": str(self.batch["dead_letters"]),
            "ibergrid.stream.checkpoint": checkpoint.to_json(),
        }

    def _audit(self, head: int | None, run_id: str, rows: pa.Table) -> None:
        """The branch head holds the batch: one row per message, all of this run. The batch's
        instants bound the scan, so planning prunes the other months' manifests and files."""
        if head is None:
            raise AuditFailed("the audit branch has no snapshot to read")
        bounds = pc.min_max(rows["ts_utc"]).as_py()
        first, last = bounds["min"].isoformat(), bounds["max"].isoformat()
        written = self.table.scan(
            snapshot_id=head,
            row_filter=f"run_id == '{run_id}' AND ts_utc >= '{first}' AND ts_utc <= '{last}'",
            selected_fields=("kafka_partition", "kafka_offset"),
        ).to_arrow()
        distinct = written.group_by(["kafka_partition", "kafka_offset"]).aggregate([]).num_rows
        if written.num_rows != rows.num_rows or distinct != rows.num_rows:
            raise AuditFailed(
                f"snapshot {head} holds {written.num_rows} rows ({distinct} distinct messages) of "
                f"run {run_id}, expected {rows.num_rows}"
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
        if self.hold_at != point:
            return
        log.warning("stream.consumer.hold", point=point, run_id=run_id)
        while True:
            self.sleep(3600)

    def _commit_group(self, offsets: dict[int, int]) -> None:
        if not offsets:
            return
        partitions = [TopicPartition(self.settings.topic, p, o) for p, o in offsets.items()]
        try:
            self.kafka.commit(offsets=partitions, asynchronous=False)
        except KafkaException as exc:  # lag visibility only: the checkpoint is in the table
            log.warning("stream.consumer.group_commit_failed", error=str(exc))

    def lag(self, offsets: dict[int, int]) -> dict[int, int]:
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

    def expire_snapshots(self) -> list[int]:
        """Expire this table's snapshots older than ``snapshot_retention_days``, keeping main's
        latest ``RETAIN_SNAPSHOTS`` and the lineage of any open branch; the checkpoint is a table
        property, which expiry leaves alone. Compaction is phase 4's."""
        older_than = self.clock() - timedelta(days=self.settings.snapshot_retention_days)
        expired = maintenance.expire_snapshots(self.table, older_than, RETAIN_SNAPSHOTS)
        log.info("stream.consumer.expired_snapshots", expired=len(expired))
        return expired

    # --- Loop ----------------------------------------------------------------------------------

    def run(self, stop: threading.Event, watchdog: Watchdog | None = None) -> None:
        """Consume until ``stop`` is set, then flush what is buffered. A failure discards the open
        run and starts again from the table's checkpoint, with backoff; stale branches are swept
        every ``stale_branch_age`` and snapshots expired once a day."""
        backoff, started, last_beat = 1.0, False, 0.0
        while not stop.is_set():
            if watchdog is not None:
                watchdog.touch()
            try:
                if not started:
                    self.start()
                    started = True
                for message in self.kafka.consume(BATCH, 1.0):
                    self.handle(message)
                self.maybe_flush()
                self._housekeeping()
            except KafkaException as exc:
                if exc.args and isinstance(exc.args[0], KafkaError) and exc.args[0].fatal():
                    raise
                started, backoff = self._failed(exc, stop, backoff)
            except Refused as exc:
                self.refused = str(exc)  # unhealthy at once: only an operator moves on
                started, backoff = self._failed(exc, stop, backoff)
            except Exception as exc:
                started, backoff = self._failed(exc, stop, backoff)
            else:
                backoff = 1.0
                if not self.buffer:  # rows waiting for a flush that keeps failing still fail
                    self.failures, self.failing_since = 0, None
            if time.monotonic() - last_beat >= BEAT_EVERY:
                beat(
                    self.settings.heartbeat_path,
                    role="consumer",
                    failures=self.failures,
                    failing_since=self.failing_since,
                    refused=self.refused,
                    last_flush_at=self.last_flush_at,
                    **self.totals,
                )
                last_beat = time.monotonic()
        if started:
            try:
                self.maybe_flush(final=True)
            except Exception as exc:
                log.error("stream.consumer.final_flush_failed", error=str(exc))

    def _check_topic(self) -> None:
        """The broker answers this consumer's credentials and the topic is still the one the
        checkpoint was taken on; ``Refused`` otherwise, ``ConsumerError`` when it cannot tell."""
        topic_id = topic_identity(self.admin, self.settings.topic)
        if topic_id != self.topic_id:
            raise Refused(
                f"{self.settings.topic} is now {topic_id}, not {self.topic_id}: it was deleted and "
                "recreated while this consumer read it. Reset deliberately: ibergrid stream reset"
            )

    def _housekeeping(self) -> None:
        # Without traffic librdkafka reports neither an unreachable broker nor a rejected
        # password to the consumer; this probe does, into the failure streak.
        if time.monotonic() - self.last_probe >= PROBE_EVERY:
            self.last_probe = time.monotonic()
            self._check_topic()
        if (self.clock() - self.last_sweep).total_seconds() >= self.settings.stale_branch_age:
            self.sweep_stale_branches()
        if time.monotonic() >= self.next_expiry:
            self.next_expiry = time.monotonic() + EXPIRE_EVERY
            self.expire_snapshots()

    def _failed(self, exc: Exception, stop: threading.Event, backoff: float) -> tuple[bool, float]:
        self.failures += 1
        if self.failing_since is None:
            self.failing_since = time.time()
        log.error(
            "stream.consumer.failed",
            error=f"{type(exc).__name__}: {exc}",
            failures=self.failures,
            retry_in=backoff,
        )
        stop.wait(backoff)
        return False, min(backoff * 2, MAX_BACKOFF)


def _timestamp(message: Message) -> datetime | None:
    kind, milliseconds = message.timestamp()
    if kind == TIMESTAMP_NOT_AVAILABLE:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, UTC)
