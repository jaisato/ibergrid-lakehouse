"""The consumer on a real Iceberg catalog (sqlite + local files) through the real WAP path, with an
in-memory broker: checkpoints, duplicates and revisions, dead letters, the buffer policy, failures
and recovery."""

import json
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitStateUnknownException
from pyiceberg.table import Table
from pyiceberg.table.update import SetPropertiesUpdate
from structlog.testing import capture_logs

from ibergrid.lake import wap
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import StreamSettings
from ibergrid.stream.consumer import (
    AuditFailed,
    ConsumerError,
    DeadLetterFailed,
    StreamConsumer,
    describe_status,
    offsets_property,
    read_checkpoint,
)
from ibergrid.stream.contract import DemandEvent, ProducerInfo, content_sha256

from .conftest import FakeBroker, FakeMessage

TOPIC = "redata.demand.realtime.v1"
DLQ = "redata.demand.realtime.dlq.v1"
GROUP = "ibergrid.stream.bronze"
PROPERTY = offsets_property(GROUP, TOPIC)
NOW = datetime(2026, 11, 18, 12, tzinfo=UTC)


class Held(Exception):
    """Stands in for the SIGKILL that ends a held consumer."""


def event(series: str = "2037", minute: int = 0, value: float = 27_000.0) -> DemandEvent:
    ts = datetime(2026, 11, 18, 10, tzinfo=UTC) + timedelta(minutes=minute)
    return DemandEvent(
        series_id=series,
        series_title="Real",
        electric_system="national",
        ts_utc=ts,
        ts_local=ts.astimezone().isoformat(),
        value=value,
        content_sha256=content_sha256(series, "national", ts, value),
        producer=ProducerInfo(name="test", version="0", instance="host", emitted_at=NOW),
    )


def send(broker: FakeBroker, *events: DemandEvent) -> None:
    for item in events:
        broker.produce(TOPIC, value=item.model_dump_json().encode(), key=item.key.encode())


def drain(stream: StreamConsumer) -> None:
    for message in stream.kafka.consume(10_000, 0):
        stream.handle(message)


def rows(table: Table) -> list[dict[str, Any]]:
    table.refresh()
    if table.current_snapshot() is None:
        return []
    return cast(list[dict[str, Any]], table.scan().to_arrow().to_pylist())


def fail_publishes(
    table: Table, monkeypatch: pytest.MonkeyPatch, *failures: Exception
) -> list[object]:
    """The catalog refuses the next publish commits (the ones setting table properties) with
    ``failures``, in order; every other commit goes through. Returns the publishes it saw."""
    commit, seen, pending = table.catalog.commit_table, [], list(failures)

    def refusing(target: Table, requirements: Any, updates: Any) -> Any:
        if any(isinstance(update, SetPropertiesUpdate) for update in updates):
            seen.append(updates)
            if pending:
                raise pending.pop(0)
        return commit(target, requirements, updates)

    monkeypatch.setattr(table.catalog, "commit_table", refusing)
    return seen


@pytest.fixture
def table(catalog: SqlCatalog) -> Table:
    ensure_table(catalog, REDATA_DEMAND_STREAM)
    return catalog.load_table(REDATA_DEMAND_STREAM.identifier)


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker({TOPIC: 3, DLQ: 1})


@pytest.fixture
def make(table: Table, broker: FakeBroker, tmp_path: Path) -> Callable[..., StreamConsumer]:
    def build(sleep: Callable[[float], None] = lambda _: None, **changes: Any) -> StreamConsumer:
        settings = StreamSettings(heartbeat_path=str(tmp_path / "heartbeat"), **changes)
        return StreamConsumer(
            table,
            cast(Consumer, broker),
            cast(Producer, broker),
            settings,
            clock=lambda: NOW,
            sleep=sleep,
        )

    return build


@pytest.fixture
def started(make: Callable[..., StreamConsumer]) -> StreamConsumer:
    stream = make()
    stream.start()
    return stream


def test_a_flush_writes_the_rows_and_publishes_their_offsets_in_one_commit(
    started: StreamConsumer, broker: FakeBroker, table: Table
) -> None:
    send(broker, *(event(minute=5 * n) for n in range(5)))
    drain(started)

    report = started.flush()

    written = rows(table)
    assert report.rows == len(written) == 5
    expected = {p: len(log) for p, log in enumerate(broker.logs[TOPIC]) if log}
    assert report.offsets == expected == read_checkpoint(table, GROUP, TOPIC)
    assert broker.commits == [expected]  # the group gets them too, for lag visibility
    assert report.lag == {p: 0 for p in expected}
    assert {row["run_id"] for row in written} == {report.run_id}
    assert {(row["kafka_partition"], row["kafka_offset"]) for row in written} == {
        (m.partition(), m.offset()) for m in broker.messages(TOPIC)
    }
    summary = cast(Any, table.current_snapshot()).summary
    assert summary["ibergrid.stream.rows"] == "5"
    assert summary["ibergrid.stream.offsets"] == table.properties[PROPERTY]
    assert wap.audit_branches(table) == []


def test_a_restart_seeks_to_the_tables_checkpoint_not_the_groups(
    started: StreamConsumer, broker: FakeBroker, make: Callable[..., StreamConsumer]
) -> None:
    send(broker, event(minute=0), event(minute=5))
    drain(started)
    started.flush()
    broker.commits.clear()  # whatever the group says, the table decides
    send(broker, event(minute=10))

    again = make()
    again.start()
    drain(again)

    assert broker.assignments[-1] == {
        p: started.checkpointed.get(p, 0) for p in range(len(broker.logs[TOPIC]))
    }
    assert [item.event.ts_utc.minute for item in again.buffer] == [10]


def test_exact_duplicates_are_dropped_and_revisions_kept_also_after_a_restart(
    started: StreamConsumer, broker: FakeBroker, table: Table, make: Callable[..., StreamConsumer]
) -> None:
    originals = [event(minute=5 * n) for n in range(4)]
    send(broker, *originals, originals[0])  # a repeat within the batch
    drain(started)
    assert started.batch["duplicates"] == 1
    started.flush()

    again = make()  # the producer restarted too and re-emits its window
    again.start()
    send(broker, *originals, event(minute=0, value=1.0), event(minute=20))
    drain(again)
    report = again.flush()

    assert (report.rows, report.duplicates, report.revisions) == (2, 4, 1)
    written = rows(table)
    assert len(written) == 6
    assert sorted(row["value"] for row in written if row["ts_utc"].minute == 0) == [1.0, 27_000.0]


def test_invalid_messages_go_to_the_dead_letter_topic_with_their_reason(
    started: StreamConsumer, broker: FakeBroker
) -> None:
    good = event()
    off_grid = json.loads(good.model_dump_json()) | {"ts_utc": "2026-11-18T10:01:00Z"}
    broker.produce(TOPIC, value=b"{not json", key=b"k")
    broker.produce(TOPIC, value=json.dumps(off_grid).encode(), key=good.key.encode())
    broker.produce(TOPIC, value=good.model_dump_json().encode(), key=b"2037|elsewhere")
    broker.produce(TOPIC, value=None, key=good.key.encode())

    drain(started)

    letters = broker.messages(DLQ)
    reasons = [dict(headers)["ibergrid.dlq.reason"] for headers in broker.headers[-4:]]
    assert sorted(reasons) == ["contract", "contract", "key", "no-value"]
    assert {m.value() for m in letters} == {
        b"{not json",
        json.dumps(off_grid).encode(),
        good.model_dump_json().encode(),
        None,
    }
    sources = {dict(headers)["ibergrid.dlq.source"] for headers in broker.headers[-4:]}
    assert all(source.startswith(f"{TOPIC}/") for source in sources)
    assert started.buffer == []
    assert started.batch["dead_letters"] == 4
    assert sum(started.next_offsets.values()) == 4


def test_progress_without_rows_is_checkpointed_at_a_bounded_cadence(
    started: StreamConsumer, broker: FakeBroker, table: Table
) -> None:
    send(broker, event())
    drain(started)
    first = started.flush()
    send(broker, event())  # a duplicate: the offsets move, no row does
    drain(started)

    assert started.maybe_flush() is None  # within checkpoint_interval of the last checkpoint
    started.last_checkpoint -= started.settings.checkpoint_interval
    checkpoint = started.maybe_flush()

    assert checkpoint is not None
    assert checkpoint.rows == 0
    assert checkpoint.snapshot_id != first.snapshot_id  # main moved: the publish was fenced
    assert sum(read_checkpoint(table, GROUP, TOPIC).values()) == 2
    assert len(rows(table)) == 1
    assert started.maybe_flush(final=True) is None  # nothing new since


def test_a_checkpoint_waits_for_the_first_rows_on_an_empty_table(
    started: StreamConsumer, broker: FakeBroker, table: Table
) -> None:
    broker.produce(TOPIC, value=b"garbage", key=b"k")
    drain(started)

    with capture_logs() as logs:
        assert started.maybe_flush(final=True) is None

    assert PROPERTY not in table.properties
    assert "stream.consumer.checkpoint_deferred" in [entry["event"] for entry in logs]


def test_the_buffer_flushes_at_its_size_or_its_age(
    make: Callable[..., StreamConsumer], broker: FakeBroker
) -> None:
    stream = make(flush_records=3, flush_age=60)
    stream.start()
    send(broker, event(minute=0), event(minute=5))
    drain(stream)
    assert stream.maybe_flush() is None

    send(broker, event(minute=10))
    drain(stream)
    assert cast(Any, stream.maybe_flush()).rows == 3

    send(broker, event(minute=15))
    drain(stream)
    assert stream.maybe_flush() is None
    stream.first_buffered = time.monotonic() - 61
    assert cast(Any, stream.maybe_flush()).rows == 1


def test_a_failed_flush_leaves_main_and_the_offsets_untouched(
    started: StreamConsumer, broker: FakeBroker, table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    send(broker, event(minute=0))
    drain(started)
    started.flush()
    head, checkpoint = table.current_snapshot(), table.properties[PROPERTY]
    send(broker, event(minute=5))
    drain(started)

    fail_publishes(table, monkeypatch, OSError("catalog unreachable"))
    with pytest.raises(OSError, match="unreachable"):
        started.flush()

    table.refresh()
    assert table.current_snapshot() == head
    assert table.properties[PROPERTY] == checkpoint
    assert wap.audit_branches(table) == []  # the run was discarded
    assert len(started.buffer) == 1  # the run loop starts again from the table


def test_an_audit_that_finds_other_rows_refuses_the_batch(
    started: StreamConsumer, broker: FakeBroker, table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    send(broker, event(minute=0), event(minute=5))
    drain(started)
    original = wap.write

    def write_one_row_twice(target: Table, audit: wap.AuditBranch, data: Any, **kw: Any) -> Any:
        return original(target, audit, data.take([0, 0]), **kw)

    monkeypatch.setattr(wap, "write", write_one_row_twice)  # a faulty write, not a catalog fault

    with pytest.raises(AuditFailed, match="2 rows \\(1 distinct messages\\)"):
        started.flush()
    assert rows(table) == []


def test_a_lost_publish_answer_is_published_again_with_the_same_arguments(
    started: StreamConsumer, broker: FakeBroker, table: Table, monkeypatch: pytest.MonkeyPatch
) -> None:
    send(broker, event(minute=0))
    drain(started)
    started.flush()
    send(broker, event(minute=0))  # a duplicate: the next flush is a checkpoint
    drain(started)
    publishes = fail_publishes(table, monkeypatch, CommitStateUnknownException("502"))

    with capture_logs() as logs:
        report = started.flush()

    assert len(publishes) == 2
    assert "stream.consumer.publish_retry" in [entry["event"] for entry in logs]
    assert sum(read_checkpoint(table, GROUP, TOPIC).values()) == 2
    checkpoints = [
        s for s in table.snapshots() if s.summary is not None and s.summary["ibergrid.checkpoint"]
    ]
    assert len(checkpoints) == 1  # resumed from its own checkpoint snapshot, not a second one
    assert report.snapshot_id == checkpoints[0].snapshot_id


def test_a_second_writer_cannot_land_offsets_the_first_already_published(
    make: Callable[..., StreamConsumer], broker: FakeBroker, table: Table
) -> None:
    send(broker, event(minute=0), event(minute=5))
    first, second = make(), make()
    first.start()
    drain(first)
    second.start()  # the fake broker has one position per partition: read everything again
    drain(second)
    assert len(first.buffer) == len(second.buffer) > 0
    first.flush()

    with pytest.raises(wap.PublishConflict, match="the checkpoint moved"):
        second.flush()

    second.start()
    drain(second)
    assert second.buffer == []
    assert len(rows(table)) == 2


def test_a_publish_that_lands_during_the_run_is_fenced_by_main(
    make: Callable[..., StreamConsumer],
    broker: FakeBroker,
    table: Table,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    send(broker, event(minute=0))
    first, second = make(), make()
    first.start()
    drain(first)
    second.start()  # the fake broker has one position per partition: read everything again
    drain(second)
    assert len(first.buffer) == len(second.buffer) > 0
    commit, raced = table.catalog.commit_table, list[bool]()

    def first_publishes_meanwhile(target: Table, requirements: Any, updates: Any) -> Any:
        if not raced and any(isinstance(update, SetPropertiesUpdate) for update in updates):
            raced.append(True)
            first.flush()  # lands between the second's audit and its publish commit
        return commit(target, requirements, updates)

    monkeypatch.setattr(table.catalog, "commit_table", first_publishes_meanwhile)

    with pytest.raises(wap.PublishConflict, match="moved while publishing"):
        second.flush()
    assert len(rows(table)) == 1


@pytest.mark.parametrize("point", ["after-write", "after-publish"])
def test_a_held_flush_stops_exactly_where_the_chaos_test_kills_it(
    make: Callable[..., StreamConsumer], broker: FakeBroker, table: Table, point: str
) -> None:
    observed: list[tuple[int, list[str], str | None]] = []

    def kill(_seconds: float) -> None:
        table.refresh()
        observed.append(
            (len(rows(table)), wap.audit_branches(table), table.properties.get(PROPERTY))
        )
        raise Held

    stream = make(sleep=kill, hold_at=point)
    stream.start()
    send(broker, event())
    drain(stream)

    with pytest.raises(Held):
        stream.flush()

    [(count, branches, checkpoint)] = observed
    if point == "after-write":
        assert (count, checkpoint) == (0, None)
        assert len(branches) == 1  # written, audited later, never published
    else:
        assert (count, branches) == (1, [])
        assert checkpoint is not None
        assert broker.commits == []  # the group never heard of it


def test_only_this_lanes_dead_branches_are_swept(
    make: Callable[..., StreamConsumer], table: Table
) -> None:
    for run_id in (
        "stream-20261118T100000000000Z-aaaaaa",  # two hours old: a dead run's
        "stream-20261118T115500000000Z-bbbbbb",  # five minutes old: maybe alive
        "another-lane-run",
    ):
        wap.start(table, run_id)

    removed = make(stale_branch_age=900).sweep_stale_branches()

    assert removed == ["audit-stream-20261118T100000000000Z-aaaaaa"]
    assert wap.audit_branches(table) == [
        "audit-another-lane-run",
        "audit-stream-20261118T115500000000Z-bbbbbb",
    ]


class NoWait(threading.Event):
    """A stop event whose waits return at once: backoffs without sleeping."""

    def wait(self, timeout: float | None = None) -> bool:
        return self.is_set()


def test_the_loop_starts_again_from_the_table_after_a_failure_and_flushes_on_stop(
    make: Callable[..., StreamConsumer],
    broker: FakeBroker,
    table: Table,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = make(flush_records=3)
    send(broker, *(event(minute=5 * n) for n in range(4)))
    stop = NoWait()
    fail_publishes(table, monkeypatch, OSError("catalog unreachable"))
    consume, calls = broker.consume, []

    def one_at_a_time(num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        calls.append(1)
        if len(calls) == 7:
            stop.set()
        return consume(1, timeout)

    monkeypatch.setattr(broker, "consume", one_at_a_time)

    with capture_logs() as logs:
        stream.run(stop)

    # Calls 1-3 fill the buffer and the flush fails; 4-6 read the same messages again from the
    # table's (empty) checkpoint and flush; 7 reads the last one, flushed on stop.
    assert "stream.consumer.failed" in [entry["event"] for entry in logs]
    assert len(broker.assignments) == 2
    written = rows(table)
    assert sorted((r["kafka_partition"], r["kafka_offset"]) for r in written) == sorted(
        (m.partition(), m.offset()) for m in broker.messages(TOPIC)
    )
    assert stream.totals["flushes"] == 2


def test_the_loop_flushes_what_is_buffered_when_stopped(
    make: Callable[..., StreamConsumer],
    broker: FakeBroker,
    table: Table,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = make()
    send(broker, event(minute=0), event(minute=5))
    stop = threading.Event()
    consume = broker.consume

    def consume_then_stop(num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        stop.set()
        return consume(num_messages, timeout)

    monkeypatch.setattr(broker, "consume", consume_then_stop)
    stream.run(stop)

    assert len(rows(table)) == 2
    assert json.loads((tmp_path / "heartbeat").read_text())["consumed"] == 2


def test_kafka_errors_are_logged_unless_fatal(started: StreamConsumer) -> None:
    transport = FakeMessage(TOPIC, 0, 0, None, None, error_=KafkaError(KafkaError._TRANSPORT))
    fatal = FakeMessage(TOPIC, 0, 0, None, None, error_=KafkaError(KafkaError._FATAL, fatal=True))

    with capture_logs() as logs:
        started.handle(cast(Any, transport))
    assert logs[0]["event"] == "stream.consumer.kafka_error"
    with pytest.raises(KafkaException):
        started.handle(cast(Any, fatal))


def test_a_fatal_kafka_error_stops_the_loop(
    make: Callable[..., StreamConsumer], broker: FakeBroker, monkeypatch: pytest.MonkeyPatch
) -> None:
    stream = make()
    fatal = FakeMessage(TOPIC, 0, 0, None, None, error_=KafkaError(KafkaError._FATAL, fatal=True))
    monkeypatch.setattr(broker, "consume", lambda *_args: [fatal])

    with pytest.raises(KafkaException):
        stream.run(threading.Event())


def test_dead_letters_the_broker_refuses_fail_the_flush(
    started: StreamConsumer, broker: FakeBroker, table: Table
) -> None:
    send(broker, event())
    broker.produce(TOPIC, value=b"garbage", key=b"k")
    broker.refuse = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)
    drain(started)

    with pytest.raises(DeadLetterFailed, match="1 refused"):
        started.flush()
    assert rows(table) == []


def test_status_shows_the_checkpoint_the_lag_and_the_dead_letters(
    started: StreamConsumer, broker: FakeBroker, table: Table
) -> None:
    send(broker, event(minute=0), event(minute=5))
    broker.produce(TOPIC, value=b"garbage", key=b"k")
    drain(started)
    started.flush()
    send(broker, event(minute=10))

    lines = describe_status(table, cast(Consumer, broker), started.settings)

    assert lines[0].startswith("table:        bronze.redata_demand_stream: 2 rows, snapshot ")
    assert lines[1] == f"checkpoint:   {PROPERTY} = {table.properties[PROPERTY]}"
    assert lines[-2] == f"lag:          1 message(s) behind {TOPIC}"
    assert lines[-1] == f"dead letters: 1 retained in {DLQ}"


def test_a_checkpoint_property_that_is_not_offsets_is_refused(table: Table) -> None:
    for raw, message in (("[1, 2]", "not a JSON object"), ('{"0": -1}', "negative")):
        with table.transaction() as tx:
            tx.set_properties({PROPERTY: raw})
        with pytest.raises(ConsumerError, match=message):
            read_checkpoint(table, GROUP, TOPIC)


def test_a_missing_topic_and_odd_checkpoints_are_reported(
    make: Callable[..., StreamConsumer], broker: FakeBroker, table: Table
) -> None:
    with pytest.raises(ConsumerError, match="broker-init creates it"):
        make(topic="redata.demand.realtime.v2").start()

    with table.transaction() as tx:
        tx.set_properties({PROPERTY: json.dumps({"0": 5})})
    with capture_logs() as logs:
        make().start()
    assert any(entry["event"] == "stream.consumer.checkpoint_ahead" for entry in logs)


def test_group_commit_and_lag_failures_only_cost_visibility(
    started: StreamConsumer, broker: FakeBroker, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_args: object, **_kwargs: object) -> Any:
        raise KafkaException(KafkaError(KafkaError._TRANSPORT))

    send(broker, event())
    drain(started)
    monkeypatch.setattr(broker, "commit", broken)
    monkeypatch.setattr(broker, "get_watermark_offsets", broken)

    with capture_logs() as logs:
        report = started.flush()

    assert report.rows == 1
    assert report.lag == {}
    events = [entry["event"] for entry in logs]
    assert "stream.consumer.group_commit_failed" in events
    assert "stream.consumer.lag_unknown" in events
