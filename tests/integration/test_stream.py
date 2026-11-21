"""The streaming lane on the running stack (`make up`): SASL/SCRAM and the ACLs of the real broker,
and recorded REData answers produced, consumed and written into a throwaway stream table through
Lakekeeper, then read back with DuckDB. Recorded answers, not the live API, so every count is
known in advance.

Nothing is written to the lane's real topics or group: the ACL probes are refused before anything
lands, and the end-to-end test runs broker-init's producer and consumer users on throwaway
topics and a throwaway group that the superuser creates, grants the same ACLs on, and removes.
"""

import json
import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import pytest
from confluent_kafka import (
    OFFSET_END,
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient

# Their defining modules: confluent_kafka.admin re-exports them without declaring it.
from confluent_kafka.admin._acl import AclBinding, AclBindingFilter, AclOperation, AclPermissionType
from confluent_kafka.admin._resource import ResourcePatternType, ResourceType
from confluent_kafka.cimpl import NewTopic
from pydantic import SecretStr

from ibergrid.lake import duck
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import BrokerSettings, StreamSettings
from ibergrid.stream.consumer import (
    ConsumerError,
    FlushReport,
    StreamConsumer,
    read_checkpoint,
    reset_checkpoint,
)
from ibergrid.stream.kafka import client_config, consumer_config, producer_config
from ibergrid.stream.producer import StreamProducer, replay_chunk

from .conftest import REPO, compose_run

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("IBERGRID_TEST_BROKER", "127.0.0.1:29092")
TOPIC = "redata.demand.realtime.v1"
DLQ = "redata.demand.realtime.dlq.v1"
GROUP = StreamSettings().group_id
FIXTURE = (
    REPO / "tests" / "fixtures" / "redata" / "demand_realtime-national-2026-11-18_2026-11-18.json"
)


def broker(user: str | None) -> BrokerSettings:
    """A client of the published Kafka listener as one of broker-init's users (keys in .env)."""
    if user is None:
        return BrokerSettings(bootstrap_servers=BOOTSTRAP)
    lines = (REPO / ".env").read_text().splitlines()
    env = dict(line.split("=", 1) for line in lines if "=" in line and not line.startswith("#"))
    variable = {"admin": "REDPANDA_ADMIN_PASSWORD", "producer": "REDPANDA_PRODUCER_PASSWORD"}.get(
        user, "REDPANDA_CONSUMER_PASSWORD"
    )
    return BrokerSettings(
        bootstrap_servers=BOOTSTRAP,
        username=f"ibergrid-{user}",
        password=SecretStr(env[variable]),
    )


def end_offsets(topic: str) -> dict[int, int]:
    admin = Consumer(client_config(broker("admin"), "it-offsets") | {"group.id": "it-offsets"})
    try:
        partitions = admin.list_topics(topic, timeout=10).topics[topic].partitions
        return {
            p: admin.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
            for p in sorted(partitions)
        }
    finally:
        admin.close()


def deliver(config: dict[str, Any], topic: str) -> list[KafkaError | None]:
    outcomes: list[KafkaError | None] = []
    producer = Producer(config | {"message.timeout.ms": 10_000})
    producer.produce(topic, value=b"{}", key=b"it", on_delivery=lambda e, _m: outcomes.append(e))
    producer.flush(30)
    return outcomes


def first_error(config: dict[str, Any], topic: str) -> KafkaError | None:
    """The first error a consumer of ``topic`` gets within 20 s, if any."""
    consumer = Consumer(config | {"group.id": GROUP, "enable.auto.commit": False})
    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_END)])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            for message in consumer.consume(10, 1.0):
                if message.error() is not None:
                    return message.error()
        return None
    finally:
        consumer.close()


def settle(futures: dict[Any, Future[Any]] | list[Future[Any]]) -> None:
    for future in futures.values() if isinstance(futures, dict) else futures:
        future.result(timeout=30)


@pytest.fixture
def lane(tmp_path: Path) -> Iterator[StreamSettings]:
    """Throwaway topics and group with the lane's ACLs for its two users, removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    settings = StreamSettings(
        topic=f"it.{suffix}.demand.v1",
        dlq_topic=f"it.{suffix}.demand.dlq.v1",
        group_id=f"it.{suffix}",
        heartbeat_path=str(tmp_path / "heartbeat"),
    )
    grants = [
        (
            ResourceType.TOPIC,
            settings.topic,
            "producer",
            (AclOperation.WRITE, AclOperation.DESCRIBE),
        ),
        (
            ResourceType.TOPIC,
            settings.topic,
            "consumer",
            (AclOperation.READ, AclOperation.DESCRIBE),
        ),
        (
            ResourceType.TOPIC,
            settings.dlq_topic,
            "consumer",
            (AclOperation.WRITE, AclOperation.DESCRIBE),
        ),
        (
            ResourceType.GROUP,
            settings.group_id,
            "consumer",
            (AclOperation.READ, AclOperation.DESCRIBE),
        ),
    ]
    fields = [
        (kind, name, ResourcePatternType.LITERAL, f"User:ibergrid-{user}", "*", operation)
        for kind, name, user, operations in grants
        for operation in operations
    ]
    acls = [AclBinding(*field, AclPermissionType.ALLOW) for field in fields]
    admin = AdminClient(client_config(broker("admin"), "it-admin"))
    settle(
        admin.create_topics([NewTopic(settings.topic, 3, 1), NewTopic(settings.dlq_topic, 1, 1)])
    )
    settle(admin.create_acls(acls))
    try:
        yield settings
    finally:
        settle(admin.delete_acls([AclBindingFilter(*f, AclPermissionType.ALLOW) for f in fields]))
        settle(admin.delete_topics([settings.topic, settings.dlq_topic]))


def test_anonymous_clients_and_wrong_passwords_are_refused() -> None:
    for config in (
        {"bootstrap.servers": BOOTSTRAP},
        client_config(broker("producer"), "it") | {"sasl.password": "wrong"},
    ):
        admin = AdminClient(config | {"socket.timeout.ms": 5000})
        with pytest.raises(KafkaException):
            admin.list_topics(timeout=8)


def test_each_user_has_only_its_own_acls() -> None:
    producer, consumer = broker("producer"), broker("consumer")

    for user, topic in ((consumer, TOPIC), (producer, DLQ)):
        [refused] = deliver(producer_config(user, "it"), topic)
        assert refused is not None
        assert refused.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED

    error = first_error(client_config(producer, "it"), TOPIC)
    assert error is not None
    assert error.code() in (
        KafkaError.TOPIC_AUTHORIZATION_FAILED,
        KafkaError.GROUP_AUTHORIZATION_FAILED,
    )


def test_topics_are_not_created_on_demand_and_the_admin_api_is_not_published() -> None:
    topic = f"it.{uuid.uuid4().hex[:8]}.v1"

    [outcome] = deliver(client_config(broker("admin"), "it"), topic)

    assert outcome is not None
    admin = AdminClient(client_config(broker("admin"), "it"))
    assert admin.list_topics(topic, timeout=10).topics[topic].error is not None
    with pytest.raises(ConnectionRefusedError):  # the admin API (9644) has no host port
        socket.create_connection(("127.0.0.1", 29644), timeout=2).close()


def test_recorded_events_land_once_with_revisions_kept_and_dead_letters_routed(
    catalog: LakeCatalog, namespace: str, con: duckdb.DuckDBPyConnection, lane: StreamSettings
) -> None:
    spec = replace(REDATA_DEMAND_STREAM, namespace=namespace)
    ensure_table(catalog, spec)
    table = catalog.load_table(spec.identifier)
    topic = lane.topic
    sink = Producer(producer_config(broker("producer"), "it-producer"))
    payload = FIXTURE.read_bytes()
    chunk = replay_chunk(payload)

    emitted = StreamProducer(sink, topic, lambda _c: payload).emit(payload, chunk).emitted
    restarted = StreamProducer(sink, topic, lambda _c: payload)  # forgets: re-emits everything
    assert restarted.emit(payload, chunk).emitted == emitted
    document = json.loads(payload)
    document["included"][0]["attributes"]["values"][0]["value"] = 1.0
    revised = json.dumps(document).encode()
    assert restarted.emit(revised, chunk).emitted == 1
    sink.produce(topic, value=b"not an event", key=b"it|garbage")
    assert sink.flush(30) == 0

    kafka = Consumer(consumer_config(broker("consumer"), "it-consumer", lane.group_id))
    stream = StreamConsumer(table, kafka, *lane_clients(), lane)
    try:
        stream.start()
        idle = 0
        while idle < 5:
            messages = kafka.consume(500, 1.0)
            for message in messages:
                stream.handle(message)
            idle = 0 if messages else idle + 1
        report = stream.maybe_flush(final=True)
        assert report is not None
        asked = [TopicPartition(topic, p) for p in report.offsets]
        committed = {tp.partition: tp.offset for tp in kafka.committed(asked, timeout=10)}
    finally:
        kafka.close()

    assert (report.rows, report.duplicates, report.revisions, report.dead_letters) == (
        emitted + 1,  # every point, plus the revision
        emitted,  # the restarted producer's whole window
        1,
        1,
    )
    end = end_offsets(topic)
    assert sum(end.values()) == 2 * emitted + 2
    checkpoint = read_checkpoint(table, lane.group_id)
    assert checkpoint is not None
    assert checkpoint.offsets == end == report.offsets == committed
    assert report.lag == {p: 0 for p in end}
    assert end_offsets(lane.dlq_topic) == {0: 1}
    name = duck.table_name(f"{namespace}.{spec.name}")
    counts = con.sql(
        "SELECT count(*), count(DISTINCT (series_id, ts_utc)), "
        "count(DISTINCT (kafka_partition, kafka_offset)) FROM iceberg_scan($t)",
        params={"t": name},
    ).fetchall()
    assert counts == [(emitted + 1, emitted, emitted + 1)]


def lane_clients() -> tuple[Producer, AdminClient]:
    """The consumer user's dead-letter producer and admin client (for the topic's id)."""
    user = broker("consumer")
    return Producer(producer_config(user, "it-dlq")), AdminClient(client_config(user, "it-admin"))


def consumed(table: Any, lane: StreamSettings) -> FlushReport | None:
    """A consumer of ``lane`` started on ``table``, run until the topic is drained, flushed."""
    kafka = Consumer(consumer_config(broker("consumer"), "it-consumer", lane.group_id))
    stream = StreamConsumer(table, kafka, *lane_clients(), lane)
    try:
        stream.start()
        idle = 0
        while idle < 5:
            messages = kafka.consume(500, 1.0)
            for message in messages:
                stream.handle(message)
            idle = 0 if messages else idle + 1
        return stream.maybe_flush(final=True)
    finally:
        kafka.close()


def recreate(topic: str) -> None:
    """Delete the topic and create it again under the same name (another topic id)."""
    admin = AdminClient(client_config(broker("admin"), "it-admin"))
    settle(admin.delete_topics([topic]))
    deadline = time.monotonic() + 30
    while True:  # the deletion settles before the name can be created again
        try:
            settle(admin.create_topics([NewTopic(topic, 3, 1)]))
            return
        except KafkaException:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def test_an_offset_out_of_range_is_an_error_not_a_jump(lane: StreamSettings) -> None:
    """The shipped consumer configuration, assigned past the end of a partition: librdkafka
    reports _AUTO_OFFSET_RESET and delivers nothing (with "earliest" it would read offset 0)."""
    sink = Producer(producer_config(broker("producer"), "it-producer"))
    for _ in range(3):
        sink.produce(lane.topic, value=b"{}", key=b"k", partition=0)
    assert sink.flush(30) == 0
    kafka = Consumer(consumer_config(broker("consumer"), "it-consumer", lane.group_id))
    try:
        kafka.assign([TopicPartition(lane.topic, 0, 103)])
        received: list[Any] = []
        deadline = time.monotonic() + 20
        while not received and time.monotonic() < deadline:
            received = kafka.consume(10, 1.0)
    finally:
        kafka.close()

    assert received
    assert [m.error() is not None for m in received] == [True] * len(received)
    assert received[0].error().code() == KafkaError._AUTO_OFFSET_RESET


def test_a_topic_recreated_under_a_running_consumer_adds_nothing(
    catalog: LakeCatalog, namespace: str, lane: StreamSettings
) -> None:
    """Deleted, created again and refilled past the running consumer's position: nothing of the
    new topic is published, and the next start refuses it."""
    spec = replace(REDATA_DEMAND_STREAM, namespace=namespace)
    ensure_table(catalog, spec)
    table = catalog.load_table(spec.identifier)
    payload = FIXTURE.read_bytes()
    chunk = replay_chunk(payload)
    sink = Producer(producer_config(broker("producer"), "it-producer"))
    StreamProducer(sink, lane.topic, lambda _c: payload).emit(payload, chunk)
    kafka = Consumer(consumer_config(broker("consumer"), "it-consumer", lane.group_id))
    stream = StreamConsumer(table, kafka, *lane_clients(), lane)
    outcome: Exception | None = None
    try:
        stream.start()
        drain(stream, kafka)
        assert stream.maybe_flush(final=True) is not None
        before = read_checkpoint(table, lane.group_id)
        recreate(lane.topic)
        for _ in range(2):  # twice the messages: past the consumer's position everywhere
            StreamProducer(sink, lane.topic, lambda _c: payload).emit(payload, chunk)
        try:
            drain(stream, kafka)
            stream.maybe_flush(final=True)
        except ConsumerError as exc:  # an error while reading, or the check before the publish
            outcome = exc
    finally:
        kafka.close()

    assert outcome is not None
    table.refresh()
    assert table.scan().count() == len(json_points(payload))
    assert read_checkpoint(table, lane.group_id) == before
    with pytest.raises(ConsumerError, match="deleted and recreated"):
        consumed(table, lane)


def drain(stream: StreamConsumer, kafka: Consumer) -> None:
    idle = 0
    while idle < 5:
        messages = kafka.consume(500, 1.0)
        for message in messages:
            stream.handle(message)
        idle = 0 if messages else idle + 1


def json_points(payload: bytes) -> list[object]:
    return [p for s in json.loads(payload)["included"] for p in s["attributes"]["values"]]


def test_a_recreated_topic_is_refused_until_a_deliberate_reset(
    catalog: LakeCatalog, namespace: str, lane: StreamSettings
) -> None:
    """R2 on the real broker: the topic is deleted, created again and refilled past the table's
    checkpoint; its id changed, so the consumer refuses until `stream reset`."""
    spec = replace(REDATA_DEMAND_STREAM, namespace=namespace)
    ensure_table(catalog, spec)
    table = catalog.load_table(spec.identifier)
    payload = FIXTURE.read_bytes()
    sink = Producer(producer_config(broker("producer"), "it-producer"))
    first = StreamProducer(sink, lane.topic, lambda _c: payload).emit(
        payload, replay_chunk(payload)
    )
    assert consumed(table, lane) is not None
    before = read_checkpoint(table, lane.group_id)
    recreate(lane.topic)
    StreamProducer(sink, lane.topic, lambda _c: payload).emit(payload, replay_chunk(payload))

    with pytest.raises(ConsumerError, match="deleted and recreated"):
        consumed(table, lane)
    kafka = Consumer(consumer_config(broker("consumer"), "it-reset", lane.group_id))
    try:
        after = reset_checkpoint(table, kafka, lane_clients()[1], lane, "earliest")
    finally:
        kafka.close()
    report = consumed(table, lane)

    assert before is not None
    assert after.topic_id != before.topic_id
    assert report is not None
    # The new incarnation from its start: repeats inside the duplicate window (three days back
    # from now; the recorded day is 2026-11-18) are dropped, the rest written again.
    assert report.rows + report.duplicates == first.emitted
    assert report.rows > 0
    ids = catalog.load_table(spec.identifier).scan(selected_fields=("kafka_topic_id",)).to_arrow()
    assert set(ids["kafka_topic_id"].to_pylist()) == {before.topic_id, after.topic_id}


def test_the_brokers_unauthenticated_http_proxy_and_schema_registry_do_not_listen() -> None:
    for port, listening in ((8081, False), (8082, False), (9092, True)):
        probe = subprocess.run(
            ["docker", "compose", "exec", "-T", "broker", "bash", "-c",
             f"exec 3<>/dev/tcp/127.0.0.1/{port}"],
            cwd=REPO, capture_output=True, text=True, timeout=60, check=False,
        )  # fmt: skip
        assert (probe.returncode == 0) is listening, (port, probe.stderr)


def test_the_stream_image_reaches_the_broker_and_the_catalog_inside_the_network() -> None:
    assert compose_run("doctor", "lake", "init").returncode == 0  # creates the stream table once

    result = compose_run("stream-consumer", "stream", "status")

    assert result.returncode == 0, result.stderr
    assert "table:        bronze.redata_demand_stream:" in result.stdout
    assert "dead letters: " in result.stdout
