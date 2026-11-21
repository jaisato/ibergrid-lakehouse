"""Heartbeats and the healthcheck that reads them; the Kafka client configuration."""

import json
import time
from pathlib import Path

import pytest
from pydantic import SecretStr

from ibergrid.settings import BrokerSettings
from ibergrid.stream import health
from ibergrid.stream.kafka import BrokerConfigError, consumer_config, producer_config


def test_a_fresh_heartbeat_is_healthy_and_carries_the_counters(tmp_path: Path) -> None:
    path = tmp_path / "heartbeat"

    health.beat(path, role="consumer", rows=3)

    ok, detail = health.check(path, max_age=10)
    assert ok
    assert json.loads(path.read_text())["rows"] == 3
    assert '"role": "consumer"' in detail
    assert [p.name for p in tmp_path.iterdir()] == ["heartbeat"]  # no temporary file left


def test_a_stale_missing_or_garbled_heartbeat_is_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "heartbeat"
    assert not health.check(path, max_age=10)[0]

    health.beat(path)
    ok, detail = health.check(path, max_age=10, now=time.time() + 60)
    assert not ok
    assert "limit 10 s" in detail

    path.write_text("{not json")
    assert not health.check(path, max_age=10)[0]


def test_a_loop_failing_for_too_long_is_unhealthy(tmp_path: Path) -> None:
    path = tmp_path / "heartbeat"
    health.beat(path, failing_since=time.time() - 120)

    assert health.check(path, max_age=10, max_failing=300)[0]
    ok, detail = health.check(path, max_age=10, max_failing=60)
    assert not ok
    assert detail.startswith("failing for 120 s (limit 60 s)")
    assert health.main(["--path", str(path), "--max-failing", "60"]) == 1


def test_the_watchdog_ends_a_process_whose_loop_stopped_touching_it() -> None:
    exits: list[int] = []
    touched = health.Watchdog(0.2, exit=exits.append, every=0.05)
    thread = touched.start()
    for _ in range(6):
        time.sleep(0.05)
        touched.touch()
    assert exits == []

    thread.join(timeout=2)  # nobody touches it any more
    assert exits == [health.WATCHDOG_EXIT]


def test_the_module_entry_point_exits_by_freshness(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "heartbeat"
    assert health.main(["--path", str(path)]) == 1
    assert "no usable heartbeat" in capsys.readouterr().err

    health.beat(path)
    assert health.main(["--path", str(path), "--max-age", "30"]) == 0
    assert "heartbeat 0 s old" in capsys.readouterr().out


def test_clients_need_scram_credentials() -> None:
    with pytest.raises(BrokerConfigError, match="IBERGRID_BROKER_USERNAME"):
        producer_config(BrokerSettings(), "x")


def test_the_producer_is_idempotent_and_the_consumer_commits_by_hand() -> None:
    broker = BrokerSettings(username="ibergrid-producer", password=SecretStr("not-a-secret"))

    produce = producer_config(broker, "ibergrid-stream-producer")
    consume = consumer_config(broker, "ibergrid-stream-consumer", "ibergrid.stream.bronze")

    assert produce["enable.idempotence"] is True
    assert produce["acks"] == "all"
    assert produce["security.protocol"] == "SASL_PLAINTEXT"
    assert produce["sasl.mechanism"] == "SCRAM-SHA-256"
    assert produce["sasl.password"] == "not-a-secret"
    assert consume["enable.auto.commit"] is False
    assert consume["group.id"] == "ibergrid.stream.bronze"
    assert str(consume["client.id"]).startswith("ibergrid-stream-consumer/")
