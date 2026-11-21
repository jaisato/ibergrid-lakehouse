"""``ibergrid stream produce|consume|health|status`` wired to an in-memory broker and a local
catalog: the settings each command needs, what it prints and how it fails."""

import os
import signal
import threading
from pathlib import Path
from typing import Any

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from typer.testing import CliRunner

from ibergrid import cli
from ibergrid.cli import app
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.lake.tables import ensure_table
from ibergrid.sources.redata.client import RedataClient, RedataError
from ibergrid.sources.redata.datasets import Chunk
from ibergrid.stream import health
from ibergrid.stream.producer import StreamProducer

from .conftest import REDATA_FIXTURES, FakeBroker, restamped

runner = CliRunner()
TOPIC = "redata.demand.realtime.v1"
DLQ = "redata.demand.realtime.dlq.v1"
FIXTURE = "demand_realtime-national-2026-11-18_2026-11-18.json"


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> FakeBroker:
    """Both clients the commands build are this broker; the SCRAM user comes from the env."""
    fake = FakeBroker({TOPIC: 3, DLQ: 1})
    monkeypatch.setattr(cli, "Producer", lambda _config: fake)
    monkeypatch.setattr(cli, "Consumer", lambda _config: fake)
    monkeypatch.setenv("IBERGRID_BROKER_USERNAME", "ibergrid-test")
    monkeypatch.setenv("IBERGRID_BROKER_PASSWORD", "not-a-secret")
    return fake


@pytest.fixture
def stream_table(catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None) -> Any:
    ensure_table(catalog, REDATA_DEMAND_STREAM)
    monkeypatch.setattr(cli, "rest_catalog", lambda _settings: catalog)
    return catalog.load_table(REDATA_DEMAND_STREAM.identifier)


def test_produce_replays_a_recorded_answer_without_any_storage_key(broker: FakeBroker) -> None:
    result = runner.invoke(
        app, ["stream", "produce", "--source-file", str(REDATA_FIXTURES / FIXTURE)]
    )

    assert result.exit_code == 0, result.output
    emitted = len(broker.messages(TOPIC))
    assert emitted > 1000
    assert result.stdout == (
        f"stream: 2026-11-18..2026-11-18: {emitted} event(s) emitted, 0 unchanged, 0 rejected\n"
    )


def test_produce_once_polls_redata_and_reports_its_failures(
    broker: FakeBroker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(RedataClient, "fetch", lambda _self, chunk: restamped(FIXTURE, *chunk.days))
    assert runner.invoke(app, ["stream", "produce", "--once"]).exit_code == 0
    assert broker.messages(TOPIC)

    def unavailable(_self: object, _chunk: Chunk) -> bytes:
        raise RedataError("REData demanda/demanda-tiempo-real: HTTP 502 after 4 attempts")

    monkeypatch.setattr(RedataClient, "fetch", unavailable)
    result = runner.invoke(app, ["stream", "produce", "--once"])
    assert result.exit_code == 1
    assert "error: REData demanda/demanda-tiempo-real: HTTP 502" in result.stderr


def test_produce_runs_its_loop_with_the_configured_cadence(
    broker: FakeBroker, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[float, str]] = []
    monkeypatch.setattr(
        StreamProducer, "run", lambda _self, _stop, interval, beat: calls.append((interval, beat))
    )
    monkeypatch.setenv("IBERGRID_STREAM_POLL_INTERVAL", "60")

    assert runner.invoke(app, ["stream", "produce"]).exit_code == 0
    assert calls == [(60.0, health.HEARTBEAT_PATH)]


def test_the_stream_commands_refuse_a_broker_without_credentials(
    monkeypatch: pytest.MonkeyPatch, stream_table: Any
) -> None:
    for command in (["produce", "--once"], ["consume"], ["status"]):
        result = runner.invoke(app, ["stream", *command])
        assert result.exit_code == 1, command
        assert "IBERGRID_BROKER_USERNAME and IBERGRID_BROKER_PASSWORD are required" in result.stderr

    monkeypatch.setenv("IBERGRID_STREAM_FLUSH_RECORDS", "0")
    result = runner.invoke(app, ["stream", "health"])
    assert result.exit_code == 2
    assert "IBERGRID_STREAM_FLUSH_RECORDS" in result.stderr


def test_consume_writes_the_topic_and_flushes_on_stop(
    broker: FakeBroker, stream_table: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner.invoke(app, ["stream", "produce", "--source-file", str(REDATA_FIXTURES / FIXTURE)])
    stop = threading.Event()
    consume = broker.consume

    def consume_then_stop(num_messages: int = 1, timeout: float = -1) -> Any:
        found = consume(num_messages, timeout)
        if not found:  # the topic is drained: SIGTERM
            stop.set()
        return found

    monkeypatch.setattr(broker, "consume", consume_then_stop)
    monkeypatch.setattr(cli, "_stop_on_signals", lambda: stop)
    monkeypatch.setenv("IBERGRID_STREAM_HEARTBEAT_PATH", str(tmp_path / "heartbeat"))

    result = runner.invoke(app, ["stream", "consume"])

    assert result.exit_code == 0, result.output
    stream_table.refresh()
    assert stream_table.scan().count() == len(broker.messages(TOPIC))


def test_consume_and_status_need_the_table(
    broker: FakeBroker, catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch, credentials: None
) -> None:
    monkeypatch.setattr(cli, "rest_catalog", lambda _settings: catalog)
    for command in ("consume", "status"):
        result = runner.invoke(app, ["stream", command])
        assert result.exit_code == 1
        assert "run `ibergrid lake init` first" in result.stderr


def test_status_prints_the_checkpoint_and_the_lag(broker: FakeBroker, stream_table: Any) -> None:
    runner.invoke(app, ["stream", "produce", "--source-file", str(REDATA_FIXTURES / FIXTURE)])

    result = runner.invoke(app, ["stream", "status"])

    assert result.exit_code == 0, result.output
    emitted = len(broker.messages(TOPIC))
    assert "bronze.redata_demand_stream: 0 rows" in result.stdout
    assert f"lag:          {emitted} message(s) behind {TOPIC}" in result.stdout


def test_health_reads_this_containers_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("IBERGRID_STREAM_HEARTBEAT_PATH", str(tmp_path / "heartbeat"))
    assert runner.invoke(app, ["stream", "health"]).exit_code == 1

    health.beat(tmp_path / "heartbeat", role="producer")
    result = runner.invoke(app, ["stream", "health", "--max-age", "60"])
    assert result.exit_code == 0
    assert '"role": "producer"' in result.stdout


def test_sigterm_asks_the_loops_to_stop() -> None:
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        stop = cli._stop_on_signals()
        os.kill(os.getpid(), signal.SIGTERM)
        assert stop.wait(5)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
