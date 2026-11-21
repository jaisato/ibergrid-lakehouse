"""The producer against recorded REData answers and an in-memory broker: windows, suppression of
repeats, revisions, delivery failures and the polling loop."""

import json
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from confluent_kafka import KafkaError, Producer

from ibergrid.sources.redata.client import RedataError
from ibergrid.sources.redata.datasets import Chunk
from ibergrid.sources.redata.normalize import InvalidResponse
from ibergrid.stream import health
from ibergrid.stream.contract import DemandEvent
from ibergrid.stream.producer import DeliveryFailed, StreamProducer, replay_chunk

from .conftest import FakeBroker, fixture, restamped

TOPIC = "redata.demand.realtime.v1"
FIXTURE = "demand_realtime-national-2026-11-18_2026-11-18.json"
POINTS = sum(len(s["attributes"]["values"]) for s in json.loads(fixture(FIXTURE))["included"])


def clock(local: str) -> Callable[[], datetime]:
    return lambda: datetime.fromisoformat(local).astimezone(UTC)


class Answers:
    """REData's answer for each requested window, from the fixture restamped to its days."""

    def __init__(self) -> None:
        self.chunks: list[Chunk] = []
        self.edit: Callable[[dict[str, object]], None] | None = None

    def __call__(self, chunk: Chunk) -> bytes:
        self.chunks.append(chunk)
        body = json.loads(restamped(FIXTURE, *chunk.days))
        if self.edit is not None:
            self.edit(body)
        return json.dumps(body).encode()


def producer(
    broker: FakeBroker, answers: Answers, now: str = "2026-11-18T12:00:00+01:00"
) -> StreamProducer:
    return StreamProducer(
        cast(Producer, broker), TOPIC, answers, clock=clock(now), instance="test-host"
    )


@pytest.fixture
def broker() -> FakeBroker:
    return FakeBroker({TOPIC: 3})


@pytest.mark.parametrize(
    ("now", "grace", "window"),
    [
        ("2026-11-18T12:00:00+01:00", timedelta(hours=2), (date(2026, 11, 18), date(2026, 11, 18))),
        ("2026-11-19T00:30:00+01:00", timedelta(hours=2), (date(2026, 11, 18), date(2026, 11, 19))),
        ("2026-11-19T02:00:00+01:00", timedelta(hours=2), (date(2026, 11, 19), date(2026, 11, 19))),
        ("2026-11-19T00:30:00+01:00", timedelta(0), (date(2026, 11, 19), date(2026, 11, 19))),
    ],
)
def test_the_window_is_today_and_yesterday_shortly_after_midnight(
    broker: FakeBroker, now: str, grace: timedelta, window: tuple[date, date]
) -> None:
    stream = StreamProducer(cast(Producer, broker), TOPIC, Answers(), midnight_grace=grace)

    chunk = stream.window(clock(now)())

    assert (chunk.start, chunk.end) == window
    assert chunk.dataset.name == "demand_realtime"
    assert chunk.system.name == "national"


def test_a_poll_emits_every_point_once_keyed_by_series_and_instant(broker: FakeBroker) -> None:
    answers = Answers()

    report = producer(broker, answers).poll_once()

    messages = broker.messages(TOPIC)
    assert (report.points, report.emitted, report.suppressed, report.rejected) == (
        POINTS,
        POINTS,
        0,
        0,
    )
    assert len(messages) == POINTS
    assert len({m.key() for m in messages}) == POINTS
    first = DemandEvent.model_validate_json(cast(bytes, messages[0].value()))
    assert messages[0].key() == first.key.encode()
    assert first.producer.instance == "test-host"
    assert first.producer.name == "ibergrid-stream-producer"
    assert [(c.start, c.end) for c in answers.chunks] == [(date(2026, 11, 18), date(2026, 11, 18))]


def test_repeats_are_suppressed_and_only_a_revised_point_goes_out_again(broker: FakeBroker) -> None:
    answers = Answers()
    stream = producer(broker, answers)
    stream.poll_once()

    assert stream.poll_once().suppressed == POINTS

    def revise(body: dict[str, object]) -> None:
        series = cast(list[dict[str, dict[str, list[dict[str, object]]]]], body["included"])
        series[0]["attributes"]["values"][0]["value"] = 1.0

    answers.edit = revise
    report = stream.poll_once()
    assert (report.emitted, report.suppressed) == (1, POINTS - 1)
    events = [
        DemandEvent.model_validate_json(cast(bytes, m.value())) for m in broker.messages(TOPIC)
    ]
    [revised] = [event for event in events if event.value == 1.0]
    assert [event.key for event in events].count(revised.key) == 2  # the original and the revision


def test_a_refused_or_undelivered_poll_raises_and_is_emitted_again(broker: FakeBroker) -> None:
    stream = producer(broker, Answers())
    broker.refuse = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)

    with pytest.raises(DeliveryFailed, match=f"0 event\\(s\\) undelivered and {POINTS} refused"):
        stream.poll_once()

    broker.refuse, broker.undelivered = None, 2
    with pytest.raises(DeliveryFailed, match="2 event"):
        stream.poll_once()
    assert stream.seen == {}

    broker.undelivered = 0
    assert stream.poll_once().emitted == POINTS


def test_a_partial_delivery_failure_invalidates_suppression_for_attempted_keys(
    broker: FakeBroker,
) -> None:
    """If a revision lands on the broker and a later event in the same poll fails delivery,
    reverting the first key to its earlier value on the next poll must not be suppressed."""
    answers = Answers()
    stream = producer(broker, answers)
    stream.poll_once()

    def revise_two(body: dict[str, object]) -> None:
        series = cast(list[dict[str, dict[str, list[dict[str, object]]]]], body["included"])
        series[0]["attributes"]["values"][0]["value"] = 1.0
        series[0]["attributes"]["values"][1]["value"] = 2.0

    answers.edit = revise_two
    original_produce = broker.produce
    calls = 0

    def fail_second(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            broker.refuse = KafkaError(KafkaError._MSG_TIMED_OUT)
        original_produce(*args, **kwargs)  # type: ignore[arg-type]

    broker.produce = fail_second  # type: ignore[method-assign]
    with pytest.raises(DeliveryFailed, match="0 event\\(s\\) undelivered and 1 refused"):
        stream.poll_once()
    assert len(broker.messages(TOPIC)) == POINTS + 1  # the first revision landed on Kafka

    # Next poll: REData reverts both points to their original values; both attempted keys go out
    # again while untouched keys stay suppressed.
    broker.produce = original_produce  # type: ignore[method-assign]
    broker.refuse = None
    answers.edit = None
    report = stream.poll_once()
    assert (report.emitted, report.suppressed) == (2, POINTS - 2)
    events = [
        DemandEvent.model_validate_json(cast(bytes, m.value())) for m in broker.messages(TOPIC)
    ]
    reverted_key = next(e.key for e in events if e.value == 1.0)
    reverted_values = [e.value for e in events if e.key == reverted_key]
    assert len(reverted_values) == 3
    assert reverted_values[1] == 1.0
    assert reverted_values[2] == reverted_values[0]


def test_a_point_off_the_five_minute_grid_is_rejected_not_emitted(
    broker: FakeBroker,
) -> None:
    answers = Answers()

    def shift(body: dict[str, object]) -> None:
        series = cast(list[dict[str, dict[str, list[dict[str, object]]]]], body["included"])
        point = series[0]["attributes"]["values"][1]
        point["datetime"] = str(point["datetime"]).replace(":05:00", ":06:00")

    answers.edit = shift

    report = producer(broker, answers).poll_once()

    assert (report.emitted, report.rejected) == (POINTS - 1, 1)


def test_an_answer_the_parser_refuses_fails_the_poll(broker: FakeBroker) -> None:
    answers = Answers()
    answers.edit = lambda body: body.update(data={"id": "dem1"})

    with pytest.raises(InvalidResponse):
        producer(broker, answers).poll_once()
    assert broker.messages(TOPIC) == []


def test_the_memory_of_emitted_points_keeps_only_the_window(broker: FakeBroker) -> None:
    answers = Answers()
    after_midnight = producer(broker, answers, "2026-11-19T00:30:00+01:00")
    after_midnight.poll_once()
    assert len(after_midnight.seen) == 2 * POINTS

    after_midnight.clock = clock("2026-11-19T12:00:00+01:00")
    after_midnight.poll_once()

    assert len(after_midnight.seen) == POINTS
    assert min(ts for ts, _ in after_midnight.seen.values()) == datetime(
        2026, 11, 18, 23, tzinfo=UTC
    )


def test_the_loop_survives_failed_polls_and_failed_deliveries_and_beats(
    broker: FakeBroker, tmp_path: Path
) -> None:
    """REData's 502, then a broker that does not acknowledge, then a good poll: the process stays
    up, the unacknowledged events go out again, and the heartbeat says how long it failed."""
    stop = threading.Event()
    answers = Answers()
    calls = 0
    heartbeat = tmp_path / "heartbeat"
    seen: list[dict[str, object]] = []

    def flaky(chunk: Chunk) -> bytes:
        nonlocal calls
        calls += 1
        seen.append(json.loads(heartbeat.read_text()))
        broker.refuse = KafkaError(KafkaError._MSG_TIMED_OUT) if calls == 2 else None
        if calls == 1:
            raise RedataError("REData demanda/demanda-tiempo-real: HTTP 502 after 4 attempts")
        if calls == 3:
            stop.set()
        return answers(chunk)

    stream = StreamProducer(
        cast(Producer, broker), TOPIC, flaky, clock=clock("2026-11-18T12:00+01:00")
    )
    dog = health.Watchdog(60)

    stream.run(stop, interval=0.01, heartbeat=str(heartbeat), watchdog=dog)

    assert calls == 3
    assert [beat["failing_since"] is None for beat in seen] == [True, False, False]
    assert seen[1]["failing_since"] == seen[2]["failing_since"]  # one streak of two failures
    assert len(broker.messages(TOPIC)) == POINTS  # refused once, sent again in full
    assert json.loads(heartbeat.read_text()) | {"at": 0} == {
        "at": 0,
        "role": "producer",
        "failing_since": None,
        "polls": 1,
        "failed_polls": 2,
        "emitted": POINTS,
        "suppressed": 0,
    }


def test_a_recorded_answer_replays_over_its_own_days() -> None:
    chunk = replay_chunk(fixture(FIXTURE))
    assert (chunk.start, chunk.end) == (date(2026, 11, 18), date(2026, 11, 18))

    with pytest.raises(RedataError, match="not a recorded REData answer"):
        replay_chunk(b"<html>")
    with pytest.raises(RedataError, match="no points"):
        replay_chunk(b'{"included": []}')
