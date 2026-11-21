"""Opt-in smoke test against the live REData API: `make test-live`. Never run by default or in CI.

One request per dataset for the day before yesterday (Madrid), and one for today's real-time
demand as the stream producer asks for it, paced at one a second; it checks that the answers
still parse, validate and come complete, and that today's points satisfy the stream contract.
Nothing is written anywhere.
"""

from datetime import datetime, timedelta
from typing import Any, cast

import pytest
from confluent_kafka import Producer

from ibergrid.sources.redata.client import RedataClient
from ibergrid.sources.redata.datasets import DATASETS, MADRID, plan
from ibergrid.sources.redata.normalize import completeness, granularities, parse
from ibergrid.stream.contract import DemandEvent
from ibergrid.stream.producer import StreamProducer

pytestmark = pytest.mark.live

CLIENT = RedataClient("https://apidatos.ree.es", timeout=120)  # one client: one pace for all


@pytest.mark.parametrize("name", list(DATASETS))
def test_a_recent_day_parses_and_is_complete(name: str) -> None:
    day = datetime.now(MADRID).date() - timedelta(days=2)
    chunk = plan(DATASETS[name], day, day)[0]

    points = parse(CLIENT.fetch(chunk), chunk)
    measured = completeness(points, granularities(points, chunk), chunk)

    assert measured.received == measured.expected > 0, measured.incomplete


def test_year_to_date_generation_comes_back_exact() -> None:
    """The plan splits a long daily window ending mid-month: each piece holds its window only
    (parse refuses any point outside it)."""
    day = datetime.now(MADRID).date() - timedelta(days=2)
    chunks = plan(DATASETS["generation_daily"], day.replace(month=1, day=1), day, ("national",))

    for chunk in chunks:
        assert parse(CLIENT.fetch(chunk), chunk)
    assert chunks[-1].end == day


def test_todays_real_time_demand_satisfies_the_stream_contract() -> None:
    """The stream producer's window right now: every point becomes a valid event; nothing is
    sent anywhere (an in-memory sink)."""
    sent: list[bytes] = []

    class Sink:
        def produce(self, _topic: str, value: bytes, key: bytes, on_delivery: Any) -> None:
            sent.append(value)
            on_delivery(None, None)

        def poll(self, _timeout: float) -> int:
            return 0

        def flush(self) -> int:
            return 0

    producer = StreamProducer(cast(Producer, Sink()), "unused", CLIENT.fetch)

    report = producer.poll_once()

    assert report.rejected == 0
    assert report.emitted == report.points == len(sent) > 0
    assert {DemandEvent.model_validate_json(value).series_id for value in sent} >= {"2037"}
