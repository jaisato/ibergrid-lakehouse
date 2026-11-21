"""Opt-in smoke test against the live REData API: `make test-live`. Never run by default or in CI.

One request per dataset for the day before yesterday (Madrid), paced at one a second; it checks
that the answers still parse, validate and come complete, nothing is written anywhere.
"""

from datetime import datetime, timedelta

import pytest

from ibergrid.sources.redata.client import RedataClient
from ibergrid.sources.redata.datasets import DATASETS, MADRID, plan
from ibergrid.sources.redata.normalize import completeness, granularities, parse

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
