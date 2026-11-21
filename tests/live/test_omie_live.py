"""Opt-in smoke test against omie.es: `make test-live`. Never run by default or in CI.

Two requests, paced at one a second: a recent delivery day's file still downloads and meets its
contract, and a day far ahead still answers 404 ("not published"); nothing is written anywhere.
"""

from datetime import datetime, timedelta

import pytest

from ibergrid.sources.omie.client import OmieClient
from ibergrid.sources.omie.marginalpdbc import MADRID, contract_for, expected_periods, parse

pytestmark = pytest.mark.live

CLIENT = OmieClient("https://www.omie.es", timeout=60)  # one client: one pace for all


def test_a_recent_day_downloads_and_meets_its_contract() -> None:
    day = datetime.now(MADRID).date() - timedelta(days=2)

    payload = CLIENT.fetch(day)

    assert payload is not None
    parsed = parse(payload, day)
    assert parsed.contract == contract_for(day)
    assert len(parsed.periods) == expected_periods(day, parsed.contract)


def test_a_day_far_ahead_is_not_published() -> None:
    assert CLIENT.fetch(datetime.now(MADRID).date() + timedelta(days=400)) is None
