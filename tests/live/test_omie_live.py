"""Opt-in smoke test against omie.es: `make test-live`. Never run by default or in CI.

Eight requests, paced at one a second: a recent delivery day's file still downloads and meets its
contract, a day OMIE republished is still found under its version (.1 answers 404, .2 is the
file), and a day far ahead still answers 404 for every version; nothing is written anywhere.
"""

from datetime import date, datetime, timedelta

import pytest

from ibergrid.sources.omie.client import OmieClient
from ibergrid.sources.omie.marginalpdbc import MADRID, contract_for, expected_periods, parse

pytestmark = pytest.mark.live

CLIENT = OmieClient("https://www.omie.es", timeout=60)  # one client: one pace for all


def test_a_recent_day_downloads_and_meets_its_contract() -> None:
    day = datetime.now(MADRID).date() - timedelta(days=2)

    found = CLIENT.fetch(day)

    assert found is not None
    parsed = parse(found.payload, day, found.version)
    assert parsed.contract == contract_for(day)
    assert len(parsed.periods) == expected_periods(day, parsed.contract)


def test_a_republished_day_is_found_under_its_version() -> None:
    found = CLIENT.fetch(date(2023, 5, 28))

    assert found is not None
    assert found.name == "marginalpdbc_20230528.2"
    assert len(parse(found.payload, found.day, found.version).periods) == 24


def test_a_day_far_ahead_is_not_published() -> None:
    assert CLIENT.fetch(datetime.now(MADRID).date() + timedelta(days=400)) is None
