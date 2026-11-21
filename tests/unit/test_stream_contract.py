"""The event contract: what it accepts, what it refuses, and the committed JSON Schema."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ibergrid.stream.contract import DemandEvent, content_sha256, describe, event_key, json_schema

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "contracts" / "redata-demand-realtime.v1.json"
TS = datetime(2026, 11, 18, 23, 55, tzinfo=UTC)


def event_fields(**changes: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "series_id": "2037",
        "series_title": "Real",
        "electric_system": "national",
        "ts_utc": "2026-11-18T23:55:00Z",
        "ts_local": "2026-11-19T00:55:00.000+01:00",
        "value": 27610.0,
        "series_last_update": "2026-11-19T23:57:06+01:00",
        "content_sha256": content_sha256("2037", "national", TS, 27610.0),
        "producer": {
            "name": "ibergrid-stream-producer",
            "version": "0.1.0",
            "instance": "host",
            "emitted_at": "2026-11-19T00:01:00Z",
        },
    }
    return fields | changes


def test_a_valid_event_round_trips_through_json_with_its_key() -> None:
    event = DemandEvent.model_validate(event_fields())

    again = DemandEvent.model_validate_json(event.model_dump_json())

    assert again == event
    assert event.key == "2037|2026-11-18T23:55:00Z" == event_key("2037", TS)
    assert json.loads(event.model_dump_json())["ts_utc"] == "2026-11-18T23:55:00Z"


def test_the_hash_covers_series_system_instant_and_value_only() -> None:
    base = content_sha256("2037", "national", TS, 27610)
    assert base == content_sha256("2037", "national", TS.astimezone(), 27610.0)
    assert base != content_sha256("2037", "national", TS, 27611.0)
    assert base != content_sha256("2052", "national", TS, 27610.0)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"ts_utc": "2026-11-19T00:55:00+01:00"}, "must be in UTC"),
        ({"ts_utc": "2026-11-18T23:56:00Z"}, "five-minute boundary"),
        ({"ts_local": "2026-11-19T00:50:00+01:00"}, "ts_local is not"),
        ({"ts_local": "2026-11-18T23:55:00"}, "ts_local is not"),
        ({"ts_local": "yesterday"}, "ts_local is not"),
        ({"value": 27611.0}, "content_sha256 does not match"),
        ({"content_sha256": "0" * 63}, "String should match pattern"),
        ({"series_id": "20|37"}, "String should match pattern"),
        ({"electric_system": "peninsular"}, "Input should be 'national'"),
        ({"unexpected": 1}, "Extra inputs are not permitted"),
    ],
)
def test_the_contract_refuses(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError) as caught:
        DemandEvent.model_validate(event_fields(**changes))

    assert message in describe(caught.value)


def test_a_non_finite_value_is_refused_in_json() -> None:
    raw = json.dumps(event_fields()).replace("27610.0", "NaN")

    with pytest.raises(ValidationError) as caught:
        DemandEvent.model_validate_json(raw)

    assert describe(caught.value).startswith("value:")


def test_the_committed_json_schema_is_the_contract() -> None:
    """Edit the model, then `make contracts`: a drifted file fails here."""
    assert SCHEMA_FILE.read_text() == json_schema()
    schema = json.loads(json_schema())
    assert schema["title"] == "redata.demand.realtime.v1"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "series_id",
        "electric_system",
        "ts_utc",
        "ts_local",
        "value",
        "content_sha256",
        "producer",
    }
