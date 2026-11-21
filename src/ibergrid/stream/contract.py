"""The event contract of ``redata.demand.realtime.v1`` (ADR 0006).

``DemandEvent`` is the contract: the producer builds every message from it, the consumer
validates every message against it, and ``contracts/redata-demand-realtime.v1.json`` is its JSON
Schema for anyone else (a unit test fails when the committed file differs from ``json_schema()``;
``make contracts`` rewrites it). A breaking change is a new topic (``.v2``), never an edit of this
one.

A message's key is ``<series_id>|<ts_utc>`` (``2037|2026-11-21T10:05:00Z``): every version of a
point lands in the same partition, in order. Its value is the event as JSON, and
``content_sha256`` fingerprints what the point says (series, system, instant and value), so a
repeat is recognisable by key and hash and a revision by key alone.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

TOPIC = "redata.demand.realtime.v1"
DLQ_TOPIC = "redata.demand.realtime.dlq.v1"
STEP = timedelta(minutes=5)


class ProducerInfo(BaseModel):
    """Who emitted the event, and when."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=100)
    version: str = Field(min_length=1, max_length=50)
    instance: str = Field(min_length=1, max_length=255, description="host name of the producer")
    emitted_at: AwareDatetime


class DemandEvent(BaseModel):
    """One five-minute point of one series (Real, Forecasted, Scheduled, Total scheduled) of
    REData's national real-time demand (``demanda/demanda-tiempo-real``), in MW."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, allow_inf_nan=False, title="redata.demand.realtime.v1"
    )

    series_id: str = Field(
        pattern=r"^[A-Za-z0-9._-]{1,64}$", description="JSON:API id of the REData series"
    )
    series_title: str | None = Field(default=None, max_length=200)
    electric_system: Literal["national"]
    ts_utc: AwareDatetime = Field(description="start of the five minutes, UTC (offset +00:00)")
    ts_local: str = Field(
        max_length=40, description="the datetime as REData delivered it, with its offset"
    )
    value: float = Field(description="MW; finite")
    series_last_update: AwareDatetime | None = Field(
        default=None, description="REData's last-update of the series when it was fetched"
    )
    content_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$",
        description=(
            "SHA-256 of the JSON {electric_system, series_id, ts_utc, value} with sorted keys, "
            "no spaces, ts_utc as YYYY-MM-DDTHH:MM:SSZ and value as a float (Python's repr)"
        ),
    )
    producer: ProducerInfo

    @field_validator("ts_utc")
    @classmethod
    def _utc_on_the_grid(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("must be in UTC (offset +00:00)")
        if value.second or value.microsecond or value.minute % 5:
            raise ValueError("must fall on a five-minute boundary")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        try:
            local = datetime.fromisoformat(self.ts_local)
        except ValueError:
            local = None
        if local is None or local.tzinfo is None or local != self.ts_utc:
            raise ValueError("ts_local is not an ISO datetime with an offset at the instant ts_utc")
        expected = content_sha256(self.series_id, self.electric_system, self.ts_utc, self.value)
        if self.content_sha256 != expected:
            raise ValueError(
                "content_sha256 does not match series_id, electric_system, ts_utc, value"
            )
        return self

    @property
    def key(self) -> str:
        return event_key(self.series_id, self.ts_utc)


def event_key(series_id: str, ts_utc: datetime) -> str:
    return f"{series_id}|{ts_utc.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}"


def content_sha256(series_id: str, electric_system: str, ts_utc: datetime, value: float) -> str:
    canonical = json.dumps(
        {
            "electric_system": electric_system,
            "series_id": series_id,
            "ts_utc": f"{ts_utc.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}",
            "value": float(value),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def json_schema() -> str:
    """The contract's JSON Schema, as committed in ``contracts/``."""
    schema = {"$schema": "https://json-schema.org/draft/2020-12/schema"}
    return json.dumps(schema | DemandEvent.model_json_schema(), indent=2, sort_keys=True) + "\n"


def describe(exc: ValidationError) -> str:
    """One clause per problem, without the input values (they may be anything)."""
    return "; ".join(
        f"{'.'.join(map(str, error['loc'])) or 'event'}: {error['msg']}"
        for error in exc.errors(include_input=False, include_url=False)
    )
