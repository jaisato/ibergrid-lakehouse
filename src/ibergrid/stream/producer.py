"""REData's national real-time demand into ``redata.demand.realtime.v1`` (ADR 0006).

Every ``poll_interval`` (five minutes) the producer asks REData for today's points, and also for
yesterday's until ``midnight_grace`` after local midnight, through the batch lane's client (at most
one request a second, bounded retries; a window without data is REData's 502, ADR 0003) and
parser, and emits each point whose content changed since it last emitted that key. That memory is
the process's own: after a restart the whole window goes out again and the consumer drops the
exact repeats. A failed poll is logged and retried at the next tick, a failed delivery too:
events the broker did not acknowledge never count as emitted, so the next poll sends them again.
"""

import json
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import version

import structlog
from confluent_kafka import KafkaError, Message, Producer
from pydantic import ValidationError

from ibergrid.sources.redata.client import RedataError
from ibergrid.sources.redata.datasets import DATASETS, MADRID, Chunk
from ibergrid.sources.redata.normalize import parse
from ibergrid.stream.contract import DemandEvent, ProducerInfo, content_sha256, describe
from ibergrid.stream.health import Watchdog, beat

log = structlog.get_logger(__name__)

DATASET = DATASETS["demand_realtime"]
NATIONAL = DATASET.systems[0]
NAME = "ibergrid-stream-producer"
BEAT_EVERY = 30.0  # seconds between heartbeats while waiting for the next poll


class DeliveryFailed(RuntimeError):
    """The broker did not acknowledge every event of a poll."""


@dataclass(frozen=True)
class PollReport:
    start: date
    end: date
    points: int
    emitted: int
    suppressed: int  # same key and content as the last emitted
    rejected: int  # points the contract refuses (logged, never emitted)


class StreamProducer:
    def __init__(
        self,
        sink: Producer,
        topic: str,
        fetch: Callable[[Chunk], bytes],
        *,
        midnight_grace: timedelta = timedelta(hours=2),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        instance: str | None = None,
    ) -> None:
        self.sink = sink
        self.topic = topic
        self.fetch = fetch
        self.midnight_grace = midnight_grace
        self.clock = clock
        self.instance = instance or socket.gethostname()
        self.seen: dict[str, tuple[datetime, str]] = {}  # key -> (ts_utc, content hash)
        self.totals = {"polls": 0, "failed_polls": 0, "emitted": 0, "suppressed": 0}

    def window(self, now: datetime) -> Chunk:
        """Today in Madrid, from yesterday while ``now`` is within the grace after midnight."""
        local = now.astimezone(MADRID)
        today = local.date()
        midnight = datetime.combine(today, datetime.min.time(), MADRID)
        start = today - timedelta(days=1) if local - midnight < self.midnight_grace else today
        return Chunk(DATASET, NATIONAL, start, today)

    def poll_once(self) -> PollReport:
        chunk = self.window(self.clock())
        return self.emit(self.fetch(chunk), chunk)

    def emit(self, payload: bytes, chunk: Chunk) -> PollReport:
        """Emit the answer's changed points and wait until the broker has acknowledged them."""
        points = parse(payload, chunk)
        info = ProducerInfo(
            name=NAME, version=version("ibergrid"), instance=self.instance, emitted_at=self.clock()
        )
        failures: list[KafkaError] = []

        def delivered(error: KafkaError | None, _message: Message) -> None:
            if error is not None:
                failures.append(error)

        pending: dict[str, tuple[datetime, str]] = {}
        suppressed = rejected = 0
        for point in points:
            try:
                event = DemandEvent(
                    series_id=point.series_id,
                    series_title=point.series_title,
                    electric_system="national",
                    ts_utc=point.ts_utc,
                    ts_local=point.ts_local,
                    value=point.value,
                    series_last_update=point.series_last_update,
                    content_sha256=content_sha256(
                        point.series_id, "national", point.ts_utc, point.value
                    ),
                    producer=info,
                )
            except ValidationError as exc:
                rejected += 1
                log.warning(
                    "stream.producer.rejected",
                    series_id=point.series_id,
                    ts_local=point.ts_local,
                    error=describe(exc),
                )
                continue
            if self.seen.get(event.key, (None, None))[1] == event.content_sha256:
                suppressed += 1
                continue
            # Forget any prior value for this key before handing the revision to the broker: if
            # this delivery lands and a later one in the same poll fails, a reversion back to the
            # old value on the next poll must still be emitted.
            self.seen.pop(event.key, None)
            self.sink.produce(
                self.topic,
                value=event.model_dump_json().encode(),
                key=event.key.encode(),
                on_delivery=delivered,
            )
            self.sink.poll(0)
            pending[event.key] = (event.ts_utc, event.content_sha256)
        undelivered = self.sink.flush()
        if undelivered or failures:
            raise DeliveryFailed(
                f"{undelivered} event(s) undelivered and {len(failures)} refused by the broker: "
                + "; ".join(str(error) for error in failures[:3])
            )
        self.seen.update(pending)
        first = chunk.window_utc[0]
        self.seen = {key: seen for key, seen in self.seen.items() if seen[0] >= first}
        return PollReport(chunk.start, chunk.end, len(points), len(pending), suppressed, rejected)

    def run(
        self,
        stop: threading.Event,
        interval: float,
        heartbeat: str,
        watchdog: Watchdog | None = None,
    ) -> None:
        """Poll every ``interval`` seconds until ``stop`` is set, beating while it waits. A poll
        that fails (REData, or a broker that did not acknowledge every event) is retried at the
        next tick: nothing of it counts as emitted, so the next poll sends it again."""
        failing_since: float | None = None

        def alive() -> None:
            if watchdog is not None:
                watchdog.touch()
            beat(heartbeat, role="producer", failing_since=failing_since, **self.totals)

        alive()
        next_poll = time.monotonic()
        while not stop.is_set():
            started = time.monotonic()
            try:
                report = self.poll_once()
            except (RedataError, DeliveryFailed) as exc:
                self.totals["failed_polls"] += 1
                failing_since = failing_since or time.time()
                log.warning("stream.producer.poll_failed", error=str(exc))
            else:
                failing_since = None
                self.totals["polls"] += 1
                self.totals["emitted"] += report.emitted
                self.totals["suppressed"] += report.suppressed
                log.info(
                    "stream.producer.polled",
                    window=f"{report.start}/{report.end}",
                    points=report.points,
                    emitted=report.emitted,
                    suppressed=report.suppressed,
                    rejected=report.rejected,
                    seconds=round(time.monotonic() - started, 2),
                )
            alive()
            next_poll = max(next_poll + interval, time.monotonic())
            while (wait := next_poll - time.monotonic()) > 0 and not stop.wait(
                min(wait, BEAT_EVERY)
            ):
                alive()


def replay_chunk(payload: bytes) -> Chunk:
    """The window of a recorded answer (``stream produce --source-file``): its first to its last
    local day, so that the parser's window check passes."""
    try:
        days = {
            datetime.fromisoformat(point["datetime"]).astimezone(MADRID).date()
            for series in json.loads(payload)["included"]
            for point in series["attributes"]["values"]
        }
    except (ValueError, KeyError, TypeError) as exc:
        raise RedataError(f"not a recorded REData answer: {exc}") from exc
    if not days:
        raise RedataError("the recorded answer has no points")
    return Chunk(DATASET, NATIONAL, min(days), max(days))
