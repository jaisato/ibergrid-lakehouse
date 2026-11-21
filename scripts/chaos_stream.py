"""Crash/restart proof of the streaming lane (`make chaos-stream`, ADR 0006).

In a throwaway compose project (its own secrets and volumes, no host ports, removed at the end)
recorded REData answers go through the real producer, broker and consumer containers while the
consumer is SIGKILLed at the points where a naive design loses or duplicates data:

1. between the write and the publish: the batch is on an audit branch, and neither main nor the
   checkpoint in the table's properties has moved;
2. between the publish and the consumer-group commit: rows and checkpoint are in main, the group
   still points before the batch;
3. right after a checkpoint-only publish (a batch of duplicates and a dead letter, no row): the
   checkpoint moved past them, the group did not.

After each kill a consumer restarts from the table's checkpoint; the last one also receives
revisions and a new day. The proof: the table holds every unique version once (one recorded
answer is produced three times, as restarted producers re-emit their window, once with revised
values), each Kafka message at most once, the dead letter once, checkpoint and group at the end of
every partition, and no audit branch left.

Needs the app image (`make build`). Exits non-zero on the first broken expectation. The project's
networks get subnets of 10.213.0.0/16, so it also runs on a Docker host whose default address
pools are used up by other projects. The hold points are a constructor argument of
StreamConsumer, so the consumers here run a few lines of Python in the consumer's image instead
of `ibergrid stream consume`.
"""

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
PROJECT = "ibergrid-chaos"
FIXTURES = REPO / "tests" / "fixtures" / "redata"
FIRST = "demand_realtime-national-2026-11-18_2026-11-18.json"
SECOND = "demand_realtime-national-2026-10-25_2026-10-25.json"
REVISED = 7  # points of FIRST whose value the third production changes
WAIT = 180  # seconds for any expected state
# The recorded answers are from 2026: a duplicate check reaching back further than the default
# three days lets it recognise them, whatever today is. The policy itself is the same (unit tests
# pin the defaults).
CONSUMER = {"FLUSH_AGE": "1", "DEDUP_DAYS": "3650"}
NETWORKS = ("storage", "catalog", "broker", "catalog-db", "orchestration", "orchestration-db")
SUBNETS = "networks:\n" + "".join(
    f"  {name}:\n    ipam:\n      config:\n        - subnet: 10.213.{n}.0/24\n"
    for n, name in enumerate(NETWORKS, start=1)
)

# A consumer as `ibergrid stream consume` builds it, optionally held at a point of its flush.
CONSUME = """
import sys
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from ibergrid.cli import _stop_on_signals
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.logging import configure_logging
from ibergrid.settings import Settings
from ibergrid.stream.consumer import StreamConsumer
from ibergrid.stream.kafka import client_config, consumer_config, producer_config

s = Settings()
configure_logging(s.log)
table = rest_catalog(s).load_table(REDATA_DEMAND_STREAM.identifier)
kafka = Consumer(consumer_config(s.broker, "chaos-consumer", s.stream.group_id))
dlq = Producer(producer_config(s.broker, "chaos-consumer-dlq"))
admin = AdminClient(client_config(s.broker, "chaos-consumer"))
hold = sys.argv[1] if len(sys.argv) > 1 else None
StreamConsumer(table, kafka, dlq, admin, s.stream, hold_at=hold).run(_stop_on_signals())
"""

# One message that is not an event, as the producer's user.
GARBAGE = """
from confluent_kafka import Producer
from ibergrid.settings import BrokerSettings, StreamSettings
from ibergrid.stream.kafka import producer_config

producer = Producer(producer_config(BrokerSettings(), "chaos-garbage"))
producer.produce(StreamSettings().topic, value=b"not an event", key=b"chaos|garbage")
assert producer.flush(30) == 0
"""

# Runs in the stream-consumer image, with its keys: the table, the checkpoint, the topic's ends.
INSPECT = """
import json
from confluent_kafka import Consumer, TopicPartition
from ibergrid.lake import wap
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.registry import REDATA_DEMAND_STREAM
from ibergrid.settings import Settings
from ibergrid.stream.consumer import read_checkpoint, topic_partitions
from ibergrid.stream.kafka import consumer_config

s = Settings()
table = rest_catalog(s).load_table(REDATA_DEMAND_STREAM.identifier)
columns = ("series_id", "ts_utc", "content_sha256", "kafka_partition", "kafka_offset")
rows = []
if table.current_snapshot() is not None:
    rows = table.scan(selected_fields=columns).to_arrow().to_pylist()
kafka = Consumer(consumer_config(s.broker, "chaos-inspect", s.stream.group_id))
ends = {}
for topic in (s.stream.topic, s.stream.dlq_topic):
    ends[topic] = {
        str(p): kafka.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
        for p in topic_partitions(kafka, topic)
    }
asked = [TopicPartition(s.stream.topic, int(p)) for p in ends[s.stream.topic]]
committed = {
    str(tp.partition): tp.offset for tp in kafka.committed(asked, timeout=10) if tp.offset >= 0
}
checkpoint = read_checkpoint(table, s.stream.group_id)
kafka.close()
print(json.dumps({
    "rows": len(rows),
    "messages": len({(r["kafka_partition"], r["kafka_offset"]) for r in rows}),
    "keys": len({(r["series_id"], r["ts_utc"].isoformat()) for r in rows}),
    "versions": len({(r["series_id"], r["ts_utc"].isoformat(), r["content_sha256"]) for r in rows}),
    "checkpoint": {} if checkpoint is None else {str(p): o for p, o in checkpoint.offsets.items()},
    "end": ends[s.stream.topic],
    "dead_letters": sum(ends[s.stream.dlq_topic].values()),
    "committed": committed,
    "branches": wap.audit_branches(table),
}))
"""


class Chaos:
    def __init__(self, work: Path) -> None:
        (work / "subnets.yaml").write_text(SUBNETS)
        self.work = work
        self.base = ["docker", "compose", "-p", PROJECT, "--env-file", str(work / ".env")]
        self.base += ["-f", "compose.yaml", "-f", str(work / "subnets.yaml")]
        self.base += ["--profile", "local-storage"]
        self.consumers: list[str] = []

    def compose(self, *args: str) -> str:
        result = subprocess.run(  # noqa: S603 (fixed docker compose command)
            [*self.base, *args], cwd=REPO, capture_output=True, text=True, timeout=600, check=False
        )
        if result.returncode:
            sys.exit(f"docker compose {' '.join(args[:4])} failed:\n{result.stderr[-3000:]}")
        return result.stdout

    def produce(self, path: Path) -> str:
        return self.compose(
            "run", "--rm", "--no-deps", "-v", f"{path.parent}:/fixtures:ro", "stream-producer",
            "stream", "produce", "--source-file", f"/fixtures/{path.name}",
        ).strip()  # fmt: skip

    def garbage(self) -> None:
        self.compose(
            "run", "--rm", "--no-deps", "--entrypoint", "python", "stream-producer", "-c", GARBAGE
        )

    def consumer(self, name: str, hold: str | None = None, **env: str) -> str:
        container = f"{PROJECT}-{name}"
        settings = [
            f"--env=IBERGRID_STREAM_{key}={value}" for key, value in (CONSUMER | env).items()
        ]
        self.compose(
            "run", "-d", "--no-deps", "--name", container, *settings, "--entrypoint", "python",
            "stream-consumer", "-c", CONSUME, *([hold] if hold else []),
        )  # fmt: skip
        self.consumers.append(container)
        return container

    def logs(self, container: str) -> str:
        return subprocess.run(  # noqa: S603
            ["docker", "logs", container],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        ).stderr

    def flushes(self, container: str) -> list[dict[str, Any]]:
        return [
            json.loads(line)
            for line in self.logs(container).splitlines()
            if '"stream.consumer.flushed"' in line
        ]

    def wait_for_log(self, container: str, event: str) -> None:
        deadline = time.monotonic() + WAIT
        while event not in self.logs(container):
            if time.monotonic() > deadline:
                sys.exit(f"{container} never logged {event}:\n{self.logs(container)[-3000:]}")
            time.sleep(1)

    def kill(self, container: str) -> None:
        subprocess.run(  # noqa: S603
            ["docker", "kill", "--signal", "KILL", container],  # noqa: S607
            capture_output=True,
            check=True,
        )

    def inspect(self) -> dict[str, Any]:
        out = self.compose(
            "run", "--rm", "--no-deps", "--entrypoint", "python", "stream-consumer", "-c", INSPECT
        )
        return dict(json.loads(out.strip().splitlines()[-1]))

    def down(self) -> None:
        for container in self.consumers:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)  # noqa: S603,S607
        subprocess.run(  # noqa: S603
            [*self.base, "--profile", "stream", "down", "--volumes", "--remove-orphans"],
            cwd=REPO,
            capture_output=True,
            check=False,
        )


def points(path: Path) -> int:
    body = json.loads(path.read_text())
    return sum(len(series["attributes"]["values"]) for series in body["included"])


def revised(work: Path) -> Path:
    """FIRST with its first REVISED values changed."""
    body = json.loads((FIXTURES / FIRST).read_text())
    for point in body["included"][0]["attributes"]["values"][:REVISED]:
        point["value"] += 1
    path = work / "revised" / FIRST
    path.parent.mkdir()
    path.write_text(json.dumps(body))
    return path


def expect(what: str, actual: object, expected: object) -> None:
    status = "ok  " if actual == expected else "FAIL"
    print(f"{status} {what}: {actual}" + ("" if actual == expected else f" (expected {expected})"))
    if actual != expected:
        raise SystemExit(1)


def main() -> None:
    started = time.monotonic()
    first, second = points(FIXTURES / FIRST), points(FIXTURES / SECOND)
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        subprocess.run([REPO / "scripts" / "gen-env.sh", work / ".env"], check=True)  # noqa: S603
        chaos = Chaos(work)
        try:
            print(f"chaos: project {PROJECT}; {first} + {second} recorded points")
            chaos.compose("run", "--rm", "doctor", "lake", "init")  # brings the stack up
            print(chaos.produce(FIXTURES / FIRST))

            # 1. SIGKILL between the write and the publish.
            # One batch of exactly the recorded points, whatever the timing.
            whole = {"FLUSH_RECORDS": str(first), "FLUSH_AGE": "600"}
            killed = chaos.consumer("write", "after-write", **whole)
            chaos.wait_for_log(killed, "stream.consumer.hold")
            chaos.kill(killed)
            state = chaos.inspect()
            print("chaos: killed between write and publish")
            expect("rows in main", state["rows"], 0)
            expect("checkpoint", state["checkpoint"], {})
            expect("audit branches left by the kill", len(state["branches"]), 1)

            # 2. SIGKILL between the publish and the group commit; this one sweeps the branch.
            killed = chaos.consumer("publish", "after-publish", STALE_BRANCH_AGE="1", **whole)
            chaos.wait_for_log(killed, "stream.consumer.hold")
            chaos.kill(killed)
            state = chaos.inspect()
            print("chaos: killed between publish and group commit")
            expect("rows in main", state["rows"], first)
            expect("checkpoint", state["checkpoint"], state["end"])
            expect("group offsets", state["committed"], {})
            expect("audit branches", state["branches"], [])

            # 3. A restarted producer re-emits its window, plus a message that is no event: the
            #    batch has no row, so its flush is a checkpoint; SIGKILL right after it.
            print(chaos.produce(FIXTURES / FIRST))
            chaos.garbage()
            killed = chaos.consumer("checkpoint", "after-publish", CHECKPOINT_INTERVAL="1")
            chaos.wait_for_log(killed, "stream.consumer.hold")
            chaos.kill(killed)
            state = chaos.inspect()
            print("chaos: killed after a checkpoint-only publish")
            expect("rows in main", state["rows"], first)
            expect(
                "checkpoint past the repeats and the dead letter", state["checkpoint"], state["end"]
            )
            expect("dead letters", state["dead_letters"], 1)
            expect("group offsets", state["committed"], {})

            # 4. Revisions and a new day; a consumer catches up and stops cleanly.
            print(chaos.produce(revised(work)))
            print(chaos.produce(FIXTURES / SECOND))
            survivor = chaos.consumer("survivor")
            deadline = time.monotonic() + WAIT
            while (state := chaos.inspect())["checkpoint"] != state["end"]:
                if time.monotonic() > deadline:
                    sys.exit(f"the consumer never caught up: {state}\n{chaos.logs(survivor)}")
                time.sleep(3)
            subprocess.run(["docker", "stop", survivor], capture_output=True, check=True)  # noqa: S603,S607
            print("chaos: restarted consumer caught up")
            unique = first + REVISED + second
            expect("rows (unique versions)", state["rows"], unique)
            expect("distinct messages", state["messages"], unique)
            expect("distinct versions", state["versions"], unique)
            expect("distinct keys", state["keys"], first + second)
            expect("messages in the topic", sum(state["end"].values()), 3 * first + 1 + second)
            expect("checkpoint", state["checkpoint"], state["end"])
            expect("group offsets", state["committed"], state["end"])
            expect("audit branches", state["branches"], [])
            expect("dead letters (not repeated)", state["dead_letters"], 1)
            flushed = chaos.flushes(survivor)
            expect("duplicates dropped", sum(f["duplicates"] for f in flushed), first - REVISED)
            expect("revisions kept", sum(f["revisions"] for f in flushed), REVISED)
            print(f"chaos: passed in {time.monotonic() - started:.0f} s")
        finally:
            chaos.down()


if __name__ == "__main__":
    main()
