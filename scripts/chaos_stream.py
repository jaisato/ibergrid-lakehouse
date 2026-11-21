"""Crash/restart proof of the streaming lane (`make chaos-stream`, ADR 0006).

In a throwaway compose project (its own secrets and volumes, no host ports, removed at the end)
recorded REData answers go through the real producer, broker and consumer containers while the
consumer is SIGKILLed at the two points where a naive design loses or duplicates data:

1. between the write and the publish: the batch is on an audit branch, and neither main nor the
   offsets in the table's properties have moved;
2. between the publish and the consumer-group commit: rows and offsets are in main, the group
   still points before the batch.

After each kill a consumer restarts from the table's checkpoint. The proof: the table holds every
unique event once (one recorded answer is produced twice, as a restarted producer re-emits its
window), each Kafka message at most once, the checkpoint and the group at the end of every
partition, and no audit branch left.

Needs the app image (`make build`). Exits non-zero on the first broken expectation. The project's
networks get subnets of 10.213.0.0/16, so it also runs on a Docker host whose default address
pools are used up by other projects.
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
WAIT = 180  # seconds for any expected state
# The recorded answers are from 2026: a duplicate check reaching back further than the default
# three days lets it recognise them, whatever today is. The policy itself is the same.
CONSUMER = {"FLUSH_AGE": "1", "DEDUP_DAYS": "3650"}

NETWORKS = ("storage", "catalog", "broker", "catalog-db", "orchestration", "orchestration-db")
SUBNETS = "networks:\n" + "".join(
    f"  {name}:\n    ipam:\n      config:\n        - subnet: 10.213.{n}.0/24\n"
    for n, name in enumerate(NETWORKS, start=1)
)

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
columns = ("series_id", "ts_utc", "kafka_partition", "kafka_offset")
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
checkpoint = read_checkpoint(table, s.stream.group_id, s.stream.topic)
kafka.close()
print(json.dumps({
    "rows": len(rows),
    "messages": len({(r["kafka_partition"], r["kafka_offset"]) for r in rows}),
    "keys": len({(r["series_id"], r["ts_utc"].isoformat()) for r in rows}),
    "checkpoint": {str(p): o for p, o in checkpoint.items()},
    "end": ends[s.stream.topic],
    "dead_letters": sum(ends[s.stream.dlq_topic].values()),
    "committed": committed,
    "branches": wap.audit_branches(table),
}))
"""


class Chaos:
    def __init__(self, work: Path) -> None:
        (work / "subnets.yaml").write_text(SUBNETS)
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

    def produce(self, fixture: str) -> str:
        mount = f"{FIXTURES}:/fixtures:ro"
        return self.compose(
            "run", "--rm", "--no-deps", "-v", mount, "stream-producer",
            "stream", "produce", "--source-file", f"/fixtures/{fixture}",
        ).strip()  # fmt: skip

    def consumer(self, name: str, **env: str) -> str:
        container = f"{PROJECT}-{name}"
        settings = [
            f"--env=IBERGRID_STREAM_{key}={value}" for key, value in (CONSUMER | env).items()
        ]
        self.compose("run", "-d", "--no-deps", "--name", container, *settings, "stream-consumer")
        self.consumers.append(container)
        return container

    def logs(self, container: str) -> str:
        return subprocess.run(  # noqa: S603
            ["docker", "logs", container],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        ).stderr

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


def points(fixture: str) -> int:
    body = json.loads((FIXTURES / fixture).read_text())
    return sum(len(series["attributes"]["values"]) for series in body["included"])


def expect(what: str, actual: object, expected: object) -> None:
    status = "ok  " if actual == expected else "FAIL"
    print(f"{status} {what}: {actual}" + ("" if actual == expected else f" (expected {expected})"))
    if actual != expected:
        raise SystemExit(1)


def main() -> None:
    started = time.monotonic()
    first, second = points(FIRST), points(SECOND)
    with tempfile.TemporaryDirectory() as work:
        subprocess.run([REPO / "scripts" / "gen-env.sh", Path(work) / ".env"], check=True)  # noqa: S603
        chaos = Chaos(Path(work))
        try:
            print(f"chaos: project {PROJECT}; {first} + {second} recorded points")
            chaos.compose("run", "--rm", "doctor", "lake", "init")  # brings the stack up
            print(chaos.produce(FIRST))

            # 1. SIGKILL between the write and the publish.
            # One batch of exactly the recorded points, whatever the timing.
            whole = {"FLUSH_RECORDS": str(first), "FLUSH_AGE": "600"}
            killed = chaos.consumer("write", HOLD_AT="after-write", **whole)
            chaos.wait_for_log(killed, "stream.consumer.hold")
            chaos.kill(killed)
            state = chaos.inspect()
            print("chaos: killed between write and publish")
            expect("rows in main", state["rows"], 0)
            expect("checkpoint", state["checkpoint"], {})
            expect("audit branches left by the kill", len(state["branches"]), 1)

            # 2. SIGKILL between the publish and the group commit; this one sweeps the branch.
            killed = chaos.consumer(
                "publish", HOLD_AT="after-publish", STALE_BRANCH_AGE="1", **whole
            )
            chaos.wait_for_log(killed, "stream.consumer.hold")
            chaos.kill(killed)
            state = chaos.inspect()
            print("chaos: killed between publish and group commit")
            expect("rows in main", state["rows"], first)
            expect("checkpoint", state["checkpoint"], state["end"])
            expect("group offsets", state["committed"], {})
            expect("audit branches", state["branches"], [])

            # 3. The producer restarts (its whole window again) and a new day arrives.
            print(chaos.produce(FIRST))
            print(chaos.produce(SECOND))
            survivor = chaos.consumer("survivor")
            deadline = time.monotonic() + WAIT
            while (state := chaos.inspect())["checkpoint"] != state["end"]:
                if time.monotonic() > deadline:
                    sys.exit(f"the consumer never caught up: {state}\n{chaos.logs(survivor)}")
                time.sleep(3)
            subprocess.run(["docker", "stop", survivor], capture_output=True, check=True)  # noqa: S603,S607
            print("chaos: restarted consumer caught up")
            expect("rows (unique events)", state["rows"], first + second)
            expect("distinct messages", state["messages"], first + second)
            expect("distinct keys", state["keys"], first + second)
            expect("messages in the topic", sum(state["end"].values()), 2 * first + second)
            expect("checkpoint", state["checkpoint"], state["end"])
            expect("group offsets", state["committed"], state["end"])
            expect("audit branches", state["branches"], [])
            expect("dead letters", state["dead_letters"], 0)
            flushed = [
                json.loads(line)
                for line in chaos.logs(survivor).splitlines()
                if '"stream.consumer.flushed"' in line
            ]
            expect("duplicates dropped", sum(f["duplicates"] for f in flushed), first)
            print(f"chaos: passed in {time.monotonic() - started:.0f} s")
        finally:
            chaos.down()


if __name__ == "__main__":
    main()
