# 0006. Streaming lane: REData real-time demand through Redpanda into Iceberg, exactly once

- Status: accepted
- Date: 2026-11-21

## Context and problem statement

The batch lane ([ADR 0003](0003-redata-ingestion.md), [ADR 0005](0005-dagster-orchestration.md))
loads closed days of REData once a day. Phase 2 adds the lane [ADR 0001](0001-stack-and-scope.md)
planned for frequent updates: REData's national real-time demand, a point every five minutes per
series, visible in the lake minutes after REData publishes it. Redpanda is already in the stack,
unused and unauthenticated (SECURITY.md, "Known gaps"). Four questions need answers:

1. How do events get from REData to Iceberg so that every event lands once in Bronze, across
   crashes, without Kafka transactions (which would not cover the Iceberg commit anyway)?
2. What is the event's contract, what happens to a message that breaks it, and what is a
   duplicate versus a revision?
3. Where do the events land, and how large are the micro-batches, given small files and the cost
   of a checkpoint ([ADR 0002](0002-iceberg-write-and-read-paths.md))?
4. How is the broker secured, in every environment, and how does production differ from a laptop?

## Decision drivers

- Exactly once into Bronze through the one fenced write path there is: `wap.publish(...,
  set_properties=...)` (ADR 0002). No second checkpoint store.
- No JVM and no new service besides the broker that is already there; one container per role.
- Polite to REData: one request per five minutes, through the existing client (at most one
  request a second, bounded retries, its 502 semantics from ADR 0003).
- Bounded small files and catalog commits: idle time must not cost a snapshot every second.
- Least privilege on the broker, the same locally and in production.
- Every claim measured, and pinned by a test where possible.

## Considered options

1. **Broker**: Redpanda (in the stack since phase 0); Apache Kafka in KRaft mode; no broker (the
   producer writes Iceberg itself).
2. **Exactly once**: offsets as table properties in the fenced publish; Kafka transactions
   (read-process-write); offsets in the consumer group with idempotent (upsert) writes.
3. **Target**: a separate append-only stream table; the batch table
   `bronze.redata_observations`.
4. **Client**: confluent-kafka (librdkafka); a pure-Python client (aiokafka, kafka-python).
5. **Image**: the app image for both services; a third image target with the Kafka client only.

## Measurements

Redpanda 26.2.2 (the pinned image), confluent-kafka 2.16.1 with librdkafka 2.16.0, Lakekeeper
0.15.1, PyIceberg 0.12.0, DuckDB 2.0.1; measured on 2026-11-21 in throwaway containers, on the
local stack and in the chaos project. Unless marked †, a result is pinned by
`tests/integration/test_stream.py`, `make chaos-stream` or a unit test.

| What | Result |
|---|---|
| `--mode dev-container` | Bundles `--unsafe-bypass-fsync`, `--check=false`, `--reserve-memory 0M`, and as bootstrap cluster properties `auto_create_topics_enabled: true` and `write_caching_default: true` (`rpk redpanda start --mode help`) † |
| Cluster properties set through the admin API | Survive the container's recreation on the same volume (`auto_create_topics_enabled=false` read back after `docker rm`) † |
| `enable_sasl=true` | Effective at once, no restart: an anonymous `rpk topic list` then fails with "SASL is required but not provided" |
| `admin_api_require_auth=true` | Only a superuser may set it ("May only be set by a superuser"); afterwards anonymous admin calls get 403. `rpk cluster health` (the broker's healthcheck) and `/v1/status/ready` keep answering without credentials † |
| `rpk security user create` on an existing user | Succeeds with the same password, fails with "User already exists" with another; `user update --new-password` then sets it † |
| Idempotent producer (`enable.idempotence`, `acks=all`) with WRITE and DESCRIBE on the topic only | Delivers: no cluster-level IDEMPOTENT_WRITE ACL is needed |
| The consumer's user produces to the data topic; the producer's to the dead-letter topic | `TOPIC_AUTHORIZATION_FAILED` |
| The producer's user consumes | `TOPIC_AUTHORIZATION_FAILED` or `GROUP_AUTHORIZATION_FAILED` |
| A consumer without READ on the group | Fetches, but `commit` fails (`GROUP_AUTHORIZATION_FAILED`) † |
| Anonymous client, wrong password | Refused (no metadata) |
| Producing to a topic that does not exist, as the superuser | Fails; the topic is not created |
| The image's `/etc/redpanda/redpanda.yaml` | Says `developer_mode: true` even without `--mode`; `rpk redpanda mode production` removes it † |
| Production mode with `--memory=1G` in a 1536 MiB container | Refused: "insufficient physical memory: needed 1073741824 available 500000000"; with `--reserve-memory=0M` it starts, 68 MiB at idle, `auto_create_topics_enabled=false`, `write_caching_default=false` † |
| confluent-kafka 2.16.1 | cp312 manylinux_2_28 wheels for x86_64 (4.96 MB) and aarch64 (5.15 MB), librdkafka with SASL/SCRAM built in; 14.6 MB installed † |
| Image sizes | app 790 MB before, 813 MB with confluent-kafka; orchestration 1.04 GB before, 1.06 GB † |
| `ibergrid version` in the app image | 0.86 s of CPU, 133 MiB maximum RSS per start † |
| 120 one-row flushes on Lakekeeper, from the host | 74.5 s (0.62 s each: branch, append, publish); with `commit.manifest-merge.enabled` `main` holds 21 manifests instead of 120, and DuckDB reads all 120 rows † |
| Chaos (`make chaos-stream`) | See "Proof" below; 46 s end to end |
| Live journey on the local stack | See "Live journey" below † |

## Decision outcome

Chosen: **Redpanda, secured with SASL/SCRAM and ACLs in every environment; a `stream-producer`
and a `stream-consumer` service from the app image, on confluent-kafka; a separate append-only
table `bronze.redata_demand_stream`; exactly once through Kafka offsets committed as table
properties in the fenced Write-Audit-Publish commit; duplicates dropped by key and content hash,
revisions kept; a dead-letter topic.**

```mermaid
flowchart LR
  ree["REData API<br/>demanda-tiempo-real"] -- "HTTPS, 1 request / 5 min" --> producer["stream-producer<br/>(app image)"]
  producer -- "SASL/SCRAM: ibergrid-producer<br/>WRITE data topic" --> topic[("redata.demand.realtime.v1<br/>3 partitions, 7 days")]
  topic -- "SASL/SCRAM: ibergrid-consumer<br/>READ data topic" --> consumer["stream-consumer<br/>(app image)"]
  consumer -- "WRITE" --> dlq[("redata.demand.realtime.dlq.v1<br/>1 partition, 30 days")]
  consumer -- "WAP: branch, append, audit,<br/>publish + offsets property" --> table[("bronze.redata_demand_stream<br/>Iceberg, Lakekeeper")]
  consumer -. "group offsets (lag only)" .-> topic
```

### The contract

`ibergrid.stream.contract.DemandEvent` (pydantic) is the contract and
`contracts/redata-demand-realtime.v1.json` its JSON Schema; a unit test fails when the committed
file differs from the generated one (`make contracts` rewrites it). An event is one point of one
series: `series_id`, `series_title`, `electric_system` (always `national`), `ts_utc` (UTC, on the
five-minute grid), `ts_local` (as delivered, the same instant), `value` (MW, finite),
`series_last_update`, `content_sha256` and `producer` (name, version, host, `emitted_at`).
`extra` fields are refused. The key is `<series_id>|<ts_utc>` (`2037|2026-11-21T10:05:00Z`), so
every version of a point lands in the same partition, in order; the producer uses Java's
partitioner (`murmur2_random`), so another client would put a key in the same partition.

`content_sha256` hashes `{electric_system, series_id, ts_utc, value}` only. REData's `percentage`
is left out of the event: it is each series' share of the sum of all series at that instant, so
it changes for every series whenever Real gets a new point, and would turn every poll into
revisions of points that did not change. A breaking change of the contract is a new topic
(`.v2`), never an edit of this one.

### The producer

Every 300 s (`IBERGRID_STREAM_POLL_INTERVAL`) the producer asks REData for today, and also for
yesterday until two hours after local midnight (`IBERGRID_STREAM_MIDNIGHT_GRACE`), because the
last points of a day arrive after it: one request for the window, through `RedataClient` and
the batch lane's parser, so the same structural checks refuse a changed answer. It remembers, per
key, the hash it last emitted and emits only points whose hash changed: the first poll of a day
emits about a thousand events, later polls the new Real points and every revision (2 to 390 a
poll in the live journey, most of them the day's programs reprogrammed). The memory is
pruned to the window. It is the process's own: after a restart the whole window goes out again,
and the consumer drops the exact repeats. A point the contract refuses (off the grid, say) is
logged and skipped; a poll that fails (no answer, REData's 502, an answer the parser refuses) is
logged and retried at the next tick. Delivery is `acks=all` with idempotence; a poll whose events
the broker did not all acknowledge within two minutes (`message.timeout.ms`) fails the process,
because unacknowledged events must not count as emitted; its container restarts it.

### The consumer

On start, the consumer:

1. discards this lane's stale run branches (below);
2. reads `ibergrid.stream.<group>.<topic>.offsets` from the table's properties (the next offset
   per partition; empty before the first publish): this, not the consumer group, is the
   position;
3. rebuilds the duplicate check from the table: the latest `content_sha256` of every key of the
   last three days (`IBERGRID_STREAM_DEDUP_DAYS`);
4. assigns every partition of the topic at the checkpoint (manual assignment, no group
   rebalancing: one writer owns the table), from the beginning where there is none, and warns
   when a checkpoint is older than the topic's retention (the gap is lost to the stream; the
   batch lane still has those days).

Each message is validated (pydantic), checked against its key and against the duplicate check:

| Message | Outcome |
|---|---|
| Not JSON, breaks the contract, its key is not the event's, or has no value | To the dead-letter topic, unchanged, with headers `ibergrid.dlq.reason` (`contract`, `key`, `no-value`), `.detail` (pydantic's errors without input values), `.source` (`topic/partition/offset`), `.group`, `.failed-at`; counted |
| Same key and hash as the latest accepted version | Dropped, counted as a duplicate |
| Same key, another hash | Kept, counted as a revision |
| A new key | Kept |

The latest version, not any earlier one, is the reference: a value that goes A, B, A again keeps
the third as a revision, so "the row with the highest offset per key" stays the source's truth.

A flush is one WAP run, `stream-<UTC start to the microsecond>Z-<hex>` (unique per attempt):

1. the dead letters produced so far are flushed and must all be acknowledged; otherwise the
   flush fails;
2. `wap.start`; then **the fence check**: the table's checkpoint at the run's base must still be
   the one this consumer resumed from. `publish` requires `main` at the base, which orders
   publishes from that base on; but a consumer that resumed earlier, while another one published
   from those same offsets, would start its run on top of that publish and land the same messages
   again. Checking the base's checkpoint closes that window (pinned: a second consumer that read
   the same offsets raises `PublishConflict`, and one whose rival publishes during its run is
   refused by `main`);
3. `wap.write` (append) of the buffered rows, with the batch's counts and offsets as snapshot
   properties;
4. the audit reads the branch head back: one row per message of this run, no more;
5. `wap.publish(..., set_properties={offsets property: next offsets})`: rows and offsets land in
   one catalog commit or not at all. A `WapError` (the catalog may have lost its answer) is
   retried twice with the same arguments, which completes the run or resumes from its own
   checkpoint snapshot (ADR 0002); `PublishConflict` is not retried;
6. the offsets go to the consumer group, for `rpk group describe` and `stream status`; a
   failure there costs visibility only.

Any failure discards the open run (best effort) and starts again from step 1 of the start, with
exponential backoff up to 60 s: that one path covers a lost race, a compaction, a catalog or
storage outage and a lost answer. A fatal librdkafka error stops the process. On SIGTERM the
consumer flushes what it buffered (60 s grace) and checkpoints progress without rows.

### Failure modes

| Where it stops | What is in the table | What happens next |
|---|---|---|
| Before `wap.start` | Nothing of the batch | Restart reads from the checkpoint: the batch is read again |
| Between write and publish (SIGKILL) | The batch on `audit-stream-...`; `main` and the offsets unchanged | Restart reads it again; the branch is swept once older than `stale_branch_age` |
| Between publish and the group commit (SIGKILL) | The batch and its offsets | Restart seeks to the table's offsets: nothing is read twice; the group catches up at the next flush |
| After the dead letters, before the publish | Nothing; the dead letters are in their topic | They are produced again on the replay: dead letters are at least once, identifiable by `ibergrid.dlq.source` |
| Lost publish answer | Either landed or not | Published again with the same arguments; if that fails too, restart from the table |
| Second consumer on the same table | One of them publishes | The other's flush conflicts (fence or `main`) and it restarts from the winner's checkpoint |
| Checkpoint older than the retention (7 days) | Earlier events never arrive | Logged as `stream.consumer.retention_gap`; the batch lane covers closed days |
| A message the contract refuses | Not in the table | In the dead-letter topic, with its reason |
| The producer restarts | Nothing new | It re-emits its window; the consumer drops the exact repeats |

**Proof** (`make chaos-stream`, a throwaway compose project with its own secrets, volumes and
subnets, removed afterwards): a recorded day (1,152 points) is produced; a consumer is held right
after `wap.write` and SIGKILLed: 0 rows in `main`, no checkpoint, one branch left. A consumer held
right after the publish is SIGKILLed: 1,152 rows, the checkpoint at the end of every partition,
no group offsets, the stale branch swept. The producer "restarts" (the same day again, 1,152
repeats) and a second day (300 points) arrives; a third consumer catches up: 1,452 rows for 2,604
messages, 1,452 distinct messages and 1,452 distinct keys, 1,152 duplicates dropped, checkpoint and
group offsets at the end of every partition, no branch, no dead letter.

### Stale run branches

A run killed between `wap.start` and its publish or discard leaves `audit-stream-<start>Z-<hex>`.
Iceberg refs carry no creation time (ADR 0002), so the run id carries its start, to the
microsecond. The consumer discards this lane's branches older than `stale_branch_age` (900 s,
far above a flush's few seconds) when it starts, which is when a kill is followed by a restart;
a branch of a live run (a misconfigured second consumer, say) is younger and kept. ADR 0005's
Dagster sweeper does not match these names (they are not Dagster run ids) and keeps them.

### Buffer sizing, small files and checkpoint cost

A data flush happens at 10,000 buffered rows (`IBERGRID_STREAM_FLUSH_RECORDS`) or 60 s after the
first one (`IBERGRID_STREAM_FLUSH_AGE`). The producer emits in bursts every five minutes, so 60 s
gathers a whole burst into one flush: at most 288 flushes a day, each one small file (19 KiB on
average in the live journey) in the month's partition. A shorter age would split bursts into more
files without making the data fresher than REData's own five-minute grid; a longer one (15 min:
96 a day) trades freshness for files. The record bound only matters for a replay (a consumer that
was down, a new table reading seven days of retention): 10,000 rows keep a flush's memory small.

Progress without rows (only duplicates or dead letters, as after a producer restart) is
checkpointed at most every 900 s (`IBERGRID_STREAM_CHECKPOINT_INTERVAL`), and never while `main`
holds no data file (ADR 0002: a checkpoint needs live data; the first flush carries rows). Each
checkpoint costs three catalog commits and a snapshot, so checkpointing every idle loop would
write one every second; the price of the bound is re-reading at most 900 s of duplicates after a
crash, which the duplicate check drops again. Nothing happens while the topic is quiet.

288 small appends a day still grow the metadata: the stream table enables
`commit.manifest-merge.enabled` (PyIceberg merges manifests once there are 100; measured: 21
manifests after 120 appends, DuckDB reads them), and snapshot expiry and compaction (phase 4) are
mandatory for it. Orphan-file cleanup, also phase 4, is what frees the storage.

### The table, and why not the batch table

`bronze.redata_demand_stream` holds one row per accepted event: the event's fields, the Kafka
coordinates (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`), `ingested_at`
(the flush's) and `run_id` (the flush's WAP run). It is append-only and partitioned by
`month(ts_utc)`, like the batch table: even with its revisions (tens of thousands of rows a day,
76 bytes a row before compaction) a day partition would be a file of a few megabytes after
compaction, and reads prune by month.

The batch table is the wrong target. Its runs replace whole windows (an overwrite of one dataset,
system and day range per chunk), its audit counts rows per window, and its rows carry a landing
path and a payload hash per chunk; a stream that appends points into it would either be
overwritten by the next batch run of the same day or break the batch audit's "rows outside the
window unchanged". Separate tables keep each lane's invariants: `bronze.redata_observations` stays
authoritative for closed days (REData's settled values, the same series ids), the stream table is
the fresh, append-only record of what was seen when, and Silver (phase 3) reconciles them: the
latest stream version per key for today, the batch values once a day closes.

### Security model

| User | Created by | May | Used by |
|---|---|---|---|
| `ibergrid-admin` | `broker-init` | Everything (superuser) | `broker-init` only |
| `ibergrid-producer` | `broker-init` | WRITE, DESCRIBE `redata.demand.realtime.v1` | `stream-producer` |
| `ibergrid-consumer` | `broker-init` | READ, DESCRIBE `redata.demand.realtime.v1`; WRITE, DESCRIBE `redata.demand.realtime.dlq.v1`; READ, DESCRIBE group `ibergrid.stream.bronze` | `stream-consumer`, `stream status` |

- `broker-init` (the Redpanda image, one-shot, idempotent) creates the superuser, sets
  `superusers`, `admin_api_require_auth`, `enable_sasl`, `auto_create_topics_enabled=false` and
  `write_caching_default=false`, creates the other users (or updates their passwords: rotation),
  the two topics and the ACLs. On a new cluster the admin API needs no authentication until it
  requires it, so the first run creates the superuser anonymously; later runs authenticate as it.
  An existing broker (the local volumes from phase 1, or a server) is hardened in place by the
  next `up`. Passwords reach `rpk` through `RPK_*` variables, never a command line.
- SCRAM-SHA-256 on every Kafka listener, also the one published on `127.0.0.1:29092` locally;
  the admin API (9644) is published nowhere and requires the superuser. The secrets are
  `REDPANDA_ADMIN_PASSWORD`, `REDPANDA_PRODUCER_PASSWORD` and `REDPANDA_CONSUMER_PASSWORD` in
  `.env` (`make env` generates them; compose refuses to start without them).
- The producer gets broker and REData settings only: no storage or catalog keys, and only the
  broker's network. The consumer gets the lake's keys and the broker, catalog and storage
  networks.
- **TLS is not enabled**: SCRAM keeps passwords off the wire, but the traffic itself is plaintext
  inside the compose network. Acceptable while the broker is reachable only on a Docker network
  (and on the host's loopback locally); required before any listener leaves the host.
- The superuser's password cannot rotate through `broker-init`, which authenticates with it:
  change it with `rpk security user update` as the old superuser, then in `.env`.

### Development and production brokers

Locally the broker keeps `--mode dev-container` (fsync bypassed, fast startup), with SASL, ACLs
and no auto-creation all the same: what differs is durability, not security. `compose.prod.yaml`
runs it in production mode: `rpk redpanda mode production` first (the image's own configuration
says `developer_mode`), then `redpanda start` without the bypass, with `--reserve-memory=0M` (the
container's 1536 MiB limit is the reserve) and only the internal listener. `broker-init` sets
`write_caching_default=false` in both, so a server whose cluster was first started in
dev-container mode also gets fsync per write.

### Health and observability

Each service rewrites a heartbeat file (JSON with its counters) as its loop goes round: the
producer after every poll and every 30 s while waiting, the consumer at most every 5 s. The
healthcheck runs `python -m ibergrid.stream.health` (standard library only) rather than the CLI,
whose 133 MiB per start would count against the service's own memory limit every 30 s; `ibergrid
stream health` is the same check for humans. A producer poll can take four REData attempts of up
to 60 s, so its heartbeat may be 330 s old; the consumer's 120 s. Logs are JSON events:
`stream.producer.polled` (window, points, emitted, suppressed, rejected, seconds),
`stream.consumer.flushed` (run id, rows, duplicates, revisions, dead letters, offsets, lag per
partition and total, seconds), `stream.consumer.dead_letter`, `.retention_gap`, `.failed`.
`ibergrid stream status` (`make stream-status`) prints the table's rows and checkpoint, the lag per
partition (high watermark minus checkpoint) and the dead letters retained.

### One image

Both services run the app image: the consumer needs PyIceberg, pyarrow and DuckDB's catalog
code like any ingestion run, and the producer would save only those in an image of its own,
which the release workflow would build, smoke-test, push and pin on every release. confluent-kafka
adds 23 MB to an image of 790 MB (3 %), so it is a runtime dependency, and the release workflow
and `deploy/remote.sh` stay as they are (the app digest already covers both services).

### Live journey

From 12:07:54 to 12:41:35 UTC on 2026-11-21 (34 minutes), on the local stack holding the phase-1
backfill (1.7 M REData rows), against the live API:

| Measure | Result |
|---|---|
| Polls | 8, 0.6 to 1.2 s each, none failed, no point rejected |
| Events emitted | 3,071 (1,020 at the first poll, then 2 to 390 a poll; 1,025 after the producer's restart) |
| Flushes | 8, 1.8 to 2.1 s each, lag 0 after every one (also in `rpk group describe`) |
| Rows in `bronze.redata_demand_stream` | 2,047 for 1,027 keys; 2,047 distinct messages; 8 data files, 151 KiB; 9 snapshots |
| Duplicates dropped | 1,024, all from the producer's restart (it re-emitted its window; one point was new) |
| Revisions kept | 1,020: Real 5, Forecasted 289, Scheduled 342, Total scheduled 384; 826 of them for instants still ahead |
| Dead letters | 0 |
| Consumer restart | Resumed at the table's checkpoint with 1,025 keys rebuilt from the table; nothing read twice |
| Against the batch lane | `ingest redata --dataset demand_realtime` for the same day once the stream stopped: all 1,027 keys in both tables, the stream's latest version equal to the batch value for every one |
| Freshness | A Real point reached the table a median 542 s (at most 561 s) after the end of its five minutes: REData's own delay, the poll's phase and the 60 s flush |
| Peak cgroup memory | Producer 94 MiB (limit 256), consumer 128 MiB (limit 384) |

The revisions were the finding: REE reprograms the rest of the day every few minutes, so the
stream writes tens of thousands of rows a day, not the 1,150 points a day has. 21 rows repeat an
earlier version's hash (a value that went back), kept by design (see "The consumer").

## Consequences

- Good: an event lands in Bronze once whatever the crash point, without Kafka transactions and
  without a second store: the offsets travel with the data in one fenced commit, and the chaos
  test proves both kill points.
- Good: the broker is authenticated and least-privilege in every environment, and a producer
  container has nothing that reaches the lake.
- Good: duplicates from producer restarts, replays and re-emitted windows cost nothing in the
  table; revisions are kept with their Kafka coordinates for Silver to resolve.
- Bad: one writer per table: a second consumer only ever wastes work (it conflicts and restarts),
  so scaling out means more tables or a partition-aware design, not more consumers.
- Bad: up to 288 small appends a day, each three catalog commits; compaction and snapshot expiry
  (phase 4) are mandatory for the stream table, and until orphan-file cleanup exists its storage
  only grows.
- Bad: dead letters are at least once (a crash between producing them and the publish repeats
  them); the duplicate check only reaches back `dedup_days`, so an exact repeat of an older point
  is stored again (Silver deduplicates by key and hash anyway).
- Bad: no TLS on the broker; the superuser's password rotates by hand.
- Neutral: the broker's healthcheck relies on `rpk cluster health` answering without credentials,
  as it does on 26.2.2; an upgrade that changes it shows up as an unhealthy broker.
- Neutral: the topic's partitions are read at start: partitions added later are only read after a
  restart.

### For the next phases

- **Phase 3, Silver.** `silver` reconciles the lanes: the latest stream version per key (highest
  `kafka_offset`) for days the batch lane has not closed, the batch table's value once it has.
- **Phase 4, quality and maintenance.** The audit step of each flush is where quality gates go;
  expiry, compaction (when no flush is open: a compaction makes the open flush conflict, and it
  restarts) and orphan-file cleanup are required for this table.
- **Phase 6, production.** TLS on the broker before any listener leaves the host; an alert on
  `stream.consumer.retention_gap`, on lag and on dead letters.

## Pros and cons of the other options

### Apache Kafka (KRaft) instead of Redpanda

- Good: the reference implementation; every client and tool targets it.
- Bad: a JVM service with a larger memory floor on a single small server; Redpanda is already in
  the stack, speaks the same protocol, and its SASL/SCRAM and ACLs are the same model.

### No broker: the producer writes Iceberg

- Good: one service fewer.
- Bad: no buffer between REData and the lake (a catalog outage would lose polls or block them),
  no replay, and the lane would teach nothing about streaming; ADR 0001 chose a broker for this.

### Kafka transactions (exactly-once read-process-write)

- Good: the standard answer for Kafka-to-Kafka pipelines.
- Bad: the output is an Iceberg commit, which no Kafka transaction covers: the offsets would
  still have to land with the data in the catalog, which is what the table property does without
  transactions.

### Offsets in the consumer group, idempotent writes

- Good: the usual consumer design; lag tools read it directly.
- Bad: at least once into Iceberg: a crash between the publish and the commit writes the batch
  twice, unless the write is an upsert by key, which an append-only Bronze (with revisions) is
  not, and a merge per flush would rewrite files on every micro-batch.

### Write into `bronze.redata_observations`

- See "The table, and why not the batch table": the batch overwrite and the stream append break
  each other's invariants.

### A pure-Python Kafka client

- Good: no native library.
- Bad: aiokafka is asyncio-only and kafka-python's maintenance is uneven; librdkafka is the client
  Confluent and Redpanda test against, with idempotence and SCRAM built in and wheels for both
  architectures.

### A third image for the stream services

- Good: an ingest image without the Kafka client.
- Bad: 23 MB saved against a third build, smoke test, push and digest pin per release; the
  consumer needs the app image's lake libraries anyway.
