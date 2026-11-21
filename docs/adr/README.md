# Architecture decision records

Decisions that shape the project, in [MADR](https://adr.github.io/madr/) format. A new decision
gets the next number and lands in the same pull request as the change it explains; a decision is
never edited to say something else, it is superseded by a new record.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-stack-and-scope.md) | Stack and scope | Accepted |
| [0002](0002-iceberg-write-and-read-paths.md) | Iceberg write and read paths | Accepted |
| [0003](0003-redata-ingestion.md) | REData ingestion into Bronze | Accepted |
| [0004](0004-omie-ingestion.md) | OMIE day-ahead prices into Bronze | Accepted |
| [0005](0005-dagster-orchestration.md) | Dagster asset-oriented orchestration of the batch lane | Accepted |
| [0006](0006-streaming-lane.md) | Streaming lane: REData real-time demand through Redpanda into Iceberg, exactly once | Accepted |
