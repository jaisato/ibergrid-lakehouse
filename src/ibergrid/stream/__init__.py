"""The streaming lane (ADR 0006): REData's five-minute national demand through Redpanda into
``bronze.redata_demand_stream``.

``contract`` is the event and its JSON Schema, ``producer`` polls REData and emits events,
``consumer`` validates, deduplicates and writes them as Write-Audit-Publish micro-batches whose
publish commits the Kafka offsets (the checkpoint), ``kafka`` builds the clients' configuration and
``health`` writes and checks the services' heartbeats.
"""
