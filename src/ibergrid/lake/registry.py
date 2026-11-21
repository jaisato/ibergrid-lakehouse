"""The tables ``ibergrid lake init`` creates: the connectors' Bronze tables."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from ibergrid.lake.tables import TableSpec

# One row per REData point, for every widget (ADR 0003). The key is (dataset, electric_system,
# series_id, ts_utc); each ingestion run replaces one dataset, system and window.
REDATA_OBSERVATIONS = TableSpec(
    namespace="bronze",
    name="redata_observations",
    schema=Schema(
        NestedField(1, "dataset", StringType(), required=True, doc="ibergrid dataset name"),
        NestedField(2, "widget", StringType(), required=True, doc="REData widget path"),
        NestedField(3, "electric_system", StringType(), required=True),
        NestedField(4, "series_id", StringType(), required=True, doc="JSON:API id of the series"),
        NestedField(5, "series_type", StringType(), required=False),
        NestedField(6, "series_title", StringType(), required=False),
        NestedField(7, "granularity", StringType(), required=True, doc="5min, 15min, hour, day"),
        NestedField(8, "ts_utc", TimestamptzType(), required=True),
        NestedField(9, "ts_local", StringType(), required=True, doc="datetime as delivered"),
        NestedField(10, "value", DoubleType(), required=True),
        NestedField(11, "percentage", DoubleType(), required=False),
        NestedField(12, "series_last_update", TimestamptzType(), required=False),
        NestedField(13, "fetched_at", TimestamptzType(), required=True),
        NestedField(14, "run_id", StringType(), required=True),
        NestedField(15, "landing_path", StringType(), required=True, doc="raw response object"),
        NestedField(
            16, "payload_sha256", StringType(), required=True, doc="the answer's, without data.meta"
        ),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dataset"),
        PartitionField(source_id=8, field_id=1001, transform=MonthTransform(), name="ts_utc_month"),
    ),
)

# One row per OMIE day-ahead period (ADR 0004). The key is (delivery_date, period); each
# ingestion run replaces whole delivery days. Partitioned by the month of delivery_date, the
# column every run replaces by, so a rerun drops whole files instead of rewriting a neighbour's.
OMIE_DAY_AHEAD_PRICES = TableSpec(
    namespace="bronze",
    name="omie_day_ahead_prices",
    schema=Schema(
        NestedField(1, "delivery_date", DateType(), required=True, doc="market day (Madrid)"),
        NestedField(2, "period", IntegerType(), required=True, doc="1-based, as in the file"),
        NestedField(3, "resolution_minutes", IntegerType(), required=True, doc="60 or 15"),
        NestedField(4, "interval_start_utc", TimestamptzType(), required=True),
        NestedField(5, "interval_end_utc", TimestamptzType(), required=True),
        NestedField(6, "price_es_eur_mwh", DoubleType(), required=True, doc="Spain, column 6"),
        NestedField(7, "price_pt_eur_mwh", DoubleType(), required=True, doc="Portugal, column 5"),
        NestedField(
            8, "contract_version", IntegerType(), required=True, doc="1 hourly, 2 quarter-hourly"
        ),
        NestedField(9, "source_file", StringType(), required=True, doc="OMIE's file name"),
        NestedField(
            10, "file_version", IntegerType(), required=True, doc="the N of the file name's .N"
        ),
        NestedField(11, "content_sha256", StringType(), required=True, doc="of the file's bytes"),
        NestedField(12, "fetched_at", TimestamptzType(), required=True),
        NestedField(13, "run_id", StringType(), required=True),
        NestedField(14, "landing_path", StringType(), required=True, doc="the original file"),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=MonthTransform(), name="delivery_date_month"
        ),
    ),
)

# One row per event the streaming lane accepted (ADR 0006), append-only: an event's key is
# (series_id, ts_utc), and a revision is a new row with another content_sha256; Silver (phase 3)
# keeps the latest. Kafka coordinates make each row traceable to its message, and the consumer's
# offsets live in this table's properties (the checkpoint). Partitioned like the batch table, by
# month: 1,150 points a day plus their revisions (REE reprograms the rest of the day every few
# minutes: tens of thousands of rows a day, ADR 0006) still make a day partition a file of a few
# megabytes after compaction (phase 4), and a month is what reads prune by. Manifest merging
# keeps hundreds of small appends a day from growing a manifest each.
REDATA_DEMAND_STREAM = TableSpec(
    namespace="bronze",
    name="redata_demand_stream",
    schema=Schema(
        NestedField(1, "series_id", StringType(), required=True, doc="JSON:API id of the series"),
        NestedField(2, "series_title", StringType(), required=False),
        NestedField(3, "electric_system", StringType(), required=True),
        NestedField(4, "ts_utc", TimestamptzType(), required=True),
        NestedField(5, "ts_local", StringType(), required=True, doc="datetime as delivered"),
        NestedField(6, "value", DoubleType(), required=True, doc="MW"),
        NestedField(7, "series_last_update", TimestamptzType(), required=False),
        NestedField(8, "content_sha256", StringType(), required=True, doc="the event's"),
        NestedField(9, "producer_instance", StringType(), required=True),
        NestedField(10, "producer_version", StringType(), required=True),
        NestedField(11, "emitted_at", TimestamptzType(), required=True),
        NestedField(12, "kafka_topic", StringType(), required=True),
        NestedField(13, "kafka_partition", IntegerType(), required=True),
        NestedField(14, "kafka_offset", LongType(), required=True),
        NestedField(15, "kafka_timestamp", TimestamptzType(), required=False),
        NestedField(16, "ingested_at", TimestamptzType(), required=True, doc="the flush's"),
        NestedField(17, "run_id", StringType(), required=True, doc="the flush's WAP run"),
    ),
    partition_spec=PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=MonthTransform(), name="ts_utc_month"),
    ),
    properties={"commit.manifest-merge.enabled": "true"},
)

TABLES: tuple[TableSpec, ...] = (REDATA_OBSERVATIONS, OMIE_DAY_AHEAD_PRICES, REDATA_DEMAND_STREAM)
