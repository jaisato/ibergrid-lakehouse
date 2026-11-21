"""The tables ``ibergrid lake init`` creates: the connectors' Bronze tables."""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

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

TABLES: tuple[TableSpec, ...] = (REDATA_OBSERVATIONS,)
