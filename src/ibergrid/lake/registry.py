"""Every table ``ibergrid lake init`` creates. The connectors add their Bronze tables here."""

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

from ibergrid.lake.tables import TableSpec

# The write-path probe: integration tests run a Write-Audit-Publish cycle on it, in the network.
SMOKE = TableSpec(
    namespace="ops",
    name="smoke",
    schema=Schema(
        NestedField(1, "run_id", StringType(), required=False, doc="The run that wrote the row"),
        NestedField(2, "written_at", TimestamptzType(), required=False),
    ),
)

TABLES: tuple[TableSpec, ...] = (SMOKE,)
