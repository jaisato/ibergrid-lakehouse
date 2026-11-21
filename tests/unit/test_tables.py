"""ensure_table on a real Iceberg catalog (sqlite + local files)."""

from dataclasses import replace

import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

from ibergrid.lake.registry import TABLES
from ibergrid.lake.tables import EnsureResult, IncompatibleTableChange, TableSpec, ensure_table

SCHEMA = Schema(
    NestedField(1, "at", TimestamptzType(), required=True),
    NestedField(2, "indicator", StringType(), required=True),
    NestedField(3, "value", DoubleType(), required=False),
    NestedField(
        4,
        "attributes",
        StructType(
            NestedField(5, "tags", ListType(6, StringType(), element_required=False)),
            NestedField(7, "extra", MapType(8, StringType(), 9, LongType(), value_required=False)),
        ),
        required=False,
    ),
)
SPEC = TableSpec(
    namespace="bronze",
    name="readings",
    schema=SCHEMA,
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="at_month")
    ),
    sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
    properties={"write.metadata.previous-versions-max": "50"},
)


def with_fields(*fields: NestedField) -> Schema:
    return Schema(*fields)


def test_creates_the_namespace_and_the_table(catalog: SqlCatalog) -> None:
    assert ensure_table(catalog, SPEC) == EnsureResult(created=True)

    table = catalog.load_table(SPEC.identifier)
    assert table.format_version == 2
    assert [f.name for f in table.schema().fields] == ["at", "indicator", "value", "attributes"]
    assert [f.name for f in table.spec().fields] == ["at_month"]
    assert table.properties["write.target-file-size-bytes"] == str(128 * 1024 * 1024)
    assert table.properties["commit.retry.num-retries"] == "8"
    assert table.properties["write.metadata.previous-versions-max"] == "50"


def test_a_second_run_changes_nothing(catalog: SqlCatalog) -> None:
    ensure_table(catalog, SPEC)
    before = catalog.load_table(SPEC.identifier).metadata_location

    assert ensure_table(catalog, SPEC) == EnsureResult()
    assert catalog.load_table(SPEC.identifier).metadata_location == before


def test_new_optional_columns_and_properties_are_applied(catalog: SqlCatalog) -> None:
    ensure_table(catalog, SPEC)
    grown = replace(
        SPEC,
        schema=with_fields(
            *SCHEMA.fields,
            NestedField(10, "source", StringType(), required=False, doc="Publisher"),
        ),
        properties={**SPEC.properties, "write.metadata.previous-versions-max": "20"},
    )

    result = ensure_table(catalog, grown)

    assert result == EnsureResult(
        added_columns=("source",), updated_properties=("write.metadata.previous-versions-max",)
    )
    table = catalog.load_table(SPEC.identifier)
    source = table.schema().find_field("source")
    assert (source.required, source.doc) == (False, "Publisher")
    assert table.properties["write.metadata.previous-versions-max"] == "20"
    assert ensure_table(catalog, grown) == EnsureResult()


def test_a_column_or_a_property_alone_is_applied(catalog: SqlCatalog) -> None:
    ensure_table(catalog, SPEC)
    column = replace(
        SPEC, schema=with_fields(*SCHEMA.fields, NestedField(10, "unit", StringType(), False))
    )
    prop = replace(column, properties={"write.metadata.previous-versions-max": "5"})

    assert ensure_table(catalog, column) == EnsureResult(added_columns=("unit",))
    assert ensure_table(catalog, prop) == EnsureResult(
        updated_properties=("write.metadata.previous-versions-max",)
    )


FIELDS = {f.name: f for f in SCHEMA.fields}


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            {"schema": with_fields(FIELDS["at"], FIELDS["indicator"], FIELDS["attributes"])},
            "column 'value' is missing from the spec",
        ),
        (
            {
                "schema": with_fields(
                    FIELDS["at"],
                    FIELDS["indicator"],
                    NestedField(3, "value", IntegerType(), required=False),
                    FIELDS["attributes"],
                )
            },
            "column 'value' changes from optional double to optional int",
        ),
        (
            {
                "schema": with_fields(
                    FIELDS["at"],
                    FIELDS["indicator"],
                    NestedField(3, "value", DoubleType(), required=True),
                    FIELDS["attributes"],
                )
            },
            "column 'value' changes from optional double to required double",
        ),
        (
            {
                "schema": with_fields(
                    *SCHEMA.fields[:3],
                    NestedField(4, "attributes", StructType(), required=False),
                )
            },
            "column 'attributes' changes",
        ),
        (
            {"schema": with_fields(*SCHEMA.fields, NestedField(10, "unit", StringType(), True))},
            "new column 'unit' must be optional",
        ),
        (
            {
                "partition_spec": PartitionSpec(
                    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="d")
                )
            },
            "partition spec changes",
        ),
        ({"sort_order": SortOrder()}, "sort order changes"),
        ({"properties": {"format-version": "3"}}, "format version changes from 2 to 3"),
    ],
)
def test_incompatible_changes_fail_loudly_and_change_nothing(
    catalog: SqlCatalog, change: dict[str, object], message: str
) -> None:
    ensure_table(catalog, SPEC)
    before = catalog.load_table(SPEC.identifier).metadata_location

    with pytest.raises(IncompatibleTableChange, match=message) as failure:
        ensure_table(catalog, replace(SPEC, **change))  # type: ignore[arg-type]

    assert str(failure.value).startswith("bronze.readings: ")
    assert catalog.load_table(SPEC.identifier).metadata_location == before


def test_every_registered_table_can_be_created(catalog: SqlCatalog) -> None:
    for spec in TABLES:
        assert ensure_table(catalog, spec).created
