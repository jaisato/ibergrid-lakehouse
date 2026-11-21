"""Declarative table definitions and the idempotent ``ensure_table`` that applies them.

A spec may only grow: new optional columns and changed properties are applied to an existing
table; anything else (a type change, a dropped or renamed column, a new required column, another
partition spec or format version) raises ``IncompatibleTableChange`` and changes nothing, because
rewriting existing data is a migration, not an ``init``. There is no sort order: PyIceberg 0.12
writes data files without one (``sort_order_id`` stays unset), so declaring it would promise
nothing.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StructType

# Explicit rather than library defaults, so that every engine and version writes the same tables.
DEFAULT_PROPERTIES: Mapping[str, str] = {
    # v2: what PyIceberg 0.12 writes fully and DuckDB 2.0.1 creates by default.
    "format-version": "2",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
}


class IncompatibleTableChange(ValueError):
    """The spec differs from the existing table in a way ``ensure_table`` does not apply."""


@dataclass(frozen=True)
class TableSpec:
    namespace: str
    name: str
    schema: Schema
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC
    properties: Mapping[str, str] = field(default_factory=dict)

    @property
    def identifier(self) -> tuple[str, str]:
        return (self.namespace, self.name)

    def table_properties(self) -> dict[str, str]:
        return {**DEFAULT_PROPERTIES, **self.properties}


@dataclass(frozen=True)
class EnsureResult:
    created: bool = False
    added_columns: tuple[str, ...] = ()
    updated_properties: tuple[str, ...] = ()


def ensure_table(catalog: Catalog, spec: TableSpec) -> EnsureResult:
    """Create the namespace and the table, or bring an existing table up to an additive spec."""
    catalog.create_namespace_if_not_exists(spec.namespace)
    try:
        table = catalog.load_table(spec.identifier)
    except NoSuchTableError:
        catalog.create_table(
            spec.identifier,
            spec.schema,
            partition_spec=spec.partition_spec,
            properties=spec.table_properties(),
        )
        return EnsureResult(created=True)

    added = _check_compatible(table, spec)
    wanted = spec.table_properties()
    wanted.pop("format-version")  # checked above: it is not a settable property
    changed = {k: v for k, v in wanted.items() if table.properties.get(k) != v}
    if added or changed:
        with table.transaction() as tx:
            if added:
                with tx.update_schema() as update:
                    for column in added:
                        update.add_column(column.name, column.field_type, doc=column.doc)
            if changed:
                tx.set_properties(changed)
    return EnsureResult(
        added_columns=tuple(column.name for column in added),
        updated_properties=tuple(sorted(changed)),
    )


def _check_compatible(table: Table, spec: TableSpec) -> list[NestedField]:
    """The spec's new columns, or ``IncompatibleTableChange`` listing every other difference."""
    current = {f.name: f for f in table.schema().fields}
    wanted = {f.name: f for f in spec.schema.fields}
    problems = []
    for name, have in current.items():
        want = wanted.get(name)
        if want is None:
            problems.append(f"column {name!r} is missing from the spec (dropped or renamed)")
        elif (_shape(have.field_type), have.required) != (_shape(want.field_type), want.required):
            problems.append(f"column {name!r} changes from {_describe(have)} to {_describe(want)}")
    added = [f for name, f in wanted.items() if name not in current]
    problems += [f"new column {f.name!r} must be optional" for f in added if f.required]
    if _partitioning(table.spec(), table.schema()) != _partitioning(
        spec.partition_spec, spec.schema
    ):
        problems.append(f"partition spec changes from {table.spec()} to {spec.partition_spec}")
    version = int(spec.table_properties()["format-version"])
    if table.format_version != version:
        problems.append(f"format version changes from {table.format_version} to {version}")
    if problems:
        raise IncompatibleTableChange(f"{'.'.join(spec.identifier)}: " + "; ".join(problems))
    return added


def _shape(field_type: IcebergType) -> object:
    """A type without its field ids, which differ between a spec and the table made from it."""
    match field_type:
        case StructType():
            return tuple((f.name, f.required, _shape(f.field_type)) for f in field_type.fields)
        case ListType():
            return ("list", field_type.element_required, _shape(field_type.element_type))
        case MapType():
            key, value = _shape(field_type.key_type), _shape(field_type.value_type)
            return ("map", key, field_type.value_required, value)
    return field_type


def _describe(column: NestedField) -> str:
    return f"{'required' if column.required else 'optional'} {column.field_type}"


def _partitioning(spec: PartitionSpec, schema: Schema) -> list[tuple[str, str, str]]:
    return [
        (schema.find_column_name(f.source_id) or "", str(f.transform), f.name) for f in spec.fields
    ]
