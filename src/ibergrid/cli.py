"""The ``ibergrid`` command line: the entrypoint of the app image."""

from datetime import datetime
from importlib.metadata import version as package_version
from typing import Annotated, NoReturn

import duckdb
import typer
from pydantic import ValidationError
from pyiceberg.exceptions import RESTError

from ibergrid.catalog_admin import (
    CatalogAdmin,
    CatalogError,
    bootstrap_catalog,
    update_credential,
)
from ibergrid.doctor import run_checks
from ibergrid.lake import duck, registry
from ibergrid.lake.catalog import rest_catalog
from ibergrid.lake.tables import EnsureResult, IncompatibleTableChange, ensure_table
from ibergrid.lake.wap import WapError
from ibergrid.logging import configure_logging
from ibergrid.settings import Settings, base_url, describe_validation_error
from ibergrid.sources.redata.client import RedataError
from ibergrid.sources.redata.datasets import DATASETS
from ibergrid.sources.redata.ingest import ChunkReport, ingest

app = typer.Typer(
    help="IberGrid Lakehouse operations.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
catalog_app = typer.Typer(
    help="Manage the Iceberg REST catalog (Lakekeeper).", no_args_is_help=True
)
app.add_typer(catalog_app, name="catalog")
lake_app = typer.Typer(help="Create and inspect the lake's Iceberg tables.", no_args_is_help=True)
app.add_typer(lake_app, name="lake")
ingest_app = typer.Typer(help="Pull source data into the Bronze tables.", no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")


def _load_settings() -> Settings:
    try:
        settings = Settings()
    except ValidationError as exc:
        typer.echo(f"error: invalid configuration\n{describe_validation_error(exc)}", err=True)
        raise typer.Exit(2) from exc
    configure_logging(settings.log)
    return settings


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(f"ibergrid {package_version('ibergrid')}")


@app.command()
def doctor(
    timeout: Annotated[float, typer.Option(min=0.1, help="Seconds allowed per check.")] = 3.0,
) -> None:
    """Check that storage, catalog and broker answer; exit 1 if any does not."""
    results = run_checks(_load_settings(), timeout)
    for result in results:
        typer.echo(f"{'ok' if result.ok else 'FAIL':<4}  {result.name:<8} {result.detail}")
    failed = [result.name for result in results if not result.ok]
    if failed:
        typer.echo(f"doctor: {len(failed)} check(s) failed: {', '.join(failed)}", err=True)
        raise typer.Exit(1)
    typer.echo("doctor: all checks passed")


@catalog_app.command("bootstrap")
def catalog_bootstrap() -> None:
    """Bootstrap Lakekeeper and create the warehouse; a no-op when both already exist."""
    settings = _load_settings()
    admin = CatalogAdmin(base_url(settings.catalog.url))
    try:
        result = bootstrap_catalog(admin, settings.catalog, settings.storage)
    except CatalogError as exc:
        _fail(exc)
    server = "bootstrapped now" if result.bootstrapped else "already bootstrapped"
    warehouse = "created" if result.warehouse_created else "already exists"
    typer.echo(f"catalog: server {server}; warehouse {settings.catalog.warehouse!r} {warehouse}")


@catalog_app.command("update-credential")
def catalog_update_credential() -> None:
    """Store the current IBERGRID_S3_* keys and storage profile in the warehouse."""
    settings = _load_settings()
    admin = CatalogAdmin(base_url(settings.catalog.url))
    try:
        update_credential(admin, settings.catalog, settings.storage)
    except CatalogError as exc:
        _fail(exc)
    typer.echo(f"catalog: warehouse {settings.catalog.warehouse!r} now uses the current S3 keys")


@lake_app.command("init")
def lake_init() -> None:
    """Create the registry's namespaces and tables; existing tables only gain optional columns
    and changed properties, and any other difference fails."""
    settings = _load_settings()
    if not registry.TABLES:
        typer.echo("lake: no tables registered")
        return
    try:
        catalog = rest_catalog(settings)
        for spec in registry.TABLES:
            result = ensure_table(catalog, spec)
            typer.echo(f"lake: {spec.namespace}.{spec.name} {_outcome(result)}")
    # OSError: requests' connection errors; RESTError: any HTTP error answer from the catalog.
    except (IncompatibleTableChange, OSError, RESTError) as exc:
        _fail(exc)


@lake_app.command("describe")
def lake_describe(
    table: Annotated[str, typer.Argument(help="<namespace>.<table>, for example ops.smoke")],
) -> None:
    """Main's current snapshot, the refs (branches and tags) and main's data files."""
    settings = _load_settings()
    try:
        name = duck.table_name(table)
        info = duck.describe(duck.connect(settings), name)
    except (ValueError, duckdb.Error) as exc:
        _fail(exc)
    if info.snapshot is None:
        snapshot = "none (the table has no snapshot yet)"
    else:
        snapshot_id, operation, committed = info.snapshot
        snapshot = f"{snapshot_id} ({operation}, {committed:%Y-%m-%d %H:%M:%S} UTC)"
    refs = ", ".join(f"{ref} -> {target} ({kind.lower()})" for ref, kind, target in info.refs)
    typer.echo(f"table:      {table}")
    typer.echo(f"snapshot:   {snapshot}")
    typer.echo(f"refs:       {refs or 'none'}")
    typer.echo(f"data files: {info.data_files} ({info.data_bytes} bytes)")


@ingest_app.command("redata")
def ingest_redata(
    dataset: Annotated[str, typer.Option(help=f"One of: {', '.join(DATASETS)}.")],
    start: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"], help="First local day.")],
    end: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"], help="Last local day.")],
    system: Annotated[
        list[str] | None,
        typer.Option(help="Electric system; repeat for several. Default: all of the dataset's."),
    ] = None,
) -> None:
    """Fetch REData for local days [start, end] and publish it to bronze.redata_observations,
    one chunk at a time; a rerun replaces the same rows. Exits 1 at the first failed chunk."""
    settings = _load_settings()
    try:
        reports = ingest(
            settings, dataset, start.date(), end.date(), system or (), on_chunk=_print_chunk
        )
    # OSError: the catalog or the bucket did not answer; RESTError: the catalog refused.
    except (RedataError, WapError, ValueError, OSError, RESTError) as exc:
        _fail(exc)
    typer.echo(f"redata: {len(reports)} chunk(s) published")


def _print_chunk(report: ChunkReport) -> None:
    chunk, measured = report.chunk, report.completeness
    share = 100 * measured.received / measured.expected if measured.expected else 0
    line = (
        f"redata: {chunk.dataset.name} {chunk.system.name} {chunk.start}..{chunk.end}: "
        f"{report.rows} rows, {measured.received}/{measured.expected} points ({share:.1f} %), "
        f"snapshot {report.snapshot_id}"
    )
    if measured.incomplete:
        days = ", ".join(
            f"{day} {received}/{expected}" for day, expected, received in measured.incomplete
        )
        line += f"; incomplete days: {days}"
    if report.violations:
        broken = ", ".join(f"{series} {count}" for series, count in report.violations.items())
        line += f"; points breaking the value rules: {broken}"
    if report.dropped_series:
        line += f"; series no longer in the answer: {', '.join(report.dropped_series)}"
    typer.echo(line)


def _outcome(result: EnsureResult) -> str:
    if result.created:
        return "created"
    changes = []
    if result.added_columns:
        changes.append("added columns " + ", ".join(result.added_columns))
    if result.updated_properties:
        changes.append("updated properties " + ", ".join(result.updated_properties))
    return "; ".join(changes) or "unchanged"


def _fail(exc: Exception) -> NoReturn:
    typer.echo(f"error: {exc}", err=True)
    raise typer.Exit(1) from exc
