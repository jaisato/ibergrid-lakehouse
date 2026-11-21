"""The ``ibergrid`` command line: the entrypoint of the app image."""

from importlib.metadata import version as package_version
from typing import Annotated, NoReturn

import typer
from pydantic import ValidationError

from ibergrid.catalog_admin import (
    CatalogAdmin,
    CatalogError,
    bootstrap_catalog,
    update_credential,
)
from ibergrid.doctor import run_checks
from ibergrid.logging import configure_logging
from ibergrid.settings import Settings, base_url, describe_validation_error

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
    """Store the current IBERGRID_S3_* keys in the warehouse (after rotating them)."""
    settings = _load_settings()
    admin = CatalogAdmin(base_url(settings.catalog.url))
    try:
        update_credential(admin, settings.catalog, settings.storage)
    except CatalogError as exc:
        _fail(exc)
    typer.echo(f"catalog: warehouse {settings.catalog.warehouse!r} now uses the current S3 keys")


def _fail(exc: CatalogError) -> NoReturn:
    typer.echo(f"error: {exc}", err=True)
    raise typer.Exit(1) from exc
