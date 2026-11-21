"""The ``ibergrid`` command line: the entrypoint of the app image."""

import signal
import threading
from collections.abc import Callable
from datetime import datetime, timedelta
from enum import StrEnum
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Annotated, NoReturn

import duckdb
import typer
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient
from pydantic import ValidationError
from pyiceberg.exceptions import NoSuchTableError, RESTError
from pyiceberg.table import Table

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
from ibergrid.settings import (
    BrokerSettings,
    LogSettings,
    RedataSettings,
    Settings,
    StreamSettings,
    base_url,
    describe_validation_error,
)
from ibergrid.sources.omie import ingest as omie_ingest
from ibergrid.sources.omie.client import OmieError
from ibergrid.sources.omie.marginalpdbc import PRICE_LIMITS
from ibergrid.sources.redata.client import RedataClient, RedataError
from ibergrid.sources.redata.datasets import DATASETS
from ibergrid.sources.redata.ingest import ChunkReport, ingest
from ibergrid.stream import health
from ibergrid.stream.consumer import (
    ConsumerError,
    StreamConsumer,
    describe_status,
    drop_property,
    read_checkpoint,
    reset_checkpoint,
)
from ibergrid.stream.kafka import (
    BrokerConfigError,
    client_config,
    consumer_config,
    producer_config,
)
from ibergrid.stream.producer import DeliveryFailed, StreamProducer, replay_chunk
from ibergrid.transform import TableReport, TransformError, run_medallion

SHOWN_DAYS = 5  # incomplete days named per chunk line


class Position(StrEnum):
    earliest = "earliest"
    latest = "latest"


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
stream_app = typer.Typer(
    help="The streaming lane: REData's real-time demand through Redpanda (ADR 0006).",
    no_args_is_help=True,
)
app.add_typer(stream_app, name="stream")
transform_app = typer.Typer(
    help="Build Silver and Gold medallion tables with dbt + DuckDB and publish via WAP (ADR 0007).",
    no_args_is_help=True,
)
app.add_typer(transform_app, name="transform")


def _load[T](factory: Callable[[], T]) -> T:
    try:
        return factory()
    except ValidationError as exc:
        typer.echo(f"error: invalid configuration\n{describe_validation_error(exc)}", err=True)
        raise typer.Exit(2) from exc


def _load_settings() -> Settings:
    settings = _load(Settings)
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
        shown = measured.incomplete[:SHOWN_DAYS]  # the snapshot summary keeps all of them
        days = ", ".join(f"{day} {received}/{expected}" for day, expected, received in shown)
        more = len(measured.incomplete) - len(shown)
        line += f"; incomplete days: {days}" + (f" and {more} more" if more else "")
    if report.violations:
        broken = ", ".join(f"{series} {count}" for series, count in report.violations.items())
        line += f"; points breaking the value rules: {broken}"
    if report.dropped_series:
        line += f"; series no longer in the answer: {', '.join(report.dropped_series)}"
    typer.echo(line)


@ingest_app.command("omie")
def ingest_omie(
    start: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"], help="First delivery day.")],
    end: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"], help="Last delivery day.")],
    require_published: Annotated[
        bool,
        typer.Option(
            "--require-published",
            help=(
                "Exit 1, not 0, when a delivery day after today is not published yet; "
                "that day's month chunk then writes nothing."
            ),
        ),
    ] = False,
) -> None:
    """Download OMIE's day-ahead marginal prices for delivery days [start, end] and publish them
    to bronze.omie_day_ahead_prices, one month at a time; a rerun replaces the same rows. Stops
    at the first day OMIE has not published yet (exit 0, or 1 with --require-published); exits 1
    at the first failed chunk."""
    settings = _load_settings()
    try:
        reports = omie_ingest.ingest(
            settings,
            start.date(),
            end.date(),
            on_chunk=_print_omie_chunk,
            require_published=require_published,
        )
    # OSError: the catalog or the bucket did not answer; RESTError: the catalog refused.
    except (OmieError, WapError, ValueError, OSError, RESTError) as exc:
        _fail(exc)
    line = f"omie: {sum(1 for r in reports if r.written)} chunk(s) published"
    if reports[-1].not_published_from is not None:
        line += f"; not published yet from {reports[-1].not_published_from}"
    typer.echo(line)


def _print_omie_chunk(report: omie_ingest.ChunkReport) -> None:
    if report.written is None:
        typer.echo(f"omie: {report.chunk.start}..{report.chunk.end}: nothing published yet")
        return
    files = ", ".join(f"v{version} {count}" for version, count in report.versions.items())
    line = (
        f"omie: {report.written.start}..{report.written.end}: "
        f"{sum(report.versions.values())} file(s) ({files}), {report.rows} rows, "
        f"snapshot {report.snapshot_id}"
    )
    if report.violations:
        low, high = PRICE_LIMITS
        broken = ", ".join(f"{column} {count}" for column, count in report.violations.items())
        line += f"; prices outside {low:g}..{high:g} EUR/MWh: {broken}"
    if report.not_published_from is not None:
        line += f"; not published yet from {report.not_published_from}"
    typer.echo(line)


@stream_app.command("produce")
def stream_produce(
    once: Annotated[bool, typer.Option("--once", help="Poll REData once and exit.")] = False,
    source_file: Annotated[
        Path | None,
        typer.Option(
            exists=True,
            dir_okay=False,
            help="Emit a recorded REData answer instead of polling the API, then exit.",
        ),
    ] = None,
) -> None:
    """Poll REData's national real-time demand every IBERGRID_STREAM_POLL_INTERVAL seconds and
    emit each changed point to the topic. Needs no storage or catalog keys."""
    broker, redata, stream, log_settings = _load(
        lambda: (BrokerSettings(), RedataSettings(), StreamSettings(), LogSettings())
    )
    configure_logging(log_settings)
    try:
        sink = Producer(producer_config(broker, "ibergrid-stream-producer"))
    except (BrokerConfigError, KafkaException) as exc:
        _fail(exc)
    producer = StreamProducer(
        sink,
        stream.topic,
        RedataClient.from_settings(redata).fetch,
        midnight_grace=timedelta(seconds=stream.midnight_grace),
    )
    try:
        if source_file is not None:
            payload = source_file.read_bytes()
            report = producer.emit(payload, replay_chunk(payload))
        elif once:
            report = producer.poll_once()
        else:
            watchdog = health.Watchdog(stream.watchdog_timeout)
            watchdog.start()
            producer.run(_stop_on_signals(), stream.poll_interval, stream.heartbeat_path, watchdog)
            return
    except (RedataError, DeliveryFailed) as exc:
        _fail(exc)
    typer.echo(
        f"stream: {report.start}..{report.end}: {report.emitted} event(s) emitted, "
        f"{report.suppressed} unchanged, {report.rejected} rejected"
    )


@stream_app.command("consume")
def stream_consume(
    allow_other_checkpoints: Annotated[
        bool,
        typer.Option(
            "--allow-other-checkpoints",
            help="Start although the table holds another consumer group's checkpoint "
            "(a deliberate migration).",
        ),
    ] = False,
) -> None:
    """Write the topic into bronze.redata_demand_stream as WAP micro-batches whose publish
    commits the checkpoint; resume from it. Runs until SIGTERM, then flushes."""
    settings = _load_settings()
    # Start the watchdog before the first catalog call: PyIceberg 0.12's REST client has no
    # timeout, so a catalog that accepts TCP and stalls during startup would otherwise hang.
    watchdog = health.Watchdog(settings.stream.watchdog_timeout)
    watchdog.start()
    table = _stream_table(settings)
    kafka, admin = _stream_clients(settings, "ibergrid-stream-consumer")
    try:
        dlq = Producer(producer_config(settings.broker, "ibergrid-stream-consumer-dlq"))
    except KafkaException as exc:
        _fail(exc)
    try:
        StreamConsumer(
            table,
            kafka,
            dlq,
            admin,
            settings.stream,
            allow_other_checkpoints=allow_other_checkpoints,
        ).run(_stop_on_signals(), watchdog)
    except KafkaException as exc:
        _fail(exc)
    finally:
        kafka.close()
        dlq.flush(30)


@stream_app.command("health")
def stream_health(
    max_age: Annotated[
        float, typer.Option(min=1, help="Seconds a heartbeat stays fresh.")
    ] = health.MAX_AGE,
    max_failing: Annotated[
        float, typer.Option(min=1, help="Seconds the loop may keep failing.")
    ] = health.MAX_FAILING,
) -> None:
    """Exit 0 while this container's stream service has beaten within --max-age seconds and has
    not been failing for longer than --max-failing."""
    ok, detail = health.check(_load(StreamSettings).heartbeat_path, max_age, max_failing)
    typer.echo(detail, err=not ok)
    if not ok:
        raise typer.Exit(1)


@stream_app.command("status")
def stream_status() -> None:
    """The checkpoint in the table, the lag behind each partition and the dead letters."""
    settings = _load_settings()
    table = _stream_table(settings)
    kafka, admin = _stream_clients(settings, "ibergrid-stream-status")
    try:
        for line in describe_status(table, kafka, admin, settings.stream):
            typer.echo(line)
    except (ConsumerError, KafkaException) as exc:
        _fail(exc)
    finally:
        kafka.close()


@stream_app.command("reset")
def stream_reset(
    to: Annotated[
        Position,
        typer.Option(help="earliest: write what the topic retains again; latest: skip it."),
    ],
    force: Annotated[
        bool, typer.Option("--force", help="Reset a checkpoint that still fits the topic.")
    ] = False,
) -> None:
    """Point the consumer's checkpoint at the topic as it is now, after the topic was recreated
    or lost its tail (the consumer refuses to start then). Stop the consumer first."""
    settings = _load_settings()
    table = _stream_table(settings)
    kafka, admin = _stream_clients(settings, "ibergrid-stream-reset")
    try:
        before = read_checkpoint(table, settings.stream.group_id)
        after = reset_checkpoint(table, kafka, admin, settings.stream, to.value, force=force)
    except (ConsumerError, KafkaException, WapError) as exc:
        _fail(exc)
    finally:
        kafka.close()
    typer.echo(f"stream: checkpoint was {None if before is None else before.to_json()}")
    typer.echo(f"stream: checkpoint is  {after.to_json()}")


@stream_app.command("drop-property")
def stream_drop_property(
    name: Annotated[str, typer.Argument(help="For example ibergrid.stream.checkpoint.<group>.")],
    force: Annotated[
        bool, typer.Option("--force", help="Also drop this consumer's own checkpoint.")
    ] = False,
) -> None:
    """Remove a stream property from the table through the fenced publish: another group's
    checkpoint after a deliberate migration, or one an earlier version left."""
    settings = _load_settings()
    table = _stream_table(settings)
    try:
        drop_property(table, settings.stream, name, force=force)
    except (ConsumerError, WapError) as exc:
        _fail(exc)
    typer.echo(f"stream: dropped {name}")


@transform_app.command("run")
def transform_run(
    select: Annotated[
        list[str] | None,
        typer.Option(
            help="Layer (`silver`, `gold`) or table name; repeat for several. Default: all."
        ),
    ] = None,
) -> None:
    """Build and test the dbt medallion in a staging DuckDB, then publish to Iceberg via WAP."""
    settings = _load_settings()
    try:
        report = run_medallion(settings, select=select or (), on_table=_print_transform_table)
    except (TransformError, WapError, ValueError, OSError, RESTError) as exc:
        _fail(exc)
    typer.echo(
        f"transform: {len(report.tables)} table(s) published, "
        f"{report.dbt_tests_passed} dbt test(s) passed in {report.total_seconds:.2f} s"
    )


def _print_transform_table(report: TableReport) -> None:
    snapshot = "none" if report.snapshot_id is None else str(report.snapshot_id)
    typer.echo(
        f"transform: {report.qualified}: {report.rows} rows, "
        f"snapshot {snapshot} ({report.seconds:.2f} s)"
    )


def _stream_clients(settings: Settings, client_id: str) -> tuple[Consumer, AdminClient]:
    try:
        config = consumer_config(settings.broker, client_id, settings.stream.group_id)
        return Consumer(config), AdminClient(client_config(settings.broker, client_id))
    except (BrokerConfigError, KafkaException) as exc:
        _fail(exc)


def _stream_table(settings: Settings) -> Table:
    try:
        table: Table = rest_catalog(settings).load_table(registry.REDATA_DEMAND_STREAM.identifier)
    except NoSuchTableError as exc:
        _fail(RuntimeError(f"{exc}: run `ibergrid lake init` first"))
    # OSError: requests' connection errors; RESTError: any HTTP error answer from the catalog.
    except (OSError, RESTError) as exc:
        _fail(exc)
    return table


def _stop_on_signals() -> threading.Event:
    """Set on SIGTERM (docker stop) or SIGINT, so a loop can finish its batch first."""
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    return stop


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
