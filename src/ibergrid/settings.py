"""Runtime configuration, read from ``IBERGRID_*`` environment variables.

Each group has its own prefix so that one variable name means the same thing in ``.env``, in the
compose files and in the application (for example ``IBERGRID_S3_ACCESS_KEY``). Secrets are
``SecretStr`` and validation errors never echo input values, so neither ``repr`` nor an error
message can leak them into logs.
"""

from typing import Literal

from pydantic import BaseModel, Field, HttpUrl, SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from ibergrid.stream.health import HEARTBEAT_PATH

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


def _config(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(env_prefix=prefix, hide_input_in_errors=True, frozen=True)


class StorageSettings(BaseSettings):
    """S3-compatible object storage: RustFS locally, Cloudflare R2 in production.

    Always addressed path-style (``<endpoint>/<bucket>``), which both accept.
    """

    model_config = _config("IBERGRID_S3_")

    endpoint_url: HttpUrl = HttpUrl("http://storage:9000")
    region: str = "us-east-1"
    bucket: str = "ibergrid"
    access_key: SecretStr = Field(min_length=1)
    secret_key: SecretStr = Field(min_length=1)


class CatalogSettings(BaseSettings):
    """Lakekeeper, the Iceberg REST catalog."""

    model_config = _config("IBERGRID_CATALOG_")

    url: HttpUrl = HttpUrl("http://catalog:8181")
    warehouse: str = "lakehouse"


class BrokerSettings(BaseSettings):
    """Redpanda, the Kafka-compatible broker: SASL/SCRAM in every environment (ADR 0006).

    Each service gets its own user (the producer, the consumer); the readiness check only opens a
    TCP connection and needs none.
    """

    model_config = _config("IBERGRID_BROKER_")

    bootstrap_servers: str = "broker:9092"
    username: str | None = None
    password: SecretStr | None = None
    sasl_mechanism: Literal["SCRAM-SHA-256", "SCRAM-SHA-512"] = "SCRAM-SHA-256"

    @field_validator("bootstrap_servers")
    @classmethod
    def _check_addresses(cls, value: str) -> str:
        for address in value.split(","):
            host, sep, port = address.strip().rpartition(":")
            if not (sep and host and port.isdigit()):
                raise ValueError(f"expected a comma-separated list of host:port, got {address!r}")
        return value

    def addresses(self) -> list[tuple[str, int]]:
        hosts_ports = (a.strip().rpartition(":") for a in self.bootstrap_servers.split(","))
        return [(host, int(port)) for host, _, port in hosts_ports]


class DuckDBSettings(BaseSettings):
    """DuckDB, the analytical engine."""

    model_config = _config("IBERGRID_DUCKDB_")

    # The app image bakes the extensions here at build time; unset, DuckDB installs them into its
    # default directory on first use (developer machines and CI).
    extension_directory: str | None = None


class RedataSettings(BaseSettings):
    """Red Eléctrica's REData API: public, no token."""

    model_config = _config("IBERGRID_REDATA_")

    base_url: HttpUrl = HttpUrl("https://apidatos.ree.es")
    # Seconds per request: a year of national daily generation took 19 s on 2026-11-21.
    timeout: float = Field(default=60.0, gt=0)
    # Politeness towards a free public API: never more than one request per second.
    max_rps: float = Field(default=1.0, gt=0, le=1)


class OmieSettings(BaseSettings):
    """OMIE's public file downloads (www.omie.es): no token."""

    model_config = _config("IBERGRID_OMIE_")

    base_url: HttpUrl = HttpUrl("https://www.omie.es")
    # Seconds per request: a day's file (2.7 KB) took under 0.6 s on 2026-11-21.
    timeout: float = Field(default=30.0, gt=0)
    # The same politeness as for REData: never more than one request per second.
    max_rps: float = Field(default=1.0, gt=0, le=1)


class StreamSettings(BaseSettings):
    """The streaming lane (ADR 0006): topics, consumer group, cadences and buffer sizes."""

    model_config = _config("IBERGRID_STREAM_")

    topic: str = "redata.demand.realtime.v1"
    dlq_topic: str = "redata.demand.realtime.dlq.v1"
    # Names the checkpoint property and receives the offsets for lag visibility only: the
    # table's property, not the group, is where the consumer resumes from.
    group_id: str = Field(default="ibergrid.stream.bronze", pattern=r"^[A-Za-z0-9._-]{1,100}$")
    # Producer: seconds between REData polls, and how long after local midnight it still asks
    # for yesterday, whose last points arrive after midnight.
    poll_interval: float = Field(default=300.0, gt=0)
    midnight_grace: float = Field(default=7200.0, ge=0)
    # Consumer: a data flush at this many buffered rows or once the oldest is this old (seconds);
    # progress without rows (duplicates, dead letters) is checkpointed at most this often.
    flush_records: int = Field(default=10_000, gt=0)
    flush_age: float = Field(default=60.0, gt=0)
    checkpoint_interval: float = Field(default=900.0, gt=0)
    # Days of the stream table read at startup to recognise exact duplicates.
    dedup_days: int = Field(default=3, gt=0)
    # A run branch older than this (seconds, from the start time in its name) is a dead run's.
    stale_branch_age: float = Field(default=900.0, gt=0)
    heartbeat_path: str = HEARTBEAT_PATH
    # Tests only (make chaos-stream): stop for good at this point of a flush, for a SIGKILL there.
    hold_at: Literal["after-write", "after-publish"] | None = None


class OrchestrationSettings(BaseSettings):
    """Dagster's code server (ADR 0005). Not part of ``Settings``: loading the definitions must
    not need the storage keys."""

    model_config = _config("IBERGRID_ORCHESTRATION_")

    # The schedules start stopped, so a local stack never calls the sources on its own; production
    # turns them on.
    schedules_running: bool = False


class LogSettings(BaseSettings):
    """structlog output: ``json`` in containers, ``console`` for humans."""

    model_config = _config("IBERGRID_LOG_")

    level: LogLevel = "INFO"
    format: Literal["json", "console"] = "console"

    @field_validator("level", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class Settings(BaseModel):
    """All groups, each read from its own prefix when the model is built."""

    model_config = {"frozen": True}

    storage: StorageSettings = Field(default_factory=StorageSettings)
    catalog: CatalogSettings = Field(default_factory=CatalogSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    duckdb: DuckDBSettings = Field(default_factory=DuckDBSettings)
    redata: RedataSettings = Field(default_factory=RedataSettings)
    omie: OmieSettings = Field(default_factory=OmieSettings)
    stream: StreamSettings = Field(default_factory=StreamSettings)
    log: LogSettings = Field(default_factory=LogSettings)


def base_url(url: HttpUrl) -> str:
    """The URL without the trailing slash that ``HttpUrl`` adds, ready to append paths to."""
    return str(url).rstrip("/")


def describe_validation_error(exc: ValidationError) -> str:
    """One line per problem, named by environment variable and without the input values."""
    groups = (
        StorageSettings,
        CatalogSettings,
        BrokerSettings,
        DuckDBSettings,
        RedataSettings,
        OmieSettings,
        StreamSettings,
        LogSettings,
    )
    prefix = {group.__name__: group.model_config.get("env_prefix", "") for group in groups}
    return "\n".join(
        f"  {prefix.get(exc.title, '')}{str(error['loc'][0]).upper()}: {error['msg']}"
        for error in exc.errors(include_input=False, include_url=False)
    )
