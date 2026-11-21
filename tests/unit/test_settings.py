import pytest
from pydantic import HttpUrl, ValidationError

from ibergrid.settings import BrokerSettings, Settings, base_url, describe_validation_error


@pytest.mark.usefixtures("credentials")
def test_defaults_match_the_compose_network() -> None:
    settings = Settings()

    assert base_url(settings.storage.endpoint_url) == "http://storage:9000"
    assert settings.storage.bucket == "ibergrid"
    assert base_url(settings.catalog.url) == "http://catalog:8181"
    assert settings.catalog.warehouse == "lakehouse"
    assert settings.broker.addresses() == [("broker", 9092)]
    assert (settings.log.level, settings.log.format) == ("INFO", "console")


@pytest.mark.usefixtures("credentials")
def test_each_group_reads_its_own_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBERGRID_S3_ENDPOINT_URL", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setenv("IBERGRID_S3_REGION", "auto")
    monkeypatch.setenv("IBERGRID_CATALOG_WAREHOUSE", "prod")
    monkeypatch.setenv("IBERGRID_BROKER_BOOTSTRAP_SERVERS", "a:9092, b:19092")
    monkeypatch.setenv("IBERGRID_LOG_LEVEL", "debug")
    monkeypatch.setenv("IBERGRID_LOG_FORMAT", "json")

    settings = Settings()

    assert base_url(settings.storage.endpoint_url) == "https://acct.r2.cloudflarestorage.com"
    assert settings.storage.region == "auto"
    assert settings.catalog.warehouse == "prod"
    assert settings.broker.addresses() == [("a", 9092), ("b", 19092)]
    assert (settings.log.level, settings.log.format) == ("DEBUG", "json")


@pytest.mark.usefixtures("credentials")
def test_secrets_never_appear_in_repr() -> None:
    settings = Settings()

    assert "test-secret-key-do-not-log" not in repr(settings)
    assert settings.storage.secret_key.get_secret_value() == "test-secret-key-do-not-log"


def test_missing_key_is_named_by_variable_without_echoing_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IBERGRID_S3_SECRET_KEY", "test-secret-key-do-not-log")

    with pytest.raises(ValidationError) as caught:
        Settings()

    assert describe_validation_error(caught.value) == "  IBERGRID_S3_ACCESS_KEY: Field required"
    assert "test-secret-key-do-not-log" not in str(caught.value)


@pytest.mark.usefixtures("credentials")
def test_invalid_value_is_named_by_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBERGRID_LOG_FORMAT", "xml")

    with pytest.raises(ValidationError) as caught:
        Settings()

    assert describe_validation_error(caught.value).startswith("  IBERGRID_LOG_FORMAT: ")


def test_empty_key_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBERGRID_S3_ACCESS_KEY", "")
    monkeypatch.setenv("IBERGRID_S3_SECRET_KEY", "test-secret-key-do-not-log")

    with pytest.raises(ValidationError) as caught:
        Settings()

    assert describe_validation_error(caught.value).startswith("  IBERGRID_S3_ACCESS_KEY: ")


@pytest.mark.parametrize("value", ["broker", "broker:kafka", ":9092", "a:9092,b"])
def test_bootstrap_servers_must_be_host_port_pairs(value: str) -> None:
    with pytest.raises(ValidationError, match="host:port"):
        BrokerSettings(bootstrap_servers=value)


def test_base_url_drops_the_trailing_slash() -> None:
    assert base_url(HttpUrl("http://catalog:8181")) == "http://catalog:8181"
    assert base_url(HttpUrl("http://catalog:8181/prefix/")) == "http://catalog:8181/prefix"


@pytest.mark.usefixtures("credentials")
def test_redata_defaults_are_polite(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()

    assert base_url(settings.redata.base_url) == "https://apidatos.ree.es"
    assert (settings.redata.timeout, settings.redata.max_rps) == (60.0, 1.0)
    monkeypatch.setenv("IBERGRID_REDATA_MAX_RPS", "0.5")
    assert Settings().redata.max_rps == 0.5


@pytest.mark.usefixtures("credentials")
def test_omie_defaults_are_polite() -> None:
    settings = Settings()

    assert base_url(settings.omie.base_url) == "https://www.omie.es"
    assert (settings.omie.timeout, settings.omie.max_rps) == (30.0, 1.0)


@pytest.mark.usefixtures("credentials")
@pytest.mark.parametrize("source", ["REDATA", "OMIE"])
@pytest.mark.parametrize(("name", "value"), [("MAX_RPS", "2"), ("MAX_RPS", "0"), ("TIMEOUT", "0")])
def test_source_limits_are_enforced(
    monkeypatch: pytest.MonkeyPatch, source: str, name: str, value: str
) -> None:
    monkeypatch.setenv(f"IBERGRID_{source}_{name}", value)

    with pytest.raises(ValidationError) as caught:
        Settings()

    assert describe_validation_error(caught.value).startswith(f"  IBERGRID_{source}_{name}: ")


@pytest.mark.usefixtures("credentials")
def test_the_stream_reads_its_own_prefix_and_the_broker_its_scram_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IBERGRID_BROKER_USERNAME", "ibergrid-consumer")
    monkeypatch.setenv("IBERGRID_BROKER_PASSWORD", "scram-password-do-not-log")
    monkeypatch.setenv("IBERGRID_STREAM_FLUSH_AGE", "30")

    settings = Settings()

    assert settings.broker.username == "ibergrid-consumer"
    assert "scram-password-do-not-log" not in repr(settings)
    assert settings.stream.topic == "redata.demand.realtime.v1"
    assert settings.stream.group_id == "redata-demand-to-bronze"
    assert (settings.stream.flush_age, settings.stream.flush_records) == (30.0, 10_000)
    assert settings.stream.watchdog_timeout == 900.0


@pytest.mark.usefixtures("credentials")
def test_transform_settings_reads_its_prefix_and_validates_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert Settings().transform.dbt_project_dir is None
    assert Settings().transform.threads == 1

    monkeypatch.setenv("IBERGRID_TRANSFORM_DBT_PROJECT_DIR", "/opt/ibergrid/dbt")
    monkeypatch.setenv("IBERGRID_TRANSFORM_THREADS", "4")
    settings = Settings()
    assert settings.transform.dbt_project_dir == "/opt/ibergrid/dbt"
    assert settings.transform.threads == 4

    monkeypatch.setenv("IBERGRID_TRANSFORM_THREADS", "0")
    with pytest.raises(ValidationError) as caught:
        Settings()
    assert describe_validation_error(caught.value).startswith("  IBERGRID_TRANSFORM_THREADS: ")


@pytest.mark.usefixtures("credentials")
def test_quality_maintenance_and_lineage_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = Settings()
    assert defaults.quality.contracts_dir is None
    assert (
        defaults.maintenance.snapshot_retention_days,
        defaults.maintenance.retain_last_snapshots,
        defaults.maintenance.orphan_grace_hours,
    ) == (7, 5, 48.0)
    assert (
        defaults.lineage.namespace,
        defaults.lineage.endpoint_url,
        defaults.lineage.sink_path,
    ) == ("ibergrid", None, None)

    # Empty strings in compose env pass-through normalize cleanly to None.
    monkeypatch.setenv("IBERGRID_LINEAGE_ENDPOINT_URL", "   ")
    monkeypatch.setenv("IBERGRID_LINEAGE_SINK_PATH", "")
    monkeypatch.setenv("IBERGRID_MAINTENANCE_ORPHAN_GRACE_HOURS", "24")
    monkeypatch.setenv("IBERGRID_QUALITY_CONTRACTS_DIR", "/opt/ibergrid/contracts/soda")
    updated = Settings()
    assert updated.lineage.endpoint_url is None
    assert updated.lineage.sink_path is None
    assert updated.maintenance.orphan_grace_hours == 24.0
    assert updated.quality.contracts_dir == "/opt/ibergrid/contracts/soda"

    monkeypatch.setenv("IBERGRID_LINEAGE_ENDPOINT_URL", "http://marquez:5000")
    assert base_url(Settings().lineage.endpoint_url) == "http://marquez:5000"  # type: ignore[arg-type]

    monkeypatch.setenv("IBERGRID_MAINTENANCE_ORPHAN_GRACE_HOURS", "-1")
    with pytest.raises(ValidationError) as caught:
        Settings()
    assert describe_validation_error(caught.value).startswith(
        "  IBERGRID_MAINTENANCE_ORPHAN_GRACE_HOURS: "
    )


@pytest.mark.usefixtures("credentials")
def test_api_settings_reads_prefix_and_validates_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = Settings().api
    assert (defaults.host, defaults.port, defaults.rate_limit_rpm) == ("0.0.0.0", 8000, 120)  # noqa: S104
    assert defaults.trust_proxy_headers is False
    assert defaults.cors_origin_list() == ["http://127.0.0.1:28000", "http://localhost:28000"]

    monkeypatch.setenv("IBERGRID_API_PORT", "8080")
    monkeypatch.setenv("IBERGRID_API_RATE_LIMIT_RPM", "60")
    monkeypatch.setenv("IBERGRID_API_CORS_ORIGINS", "https://a.example, https://b.example ")
    monkeypatch.setenv("IBERGRID_API_PORTAL_DIR", "  ")
    monkeypatch.setenv("IBERGRID_API_BENCHMARKS_PATH", "")
    monkeypatch.setenv("IBERGRID_API_TRUST_PROXY_HEADERS", "true")
    updated = Settings().api
    assert (updated.port, updated.rate_limit_rpm, updated.trust_proxy_headers) == (8080, 60, True)
    assert updated.portal_dir is None
    assert updated.benchmarks_path is None
    assert updated.cors_origin_list() == ["https://a.example", "https://b.example"]

    monkeypatch.setenv("IBERGRID_API_RATE_LIMIT_RPM", "0")
    with pytest.raises(ValidationError) as caught:
        Settings()
    assert describe_validation_error(caught.value).startswith("  IBERGRID_API_RATE_LIMIT_RPM: ")
