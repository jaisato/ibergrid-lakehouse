import pytest
from pydantic import HttpUrl, ValidationError

from ibergrid.settings import BrokerSettings, Settings, base_url, describe_validation_error


@pytest.mark.usefixtures("credentials")
def test_defaults_match_the_compose_network() -> None:
    settings = Settings()

    assert base_url(settings.storage.endpoint_url) == "http://storage:9000"
    assert settings.storage.bucket == "ibergrid"
    assert settings.storage.path_style is True
    assert base_url(settings.catalog.url) == "http://catalog:8181"
    assert settings.catalog.warehouse == "lakehouse"
    assert settings.broker.addresses() == [("broker", 9092)]
    assert (settings.log.level, settings.log.format) == ("INFO", "console")


@pytest.mark.usefixtures("credentials")
def test_each_group_reads_its_own_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IBERGRID_S3_ENDPOINT_URL", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setenv("IBERGRID_S3_REGION", "auto")
    monkeypatch.setenv("IBERGRID_S3_PATH_STYLE", "false")
    monkeypatch.setenv("IBERGRID_CATALOG_WAREHOUSE", "prod")
    monkeypatch.setenv("IBERGRID_BROKER_BOOTSTRAP_SERVERS", "a:9092, b:19092")
    monkeypatch.setenv("IBERGRID_LOG_LEVEL", "debug")
    monkeypatch.setenv("IBERGRID_LOG_FORMAT", "json")

    settings = Settings()

    assert base_url(settings.storage.endpoint_url) == "https://acct.r2.cloudflarestorage.com"
    assert settings.storage.region == "auto"
    assert settings.storage.path_style is False
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


@pytest.mark.parametrize("value", ["broker", "broker:kafka", ":9092", "a:9092,b"])
def test_bootstrap_servers_must_be_host_port_pairs(value: str) -> None:
    with pytest.raises(ValidationError, match="host:port"):
        BrokerSettings(bootstrap_servers=value)


def test_base_url_drops_the_trailing_slash() -> None:
    assert base_url(HttpUrl("http://catalog:8181")) == "http://catalog:8181"
    assert base_url(HttpUrl("http://catalog:8181/prefix/")) == "http://catalog:8181/prefix"
