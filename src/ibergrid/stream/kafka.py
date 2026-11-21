"""librdkafka configuration (confluent-kafka) for the stream services: SASL/SCRAM always."""

from importlib.metadata import version

from ibergrid.settings import BrokerSettings

type Config = dict[str, str | int | float | bool]  # what confluent-kafka's clients take


class BrokerConfigError(ValueError):
    """The broker settings lack what an authenticated client needs."""


def client_config(broker: BrokerSettings, client_id: str) -> Config:
    """Bootstrap servers and SCRAM credentials; plaintext inside the compose network (ADR 0006:
    TLS is a known gap)."""
    if broker.username is None or broker.password is None:
        raise BrokerConfigError(
            "IBERGRID_BROKER_USERNAME and IBERGRID_BROKER_PASSWORD are required: the broker "
            "accepts SASL/SCRAM clients only"
        )
    return {
        "bootstrap.servers": broker.bootstrap_servers,
        "client.id": f"{client_id}/{version('ibergrid')}",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": broker.sasl_mechanism,
        "sasl.username": broker.username,
        "sasl.password": broker.password.get_secret_value(),
    }


def producer_config(broker: BrokerSettings, client_id: str) -> Config:
    """Idempotent and fully acknowledged: a retried send is written once, and only once every
    in-sync replica has it. Java's partitioner, so other clients put a key where this one does."""
    return {
        **client_config(broker, client_id),
        "enable.idempotence": True,
        "acks": "all",
        "partitioner": "murmur2_random",
        "compression.type": "zstd",
        "linger.ms": 50,
        # A send the broker has not acknowledged after two minutes fails, and so does the poll.
        "message.timeout.ms": 120_000,
    }


def consumer_config(broker: BrokerSettings, client_id: str, group_id: str) -> Config:
    """Manual assignment and commits: the table's checkpoint decides where reading starts, and
    the group only receives the offsets for lag visibility. An offset out of range is an error
    (``_AUTO_OFFSET_RESET``), never a silent jump: the consumer starts again from its checkpoint,
    whose checks decide (ADR 0006)."""
    return {
        **client_config(broker, client_id),
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
        "isolation.level": "read_committed",
    }
