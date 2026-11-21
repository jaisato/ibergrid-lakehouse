"""The PyIceberg catalog: Lakekeeper's Iceberg REST API, with this client's own S3 access."""

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import FileIO
from pyiceberg.typedef import EMPTY_DICT, Properties

from ibergrid.settings import Settings, StorageSettings, base_url


def s3_properties(storage: StorageSettings) -> dict[str, str]:
    """PyIceberg FileIO properties for this client's endpoint and keys. Holds the secret key."""
    return {
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": base_url(storage.endpoint_url),
        "s3.region": storage.region,
        "s3.access-key-id": storage.access_key.get_secret_value(),
        "s3.secret-access-key": storage.secret_key.get_secret_value(),
        "s3.path-style-access": "true",
        "s3.remote-signing-enabled": "false",
    }


class LakeCatalog(RestCatalog):
    """A REST catalog whose FileIO always uses the client's S3 properties.

    Lakekeeper answers every loadTable with the warehouse's endpoint (``http://storage:9000``,
    resolvable only inside the compose network), and PyIceberg lets that table config override the
    catalog properties. Applying the client's properties last is what makes the same settings work
    from containers and from the host (ADR 0002).
    """

    def __init__(self, name: str, client_io: Properties, **properties: str) -> None:
        self._client_io = dict(client_io)
        super().__init__(name, **properties)

    # shortcut: overrides PyIceberg 0.12's private FileIO hook (a unit test pins it); re-check on
    # every PyIceberg upgrade.
    def _load_file_io(
        self, properties: Properties = EMPTY_DICT, location: str | None = None
    ) -> FileIO:
        return super()._load_file_io({**properties, **self._client_io}, location)


def rest_catalog(settings: Settings) -> LakeCatalog:
    """The warehouse's catalog; connects (``GET /v1/config``) when it is built."""
    return LakeCatalog(
        settings.catalog.warehouse,
        s3_properties(settings.storage),
        uri=f"{base_url(settings.catalog.url)}/catalog",
        warehouse=settings.catalog.warehouse,
    )
