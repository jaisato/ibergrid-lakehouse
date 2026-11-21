"""The REST catalog against a local server that answers like Lakekeeper 0.15.1."""

import json
from collections.abc import Callable
from typing import Any

from pydantic import HttpUrl, SecretStr
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.types import LongType, NestedField

from ibergrid.lake.catalog import VENDED_KEYS, rest_catalog
from ibergrid.settings import CatalogSettings, Settings, StorageSettings

from .conftest import LocalServer, Recorded, Responder

Serve = Callable[[Responder], LocalServer]
LOCATION = "s3://ibergrid/lakehouse/0001"
# What Lakekeeper puts in every loadTable config: the in-network endpoint, and remote signing on
# warehouses created before ADR 0002 disabled it.
LAKEKEEPER_TABLE_CONFIG = {
    "s3.endpoint": "http://storage:9000/",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
    "s3.remote-signing-enabled": "true",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    # Vended credentials, as a catalog with STS would add them: they must not mix with our keys.
    "s3.session-token": "vended-session-token",
    "client.access-key-id": "vended-access-key",
}


def lakekeeper(request: Recorded) -> tuple[int, Any]:
    if request.path == "/catalog/v1/config?warehouse=lakehouse":
        return 200, {"defaults": {"prefix": "wh-id"}, "overrides": {}}
    if request.path in (
        "/catalog/v1/wh-id/namespaces/ops/tables/smoke",  # loadTable
        "/catalog/v1/wh-id/namespaces/ops/tables",  # createTable
    ):
        metadata = new_table_metadata(
            Schema(NestedField(1, "x", LongType(), required=False)),
            UNPARTITIONED_PARTITION_SPEC,
            UNSORTED_SORT_ORDER,
            LOCATION,
        )
        return 200, {
            "metadata-location": f"{LOCATION}/metadata/00000.metadata.json",
            "metadata": json.loads(metadata.model_dump_json()),
            "config": LAKEKEEPER_TABLE_CONFIG,
        }
    return 404, {"error": {"type": "NotFound", "message": request.path, "code": 404}}


def settings_for(catalog_url: str) -> Settings:
    return Settings(
        storage=StorageSettings(
            endpoint_url=HttpUrl("http://127.0.0.1:29000"),
            access_key=SecretStr("test-access-key"),
            secret_key=SecretStr("test-secret-key-do-not-log"),
        ),
        catalog=CatalogSettings(url=HttpUrl(catalog_url)),
    )


def test_the_clients_storage_settings_override_the_table_config(serve: Serve) -> None:
    server = serve(lakekeeper)
    catalog = rest_catalog(settings_for(server.url))

    loaded = catalog.load_table(("ops", "smoke"))
    created = catalog.create_table(("ops", "smoke"), Schema(NestedField(1, "x", LongType())))

    for table in (loaded, created):
        assert isinstance(table.io, PyArrowFileIO)
        assert table.io.properties["s3.endpoint"] == "http://127.0.0.1:29000"
        assert table.io.properties["s3.access-key-id"] == "test-access-key"
        assert table.io.properties["s3.remote-signing-enabled"] == "false"
        assert not VENDED_KEYS & set(table.io.properties)
    # The keys stay on the client: nothing sent to the catalog carries them.
    sent = str([(r.path, r.headers, r.body) for r in server.requests])
    assert "test-secret-key-do-not-log" not in sent
