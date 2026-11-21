"""The one Dagster resource: ibergrid's settings, and where a run writes."""

import dagster as dg
from pydantic import HttpUrl

from ibergrid.lake.tables import TableSpec
from ibergrid.logging import configure_logging
from ibergrid.settings import LogSettings, Settings


class Lake(dg.ConfigurableResource):  # type: ignore[type-arg]  # generic only for its value type
    """``Settings()`` from the ``IBERGRID_*`` environment, as for the CLI.

    The optional fields point a run elsewhere: the integration tests use them for the recorded
    sources, a throwaway namespace and a landing prefix of their own. Production sets none.
    """

    redata_base_url: str | None = None
    omie_base_url: str | None = None
    namespace: str | None = None  # instead of each table's own (bronze)
    landing_prefix: str | None = None  # in front of each connector's landing prefix

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        # The connectors log through structlog: JSON lines in the run's logs, as in containers.
        configure_logging(LogSettings())

    def settings(self) -> Settings:
        settings = Settings()
        if self.redata_base_url is not None:
            redata = settings.redata.model_copy(update={"base_url": HttpUrl(self.redata_base_url)})
            settings = settings.model_copy(update={"redata": redata})
        if self.omie_base_url is not None:
            omie = settings.omie.model_copy(update={"base_url": HttpUrl(self.omie_base_url)})
            settings = settings.model_copy(update={"omie": omie})
        return settings

    def table(self, spec: TableSpec) -> tuple[str, str]:
        return spec.identifier if self.namespace is None else (self.namespace, spec.name)

    def landing(self, prefix: str) -> str:
        return prefix if self.landing_prefix is None else f"{self.landing_prefix}/{prefix}"
