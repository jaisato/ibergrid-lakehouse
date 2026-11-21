"""Medallion dbt transforms on the running stack: host runner and orchestration container."""

from dataclasses import replace
from datetime import date
from pathlib import Path

import duckdb
import pytest
from pyarrow import fs

from ibergrid.lake import duck, registry, wap
from ibergrid.lake.catalog import LakeCatalog
from ibergrid.lake.tables import ensure_table
from ibergrid.settings import Settings
from ibergrid.sources.omie import ingest as omie_ingest
from ibergrid.sources.omie.client import OmieClient
from ibergrid.sources.redata import ingest as redata_ingest
from ibergrid.sources.redata.client import RedataClient
from ibergrid.transform import run_medallion

from ..unit.conftest import FakeOmie, FakeRedata, LocalServer
from .conftest import compose_run

pytestmark = pytest.mark.integration


def _no_sleep(_seconds: float) -> None:
    pass


def test_medallion_runs_end_to_end_on_lakekeeper_and_duckdb(
    catalog: LakeCatalog,
    namespace: str,
    settings: Settings,
    con: duckdb.DuckDBPyConnection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ingest real recorded fixtures into a throwaway namespace, run dbt+WAP, query via DuckDB."""
    for spec in registry.TABLES:
        ensure_table(catalog, replace(spec, namespace=namespace))

    redata_srv = LocalServer(FakeRedata())
    omie_srv = LocalServer(FakeOmie())
    try:
        pointed = settings.model_copy(
            update={
                "redata": settings.redata.model_copy(update={"base_url": redata_srv.url}),
                "omie": settings.omie.model_copy(update={"base_url": omie_srv.url}),
            }
        )
        landing = fs.SubTreeFileSystem(str(tmp_path), fs.LocalFileSystem())
        for mod in (redata_ingest, omie_ingest):
            monkeypatch.setattr(mod, "s3_filesystem", lambda *_a, **_kw: landing)
        monkeypatch.setattr(omie_ingest, "madrid_today", lambda: date(2026, 11, 21))
        for cls in (RedataClient, OmieClient):
            monkeypatch.setattr(
                cls,
                "from_settings",
                classmethod(
                    lambda c, s: c(
                        str(s.base_url).rstrip("/"), timeout=5, max_rps=1e9, sleep=_no_sleep
                    )
                ),
            )

        for day in (date(2025, 4, 28), date(2025, 9, 30), date(2025, 10, 1), date(2026, 11, 18)):
            omie_ingest.ingest(
                pointed, day, day, table=(namespace, registry.OMIE_DAY_AHEAD_PRICES.name)
            )
        redata_ingest.ingest(
            pointed,
            "market_prices",
            date(2025, 4, 28),
            date(2025, 4, 28),
            table=(namespace, registry.REDATA_OBSERVATIONS.name),
        )
        redata_ingest.ingest(
            pointed,
            "demand_hourly",
            date(2026, 11, 18),
            date(2026, 11, 18),
            ("peninsular", "canarias"),
            table=(namespace, registry.REDATA_OBSERVATIONS.name),
        )
        redata_ingest.ingest(
            pointed,
            "generation_daily",
            date(2026, 11, 18),
            date(2026, 11, 18),
            ("canarias",),
            table=(namespace, registry.REDATA_OBSERVATIONS.name),
        )
    finally:
        redata_srv.close()
        omie_srv.close()

    report = run_medallion(
        settings,
        run_id_prefix="it-medallion",
        bronze_namespace=namespace,
        target_namespace=namespace,
    )

    assert len(report.tables) == len(registry.MEDALLION_TABLES)
    assert report.dbt_tests_passed > 50
    for spec in registry.MEDALLION_TABLES:
        assert wap.audit_branches(catalog.load_table((namespace, spec.name))) == []

    # Verify DuckDB reads the published Iceberg Silver and Gold tables cleanly.
    summary = duck.scan(con, duck.table_name(f"{namespace}.mibel_daily_summary")).fetchall()
    assert len(summary) == 4
    sys_daily = duck.scan(con, duck.table_name(f"{namespace}.system_demand_daily")).fetchall()
    assert len(sys_daily) == 2
    by_sys = {row[1]: row for row in sys_daily}
    assert by_sys["peninsular"][2] == "peninsular"
    assert by_sys["peninsular"][3] is True
    assert by_sys["canarias"][2] == "non_peninsular"
    assert by_sys["canarias"][3] is False


def test_the_orchestration_image_ships_dbt_project_and_transform_cli() -> None:
    """Verify the orchestration image's baked /opt/ibergrid/dbt and transform CLI entrypoint."""
    result = compose_run("transform", "--help")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Build Silver and Gold medallion tables" in result.stdout

    check = compose_run(
        "transform",
        "-c",
        "from ibergrid.settings import Settings; "
        "from ibergrid.transform.runner import find_dbt_project; "
        "print(find_dbt_project(Settings()))",
        entrypoint="python",
    )
    assert check.returncode == 0, check.stdout + check.stderr
    assert check.stdout.strip() == "/opt/ibergrid/dbt"
