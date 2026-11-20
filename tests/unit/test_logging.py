import json

import pytest
import structlog

from ibergrid.logging import configure_logging
from ibergrid.settings import LogSettings


def test_json_format_writes_one_object_per_line_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(LogSettings(format="json"))

    structlog.get_logger().info("catalog.bootstrap", changed=True)

    captured = capsys.readouterr()
    assert captured.out == ""
    record = json.loads(captured.err)
    assert record["event"] == "catalog.bootstrap"
    assert record["changed"] is True
    assert record["level"] == "info"
    assert record["timestamp"].endswith("Z")


def test_console_format_is_plain_text_without_colours_off_a_terminal(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(LogSettings(format="console"))

    structlog.get_logger().warning("catalog.waiting", attempt=2)

    err = capsys.readouterr().err
    assert "catalog.waiting" in err
    assert "attempt=2" in err
    assert "\x1b[" not in err


def test_level_filters_lower_events(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(LogSettings(level="WARNING", format="json"))

    structlog.get_logger().info("hidden")
    structlog.get_logger().error("shown")

    lines = capsys.readouterr().err.splitlines()
    assert [json.loads(line)["event"] for line in lines] == ["shown"]
