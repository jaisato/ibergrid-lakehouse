"""structlog configuration: one JSON object per line in containers, readable lines locally.

Logs go to stderr so that command output on stdout (for example the ``doctor`` report) stays
clean for pipes.
"""

import logging
import sys

import structlog

from ibergrid.settings import LogSettings


def configure_logging(settings: LogSettings) -> None:
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if settings.format == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.level]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )
