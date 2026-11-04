"""Logging setup stays local to this application's logger."""

import logging

import pytest

from green_taxi.logging import configure_logging
from green_taxi.settings import PipelineSettings

pytestmark = pytest.mark.unit


def test_logging_reuses_handler_without_changing_root():
    """Repeated setup must not print every message twice or reconfigure libraries."""
    root_handlers = logging.getLogger().handlers[:]
    logger = logging.getLogger("green_taxi")
    previous = logger.handlers[:], logger.level, logger.propagate
    try:
        first = configure_logging(PipelineSettings.from_file())
        handlers = first.handlers[:]
        second = configure_logging(
            PipelineSettings.from_file().model_copy(update={"log_level": "DEBUG"})
        )
        assert second.handlers == handlers
        assert second.level == logging.DEBUG
        assert logging.getLogger().handlers == root_handlers
    finally:
        for handler in logger.handlers:
            if handler not in previous[0]:
                handler.close()
        logger.handlers, logger.level, logger.propagate = previous
