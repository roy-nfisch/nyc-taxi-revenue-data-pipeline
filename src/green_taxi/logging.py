"""Application logging, configured explicitly rather than during imports."""

import logging

from green_taxi.settings import PipelineSettings


def configure_logging(settings: PipelineSettings) -> logging.Logger:
    """Return the configured application logger; repeated calls reuse its handler."""
    logger = logging.getLogger("green_taxi")
    logger.setLevel(settings.log_level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
    return logger
