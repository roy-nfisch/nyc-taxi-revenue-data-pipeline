"""Set up this application's logger only when an entry point requests it.

Importing this module does not configure logging or start pipeline work.
"""

import logging

from green_taxi.settings import PipelineSettings


def configure_logging(settings: PipelineSettings) -> logging.Logger:
    """Configure and return the shared green_taxi logger.

    Args:
        settings: Validated PipelineSettings; only log_level is used here.
    Returns:
        The named logging.Logger for application messages and child loggers.
    Side effects:
        Changes that logger's level and disables propagation to root handlers.
        If it has no handler, adds a stderr handler showing level, name, and
        message. Repeated calls reuse existing handlers rather than add duplicates.

    Does not change the root logger or configure logging for other libraries.
    The current settings-inspection command does not call this function; future
    pipeline entry points must call it explicitly before emitting run messages.
    """
    logger = logging.getLogger("green_taxi")
    logger.setLevel(settings.log_level)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
    return logger
