"""
log levels:

logger.debug("This is a debug message")
logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")
"""

import logging
import os
from datetime import datetime


def is_quiet_terminal():
    """When True, logs go to file only; keep terminal for explicit prints (e.g. Kafka sent)."""
    return os.environ.get("REACTION_QUIET", "").lower() in ("1", "true", "yes")


def format_run_timestamp(dt: datetime) -> str:
    """Run id folder/name: dd.mm.yy-HH:MM:SS (zero-padded)."""
    return (
        f"{dt.day:02d}.{dt.month:02d}.{dt.year % 100:02d}-"
        f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
    )


def setup_logger():
    """Set up logging configuration.

    Console handler is attached here; file handler is attached per run via attach_run_log_file().
    """

    logger = logging.getLogger("Reaction")

    # If logger already has handlers, return it
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if not is_quiet_terminal():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def attach_run_log_file(run_root: str, run_ts: str) -> str:
    """
    Attach file logging to run_root/reaction_<run_ts>.log.
    Removes any existing FileHandlers on the Reaction logger.
    """
    os.makedirs(run_root, exist_ok=True)

    logger = logging.getLogger("Reaction")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    for h in list(logger.handlers):
        if isinstance(h, logging.FileHandler):
            logger.removeHandler(h)
            h.close()

    log_path = os.path.join(run_root, f"reaction_{run_ts}.log")
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return log_path