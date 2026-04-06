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
    """Run id folder/name: dd.mm.yy:HH:MM:SS (zero-padded)."""
    return (
        f"{dt.day:02d}.{dt.month:02d}.{dt.year % 100:02d}:"
        f"{dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
    )


def _format_date_time(dt: datetime) -> str:
    """Format date-time as d.m.yy-H:M:S without leading zeros (e.g. 20.2.23-11:2:03)."""
    return f"{dt.day}.{dt.month}.{dt.year % 100}-{dt.hour}:{dt.minute}:{dt.second}"


def setup_logger():
    """Set up logging configuration to write to both file and console.

    Logs are stored under `logs/<day.month.yy>/reaction_<date-time>.log`.
    """
    base_logs_dir = "logs"
    if not os.path.exists(base_logs_dir):
        os.makedirs(base_logs_dir)

    now = datetime.now()
    day_folder_name = f"{now.day}.{now.month}.{now.year % 100}"
    day_logs_dir = os.path.join(base_logs_dir, day_folder_name)
    if not os.path.exists(day_logs_dir):
        os.makedirs(day_logs_dir)

    logger = logging.getLogger("Reaction")

    # If logger already has handlers, return it
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    current_time_str = _format_date_time(now)
    log_filename = os.path.join(day_logs_dir, f"reaction_{current_time_str}.log")
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    if not is_quiet_terminal():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def attach_run_log_file(run_root: str, run_ts: str) -> str:
    """
    Move file logging to run_root/reaction_<run_ts>.log.
    Removes existing FileHandlers on the Reaction logger (e.g. default logs/ from setup_logger).
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