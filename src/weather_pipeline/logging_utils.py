from __future__ import annotations

import logging

from pathlib import Path


def configure_logging(
    log_dir: Path,
) -> logging.Logger:
    """
    Configure console and file logging
    for the weather pipeline.
    """

    log_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger = logging.getLogger(
        "weather_pipeline"
    )

    logger.setLevel(
        logging.INFO
    )


    # Avoid adding duplicate handlers when
    # configure_logging() is called more than once.
    if logger.handlers:
        return logger


    formatter = logging.Formatter(

        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )


    # Console handler
    stream_handler = (
        logging.StreamHandler()
    )

    stream_handler.setFormatter(
        formatter
    )


    # File handler
    file_handler = (
        logging.FileHandler(

            log_dir /
            "pipeline.log",

            encoding="utf-8",
        )
    )

    file_handler.setFormatter(
        formatter
    )


    logger.addHandler(
        stream_handler
    )

    logger.addHandler(
        file_handler
    )


    return logger