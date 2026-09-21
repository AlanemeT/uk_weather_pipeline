from __future__ import annotations

import argparse
import logging

from weather_pipeline.clean import clean_incremental

from weather_pipeline.config import load_settings

from weather_pipeline.database import load_to_postgres

from weather_pipeline.extract import extract_all

from weather_pipeline.logging_utils import configure_logging

from weather_pipeline.transform import (
    load_clean,
    write_transforms,
)


def run(
    config_path: str,
    skip_db: bool = False,
) -> None:
    """
    Execute the complete weather pipeline.

    Stages:
        1. Load configuration
        2. Extract Open-Meteo data
        3. Clean / flatten data
        4. Transform using Polars
        5. Load into PostgreSQL
    """

    # ----------------------------------
    # Configuration
    # ----------------------------------

    settings = load_settings(
        config_path
    )


    # ----------------------------------
    # Logging
    # ----------------------------------

    configure_logging(
        settings.paths.logs
    )


    logger = logging.getLogger(
        "weather_pipeline"
    )


    logger.info(
        "Pipeline started"
    )


    # ==================================
    # EXTRACT
    # ==================================

    logger.info(
        "Starting extract stage"
    )

    extract_all(
        settings
    )


    # ==================================
    # CLEAN
    # ==================================

    logger.info(
        "Starting clean stage"
    )

    clean_incremental(

        settings.paths.raw,

        settings.paths.clean,
    )


    # ==================================
    # READ CLEAN DATA
    # ==================================

    hourly_data = load_clean(
        settings.paths.clean
    )


    if hourly_data.is_empty():

        logger.warning(
            "No clean rows available; "
            "stopping before transform/load"
        )

        return


    # ==================================
    # TRANSFORM
    # ==================================

    logger.info(
        "Starting transform stage"
    )


    write_transforms(

        hourly_data,

        settings.paths.transform,

        settings.alerts,
    )


    # ==================================
    # LOAD
    # ==================================

    if not skip_db:

        logger.info(
            "Starting PostgreSQL load"
        )

        load_to_postgres(
            settings.paths.transform
        )

    else:

        logger.info(
            "PostgreSQL load skipped"
        )


    logger.info(
        "Pipeline completed"
    )


def main() -> None:
    """
    Command-line entry point.
    """

    parser = argparse.ArgumentParser(

        description=(
            "Run the UK weather data pipeline"
        )
    )


    parser.add_argument(

        "--config",

        default=(
            "config/settings.yaml"
        ),

        help=(
            "Path to pipeline "
            "configuration file"
        ),
    )


    parser.add_argument(

        "--skip-db",

        action="store_true",

        help=(
            "Run extract, clean and "
            "transform without PostgreSQL"
        ),
    )


    args = parser.parse_args()


    run(
        args.config,
        args.skip_db,
    )


if __name__ == "__main__":
    main()