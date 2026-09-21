import logging

from weather_pipeline.logging_utils import (
    configure_logging,
)


def test_logging_configuration(
    tmp_path,
):

    logger = configure_logging(
        tmp_path
    )

    assert isinstance(
        logger,
        logging.Logger,
    )

    assert logger.level == logging.INFO

    assert (
        tmp_path /
        "pipeline.log"
    ).exists()