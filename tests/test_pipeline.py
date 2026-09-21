from unittest.mock import MagicMock, patch

from weather_pipeline.pipeline import run


def test_pipeline_calls_stages_in_order(
    sample_settings,
):
    """
    Verify that the main pipeline stages are executed
    in the expected order.

    Expected order:

        extract
        clean
        load_clean
        transform
        database
    """

    execution_order = []

    # Fake clean data returned by load_clean().
    hourly_data = MagicMock()

    # Tell the pipeline that clean data exists.
    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all",
            side_effect=lambda settings:
                execution_order.append(
                    "extract"
                ),
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental",
            side_effect=lambda raw, clean:
                execution_order.append(
                    "clean"
                ),
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            side_effect=lambda clean: (
                execution_order.append(
                    "load_clean"
                )
                or hourly_data
            ),
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms",
            side_effect=lambda data, path, alerts:
                execution_order.append(
                    "transform"
                ),
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres",
            side_effect=lambda path:
                execution_order.append(
                    "database"
                ),
        ),
    ):

        run(
            "config/settings.yaml"
        )


    assert execution_order == [
        "extract",
        "clean",
        "load_clean",
        "transform",
        "database",
    ]


def test_pipeline_calls_extract(
    sample_settings,
):
    """
    Verify that the extract stage is called.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ) as extract_mock,

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ),
    ):

        run(
            "config/settings.yaml"
        )


    extract_mock.assert_called_once_with(
        sample_settings
    )


def test_pipeline_calls_clean_stage(
    sample_settings,
):
    """
    Verify that the clean stage receives the
    configured raw and clean directories.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ) as clean_mock,

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ),
    ):

        run(
            "config/settings.yaml"
        )


    clean_mock.assert_called_once_with(
        sample_settings.paths.raw,
        sample_settings.paths.clean,
    )


def test_pipeline_calls_transform_stage(
    sample_settings,
):
    """
    Verify that the transform stage receives:

        clean hourly data
        transform directory
        alert configuration
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ) as transform_mock,

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ),
    ):

        run(
            "config/settings.yaml"
        )


    transform_mock.assert_called_once_with(
        hourly_data,
        sample_settings.paths.transform,
        sample_settings.alerts,
    )


def test_pipeline_loads_database_by_default(
    sample_settings,
):
    """
    Verify that PostgreSQL loading occurs when
    skip_db is False.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ) as database_mock,
    ):

        run(
            "config/settings.yaml",
            skip_db=False,
        )


    database_mock.assert_called_once_with(
        sample_settings.paths.transform
    )


def test_pipeline_can_skip_database(
    sample_settings,
):
    """
    Verify that PostgreSQL is not called when
    skip_db=True.

    This is useful for development and testing where
    we want to run:

        extract
        clean
        transform

    without requiring PostgreSQL.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ) as transform_mock,

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ) as database_mock,
    ):

        run(
            "config/settings.yaml",
            skip_db=True,
        )


    # Transform should still run.
    transform_mock.assert_called_once()


    # PostgreSQL should not run.
    database_mock.assert_not_called()


def test_pipeline_stops_when_no_clean_data(
    sample_settings,
):
    """
    If load_clean() returns no observations,
    the pipeline should stop before transform
    and database loading.
    """

    hourly_data = MagicMock()

    # Simulate an empty clean dataset.
    hourly_data.is_empty.return_value = True


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ) as extract_mock,

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ) as clean_mock,

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ) as load_clean_mock,

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ) as transform_mock,

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ) as database_mock,
    ):

        run(
            "config/settings.yaml"
        )


    # These stages should still happen.
    extract_mock.assert_called_once()

    clean_mock.assert_called_once()

    load_clean_mock.assert_called_once()


    # These must not happen because there
    # is no clean data to process.
    transform_mock.assert_not_called()

    database_mock.assert_not_called()


def test_pipeline_stops_before_database_when_no_clean_data_even_if_db_enabled(
    sample_settings,
):
    """
    Even when skip_db=False, an empty clean
    dataset should prevent the database stage
    from running.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = True


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ) as transform_mock,

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ) as database_mock,
    ):

        run(
            "config/settings.yaml",
            skip_db=False,
        )


    transform_mock.assert_not_called()

    database_mock.assert_not_called()


def test_pipeline_uses_configured_clean_directory(
    sample_settings,
):
    """
    Verify that load_clean() reads from the
    clean directory defined in project settings.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ),

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ) as load_clean_mock,

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ),
    ):

        run(
            "config/settings.yaml"
        )


    load_clean_mock.assert_called_once_with(
        sample_settings.paths.clean
    )


def test_pipeline_configures_logging(
    sample_settings,
):
    """
    Verify that logging is configured using
    the logs directory from settings.
    """

    hourly_data = MagicMock()

    hourly_data.is_empty.return_value = False


    with (
        patch(
            "weather_pipeline.pipeline.load_settings",
            return_value=sample_settings,
        ),

        patch(
            "weather_pipeline.pipeline.configure_logging"
        ) as logging_mock,

        patch(
            "weather_pipeline.pipeline.extract_all"
        ),

        patch(
            "weather_pipeline.pipeline.clean_incremental"
        ),

        patch(
            "weather_pipeline.pipeline.load_clean",
            return_value=hourly_data,
        ),

        patch(
            "weather_pipeline.pipeline.write_transforms"
        ),

        patch(
            "weather_pipeline.pipeline.load_to_postgres"
        ),
    ):

        run(
            "config/settings.yaml"
        )


    logging_mock.assert_called_once_with(
        sample_settings.paths.logs
    )