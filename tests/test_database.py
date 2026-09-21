from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl

from weather_pipeline.database import (
    _records,
    apply_schema,
    connection_string,
    load_to_postgres,
)


# =========================================================
# _records() TEST
# =========================================================


def test_records_conversion():
    """
    Verify that a Polars DataFrame can be converted
    into tuples suitable for psycopg executemany().
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
                "brighton",
            ],
            "temperature_c": [
                20.0,
                18.0,
            ],
        }
    )

    result = _records(
        dataframe,
        [
            "city_id",
            "temperature_c",
        ],
    )

    assert result == [
        ("london", 20.0),
        ("brighton", 18.0),
    ]


# =========================================================
# CONNECTION STRING TEST
# =========================================================


def test_connection_string(
    monkeypatch,
):
    """
    Verify that the PostgreSQL connection string
    is built from environment variables.
    """

    monkeypatch.setenv(
        "POSTGRES_HOST",
        "test-host",
    )

    monkeypatch.setenv(
        "POSTGRES_PORT",
        "5555",
    )

    monkeypatch.setenv(
        "POSTGRES_DB",
        "test_weather",
    )

    monkeypatch.setenv(
        "POSTGRES_USER",
        "test_user",
    )

    monkeypatch.setenv(
        "POSTGRES_PASSWORD",
        "test_password",
    )

    result = connection_string()

    assert "host=test-host" in result

    assert "port=5555" in result

    assert "dbname=test_weather" in result

    assert "user=test_user" in result

    assert "password=test_password" in result


# =========================================================
# APPLY SCHEMA TEST
# =========================================================


def test_apply_schema(
    tmp_path,
):
    """
    Verify that apply_schema() reads the SQL schema
    file and executes it using a database cursor.
    """

    schema_file = (
        tmp_path
        / "schema.sql"
    )

    schema_sql = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY
    );
    """

    schema_file.write_text(
        schema_sql,
        encoding="utf-8",
    )


    connection = MagicMock()

    cursor = MagicMock()


    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )


    apply_schema(
        connection,
        schema_file,
    )


    cursor.execute.assert_called_once()


    executed_sql = (
        cursor
        .execute
        .call_args
        .args[0]
    )


    assert (
        "CREATE TABLE test_table"
        in executed_sql
    )


    connection.commit.assert_called_once()


# =========================================================
# DATABASE LOAD WITH NO FILES
# =========================================================


def test_database_load_skips_when_no_hourly_files(
    tmp_path,
):
    """
    When no transformed hourly Parquet files exist,
    load_to_postgres() should stop without attempting
    a PostgreSQL connection.
    """

    transform_dir = tmp_path


    with patch(
        "weather_pipeline.database.psycopg.connect"
    ) as connect_mock:

        load_to_postgres(
            transform_dir
        )


    connect_mock.assert_not_called()


# =========================================================
# HELPER FUNCTION FOR DATABASE TEST DATA
# =========================================================


def create_transformed_test_files(
    transform_dir,
):
    """
    Create small transformed Parquet datasets that
    look like the output produced by transform.py.

    These files allow us to test database.py without
    running the real extract/transform pipeline.
    """

    hourly_dir = (
        transform_dir
        / "hourly"
    )

    hourly_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    # -----------------------------------------------------
    # Hourly weather
    # -----------------------------------------------------

    hourly = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "city_name": [
                "London",
            ],

            "latitude": [
                51.5074,
            ],

            "longitude": [
                -0.1278,
            ],

            "elevation_m": [
                25.0,
            ],

            "timezone": [
                "Europe/London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                    0,
                )
            ],

            "temperature_c": [
                20.0,
            ],

            "relative_humidity_pct": [
                70.0,
            ],

            "precipitation_mm": [
                0.0,
            ],

            "wind_speed_kmh": [
                15.0,
            ],

            "weather_code": [
                1,
            ],

            "source_file": [
                "london_20260914.json",
            ],

            "ingested_at_utc": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                    30,
                    tzinfo=timezone.utc,
                )
            ],
        }
    )


    hourly.write_parquet(
        hourly_dir
        / "london.parquet"
    )


    # -----------------------------------------------------
    # Daily summary
    # -----------------------------------------------------

    daily = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "weather_date": [
                date(
                    2026,
                    9,
                    14,
                )
            ],

            "min_temperature_c": [
                15.0,
            ],

            "max_temperature_c": [
                22.0,
            ],

            "avg_temperature_c": [
                18.5,
            ],

            "total_precipitation_mm": [
                2.0,
            ],

            "avg_wind_speed_kmh": [
                14.0,
            ],
        }
    )


    daily.write_parquet(
        transform_dir
        / "daily_summary.parquet"
    )


    # -----------------------------------------------------
    # Alerts
    # -----------------------------------------------------

    alerts = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                    0,
                )
            ],

            "alert_type": [
                "HIGH_WIND",
            ],

            "temperature_c": [
                20.0,
            ],

            "precipitation_mm": [
                0.0,
            ],

            "wind_speed_kmh": [
                70.0,
            ],
        }
    )


    alerts.write_parquet(
        transform_dir
        / "weather_alerts.parquet"
    )


# =========================================================
# INSERTION TEST
# =========================================================


def test_database_inserts_transformed_data(
    tmp_path,
):
    """
    Verify that load_to_postgres() attempts to insert
    cities, hourly weather, daily summaries and alerts.

    PostgreSQL itself is mocked, so no real database
    connection is made.
    """

    create_transformed_test_files(
        tmp_path
    )


    connection = MagicMock()

    cursor = MagicMock()


    # psycopg.connect(...) is used as a context manager:
    #
    # with psycopg.connect(...) as connection:
    #
    connection.__enter__.return_value = (
        connection
    )

    connection.__exit__.return_value = (
        False
    )


    # connection.cursor() is also used
    # as a context manager.
    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    connection.cursor.return_value.__exit__.return_value = (
        False
    )


    with (
        patch(
            "weather_pipeline.database.psycopg.connect",
            return_value=connection,
        ),

        patch(
            "weather_pipeline.database.apply_schema"
        ) as schema_mock,
    ):

        load_to_postgres(
            tmp_path,
            dsn="fake-database",
        )


    schema_mock.assert_called_once_with(
        connection
    )


    # We expect four executemany calls:
    #
    # 1. cities
    # 2. weather_hourly
    # 3. weather_daily_summary
    # 4. weather_alerts

    assert (
        cursor.executemany.call_count
        == 4
    )


    connection.commit.assert_called()


# =========================================================
# DUPLICATE PROTECTION - HOURLY
# =========================================================


def test_hourly_insert_uses_duplicate_protection(
    tmp_path,
):
    """
    Verify that hourly weather inserts contain
    PostgreSQL duplicate protection.

    The natural key is:

        city_id + observation_timestamp

    and duplicate rows should use:

        ON CONFLICT (...) DO NOTHING
    """

    create_transformed_test_files(
        tmp_path
    )


    connection = MagicMock()

    cursor = MagicMock()


    connection.__enter__.return_value = (
        connection
    )

    connection.__exit__.return_value = (
        False
    )


    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    connection.cursor.return_value.__exit__.return_value = (
        False
    )


    with (
        patch(
            "weather_pipeline.database.psycopg.connect",
            return_value=connection,
        ),

        patch(
            "weather_pipeline.database.apply_schema"
        ),
    ):

        load_to_postgres(
            tmp_path,
            dsn="fake-database",
        )


    # Call order:
    #
    # 0 = cities
    # 1 = hourly
    # 2 = daily
    # 3 = alerts

    hourly_sql = (
        cursor
        .executemany
        .call_args_list[1]
        .args[0]
    )


    normalized_sql = (
        " ".join(
            hourly_sql.split()
        )
        .upper()
    )


    assert (
        "INSERT INTO WEATHER_HOURLY"
        in normalized_sql
    )


    assert (
        "ON CONFLICT"
        in normalized_sql
    )


    assert (
        "CITY_ID"
        in normalized_sql
    )


    assert (
        "OBSERVATION_TIMESTAMP"
        in normalized_sql
    )


    assert (
        "DO NOTHING"
        in normalized_sql
    )


# =========================================================
# DUPLICATE PROTECTION - DAILY SUMMARY
# =========================================================


def test_daily_summary_uses_upsert(
    tmp_path,
):
    """
    Verify that daily summary records are updated
    when the same city/date already exists.

    Daily summaries can change if additional hourly
    observations become available later.
    """

    create_transformed_test_files(
        tmp_path
    )


    connection = MagicMock()

    cursor = MagicMock()


    connection.__enter__.return_value = (
        connection
    )

    connection.__exit__.return_value = (
        False
    )


    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    connection.cursor.return_value.__exit__.return_value = (
        False
    )


    with (
        patch(
            "weather_pipeline.database.psycopg.connect",
            return_value=connection,
        ),

        patch(
            "weather_pipeline.database.apply_schema"
        ),
    ):

        load_to_postgres(
            tmp_path,
            dsn="fake-database",
        )


    daily_sql = (
        cursor
        .executemany
        .call_args_list[2]
        .args[0]
    )


    normalized_sql = (
        " ".join(
            daily_sql.split()
        )
        .upper()
    )


    assert (
        "INSERT INTO WEATHER_DAILY_SUMMARY"
        in normalized_sql
    )


    assert (
        "ON CONFLICT"
        in normalized_sql
    )


    assert (
        "CITY_ID"
        in normalized_sql
    )


    assert (
        "WEATHER_DATE"
        in normalized_sql
    )


    assert (
        "DO UPDATE SET"
        in normalized_sql
    )


# =========================================================
# DUPLICATE PROTECTION - ALERTS
# =========================================================


def test_weather_alert_insert_uses_duplicate_protection(
    tmp_path,
):
    """
    Verify that duplicate weather alerts are prevented.

    Alert uniqueness is defined using:

        city_id
        observation_timestamp
        alert_type
    """

    create_transformed_test_files(
        tmp_path
    )


    connection = MagicMock()

    cursor = MagicMock()


    connection.__enter__.return_value = (
        connection
    )

    connection.__exit__.return_value = (
        False
    )


    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    connection.cursor.return_value.__exit__.return_value = (
        False
    )


    with (
        patch(
            "weather_pipeline.database.psycopg.connect",
            return_value=connection,
        ),

        patch(
            "weather_pipeline.database.apply_schema"
        ),
    ):

        load_to_postgres(
            tmp_path,
            dsn="fake-database",
        )


    alert_sql = (
        cursor
        .executemany
        .call_args_list[3]
        .args[0]
    )


    normalized_sql = (
        " ".join(
            alert_sql.split()
        )
        .upper()
    )


    assert (
        "INSERT INTO WEATHER_ALERTS"
        in normalized_sql
    )


    assert (
        "ON CONFLICT"
        in normalized_sql
    )


    assert (
        "ALERT_TYPE"
        in normalized_sql
    )


    assert (
        "DO NOTHING"
        in normalized_sql
    )


# =========================================================
# TEST DATA PASSED TO HOURLY INSERT
# =========================================================


def test_hourly_insert_receives_correct_data(
    tmp_path,
):
    """
    Verify that the application sends the expected
    hourly observation values to executemany().
    """

    create_transformed_test_files(
        tmp_path
    )


    connection = MagicMock()

    cursor = MagicMock()


    connection.__enter__.return_value = (
        connection
    )

    connection.__exit__.return_value = (
        False
    )


    connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    connection.cursor.return_value.__exit__.return_value = (
        False
    )


    with (
        patch(
            "weather_pipeline.database.psycopg.connect",
            return_value=connection,
        ),

        patch(
            "weather_pipeline.database.apply_schema"
        ),
    ):

        load_to_postgres(
            tmp_path,
            dsn="fake-database",
        )


    hourly_call = (
        cursor
        .executemany
        .call_args_list[1]
    )


    hourly_rows = (
        hourly_call.args[1]
    )


    assert len(
        hourly_rows
    ) == 1


    first_row = (
        hourly_rows[0]
    )


    assert (
        first_row[0]
        == "london"
    )


    assert (
        first_row[2]
        == 20.0
    )


    assert (
        first_row[3]
        == 70.0
    )


    assert (
        first_row[4]
        == 0.0
    )


    assert (
        first_row[5]
        == 15.0
    )