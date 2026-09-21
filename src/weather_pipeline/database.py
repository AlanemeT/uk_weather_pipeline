from __future__ import annotations

import logging
import os

from pathlib import Path

import polars as pl
import psycopg

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo


LOGGER = logging.getLogger(
    "weather_pipeline.database"
)


# Load environment variables from the local .env file.
#
# The .env file must NOT be committed to GitHub.
load_dotenv()


def connection_string() -> str:
    """
    Build the PostgreSQL connection string using
    environment variables.

    Sensitive information such as the PostgreSQL
    password is never hard-coded in the source code.

    Required environment variable:
        POSTGRES_PASSWORD

    Optional environment variables:
        POSTGRES_HOST
        POSTGRES_PORT
        POSTGRES_DB
        POSTGRES_USER
    """

    host = os.getenv(
        "POSTGRES_HOST",
        "localhost",
    )

    port = os.getenv(
        "POSTGRES_PORT",
        "5432",
    )

    database = os.getenv(
        "POSTGRES_DB",
        "weather",
    )

    user = os.getenv(
        "POSTGRES_USER",
        "postgres",
    )

    password = os.getenv(
        "POSTGRES_PASSWORD"
    )


    # The password deliberately has no default value.
    #
    # This prevents accidentally falling back to a
    # hard-coded credential if the .env file is missing
    # or incorrectly configured.
    if not password:

        raise ValueError(
            "POSTGRES_PASSWORD environment variable "
            "is not set. Add it to your .env file."
        )


    # psycopg's make_conninfo safely builds the
    # PostgreSQL connection string.
    return make_conninfo(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )


def apply_schema(
    connection: psycopg.Connection,
    schema_file: Path = Path(
        "sql/schema.sql"
    ),
) -> None:
    """
    Create database tables and indexes
    when they do not already exist.
    """

    with (
        schema_file.open(
            "r",
            encoding="utf-8",
        ) as handle,
        connection.cursor() as cursor,
    ):

        cursor.execute(
            handle.read()
        )

    connection.commit()


def _records(
    dataframe: pl.DataFrame,
    columns: list[str],
) -> list[tuple]:
    """
    Convert selected Polars columns into
    tuples suitable for PostgreSQL executemany().
    """

    return [

        tuple(row)

        for row in (
            dataframe
            .select(columns)
            .iter_rows()
        )
    ]


def load_to_postgres(
    transform_dir: Path,
    dsn: str | None = None,
) -> None:
    """
    Load transformed datasets into PostgreSQL.

    Inserts are implemented using PostgreSQL
    conflict handling to make the load safe
    to rerun.
    """

    hourly_files = list(

        (
            transform_dir
            / "hourly"
        ).glob(
            "*.parquet"
        )
    )


    if not hourly_files:

        LOGGER.warning(
            "No transformed hourly files found; "
            "skipping database load"
        )

        return


    # =====================================================
    # Load hourly transformed Parquet files
    # =====================================================

    hourly = (

        pl.concat(
            [
                pl.read_parquet(
                    path
                )
                for path
                in hourly_files
            ]
        )

        # Extra protection against duplicate observations.
        #
        # A weather observation is uniquely identified by:
        #
        # city_id + observation_timestamp
        .unique(
            subset=[
                "city_id",
                "observation_timestamp",
            ]
        )
    )


    # =====================================================
    # Load daily summary
    # =====================================================

    daily = pl.read_parquet(

        transform_dir
        / "daily_summary.parquet"
    )


    # =====================================================
    # Load weather alerts
    # =====================================================

    alerts = pl.read_parquet(

        transform_dir
        / "weather_alerts.parquet"
    )


    # =====================================================
    # PostgreSQL connection
    # =====================================================

    with psycopg.connect(
        dsn or connection_string()
    ) as connection:


        # Create tables/indexes if they do not
        # already exist.
        apply_schema(
            connection
        )


        with connection.cursor() as cursor:


            # =================================================
            # Cities
            # =================================================

            city_columns = [

                "city_id",

                "city_name",

                "latitude",

                "longitude",

                "elevation_m",

                "timezone",
            ]


            city_rows = _records(

                hourly.unique(
                    subset=[
                        "city_id"
                    ]
                ),

                city_columns,
            )


            cursor.executemany(

                """
                INSERT INTO cities
                (
                    city_id,
                    city_name,
                    latitude,
                    longitude,
                    elevation_m,
                    timezone
                )

                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT (city_id)

                DO UPDATE SET

                    city_name =
                        EXCLUDED.city_name,

                    latitude =
                        EXCLUDED.latitude,

                    longitude =
                        EXCLUDED.longitude,

                    elevation_m =
                        EXCLUDED.elevation_m,

                    timezone =
                        EXCLUDED.timezone
                """,

                city_rows,
            )


            # =================================================
            # Hourly weather
            # =================================================

            hourly_columns = [

                "city_id",

                "observation_timestamp",

                "temperature_c",

                "relative_humidity_pct",

                "precipitation_mm",

                "wind_speed_kmh",

                "weather_code",

                "source_file",

                "ingested_at_utc",
            ]


            cursor.executemany(

                """
                INSERT INTO weather_hourly
                (
                    city_id,

                    observation_timestamp,

                    temperature_c,

                    relative_humidity_pct,

                    precipitation_mm,

                    wind_speed_kmh,

                    weather_code,

                    source_file,

                    ingested_at_utc
                )

                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT
                (
                    city_id,
                    observation_timestamp
                )

                DO NOTHING
                """,

                _records(
                    hourly,
                    hourly_columns,
                ),
            )


            # =================================================
            # Daily summary
            # =================================================

            daily_columns = [

                "city_id",

                "weather_date",

                "min_temperature_c",

                "max_temperature_c",

                "avg_temperature_c",

                "total_precipitation_mm",

                "avg_wind_speed_kmh",
            ]


            cursor.executemany(

                """
                INSERT INTO
                    weather_daily_summary
                (
                    city_id,

                    weather_date,

                    min_temperature_c,

                    max_temperature_c,

                    avg_temperature_c,

                    total_precipitation_mm,

                    avg_wind_speed_kmh
                )

                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT
                (
                    city_id,
                    weather_date
                )

                DO UPDATE SET

                    min_temperature_c =
                        EXCLUDED.min_temperature_c,

                    max_temperature_c =
                        EXCLUDED.max_temperature_c,

                    avg_temperature_c =
                        EXCLUDED.avg_temperature_c,

                    total_precipitation_mm =
                        EXCLUDED.total_precipitation_mm,

                    avg_wind_speed_kmh =
                        EXCLUDED.avg_wind_speed_kmh
                """,

                _records(
                    daily,
                    daily_columns,
                ),
            )


            # =================================================
            # Weather alerts
            # =================================================

            alert_columns = [

                "city_id",

                "observation_timestamp",

                "alert_type",

                "temperature_c",

                "precipitation_mm",

                "wind_speed_kmh",
            ]


            if alerts.height:

                cursor.executemany(

                    """
                    INSERT INTO weather_alerts
                    (
                        city_id,

                        observation_timestamp,

                        alert_type,

                        temperature_c,

                        precipitation_mm,

                        wind_speed_kmh
                    )

                    VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )

                    ON CONFLICT
                    (
                        city_id,
                        observation_timestamp,
                        alert_type
                    )

                    DO NOTHING
                    """,

                    _records(
                        alerts,
                        alert_columns,
                    ),
                )


        # Commit all inserts/updates.
        connection.commit()


        LOGGER.info(
            "Loaded transformed datasets "
            "into PostgreSQL"
        )