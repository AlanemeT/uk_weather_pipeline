from __future__ import annotations

import logging

from pathlib import Path

import polars as pl

from weather_pipeline.config import AlertConfig


LOGGER = logging.getLogger(
    "weather_pipeline.transform"
)


def load_clean(
    clean_dir: Path,
) -> pl.DataFrame:
    """
    Load all clean Parquet files.

    Duplicates are removed using city_id and
    observation_timestamp.
    """

    files = list(
        clean_dir.glob("*.parquet")
    )

    if not files:
        return pl.DataFrame()

    return (
        pl.scan_parquet(
            [
                str(path)
                for path in files
            ]
        )

        .unique(
            subset=[
                "city_id",
                "observation_timestamp",
            ],
            keep="first",
        )

        .sort(
            [
                "city_id",
                "observation_timestamp",
            ]
        )

        .collect()
    )


def build_daily_summary(
    hourly: pl.DataFrame,
) -> pl.DataFrame:
    """
    Create daily weather statistics for each city.
    """

    return (

        hourly

        .with_columns(

            pl.col(
                "observation_timestamp"
            )

            .dt.date()

            .alias(
                "weather_date"
            )
        )

        .group_by(
            [
                "city_id",
                "city_name",
                "weather_date",
            ]
        )

        .agg(

            pl.col(
                "temperature_c"
            )
            .min()
            .alias(
                "min_temperature_c"
            ),

            pl.col(
                "temperature_c"
            )
            .max()
            .alias(
                "max_temperature_c"
            ),

            pl.col(
                "temperature_c"
            )
            .mean()
            .alias(
                "avg_temperature_c"
            ),

            pl.col(
                "precipitation_mm"
            )
            .sum()
            .alias(
                "total_precipitation_mm"
            ),

            pl.col(
                "wind_speed_kmh"
            )
            .mean()
            .alias(
                "avg_wind_speed_kmh"
            ),
        )

        .sort(
            [
                "city_id",
                "weather_date",
            ]
        )
    )


def build_alerts(
    hourly: pl.DataFrame,
    config: AlertConfig,
) -> pl.DataFrame:
    """
    Identify extreme weather observations.

    One observation may generate multiple alerts.

    Example:
        temperature > 30
        precipitation > 10
        wind > 60

    could generate three alerts for the same hour.
    """

    alert_frames: list[
        pl.DataFrame
    ] = []


    rules = [

        (
            pl.col(
                "temperature_c"
            )
            >= config.extreme_hot_c,

            "EXTREME_HEAT",
        ),

        (
            pl.col(
                "temperature_c"
            )
            <= config.extreme_cold_c,

            "EXTREME_COLD",
        ),

        (
            pl.col(
                "precipitation_mm"
            )
            >= config.heavy_rain_mm,

            "HEAVY_RAIN",
        ),

        (
            pl.col(
                "wind_speed_kmh"
            )
            >= config.high_wind_kmh,

            "HIGH_WIND",
        ),
    ]


    selected_columns = [

        "city_id",

        "city_name",

        "observation_timestamp",

        "temperature_c",

        "precipitation_mm",

        "wind_speed_kmh",
    ]


    for predicate, alert_type in rules:

        alert_frame = (

            hourly

            .filter(
                predicate
            )

            .select(
                selected_columns
            )

            .with_columns(

                pl.lit(
                    alert_type
                ).alias(
                    "alert_type"
                )
            )

            .select(
                "city_id",
                "city_name",
                "observation_timestamp",
                "alert_type",
                "temperature_c",
                "precipitation_mm",
                "wind_speed_kmh",
            )
        )

        if alert_frame.height:

            alert_frames.append(
                alert_frame
            )


    # If no weather alerts were generated,
    # return an empty DataFrame with a known schema.
    if not alert_frames:

        return pl.DataFrame(

            schema={

                "city_id":
                    pl.String,

                "city_name":
                    pl.String,

                "observation_timestamp":
                    pl.Datetime,

                "alert_type":
                    pl.String,

                "temperature_c":
                    pl.Float64,

                "precipitation_mm":
                    pl.Float64,

                "wind_speed_kmh":
                    pl.Float64,
            }
        )


    return (

        pl.concat(
            alert_frames
        )

        .unique(
            subset=[
                "city_id",
                "observation_timestamp",
                "alert_type",
            ]
        )

        .sort(
            [
                "city_id",
                "observation_timestamp",
                "alert_type",
            ]
        )
    )


def write_transforms(
    hourly: pl.DataFrame,
    transform_dir: Path,
    config: AlertConfig,
) -> list[Path]:
    """
    Write all transformed weather datasets.
    """

    transform_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    hourly_dir = (
        transform_dir /
        "hourly"
    )

    hourly_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    outputs: list[Path] = []


    # ----------------------------------
    # Hourly dataset for each city
    # ----------------------------------

    city_ids = (

        hourly

        .get_column(
            "city_id"
        )

        .unique()

        .to_list()
    )


    for city_id in city_ids:

        city_dataframe = (
            hourly.filter(
                pl.col(
                    "city_id"
                )
                == city_id
            )
        )

        output_path = (
            hourly_dir /
            f"{city_id}.parquet"
        )

        city_dataframe.write_parquet(
            output_path
        )

        outputs.append(
            output_path
        )


    # ----------------------------------
    # Daily summary
    # ----------------------------------

    daily_summary_path = (
        transform_dir /
        "daily_summary.parquet"
    )

    daily_summary = (
        build_daily_summary(
            hourly
        )
    )

    daily_summary.write_parquet(
        daily_summary_path
    )

    outputs.append(
        daily_summary_path
    )


    # ----------------------------------
    # Weather alerts
    # ----------------------------------

    alerts_path = (
        transform_dir /
        "weather_alerts.parquet"
    )

    alerts = build_alerts(
        hourly,
        config,
    )

    alerts.write_parquet(
        alerts_path
    )

    outputs.append(
        alerts_path
    )


    LOGGER.info(
        "Wrote %s transformed datasets",
        len(outputs),
    )

    return outputs