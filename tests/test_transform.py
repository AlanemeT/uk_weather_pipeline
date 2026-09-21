from datetime import datetime

import polars as pl

from weather_pipeline.config import AlertConfig

from weather_pipeline.transform import (
    build_alerts,
    build_daily_summary,
    load_clean,
    write_transforms,
)


def sample_weather_df() -> pl.DataFrame:
    """
    Create a small sample hourly weather dataset
    that can be reused by several transform tests.
    """

    return pl.DataFrame(
        {
            "city_id": [
                "london",
                "london",
                "london",
            ],

            "city_name": [
                "London",
                "London",
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    10,
                ),
                datetime(
                    2026,
                    9,
                    14,
                    11,
                ),
                datetime(
                    2026,
                    9,
                    14,
                    12,
                ),
            ],

            "temperature_c": [
                10.0,
                20.0,
                30.0,
            ],

            "precipitation_mm": [
                1.0,
                2.0,
                7.0,
            ],

            "wind_speed_kmh": [
                10.0,
                20.0,
                30.0,
            ],
        }
    )


def alert_config() -> AlertConfig:
    """
    Provide alert thresholds for tests.
    """

    return AlertConfig(
        extreme_hot_c=30.0,
        extreme_cold_c=-5.0,
        heavy_rain_mm=10.0,
        high_wind_kmh=60.0,
    )


# =========================================================
# DAILY SUMMARY TESTS
# =========================================================


def test_daily_summary():
    """
    Verify that hourly observations for one day
    are grouped into one daily summary row.
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert result.height == 1


def test_min_temperature():
    """
    Verify minimum daily temperature.
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert (
        result[
            0,
            "min_temperature_c",
        ]
        == 10.0
    )


def test_max_temperature():
    """
    Verify maximum daily temperature.
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert (
        result[
            0,
            "max_temperature_c",
        ]
        == 30.0
    )


def test_average_temperature():
    """
    Verify average daily temperature.

    (10 + 20 + 30) / 3 = 20
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert (
        result[
            0,
            "avg_temperature_c",
        ]
        == 20.0
    )


def test_total_precipitation():
    """
    Verify total precipitation.

    1 + 2 + 7 = 10 mm
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert (
        result[
            0,
            "total_precipitation_mm",
        ]
        == 10.0
    )


def test_average_wind_speed():
    """
    Verify average wind speed.

    (10 + 20 + 30) / 3 = 20 km/h
    """

    result = build_daily_summary(
        sample_weather_df()
    )

    assert (
        result[
            0,
            "avg_wind_speed_kmh",
        ]
        == 20.0
    )


# =========================================================
# WEATHER ALERT TESTS
# =========================================================


def test_extreme_temperature_alert():
    """
    Temperature above the configured threshold
    should generate an EXTREME_HEAT alert.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "city_name": [
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                35.0,
            ],

            "precipitation_mm": [
                0.0,
            ],

            "wind_speed_kmh": [
                10.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    assert (
        "EXTREME_HEAT"
        in result[
            "alert_type"
        ].to_list()
    )


def test_extreme_cold_alert():
    """
    Temperature below the configured cold threshold
    should generate an EXTREME_COLD alert.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "edinburgh",
            ],

            "city_name": [
                "Edinburgh",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    1,
                    10,
                    6,
                )
            ],

            "temperature_c": [
                -8.0,
            ],

            "precipitation_mm": [
                0.0,
            ],

            "wind_speed_kmh": [
                15.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    assert (
        "EXTREME_COLD"
        in result[
            "alert_type"
        ].to_list()
    )


def test_heavy_rain_alert():
    """
    Precipitation above the configured threshold
    should generate a HEAVY_RAIN alert.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "manchester",
            ],

            "city_name": [
                "Manchester",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                18.0,
            ],

            "precipitation_mm": [
                15.0,
            ],

            "wind_speed_kmh": [
                20.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    assert (
        "HEAVY_RAIN"
        in result[
            "alert_type"
        ].to_list()
    )


def test_high_wind_alert():
    """
    Wind speed above the configured threshold
    should generate a HIGH_WIND alert.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "brighton",
            ],

            "city_name": [
                "Brighton",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                20.0,
            ],

            "precipitation_mm": [
                0.0,
            ],

            "wind_speed_kmh": [
                75.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    assert (
        "HIGH_WIND"
        in result[
            "alert_type"
        ].to_list()
    )


def test_no_weather_alerts():
    """
    Normal weather conditions should produce
    no weather alerts.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "city_name": [
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                20.0,
            ],

            "precipitation_mm": [
                1.0,
            ],

            "wind_speed_kmh": [
                20.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    assert result.is_empty()


def test_multiple_alerts_for_same_hour():
    """
    One hourly observation can generate multiple
    alerts at the same time.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "city_name": [
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                35.0,
            ],

            "precipitation_mm": [
                15.0,
            ],

            "wind_speed_kmh": [
                75.0,
            ],
        }
    )

    result = build_alerts(
        dataframe,
        alert_config(),
    )

    alert_types = set(
        result[
            "alert_type"
        ].to_list()
    )

    assert alert_types == {
        "EXTREME_HEAT",
        "HEAVY_RAIN",
        "HIGH_WIND",
    }


# =========================================================
# LOAD CLEAN TESTS
# =========================================================


def test_load_clean_empty_directory(
    tmp_path,
):
    """
    An empty clean directory should return
    an empty Polars DataFrame.
    """

    result = load_clean(
        tmp_path
    )

    assert result.is_empty()


def test_load_clean_parquet(
    tmp_path,
):
    """
    Verify that load_clean can read clean
    Parquet files.
    """

    dataframe = sample_weather_df()

    dataframe.write_parquet(
        tmp_path
        / "weather.parquet"
    )

    result = load_clean(
        tmp_path
    )

    assert result.height == 3

    assert (
        result[
            "city_id"
        ].to_list()
        == [
            "london",
            "london",
            "london",
        ]
    )


def test_load_clean_removes_duplicates(
    tmp_path,
):
    """
    Duplicate city/timestamp observations
    should be removed by load_clean.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
                "london",
            ],

            "city_name": [
                "London",
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    10,
                ),
                datetime(
                    2026,
                    9,
                    14,
                    10,
                ),
            ],

            "temperature_c": [
                18.0,
                18.0,
            ],

            "precipitation_mm": [
                0.0,
                0.0,
            ],

            "wind_speed_kmh": [
                10.0,
                10.0,
            ],
        }
    )

    dataframe.write_parquet(
        tmp_path
        / "duplicate.parquet"
    )

    result = load_clean(
        tmp_path
    )

    assert result.height == 1


# =========================================================
# WRITE TRANSFORM TESTS
# =========================================================


def test_hourly_weather_output(
    tmp_path,
):
    """
    Verify that write_transforms creates
    an hourly Parquet file for the city.
    """

    dataframe = sample_weather_df()

    write_transforms(
        dataframe,
        tmp_path,
        alert_config(),
    )

    hourly_file = (
        tmp_path
        / "hourly"
        / "london.parquet"
    )

    assert hourly_file.exists()

    result = pl.read_parquet(
        hourly_file
    )

    assert result.height == 3

    assert (
        result[
            "city_id"
        ].unique().to_list()
        == ["london"]
    )


def test_write_daily_summary_file(
    tmp_path,
):
    """
    Verify that daily_summary.parquet is created.
    """

    dataframe = sample_weather_df()

    write_transforms(
        dataframe,
        tmp_path,
        alert_config(),
    )

    daily_file = (
        tmp_path
        / "daily_summary.parquet"
    )

    assert daily_file.exists()

    result = pl.read_parquet(
        daily_file
    )

    assert result.height == 1


def test_write_weather_alert_file(
    tmp_path,
):
    """
    Verify that weather_alerts.parquet is created.
    """

    dataframe = pl.DataFrame(
        {
            "city_id": [
                "london",
            ],

            "city_name": [
                "London",
            ],

            "observation_timestamp": [
                datetime(
                    2026,
                    9,
                    14,
                    12,
                )
            ],

            "temperature_c": [
                35.0,
            ],

            "precipitation_mm": [
                15.0,
            ],

            "wind_speed_kmh": [
                75.0,
            ],
        }
    )

    write_transforms(
        dataframe,
        tmp_path,
        alert_config(),
    )

    alert_file = (
        tmp_path
        / "weather_alerts.parquet"
    )

    assert alert_file.exists()

    result = pl.read_parquet(
        alert_file
    )

    assert result.height == 3


def test_write_transforms_returns_output_paths(
    tmp_path,
):
    """
    write_transforms should return the paths
    of the files it creates.
    """

    dataframe = sample_weather_df()

    outputs = write_transforms(
        dataframe,
        tmp_path,
        alert_config(),
    )

    assert len(outputs) == 3

    assert (
        tmp_path
        / "hourly"
        / "london.parquet"
    ) in outputs

    assert (
        tmp_path
        / "daily_summary.parquet"
    ) in outputs

    assert (
        tmp_path
        / "weather_alerts.parquet"
    ) in outputs