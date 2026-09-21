from pathlib import Path

import pytest

from weather_pipeline.config import Settings


@pytest.fixture
def sample_api_payload():
    """
    Example Open-Meteo API response used by multiple tests.

    This avoids calling the real Open-Meteo API
    during unit tests.
    """

    return {
        "latitude": 51.5,
        "longitude": -0.12,
        "generationtime_ms": 0.2,
        "utc_offset_seconds": 3600,
        "timezone": "Europe/London",
        "timezone_abbreviation": "BST",
        "elevation": 25.0,

        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "wind_speed_10m": "km/h",
            "weather_code": "wmo code",
        },

        "hourly": {
            "time": [
                "2026-09-14T10:00",
                "2026-09-14T11:00",
            ],

            "temperature_2m": [
                18.0,
                20.0,
            ],

            "relative_humidity_2m": [
                70.0,
                65.0,
            ],

            "precipitation": [
                0.0,
                12.0,
            ],

            "wind_speed_10m": [
                15.0,
                70.0,
            ],

            "weather_code": [
                1,
                63,
            ],
        },
    }


@pytest.fixture
def sample_settings(tmp_path: Path) -> Settings:
    """
    Provide temporary test settings.

    pytest's tmp_path fixture creates a temporary
    folder so tests do not write into the real
    data/raw, data/clean, data/transform or logs folders.
    """

    return Settings.model_validate(
        {
            "api": {
                "base_url": (
                    "https://api.open-meteo.com/v1/forecast"
                ),

                "timeout_seconds": 5,

                "past_days": 2,

                "forecast_days": 1,

                "timezone": "Europe/London",

                "hourly_fields": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "wind_speed_10m",
                    "weather_code",
                ],
            },

            "cities": [
                {
                    "id": "london",
                    "name": "London",
                    "latitude": 51.5074,
                    "longitude": -0.1278,
                },

                {
                    "id": "brighton",
                    "name": "Brighton",
                    "latitude": 50.8225,
                    "longitude": -0.1372,
                },

                {
                    "id": "manchester",
                    "name": "Manchester",
                    "latitude": 53.4808,
                    "longitude": -2.2426,
                },

                {
                    "id": "birmingham",
                    "name": "Birmingham",
                    "latitude": 52.4862,
                    "longitude": -1.8904,
                },

                {
                    "id": "edinburgh",
                    "name": "Edinburgh",
                    "latitude": 55.9533,
                    "longitude": -3.1883,
                },
            ],

            "paths": {
                "raw": tmp_path / "raw",
                "clean": tmp_path / "clean",
                "transform": tmp_path / "transform",
                "logs": tmp_path / "logs",
            },

            "alerts": {
                "extreme_hot_c": 30.0,
                "extreme_cold_c": -5.0,
                "heavy_rain_mm": 10.0,
                "high_wind_kmh": 60.0,
            },
        }
    )