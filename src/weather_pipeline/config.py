from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, HttpUrl


class ApiConfig(BaseModel):
    """
    Configuration for the Open-Meteo API.
    """

    base_url: HttpUrl
    timeout_seconds: int = Field(gt=0)
    past_days: int = Field(ge=0, le=92)
    forecast_days: int = Field(ge=1, le=16)
    timezone: str
    hourly_fields: list[str]


class CityConfig(BaseModel):
    """
    Configuration for an individual city.
    """

    id: str
    name: str

    latitude: float = Field(
        ge=-90,
        le=90,
    )

    longitude: float = Field(
        ge=-180,
        le=180,
    )


class PathsConfig(BaseModel):
    """
    Locations used by the pipeline.
    """

    raw: Path
    clean: Path
    transform: Path
    logs: Path


class AlertConfig(BaseModel):
    """
    Thresholds used to identify extreme weather.
    """

    extreme_hot_c: float
    extreme_cold_c: float

    heavy_rain_mm: float = Field(ge=0)

    high_wind_kmh: float = Field(ge=0)


class Settings(BaseModel):
    """
    Main pipeline configuration.
    """

    api: ApiConfig

    # The assignment requires at least five cities.
    cities: list[CityConfig] = Field(min_length=5)

    paths: PathsConfig

    alerts: AlertConfig


def load_settings(
    path: str | Path = "config/settings.yaml",
) -> Settings:
    """
    Load and validate the YAML configuration file.

    Parameters
    ----------
    path:
        Path to the configuration file.

    Returns
    -------
    Settings
        Validated project settings.
    """

    with Path(path).open(
        "r",
        encoding="utf-8",
    ) as handle:

        payload: dict[str, Any] = yaml.safe_load(handle)

    return Settings.model_validate(payload)