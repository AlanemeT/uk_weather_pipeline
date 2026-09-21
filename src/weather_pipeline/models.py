from __future__ import annotations

from pydantic import (
    BaseModel,
    ConfigDict,
    model_validator,
)


class HourlyUnits(BaseModel):
    """
    Units returned by Open-Meteo for hourly weather fields.
    """

    model_config = ConfigDict(extra="allow")

    time: str
    temperature_2m: str
    relative_humidity_2m: str
    precipitation: str
    wind_speed_10m: str
    weather_code: str


class HourlyData(BaseModel):
    """
    Actual hourly observations returned by Open-Meteo.
    """

    time: list[str]

    temperature_2m: list[float | None]

    relative_humidity_2m: list[float | None]

    precipitation: list[float | None]

    wind_speed_10m: list[float | None]

    weather_code: list[int | None]


    @model_validator(mode="after")
    def equal_length_arrays(self) -> "HourlyData":
        """
        Ensure every hourly field contains the same
        number of observations.
        """

        lengths = {
            len(self.time),
            len(self.temperature_2m),
            len(self.relative_humidity_2m),
            len(self.precipitation),
            len(self.wind_speed_10m),
            len(self.weather_code),
        }

        if len(lengths) != 1:
            raise ValueError(
                "All hourly arrays must have equal length"
            )

        return self


class OpenMeteoResponse(BaseModel):
    """
    Schema for an Open-Meteo API response.
    """

    latitude: float

    longitude: float

    generationtime_ms: float

    utc_offset_seconds: int

    timezone: str

    timezone_abbreviation: str

    elevation: float

    hourly_units: HourlyUnits

    hourly: HourlyData