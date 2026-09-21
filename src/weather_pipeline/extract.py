from __future__ import annotations

import json
import logging

from datetime import datetime, timezone
from pathlib import Path

import httpx

from weather_pipeline.config import CityConfig, Settings
from weather_pipeline.models import OpenMeteoResponse


LOGGER = logging.getLogger(
    "weather_pipeline.extract"
)


def fetch_city_weather(
    client: httpx.Client,
    settings: Settings,
    city: CityConfig,
) -> OpenMeteoResponse:
    """
    Retrieve hourly weather information for one city.

    The API response is validated using Pydantic
    before being returned.
    """

    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,

        "hourly": ",".join(
            settings.api.hourly_fields
        ),

        "timezone": settings.api.timezone,

        "past_days": settings.api.past_days,

        "forecast_days": settings.api.forecast_days,
    }

    response = client.get(
        str(settings.api.base_url),
        params=params,
    )

    # Raises an exception for HTTP 4xx / 5xx
    response.raise_for_status()

    # Validate API response.
    validated_response = (
        OpenMeteoResponse.model_validate(
            response.json()
        )
    )

    return validated_response


def save_raw_immutable(
    raw_dir: Path,
    city: CityConfig,
    payload: OpenMeteoResponse,
) -> Path:
    """
    Save an API response as an immutable JSON file.

    Each execution generates a new timestamped file.

    Existing raw files are never overwritten.
    """

    raw_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    run_timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    file_path = raw_dir / (
        f"{city.id}_{run_timestamp}.json"
    )

    envelope = {

        "city": city.model_dump(),

        "ingested_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "response": payload.model_dump(
            mode="json"
        ),
    }

    # "x" is deliberately used instead of "w".
    #
    # x = create new file and fail if it exists.
    # w = overwrite existing file.
    #
    # This helps guarantee immutable raw storage.
    with file_path.open(
        "x",
        encoding="utf-8",
    ) as handle:

        json.dump(
            envelope,
            handle,
            indent=2,
        )

    return file_path


def extract_all(
    settings: Settings,
) -> list[Path]:
    """
    Extract weather information for all configured cities.
    """

    outputs: list[Path] = []

    with httpx.Client(
        timeout=settings.api.timeout_seconds
    ) as client:

        for city in settings.cities:

            try:

                validated_response = (
                    fetch_city_weather(
                        client,
                        settings,
                        city,
                    )
                )

                output_path = (
                    save_raw_immutable(
                        settings.paths.raw,
                        city,
                        validated_response,
                    )
                )

                outputs.append(
                    output_path
                )

                LOGGER.info(
                    "Saved raw weather for %s to %s",
                    city.name,
                    output_path,
                )

            except (
                httpx.HTTPError,
                ValueError,
            ) as exc:

                LOGGER.exception(
                    "Failed extracting %s: %s",
                    city.name,
                    exc,
                )

    return outputs