import json

import httpx
import pytest
import respx

from pydantic import ValidationError

from weather_pipeline.extract import (
    fetch_city_weather,
    save_raw_immutable,
)

from weather_pipeline.models import (
    OpenMeteoResponse,
)


API_URL = (
    "https://api.open-meteo.com/v1/forecast"
)


@respx.mock
def test_successful_api_request(
    sample_settings,
    sample_api_payload,
):
    """
    A successful Open-Meteo response should
    be validated and returned.
    """

    respx.get(API_URL).mock(
        return_value=httpx.Response(
            200,
            json=sample_api_payload,
        )
    )

    city = sample_settings.cities[0]

    with httpx.Client(
        timeout=5
    ) as client:

        result = fetch_city_weather(
            client,
            sample_settings,
            city,
        )

    assert isinstance(
        result,
        OpenMeteoResponse,
    )

    assert result.latitude == 51.5

    assert (
        result.hourly.temperature_2m
        == [18.0, 20.0]
    )


@respx.mock
def test_api_timeout(
    sample_settings,
):
    """
    API timeouts should propagate as an
    HTTPX timeout exception.
    """

    respx.get(API_URL).mock(
        side_effect=httpx.ReadTimeout(
            "Open-Meteo request timed out"
        )
    )

    city = sample_settings.cities[0]

    with httpx.Client(
        timeout=5
    ) as client:

        with pytest.raises(
            httpx.ReadTimeout
        ):

            fetch_city_weather(
                client,
                sample_settings,
                city,
            )


@respx.mock
def test_api_http_error(
    sample_settings,
):
    """
    HTTP error responses such as 500 should
    raise HTTPStatusError.
    """

    respx.get(API_URL).mock(
        return_value=httpx.Response(
            500,
            json={
                "error": "Server error"
            },
        )
    )

    city = sample_settings.cities[0]

    with httpx.Client(
        timeout=5
    ) as client:

        with pytest.raises(
            httpx.HTTPStatusError
        ):

            fetch_city_weather(
                client,
                sample_settings,
                city,
            )


@respx.mock
def test_invalid_api_response(
    sample_settings,
    sample_api_payload,
):
    """
    Invalid Open-Meteo responses should fail
    Pydantic validation.
    """

    # Deliberately make one hourly array shorter.
    sample_api_payload[
        "hourly"
    ][
        "wind_speed_10m"
    ] = [15.0]

    respx.get(API_URL).mock(
        return_value=httpx.Response(
            200,
            json=sample_api_payload,
        )
    )

    city = sample_settings.cities[0]

    with httpx.Client(
        timeout=5
    ) as client:

        with pytest.raises(
            ValidationError
        ):

            fetch_city_weather(
                client,
                sample_settings,
                city,
            )


def test_raw_file_created(
    tmp_path,
    sample_settings,
    sample_api_payload,
):
    """
    Valid API data should be stored as a
    raw JSON file.
    """

    city = sample_settings.cities[0]

    payload = (
        OpenMeteoResponse.model_validate(
            sample_api_payload
        )
    )

    raw_dir = tmp_path / "raw"

    output_path = save_raw_immutable(
        raw_dir,
        city,
        payload,
    )

    assert output_path.exists()

    assert output_path.suffix == ".json"

    with output_path.open(
        "r",
        encoding="utf-8",
    ) as file:

        saved_data = json.load(file)

    assert (
        saved_data["city"]["id"]
        == "london"
    )

    assert (
        "response"
        in saved_data
    )


def test_existing_raw_file_not_overwritten(
    tmp_path,
    sample_settings,
    sample_api_payload,
):
    """
    Running raw storage multiple times should
    create new files rather than modify the
    previous raw file.
    """

    city = sample_settings.cities[0]

    payload = (
        OpenMeteoResponse.model_validate(
            sample_api_payload
        )
    )

    raw_dir = tmp_path / "raw"

    first_file = save_raw_immutable(
        raw_dir,
        city,
        payload,
    )

    first_content = (
        first_file.read_text(
            encoding="utf-8"
        )
    )

    second_file = save_raw_immutable(
        raw_dir,
        city,
        payload,
    )

    assert first_file != second_file

    assert first_file.exists()

    assert second_file.exists()

    assert (
        first_file.read_text(
            encoding="utf-8"
        )
        == first_content
    )

    assert len(
        list(
            raw_dir.glob("*.json")
        )
    ) == 2