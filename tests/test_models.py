import pytest

from pydantic import ValidationError

from weather_pipeline.models import (
    OpenMeteoResponse,
)


def test_valid_response(
    sample_api_payload,
):
    """
    A correctly structured response should
    successfully pass Pydantic validation.
    """

    model = (
        OpenMeteoResponse.model_validate(
            sample_api_payload
        )
    )

    assert (
        model.hourly.temperature_2m
        == [18.0, 20.0]
    )


def test_hourly_arrays_must_match(
    sample_api_payload,
):
    """
    Hourly arrays with different lengths
    should fail validation.
    """

    sample_api_payload[
        "hourly"
    ][
        "wind_speed_10m"
    ] = [
        15.0
    ]


    with pytest.raises(
        ValidationError
    ):

        OpenMeteoResponse.model_validate(
            sample_api_payload
        )