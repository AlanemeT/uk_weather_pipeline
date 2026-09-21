from weather_pipeline.clean import (
    flatten_raw_envelope,
)


def test_flatten_response(
    sample_api_payload,
):
    """
    API arrays should be flattened into
    individual hourly records.
    """

    envelope = {

        "city": {

            "id": "london",

            "name": "London",

            "latitude": 51.5,

            "longitude": -0.12,
        },

        "ingested_at_utc":
            "2026-09-14T12:00:00+00:00",

        "response":
            sample_api_payload,
    }


    rows = flatten_raw_envelope(

        envelope,

        "london.json",
    )


    assert len(rows) == 2


    assert (
        rows[0]["city_id"]
        == "london"
    )


    assert (
        rows[1]["precipitation_mm"]
        == 12.0
    )