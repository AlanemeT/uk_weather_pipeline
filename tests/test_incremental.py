import json

from pathlib import Path

from weather_pipeline.clean import (
    clean_incremental,
)


def test_clean_is_idempotent(
    tmp_path: Path,
    sample_api_payload,
):
    """
    Running the clean stage twice against the
    same raw data should not generate additional
    clean records.
    """

    raw_dir = (
        tmp_path /
        "raw"
    )

    clean_dir = (
        tmp_path /
        "clean"
    )


    raw_dir.mkdir()


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


    raw_file = (
        raw_dir /
        "one.json"
    )


    raw_file.write_text(

        json.dumps(
            envelope
        ),

        encoding="utf-8",
    )


    # First execution should generate
    # one clean parquet file.
    first_run = clean_incremental(

        raw_dir,

        clean_dir,
    )


    # Second execution should find all
    # observations already processed.
    second_run = clean_incremental(

        raw_dir,

        clean_dir,
    )


    assert len(first_run) == 1


    assert second_run == []