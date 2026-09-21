from __future__ import annotations

import json
import logging

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from weather_pipeline.models import OpenMeteoResponse


LOGGER = logging.getLogger(
    "weather_pipeline.clean"
)


CLEAN_SCHEMA = pa.schema(
    [

        pa.field(
            "city_id",
            pa.string(),
            nullable=False,
        ),

        pa.field(
            "city_name",
            pa.string(),
            nullable=False,
        ),

        pa.field(
            "latitude",
            pa.float64(),
            nullable=False,
        ),

        pa.field(
            "longitude",
            pa.float64(),
            nullable=False,
        ),

        pa.field(
            "elevation_m",
            pa.float64(),
            nullable=False,
        ),

        pa.field(
            "timezone",
            pa.string(),
            nullable=False,
        ),

        pa.field(
            "timezone_abbreviation",
            pa.string(),
            nullable=False,
        ),

        pa.field(
            "utc_offset_seconds",
            pa.int32(),
            nullable=False,
        ),

        pa.field(
            "generationtime_ms",
            pa.float64(),
            nullable=False,
        ),

        pa.field(
            "observation_timestamp",
            pa.timestamp("us"),
            nullable=False,
        ),

        pa.field(
            "temperature_c",
            pa.float64(),
        ),

        pa.field(
            "relative_humidity_pct",
            pa.float64(),
        ),

        pa.field(
            "precipitation_mm",
            pa.float64(),
        ),

        pa.field(
            "wind_speed_kmh",
            pa.float64(),
        ),

        pa.field(
            "weather_code",
            pa.int16(),
        ),

        pa.field(
            "source_file",
            pa.string(),
            nullable=False,
        ),

        pa.field(
            "ingested_at_utc",
            pa.timestamp(
                "us",
                tz="UTC",
            ),
            nullable=False,
        ),
    ]
)


def flatten_raw_envelope(
    envelope: dict,
    source_file: str,
) -> list[dict]:
    """
    Flatten an Open-Meteo response into one row
    for each city/hour observation.
    """

    city = envelope["city"]

    response = (
        OpenMeteoResponse.model_validate(
            envelope["response"]
        )
    )

    ingested_at = datetime.fromisoformat(
        envelope[
            "ingested_at_utc"
        ].replace(
            "Z",
            "+00:00",
        )
    )

    rows: list[dict] = []

    hourly = response.hourly

    for index, timestamp in enumerate(
        hourly.time
    ):

        row = {

            "city_id":
                city["id"],

            "city_name":
                city["name"],

            "latitude":
                response.latitude,

            "longitude":
                response.longitude,

            "elevation_m":
                response.elevation,

            "timezone":
                response.timezone,

            "timezone_abbreviation":
                response.timezone_abbreviation,

            "utc_offset_seconds":
                response.utc_offset_seconds,

            "generationtime_ms":
                response.generationtime_ms,

            "observation_timestamp":
                datetime.fromisoformat(
                    timestamp
                ),

            "temperature_c":
                hourly.temperature_2m[
                    index
                ],

            "relative_humidity_pct":
                hourly.relative_humidity_2m[
                    index
                ],

            "precipitation_mm":
                hourly.precipitation[
                    index
                ],

            "wind_speed_kmh":
                hourly.wind_speed_10m[
                    index
                ],

            "weather_code":
                hourly.weather_code[
                    index
                ],

            "source_file":
                source_file,

            "ingested_at_utc":
                ingested_at,
        }

        rows.append(row)

    return rows


def _existing_keys(
    clean_dir: Path,
) -> set[tuple[str, datetime]]:
    """
    Read existing clean files and determine which
    city/timestamp combinations have already been
    processed.
    """

    keys: set[
        tuple[str, datetime]
    ] = set()

    for parquet_path in clean_dir.glob(
        "*.parquet"
    ):

        table = pq.read_table(
            parquet_path,
            columns=[
                "city_id",
                "observation_timestamp",
            ],
        )

        cities = table.column(
            "city_id"
        ).to_pylist()

        timestamps = table.column(
            "observation_timestamp"
        ).to_pylist()

        keys.update(
            zip(
                cities,
                timestamps,
            )
        )

    return keys


def clean_incremental(
    raw_dir: Path,
    clean_dir: Path,
) -> list[Path]:
    """
    Process raw JSON files incrementally.

    Observations already present in clean storage are
    not written again.
    """

    clean_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    existing_keys = _existing_keys(
        clean_dir
    )

    output_files: list[Path] = []

    for raw_path in sorted(
        raw_dir.glob("*.json")
    ):

        with raw_path.open(
            "r",
            encoding="utf-8",
        ) as handle:

            envelope = json.load(
                handle
            )

        rows = flatten_raw_envelope(
            envelope,
            raw_path.name,
        )

        new_rows = [
            row
            for row in rows

            if (
                row["city_id"],
                row[
                    "observation_timestamp"
                ],
            )
            not in existing_keys
        ]

        # Nothing new in this file.
        if not new_rows:
            continue

        # Immediately update our in-memory key set
        # so duplicates from subsequent raw files in
        # the same run are also prevented.
        for row in new_rows:

            existing_keys.add(
                (
                    row["city_id"],
                    row[
                        "observation_timestamp"
                    ],
                )
            )

        table = pa.Table.from_pylist(
            new_rows,
            schema=CLEAN_SCHEMA,
        )

        output_path = clean_dir / (
            f"clean_{raw_path.stem}.parquet"
        )

        pq.write_table(
            table,
            output_path,
            compression="snappy",
        )

        output_files.append(
            output_path
        )

        LOGGER.info(
            "Wrote %s clean rows to %s",
            len(new_rows),
            output_path,
        )

    return output_files
