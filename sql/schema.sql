CREATE TABLE IF NOT EXISTS cities (

    city_id TEXT PRIMARY KEY,

    city_name TEXT NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,

    longitude DOUBLE PRECISION NOT NULL,

    elevation_m DOUBLE PRECISION NOT NULL,

    timezone TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS weather_hourly (

    city_id TEXT NOT NULL
        REFERENCES cities(city_id),

    observation_timestamp TIMESTAMP NOT NULL,

    temperature_c DOUBLE PRECISION,

    relative_humidity_pct DOUBLE PRECISION,

    precipitation_mm DOUBLE PRECISION,

    wind_speed_kmh DOUBLE PRECISION,

    weather_code SMALLINT,

    source_file TEXT NOT NULL,

    ingested_at_utc TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (
        city_id,
        observation_timestamp
    )
);


CREATE INDEX IF NOT EXISTS
idx_weather_hourly_timestamp

ON weather_hourly(
    observation_timestamp
);


CREATE INDEX IF NOT EXISTS
idx_weather_hourly_city_temp

ON weather_hourly(
    city_id,
    temperature_c
);


CREATE TABLE IF NOT EXISTS
weather_daily_summary (

    city_id TEXT NOT NULL
        REFERENCES cities(city_id),

    weather_date DATE NOT NULL,

    min_temperature_c
        DOUBLE PRECISION,

    max_temperature_c
        DOUBLE PRECISION,

    avg_temperature_c
        DOUBLE PRECISION,

    total_precipitation_mm
        DOUBLE PRECISION,

    avg_wind_speed_kmh
        DOUBLE PRECISION,

    PRIMARY KEY (
        city_id,
        weather_date
    )
);


CREATE INDEX IF NOT EXISTS
idx_daily_summary_date

ON weather_daily_summary(
    weather_date
);


CREATE TABLE IF NOT EXISTS
weather_alerts (

    city_id TEXT NOT NULL
        REFERENCES cities(city_id),

    observation_timestamp
        TIMESTAMP NOT NULL,

    alert_type TEXT NOT NULL,

    temperature_c
        DOUBLE PRECISION,

    precipitation_mm
        DOUBLE PRECISION,

    wind_speed_kmh
        DOUBLE PRECISION,

    PRIMARY KEY (
        city_id,
        observation_timestamp,
        alert_type
    )
);


CREATE INDEX IF NOT EXISTS
idx_weather_alerts_type_time

ON weather_alerts(
    alert_type,
    observation_timestamp
);