# UK Weather Data Pipeline

## Overview

This project implements an incremental Python data pipeline that collects hourly weather data for multiple UK cities using the free Open-Meteo API.

The pipeline retrieves weather data for five UK cities:

- London
- Brighton
- Manchester
- Birmingham
- Edinburgh

The collected data includes:

- Temperature
- Relative humidity
- Precipitation
- Wind speed
- Weather condition/code
- Observation timestamp
- Location and API metadata

The project demonstrates a complete data engineering workflow including extraction, validation, raw storage, schema enforcement, transformation, incremental processing, PostgreSQL loading, logging, error handling and automated testing.

---

## Pipeline Architecture

```text
Open-Meteo API
      |
      v
Pydantic Validation
      |
      v
Raw JSON Storage
      |
      v
Flatten + PyArrow Schema
      |
      v
Clean Parquet Storage
      |
      v
Polars Transformations
      |
      +-------------------+
      |                   |
      v                   v
Hourly Weather       Daily Summary
      |
      v
Weather Alerts
      |
      v
PostgreSQL
```

The main pipeline is orchestrated by:

```text
src/weather_pipeline/pipeline.py
```

---

## Project Structure

```text
uk_weather_pipeline/
|
├── config/
│   └── settings.yaml
|
├── data/
│   ├── raw/
│   ├── clean/
│   └── transform/
|
├── logs/
|
├── sql/
│   └── schema.sql
|
├── src/
│   └── weather_pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── models.py
│       ├── extract.py
│       ├── clean.py
│       ├── transform.py
│       ├── database.py
│       ├── logging_utils.py
│       └── pipeline.py
|
├── tests/
│   ├── conftest.py
│   ├── test_clean.py
│   ├── test_database.py
│   ├── test_extract.py
│   ├── test_incremental.py
│   ├── test_logging_utils.py
│   ├── test_models.py
│   ├── test_pipeline.py
│   └── test_transform.py
|
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

# How the Pipeline Works

## 1. Configuration

Pipeline configuration is stored in:

```text
config/settings.yaml
```

The configuration contains:

- Open-Meteo API URL
- API timeout
- Weather fields to request
- UK cities
- Latitude and longitude coordinates
- Data directories
- Weather alert thresholds

Keeping this information outside the Python source code makes the pipeline easier to configure and maintain.

## 2. Extract

The extraction stage is implemented in `src/weather_pipeline/extract.py`.

The pipeline makes HTTP requests to the Open-Meteo API for each configured city. The API returns hourly values including temperature, humidity, precipitation, wind speed, weather code and observation timestamp.

The API response is validated using Pydantic models before it is accepted by the pipeline. Invalid responses are rejected rather than being allowed to move further through the pipeline.

## 3. Raw Data Storage

Validated API responses are stored as JSON files inside `data/raw/`.

The raw layer is append-only. Each API request generates a new file containing the city name and extraction timestamp, for example:

```text
london_20260921T104940027192Z.json
brighton_20260921T104940064827Z.json
```

Existing raw files are never overwritten. This provides an immutable record of the data received from the source API.

## 4. Clean Layer

The cleaning stage is implemented in `src/weather_pipeline/clean.py`.

The nested Open-Meteo response is flattened into one record per city and observation timestamp.

A PyArrow schema is used to enforce data types including strings, timestamps, floating-point measurements and weather codes.

The resulting clean data is stored as Parquet files in `data/clean/`.

## 5. Transformations

Transformations are implemented using Polars in `src/weather_pipeline/transform.py`.

Three types of datasets are produced.

### Hourly Weather

A separate hourly Parquet dataset is produced for each city, for example:

```text
data/transform/hourly/london.parquet
data/transform/hourly/brighton.parquet
```

### Daily Weather Summary

The daily summary calculates:

- Minimum temperature
- Maximum temperature
- Average temperature
- Total precipitation
- Average wind speed

The result is stored in `data/transform/daily_summary.parquet`.

### Weather Alerts

The pipeline identifies potentially extreme weather conditions using configured thresholds.

Alerts include:

- `EXTREME_HEAT`
- `EXTREME_COLD`
- `HEAVY_RAIN`
- `HIGH_WIND`

The result is stored in `data/transform/weather_alerts.parquet`.

A single weather observation may generate multiple alerts.

---

# PostgreSQL Database Model

The PostgreSQL schema is defined in `sql/schema.sql`.

The database contains four main tables.

## cities

Stores city information.

Primary key:

```text
city_id
```

Columns include city name, latitude, longitude, elevation and timezone.

## weather_hourly

Stores hourly weather observations.

Primary key:

```text
(city_id, observation_timestamp)
```

The `city_id` column is also a foreign key referencing `cities.city_id`.

Important columns include:

```text
city_id
observation_timestamp
temperature_c
relative_humidity_pct
precipitation_mm
wind_speed_kmh
weather_code
source_file
ingested_at_utc
```

## weather_daily_summary

Stores aggregated daily weather information.

Primary key:

```text
(city_id, weather_date)
```

The `city_id` column references the `cities` table.

## weather_alerts

Stores weather alerts.

Primary key:

```text
(city_id, observation_timestamp, alert_type)
```

This allows different alert types to exist for the same city and hour while preventing the same alert from being inserted twice.

---

# Duplicate Prevention and Incremental Processing

The pipeline is designed to be safe to rerun.

Duplicate prevention happens at several layers.

## Raw Layer

Raw API responses are immutable. Every extraction creates a new timestamped JSON file and existing raw files are never modified.

## Clean Layer

A weather observation is uniquely identified using:

```text
(city_id, observation_timestamp)
```

Before writing clean data, the pipeline checks which observations already exist. Only previously unseen observations are written to new clean Parquet files.

## Transform Layer

Polars removes duplicates using `city_id` and `observation_timestamp` before transformed datasets are generated.

## PostgreSQL Layer

Hourly weather uses:

```sql
ON CONFLICT
(
    city_id,
    observation_timestamp
)
DO NOTHING
```

Weather alerts use:

```sql
ON CONFLICT
(
    city_id,
    observation_timestamp,
    alert_type
)
DO NOTHING
```

Daily summaries use an upsert because the summary for a day may change when additional hourly observations become available:

```sql
ON CONFLICT
(
    city_id,
    weather_date
)
DO UPDATE
```

This makes the pipeline idempotent and safe to rerun.

---

# Installation

## 1. Clone the Repository

```bash
git clone <repository-url>
cd uk_weather_pipeline
```

## 2. Create a Virtual Environment

Windows:

```powershell
python -m venv ukwpenv
```

Activate it:

```powershell
.\ukwpenv\Scripts\Activate.ps1
```

## 3. Install Dependencies

```powershell
python -m pip install -r requirements.txt
python -m pip install -e .
```

---

# PostgreSQL Configuration

Create a local `.env` file:

```text
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=weather
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_actual_password
```

The `.env` file is excluded from Git using `.gitignore`.

The repository contains `.env.example` showing the required environment variables without exposing real credentials.

---

# Running the Pipeline

## Run Without PostgreSQL

```powershell
python -m weather_pipeline.pipeline --skip-db
```

This executes extraction, raw storage, cleaning and transformation without loading PostgreSQL.

## Run the Full Pipeline

Make sure PostgreSQL is running and the `weather` database exists, then run:

```powershell
python -m weather_pipeline.pipeline
```

The complete pipeline executes:

```text
Open-Meteo
→ Raw JSON
→ Clean Parquet
→ Polars Transformations
→ PostgreSQL
```

---

# Logging and Error Handling

The project uses Python logging. Pipeline activity is written to `logs/pipeline.log`.

Logs include:

- pipeline start/completion
- API extraction
- raw file creation
- clean data processing
- transformation output
- PostgreSQL loading
- failures and exceptions

HTTP errors and invalid API responses are handled so failures can be identified clearly.

---

# Testing

The project uses pytest.

Run all tests with:

```powershell
pytest -v
```

Run the test suite with coverage:

```powershell
pytest --cov=weather_pipeline --cov-report=term-missing
```

The project currently contains 49 tests and achieves approximately 93% overall code coverage.

The tests cover:

- Pydantic API validation
- API success and failure scenarios
- API timeout handling
- immutable raw file creation
- incremental clean processing
- duplicate prevention
- daily weather aggregation
- weather alerts
- PostgreSQL SQL generation
- PostgreSQL conflict handling
- pipeline orchestration
- skip-database behaviour
- empty-data behaviour
- logging configuration

External services such as Open-Meteo and PostgreSQL are mocked in unit tests where appropriate.

---

# Production Improvements

A production data pipeline used within a company would require additional capabilities beyond the current implementation.

Three improvements are recommended.

## 1. Workflow Orchestration

Use a workflow orchestration platform such as Apache Airflow, Prefect or Dagster.

This would provide scheduled execution, retries, dependency management, failure notifications, execution history and monitoring.

## 2. Cloud Data Storage and Data Lake Architecture

Instead of storing raw and Parquet files only on a local machine, production datasets could be stored in Azure Data Lake Storage, Amazon S3 or Google Cloud Storage.

Data could also be partitioned by city and date, for example:

```text
raw/
year=2026/
month=09/
day=21/
city=london/
```

This would improve scalability, durability and historical data management.

## 3. Continuous Integration and Continuous Delivery

A CI/CD pipeline can automatically validate every change pushed to the repository.

For example, GitHub Actions can install Python, install dependencies, run unit tests, calculate test coverage and reject changes when tests fail.

This reduces the risk of broken code being merged into the main branch.

For this project, GitHub Actions CI is the selected production improvement.

---

# Implemented Production Improvement: GitHub Actions CI

The project uses GitHub Actions to automatically run the test suite whenever code is pushed or a pull request is created.

The workflow is stored in:

```text
.github/workflows/tests.yml
```

The CI workflow:

1. Checks out the repository.
2. Installs Python.
3. Installs project dependencies.
4. Installs the project package.
5. Runs pytest.
6. Produces a coverage report.

This means changes can be automatically validated before being merged into the `main` branch.

---

# Summary

This project demonstrates a complete incremental data engineering pipeline using:

- Python
- Open-Meteo API
- Pydantic
- PyArrow
- Parquet
- Polars
- PostgreSQL
- pytest
- Git
- GitHub Actions

The design provides immutable raw storage, schema validation, incremental processing, transformation, relational loading, duplicate protection and automated testing.


Implementation will be added through a feature branch and pull request.
