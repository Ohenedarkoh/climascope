# Phase 10: Final Documentation and Portfolio Presentation

## 1. Objective

The objective of this phase is to prepare ClimaScope for its final release and portfolio presentation.

The implementation and end-to-end testing phases are complete. This phase focuses on making the project understandable, reproducible, maintainable, and easy for another developer or recruiter to evaluate.

The final documentation should explain:

- The problem the project addresses.
- The architecture and technologies used.
- How weather data is extracted, processed, validated, and loaded.
- How Apache Airflow orchestrates the pipeline.
- How to configure and run the project.
- How to verify that the pipeline is working.
- Important engineering decisions and known limitations.

## 2. Project Summary

ClimaScope is an automated historical weather data pipeline that retrieves daily weather observations for three cities in Ghana: Accra, Kumasi, and Tamale.

The pipeline uses the Open-Meteo Historical Weather API as its data source. Python handles extraction, processing, validation, and database loading. PostgreSQL stores the resulting records, while Apache Airflow manages and schedules the workflow.

Docker Compose provides the containerized environment required to run the services.

The pipeline supports both scheduled ingestion and manual historical backfills.

## 3. Architecture

The pipeline follows this workflow:

1. Apache Airflow triggers the weather ingestion workflow.
2. Python sends requests to the Open-Meteo Historical Weather API.
3. The extracted API responses are saved as raw JSON files.
4. The processing component converts the raw responses into structured records.
5. Data validation checks the processed records before loading.
6. The database loader inserts records into PostgreSQL.
7. Existing records are updated when the same city and date are encountered.

### Architecture flow

```text
Open-Meteo Historical Weather API
              |
              v
       Python Extraction
              |
              v
       Raw JSON Files
              |
              v
    Processing and Validation
              |
              v
       Processed Data
              |
              v
       PostgreSQL Loader
              |
              v
       PostgreSQL Database
        daily_weather table

       Apache Airflow
       orchestrates the workflow

       Docker Compose
       runs the services
```

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data extraction, processing, validation, and loading |
| Open-Meteo API | Historical weather data source |
| PostgreSQL | Persistent relational data storage |
| Apache Airflow 3.3.2 | Workflow orchestration and scheduling |
| Docker | Containerization |
| Docker Compose | Multi-container service management |
| pytest | Automated testing |
| pandas | Data processing |

## 5. Data Coverage and Database Design

The pipeline retrieves the following weather variables:

- Mean temperature (`temperature_2m_mean`)
- Maximum temperature (`temperature_2m_max`)
- Minimum temperature (`temperature_2m_min`)
- Total precipitation (`precipitation_sum`)
- Maximum wind speed (`wind_speed_10m_max`)
- Weather condition code (`weather_code`)

The dataset covers January 1 to November 4, 2026.

The three locations are:

| City | Latitude | Longitude |
|---|---:|---:|
| Accra | 5.6037 | -0.1870 |
| Kumasi | 6.6885 | -1.6244 |
| Tamale | 9.4075 | -0.8533 |

The PostgreSQL database contains a `daily_weather` table.

The table uses a composite uniqueness constraint on `(city, date)`. This prevents multiple records for the same city and date and supports idempotent loading through PostgreSQL's `ON CONFLICT DO UPDATE` functionality.

### Final dataset

| Metric | Result |
|---|---:|
| Total records | 924 |
| Cities | 3 |
| Records per city | 308 |
| First date | 2026-01-01 |
| Last date | 2026-11-04 |
| Duplicate city/date combinations | 0 |

## 6. Pipeline Scheduling and Backfills

The Airflow DAG is named:

`climascope_weather_pipeline`

The pipeline is configured to run daily at midnight UTC.

The daily ingestion process uses a seven-day rolling lookback. This allows recently updated historical observations to be retrieved again rather than assuming that previously published weather data can never change.

The DAG also supports manual backfills through the `start_date` and `end_date` parameters.

`catchup=False` prevents Airflow from automatically creating runs for every missed scheduled interval when the scheduler has been unavailable.

## 7. Running the Project

### Prerequisites

Before running the project, install or configure:

- Git
- Python
- Docker Desktop
- Docker Compose

Clone the repository and move into the project directory:

```powershell
git clone <repository-url>
cd climascope
```

Create and activate a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install the Python dependencies:

```powershell
pip install -r requirements.txt
```

Configure the environment variables using the project's `.env.example` file as a reference.

Do not commit the actual `.env` file or expose database credentials.

### Start the Docker services

Build the custom Airflow image:

```powershell
docker compose -p weather-api-pipeline build
```

Start the services:

```powershell
docker compose -p weather-api-pipeline up -d
```

Check the service status:

```powershell
docker compose -p weather-api-pipeline ps
```

The Airflow interface is available at:

`http://localhost:8081`

The Airflow login credentials are generated by the Simple Auth Manager during initialization. Follow the project's setup instructions to retrieve the generated credentials.

### Access PostgreSQL

Connect to the weather database:

```powershell
docker compose -p weather-api-pipeline exec postgres psql -U weather_user -d weather
```

Check the number of records:

```sql
SELECT COUNT(*) AS total_records
FROM daily_weather;
```

Check the number of cities:

```sql
SELECT COUNT(DISTINCT city) AS total_cities
FROM daily_weather;
```

Exit PostgreSQL:

```sql
\q
```

### Stop the services

```powershell
docker compose -p weather-api-pipeline down
```

This stops and removes the containers without deleting the persistent PostgreSQL volume.

**Important:** Do not use `docker compose down -v` unless you intentionally want to delete the persistent database volume and its contents.

## 8. Testing and Verification

Automated tests are executed using:

```powershell
python -m pytest tests -v
```

The completed test suite contains nine tests covering:

- Successful API requests.
- HTTP errors.
- Request timeouts.
- Saving raw weather data.
- Handling empty DataFrames.
- Successful database loading.
- Database rollback after a loading failure.
- Successful data processing.
- Rejection of missing required weather variables.

**Final automated test result:** 9 passed.

### End-to-end verification

The complete workflow was also verified using Docker and Airflow.

The verification included:

- Confirming that the required Docker services were running.
- Confirming that the Airflow DAG was registered and unpaused.
- Confirming that PostgreSQL was accessible.
- Running the historical ingestion workflow.
- Confirming the expected 924 records across the three cities.
- Checking for duplicate city/date combinations.
- Checking for missing values in required fields.
- Checking that minimum temperatures did not exceed maximum temperatures.
- Running a manual backfill and confirming that the record count remained unchanged.

The unchanged record count after rerunning an overlapping date range confirmed that the database loading process was idempotent.

## 9. Important Engineering Decisions

### Separate extraction, processing, and loading

The pipeline separates its major responsibilities into distinct components. This improves maintainability and makes individual stages easier to test and debug.

### Raw data preservation

API responses are saved as raw JSON before processing. This preserves the original source response and makes it possible to investigate extraction or processing issues.

### Passing file paths through XCom

Airflow XCom is used to pass small pieces of metadata, such as file paths, between tasks instead of transferring entire datasets through Airflow's metadata database.

### Database upserts

The `(city, date)` uniqueness constraint and `ON CONFLICT DO UPDATE` logic allow records to be safely reprocessed without creating duplicates.

### Seven-day rolling lookback

The rolling lookback accounts for possible delayed updates to recently published historical observations.

### Dockerized services

Docker Compose provides a consistent environment for the PostgreSQL database and Airflow services.

## 10. Limitations and Future Improvements

The current implementation provides a working historical weather ingestion pipeline. It also leaves room for further engineering improvements.

Potential extensions include:

- Deploying the pipeline to a cloud environment.
- Adding monitoring and alert notifications.
- Implementing stronger retry and recovery strategies.
- Adding data freshness and completeness monitoring.
- Supporting more cities and weather variables.
- Introducing a data warehouse and analytical data models.
- Adding CI/CD for automated testing and deployment.
- Implementing infrastructure as code.
- Adding dashboards for weather trends and comparisons.

These are potential extensions, not features claimed as already implemented.

## 11. Final Project Outcome

ClimaScope demonstrates the development of an orchestrated data pipeline from API extraction through database storage.

The completed implementation includes:

- Historical API ingestion.
- Raw data preservation.
- Data processing and validation.
- PostgreSQL database loading.
- Idempotent database operations.
- Apache Airflow orchestration.
- Docker-based service management.
- Automated unit tests.
- End-to-end pipeline verification.
- Manual historical backfills.
- Scheduled daily ingestion.

The final dataset contains 924 records covering three Ghanaian cities over 308 days.

The project demonstrates practical experience with Python-based data engineering, relational databases, workflow orchestration, containerization, testing, and pipeline reliability.