# ClimaScope 
### An Orchestrated Historical Weather Data Pipeline

ClimaScope is an automated data engineering project that extracts historical weather data for selected cities in Ghana, processes and validates the data, and loads it into PostgreSQL.

The pipeline uses Python for data extraction and transformation, Apache Airflow for workflow orchestration, PostgreSQL for persistent storage, and Docker Compose to manage the application services.

It supports scheduled daily ingestion as well as manual historical backfills.

---

## Project Overview

Weather data can be useful for understanding environmental patterns and comparing conditions across different locations. However, raw API responses are not always structured for direct analysis.

ClimaScope addresses this by building a repeatable pipeline that retrieves weather observations, preserves the raw API responses, transforms the data into a structured format, validates records, and stores the results in a relational database.

### Key Features

- Historical weather data ingestion from the Open-Meteo API.
- Extraction for three cities in Ghana.
- Raw JSON preservation before transformation.
- Data processing and validation using Python.
- PostgreSQL storage with idempotent upsert operations.
- Apache Airflow workflow orchestration.
- Daily scheduled ingestion with a seven-day rolling lookback.
- Manual historical backfill using Airflow DAG parameters.
- Dockerized services for consistent execution.
- Automated tests using pytest.

---

## Architecture

The pipeline follows a modular Extract, Transform, Load (ETL) architecture.

```text
              Open-Meteo Historical API
                         |
                         v
                 Python Extraction
                         |
                         v
                  Raw JSON Files
                         |
                         v
              Processing & Validation
                         |
                         v
                  Processed Data
                         |
                         v
                 PostgreSQL Loader
                         |
                         v
                 PostgreSQL Database
                    daily_weather

              Apache Airflow
              Workflow Orchestration

              Docker Compose
              Service Management
```

### Pipeline Workflow

1. **Extraction:** Python sends requests to the Open-Meteo Historical Weather API for each configured city.
2. **Raw storage:** API responses are saved as JSON files to preserve the original source data.
3. **Processing:** Raw responses are converted into structured records.
4. **Validation:** Required weather variables and data conditions are checked before loading.
5. **Loading:** Validated records are inserted into PostgreSQL.
6. **Upsert:** Existing records with the same city and date are updated instead of duplicated.
7. **Orchestration:** Airflow coordinates task execution, scheduling, and manual backfills.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Extraction, processing, validation and loading |
| Open-Meteo API | Historical weather data source |
| pandas | Data processing |
| PostgreSQL | Relational database |
| SQLAlchemy | Database interaction |
| Apache Airflow 3.3.2 | Workflow orchestration |
| Docker | Containerization |
| Docker Compose | Multi-container management |
| pytest | Automated testing |

---

## Data Source and Coverage

ClimaScope uses the [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).

**API endpoint:**

`https://archive-api.open-meteo.com/v1/archive`

### Locations

The project retrieves weather data for three cities in Ghana.

| City | Latitude | Longitude |
|---|---:|---:|
| Accra | 5.6037 | -0.1870 |
| Kumasi | 6.6885 | -1.6244 |
| Tamale | 9.4075 | -0.8533 |

### Weather Variables

| Variable | Description |
|---|---|
| `temperature_2m_mean` | Daily mean temperature |
| `temperature_2m_max` | Daily maximum temperature |
| `temperature_2m_min` | Daily minimum temperature |
| `precipitation_sum` | Total daily precipitation |
| `wind_speed_10m_max` | Maximum daily wind speed |
| `weather_code` | Weather condition code |

### Historical Coverage

The completed historical ingestion covers January 1 to November 4, 2026.

| Metric | Result |
|---|---:|
| Cities | 3 |
| Days per city | 308 |
| Total records | 924 |
| First date | 2026-01-01 |
| Last date | 2026-11-04 |

---

## Database Design

The project uses PostgreSQL to store the processed weather observations.

The primary data table is:

`daily_weather`

Each record represents the daily weather conditions for one city.

A composite uniqueness constraint on `(city, date)` ensures that the same city's weather observation for a given date cannot be inserted more than once.

The loader uses PostgreSQL's `ON CONFLICT DO UPDATE` functionality to update existing records when the same city and date are encountered again.

This makes the loading process idempotent and allows overlapping ingestion windows to be safely reprocessed.

---

## Airflow Orchestration

Apache Airflow manages the pipeline through the DAG:

`climascope_weather_pipeline`

The DAG coordinates the extraction, processing, and database loading stages.

### Scheduling

The pipeline is configured to run daily at midnight UTC.

A seven-day rolling lookback is used during scheduled ingestion. This allows recently published historical observations to be retrieved again in case the source data has been updated.

### Historical Backfills

The DAG also supports manual backfills through the `start_date` and `end_date` parameters.

This allows a user to specify a historical date range without modifying the pipeline code.

The DAG uses `catchup=False` to prevent Airflow from automatically creating runs for every missed scheduled interval.

---

## Project Structure

```text
climascope/
│
├── dags/
│   └── climascope_weather_dag.py
│
├── src/
│   ├── extraction/
│   ├── processing/
│   ├── database/
│   └── etl/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── tests/
│
├── docker/
│
├── docs/
│   ├── 01-git-and-repository-setup.md
│   ├── 02-api-exploration.md
│   ├── 03-environment-and-service-setup.md
│   ├── 04-api-ingestion-logic.md
│   ├── 05-data-processing-and-validation.md
│   ├── 06-postgresql-database-design.md
│   ├── 07-database-loading.md
│   ├── 08-airflow-orchestration.md
│   ├── 09-end-to-end-integration-and-testing.md
│   └── 10-final-documentation-and-portfolio-presentation.md
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

*The structure above summarizes the main project components. Some folders may contain additional files and subdirectories.*

---

## Getting Started

### Prerequisites

Make sure the following tools are installed:

- Git
- Python 3.10 or later
- Docker Desktop
- Docker Compose

### 1. Clone the repository

```powershell
git clone <repository-url>
cd climascope
```

### 2. Create a virtual environment

```powershell
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

### 4. Configure environment variables

Create your local `.env` file using `.env.example` as a reference.

Ensure the required database and Airflow environment variables are configured.

Do not commit your actual `.env` file or expose credentials in the repository.

### 5. Build the Docker services

```powershell
docker compose -p weather-api-pipeline build
```

### 6. Start the services

```powershell
docker compose -p weather-api-pipeline up -d
```

Check the service status:

```powershell
docker compose -p weather-api-pipeline ps
```

### 7. Access Airflow

Open the Airflow interface:

[http://localhost:8081](http://localhost:8081)

Log in using the credentials generated by the Airflow Simple Auth Manager during initialization.

Locate `climascope_weather_pipeline` and use the Airflow interface to monitor or trigger the DAG.

### 8. Access PostgreSQL

Connect to the weather database:

```powershell
docker compose -p weather-api-pipeline exec postgres psql -U weather_user -d weather
```

Check the total number of records:

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

### 9. Stop the services

```powershell
docker compose -p weather-api-pipeline down
```

This stops the containers while preserving the persistent PostgreSQL volume.

**Important:** Avoid using `docker compose down -v` unless you intentionally want to delete the database volume and its stored data.

---

## Testing and Validation

Automated tests are implemented using pytest.

Run the test suite:

```powershell
python -m pytest tests -v
```

### Test Coverage

The completed test suite covers:

- Successful API requests.
- HTTP request errors.
- Request timeouts.
- Raw JSON file storage.
- Empty DataFrame handling.
- Successful database loading.
- Database rollback after a loading failure.
- Weather data processing.
- Rejection of missing required weather variables.

<kbd>Final test result: 9 passed</kbd>

### End-to-End Verification

The pipeline was also tested through its Docker and Airflow environment.

The final verification confirmed:

- Required Docker services were running.
- The Airflow DAG was registered and unpaused.
- PostgreSQL was accessible.
- Historical ingestion completed successfully.
- The database contained 924 records across three cities.
- No duplicate city/date combinations existed.
- Required weather fields contained no missing values.
- No records had minimum temperatures greater than maximum temperatures.
- Reprocessing an overlapping historical date range did not increase the record count.

These checks confirmed that the pipeline's major components work together and that the database loading process is idempotent.

---

## Engineering Decisions

A number of decisions were made to improve the reliability and maintainability of the pipeline.

**Modular design:** Extraction, processing, validation, and loading responsibilities are separated into different components. This makes individual stages easier to test and maintain.

**Raw data preservation:** API responses are saved before transformation, providing a source copy for troubleshooting and reprocessing.

**XCom usage:** Airflow XCom passes small metadata, such as file paths, between tasks instead of transferring entire datasets through the Airflow metadata database.

**Idempotent loading:** The composite uniqueness constraint and upsert logic prevent duplicate records when date ranges overlap.

**Rolling lookback:** The seven-day ingestion window accounts for potential updates to recently published historical observations.

**Containerization:** Docker Compose provides a consistent environment for the Airflow and PostgreSQL services.

---

## Limitations and Future Improvements

The current implementation is a working historical weather ingestion pipeline. There are several opportunities to extend it.

Potential improvements include:

- Deploying the pipeline to AWS or another cloud platform.
- Adding automated failure notifications.
- Introducing data freshness and completeness monitoring.
- Expanding coverage to additional cities.
- Supporting additional weather variables.
- Building analytical dashboards for weather trends.
- Introducing CI/CD for automated testing and deployment.
- Implementing infrastructure as code.
- Adding stronger retry and recovery mechanisms.

These are proposed improvements rather than features currently implemented.

---

## Documentation

Detailed documentation covering the project's development and implementation is available in the [`docs/`](docs/) directory.

The documentation covers:

- Repository setup and Git workflow.
- API exploration.
- Environment and Docker setup.
- API extraction.
- Data processing and validation.
- PostgreSQL database design.
- Database loading.
- Airflow orchestration.
- End-to-end integration and testing.
- Final project documentation and presentation.

For the complete engineering walkthrough, start with [`docs/01-git-and-repository-setup.md`](docs/01-git-and-repository-setup.md).

---

## Project Outcome

ClimaScope demonstrates the development of an automated ETL pipeline using Python, PostgreSQL, and Apache Airflow.

The completed project successfully ingests and stores 924 historical weather records across Accra, Kumasi, and Tamale, with automated tests and end-to-end verification.

It provides practical experience in API ingestion, data processing, database design, workflow orchestration, containerization, testing, and reliable data loading.