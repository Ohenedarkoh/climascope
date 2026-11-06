# Phase 8: Airflow Orchestration

## 1. Overview

This phase introduces Apache Airflow to ClimaScope to orchestrate the weather data pipeline.

Before this phase, the pipeline components could be executed individually. However, there was no central workflow manager responsible for coordinating extraction, processing and database loading.

Airflow provides workflow orchestration by allowing the pipeline to run as a sequence of dependent tasks. It also supports scheduled execution, manual backfills, task monitoring and execution history.

The DAG is named:

`climascope_weather_pipeline`

The pipeline follows this workflow:

1. Extract historical weather data from the Open-Meteo API.
2. Save the raw API responses as JSON files.
3. Process and validate the extracted data.
4. Load the processed records into PostgreSQL.

## 2. Airflow Architecture

Airflow runs inside the existing Docker Compose environment.

The services used for orchestration are:

| Service | Responsibility |
|---|---|
| `airflow-apiserver` | Provides the Airflow web interface and API |
| `airflow-scheduler` | Schedules DAG runs and coordinates task execution |
| `airflow-dag-processor` | Processes DAG files and makes workflows available to Airflow |
| `postgres` | Stores Airflow metadata, including DAG runs and task states |

The Airflow services use the `weather-api-airflow:latest` custom image, built from the project's Dockerfile using Apache Airflow 3.3.2.

The Airflow environment uses `LocalExecutor`, which allows tasks to execute locally through the scheduler environment.

The Airflow metadata database is separate from the ClimaScope weather database. The weather records are stored in the `weather` database, while Airflow uses its own metadata database.

### Docker volume mounts

The following project directories are mounted into the Airflow containers:

| Host directory | Container directory | Purpose |
|---|---|---|
| `./dags` | `/opt/airflow/dags` | Airflow DAG definitions |
| `./src` | `/opt/airflow/src` | Pipeline source code |
| `./logs` | `/opt/airflow/logs` | Airflow execution logs |
| `./data` | `/opt/airflow/data` | Raw and processed data |

The `PYTHONPATH` environment variable is configured as:

`PYTHONPATH: /opt/airflow`

This allows the DAG to import the project's Python modules from the mounted source directory.

## 3. DAG Configuration

The DAG uses the following configuration:

| Setting | Value | Purpose |
|---|---|---|
| DAG ID | `climascope_weather_pipeline` | Identifies the workflow |
| Schedule | `@daily` | Runs once per day |
| Start date | `2026-01-01` | Defines the beginning of the DAG schedule |
| Catchup | `False` | Prevents automatic creation of missed historical runs |
| Max active runs | `1` | Prevents overlapping DAG runs |
| Parameters | `start_date`, `end_date` | Support manual historical backfills |
| Executor | `LocalExecutor` | Executes tasks within the Airflow environment |

The DAG uses the Airflow SDK:

- `dag` to define the workflow.
- `task` to define individual tasks.
- `get_current_context` to access the current DAG run context and parameters.

## 4. Pipeline Task Flow

The workflow is divided into separate tasks so that each stage has a clear responsibility.

### 4.1 Extraction

The extraction task retrieves weather data from the Open-Meteo Historical Weather API.

The API endpoint is:

`https://archive-api.open-meteo.com/v1/archive`

The pipeline retrieves data for:

| City | Latitude | Longitude |
|---|---:|---:|
| Accra | 5.6037 | -0.1870 |
| Kumasi | 6.6885 | -1.6244 |
| Tamale | 9.4075 | -0.8533 |

The extraction task uses the existing API client rather than duplicating the HTTP request logic inside the DAG.

The task determines the date range based on whether the DAG run is a manual backfill or a scheduled execution.

**Manual backfill**

When both `start_date` and `end_date` parameters are supplied, the task:

1. Reads the parameters from the Airflow context.
2. Converts the values into Python dates using `date.fromisoformat()`.
3. Checks that both dates are provided.
4. Validates that the start date is not later than the end date.
5. Uses the supplied date range for extraction.

**Scheduled execution**

When no manual dates are supplied, the task uses the Airflow data interval.

The implementation calculates:

- `interval_start` from `data_interval_start`.
- `interval_end` from `data_interval_end`, subtracting one day.
- `start` by subtracting six days from the interval start.
- `end` using the interval end.

This creates a seven-day rolling lookback window.

The lookback helps capture historical changes or delayed updates from the weather API rather than assuming that the previous day's data will always be immediately complete.

The extraction task loops through the configured cities, retrieves their weather data and saves each raw response using the existing storage module.

### 4.2 Raw Data Storage

The API responses are saved as JSON files under:

`data/raw/<city>/<start>_<end>.json`

This preserves the original API responses before transformation.

The task returns the resulting file paths for downstream processing.

### 4.3 Processing and Validation

The processing task reads the extracted raw files and uses the existing processing module.

It converts the API responses into structured tabular data and applies the validation rules implemented in the processing phase.

These checks include:

- Required weather variables are present.
- Numeric weather fields can be converted appropriately.
- City and date values are available.
- Duplicate city/date combinations are rejected.
- Minimum temperature does not exceed maximum temperature.

The processed data is saved under:

`data/processed/`

### 4.4 Database Loading

The loading task passes the processed data to the existing PostgreSQL loader.

The loader writes records into:

- Database: `weather`
- Table: `daily_weather`

The table uses `(city, date)` as its unique key.

The loader uses an upsert operation with `ON CONFLICT DO UPDATE`. This allows existing city/date records to be updated instead of inserting duplicate records.

## 5. XCom and Task Communication

Airflow uses XCom (cross-communication) to pass information between tasks.

In ClimaScope, the extraction task returns file paths and small metadata rather than passing the complete weather datasets through XCom.

This is intentional.

Large datasets should be stored in an appropriate storage location rather than passed through Airflow's metadata database.

The downstream tasks use the returned paths to locate the files they need to process.

This keeps task communication lightweight and separates orchestration metadata from the actual data.

## 6. Scheduling and Backfill Behaviour

### Daily scheduling

The DAG uses:

`schedule="@daily"`

The schedule runs at midnight UTC.

Ghana operates on UTC, so this corresponds to midnight Ghana time.

The scheduled extraction uses the seven-day rolling lookback described earlier.

### Catchup behaviour

The DAG uses:

`catchup=False`

This prevents Airflow from automatically generating every missed scheduled run when the scheduler is unavailable.

If the local computer is switched off or Docker is stopped, scheduled ingestion cannot run during that period. Missed runs are not automatically recreated by this configuration.

Historical data can instead be retrieved through a manual backfill.

### Manual historical backfill

The DAG accepts two parameters:

- `start_date`
- `end_date`

Both parameters must be provided for a manual backfill.

Example:

```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-11-04"
}
```

The dates are supplied through the Airflow UI when manually triggering the DAG.

This allows a user to retrieve a historical date range without changing the DAG's normal daily schedule.

## 7. Airflow and Docker Configuration

The following configuration was important when integrating the pipeline with Airflow.

### Python module imports

```yaml
PYTHONPATH: /opt/airflow
```

This makes the mounted project source code importable by the DAG.

### Execution API server URL

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
```

This setting provides the execution API server address used by Airflow task execution.

It was added after task execution encountered a connection-refused error when the task supervisor attempted to connect to the execution API.

### Authentication

The project uses Airflow's Simple Auth Manager.

The configured administrator username is supplied through the environment variables.

The generated password file is located inside the Airflow API server container at:

`/opt/airflow/simple_auth_manager_passwords.json.generated`

Do not commit generated credentials or other secrets to Git.

## 8. Docker Commands Used

The project directory was renamed to `climascope`, but the existing Docker Compose project name remains `weather-api-pipeline`.

Therefore, Docker commands must continue using:

`-p weather-api-pipeline`

This ensures commands operate on the existing containers and persistent database volume.

### Validate the Compose configuration

```powershell
docker compose -p weather-api-pipeline config --quiet
```

Checks the Docker Compose configuration for errors without starting the services.

### Start the Airflow services

```powershell
docker compose -p weather-api-pipeline up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

Starts the Airflow API server, scheduler and DAG processor in detached mode.

### Check running services

```powershell
docker compose -p weather-api-pipeline ps
```

Displays the state of the services, including PostgreSQL health.

### List registered DAGs

```powershell
docker compose -p weather-api-pipeline exec airflow-scheduler airflow dags list
```

Checks whether Airflow has discovered the ClimaScope DAG.

The expected DAG ID is:

`climascope_weather_pipeline`

### Retrieve the generated Airflow credentials

```powershell
docker compose -p weather-api-pipeline exec airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Displays the generated authentication file. Keep the output private.

### Airflow web interface

The Airflow interface is available locally at:

`http://localhost:8081`

## 9. Operational Considerations

The current deployment runs locally using Docker.

The scheduled pipeline continues to run while:

- The computer remains powered on and awake.
- Docker Desktop is running.
- The required Airflow and PostgreSQL services remain operational.

Stopping Docker or shutting down the computer interrupts scheduled ingestion.

For continuous unattended execution, the pipeline would need to be deployed to a cloud environment.

The PostgreSQL database uses a persistent Docker named volume. Avoid running `docker compose down -v` unless intentionally deleting the stored database volume.

## 10. Phase Outcome

Airflow successfully orchestrates the ClimaScope weather pipeline.

The implementation supports:

- Automated daily scheduling.
- Seven-day rolling lookback extraction.
- Manual historical backfills.
- Separate extraction, processing and loading tasks.
- Lightweight XCom communication.
- PostgreSQL upserts.
- Monitoring through the Airflow interface.

The DAG was successfully registered, activated and executed, including a full historical backfill.

This completed the Airflow orchestration phase of ClimaScope.