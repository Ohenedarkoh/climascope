# Phase 9: End-to-End Integration and Testing

## 1. Overview

The purpose of this phase was to verify that the ClimaScope pipeline works as an integrated system.

The earlier development phases tested individual components, including API extraction, raw data storage, processing, PostgreSQL schema creation and database loading.

This phase focused on verifying that these components work together through Airflow and that the resulting database contains complete and consistent weather records.

Testing covered:

- Automated component tests.
- Docker service availability.
- Airflow DAG registration and activation.
- PostgreSQL record verification.
- Data completeness and quality.
- Duplicate detection.
- Manual historical backfill.
- Idempotency.

## 2. Automated Component Testing

The project uses `pytest` for automated testing.

The tests are organised into separate modules covering the main pipeline components.

### Run the test suite

From the project root directory, run:

```powershell
python -m pytest tests -v
```

The `-v` option displays individual test names and their results.

### Test coverage

| Test module | Tests | Purpose |
|---|---:|---|
| `tests/test_api_client.py` | 3 | API success, HTTP errors and request timeouts |
| `tests/test_api_storage.py` | 1 | Saving raw API responses |
| `tests/test_database_loader.py` | 3 | Empty DataFrames, successful loading and rollback on failure |
| `tests/test_processing.py` | 2 | Successful processing and rejection of missing variables |
| **Total** | **9** | |

### Actual test results

The test suite completed successfully:

```text
collected 9 items

tests/test_api_client.py::test_fetch_weather_success PASSED
tests/test_api_client.py::test_fetch_weather_http_error PASSED
tests/test_api_client.py::test_fetch_weather_timeout PASSED
tests/test_api_storage.py::test_save_raw_weather PASSED
tests/test_database_loader.py::test_load_weather_data_empty_dataframe PASSED
tests/test_database_loader.py::test_load_weather_data_success PASSED
tests/test_database_loader.py::test_load_weather_data_rolls_back_on_failure PASSED
tests/test_processing.py::test_process_weather_data PASSED
tests/test_processing.py::test_process_weather_data_rejects_missing_variable PASSED

========================= 9 passed in 3.62s =========================
```

**Result: 9 tests passed.**

These tests provide component-level verification. Some external interactions are mocked, so passing unit tests alone does not prove that the entire live pipeline works. That is why the following integration checks were also performed.

## 3. Docker Service Verification

Before testing the complete pipeline, the existing Docker environment was checked.

The project directory is named `climascope`, but the existing Docker Compose project name remains `weather-api-pipeline`.

All Docker commands therefore use the explicit project name:

`-p weather-api-pipeline`

### Check running containers

```powershell
docker compose -p weather-api-pipeline ps
```

This displays the status of the Docker Compose services.

The following services were verified:

| Service | Expected state |
|---|---|
| `postgres` | Running and healthy |
| `airflow-apiserver` | Running |
| `airflow-scheduler` | Running |
| `airflow-dag-processor` | Running |

The services were running successfully during the integration checks.

PostgreSQL reported:

`Up (healthy)`

## 4. Airflow DAG Verification

The next check confirmed that Airflow could discover the ClimaScope DAG.

### List registered DAGs

```powershell
docker compose -p weather-api-pipeline exec airflow-scheduler airflow dags list
```

The expected DAG was:

`climascope_weather_pipeline`

The command confirmed that the DAG was registered and showed:

`is_paused = False`

This confirmed that the DAG was active and available for scheduled execution.

The Airflow interface was accessible at:

`http://localhost:8081`

The DAG was also manually triggered through the interface to verify its execution.

## 5. PostgreSQL Integration Verification

The database was inspected directly to verify that the data loaded through the pipeline was available.

### Connect to PostgreSQL

```powershell
docker compose -p weather-api-pipeline exec postgres psql -U weather_user -d weather
```

This opens a PostgreSQL command-line session inside the existing PostgreSQL container.

The database used for the weather records is:

`weather`

The target table is:

`daily_weather`

Exit the PostgreSQL session using:

```sql
\q
```

### 5.1 Verify the total number of records

The historical backfill covered January 1 to November 4, 2026.

The expected number of days was 308.

With three cities, the expected number of records was:

308 days × 3 cities = 924 records.

The following query was used:

```sql
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT city) AS total_cities,
    COUNT(DISTINCT date) AS total_dates,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM daily_weather;
```

The initial result was:

| Metric | Result |
|---|---:|
| Total records | 925 |
| Distinct cities | 4 |
| Distinct dates | 308 |
| Earliest date | 2026-01-01 |
| Latest date | 2026-11-04 |

The result revealed one additional record and a fourth city.

This was investigated rather than assuming the expected record count was correct.

### 5.2 Investigate records by city

The following query identified the source of the additional record:

```sql
SELECT
    city,
    COUNT(*) AS total_records,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM daily_weather
GROUP BY city
ORDER BY city;
```

The result was:

| City | Records | Earliest date | Latest date |
|---|---:|---|---|
| Accra | 308 | 2026-01-01 | 2026-11-04 |
| Kumasi | 308 | 2026-01-01 | 2026-11-04 |
| Tamale | 308 | 2026-01-01 | 2026-11-04 |
| TestCity | 1 | 2026-09-01 | 2026-09-01 |

The additional record was identified as `TestCity`, a test record inserted during earlier database-loader testing.

It was not part of the actual ClimaScope dataset.

### 5.3 Remove the test record

The test record was removed using:

```sql
DELETE FROM daily_weather
WHERE city = 'TestCity';
```

The record count was then checked again:

```sql
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT city) AS total_cities
FROM daily_weather;
```

The expected final result was:

| Metric | Expected result |
|---|---:|
| Total records | 924 |
| Distinct cities | 3 |

This restored the database to the expected project dataset.

## 6. Data Quality Verification

Several SQL queries were executed against the actual PostgreSQL database to verify the quality of the stored records.

### 6.1 Duplicate city/date records

The database uses `(city, date)` as its unique key.

The following query checks whether any city/date combination appears more than once:

```sql
SELECT
    city,
    date,
    COUNT(*) AS record_count
FROM daily_weather
GROUP BY city, date
HAVING COUNT(*) > 1;
```

**Expected result:** Zero rows.

**Actual result:** Zero rows.

This confirmed that no duplicate city/date combinations existed.

### 6.2 Missing values

The following query checks the required weather fields:

```sql
SELECT
    COUNT(*) FILTER (WHERE city IS NULL) AS missing_city,
    COUNT(*) FILTER (WHERE date IS NULL) AS missing_date,
    COUNT(*) FILTER (WHERE temperature_2m_mean IS NULL) AS missing_mean_temp,
    COUNT(*) FILTER (WHERE temperature_2m_max IS NULL) AS missing_max_temp,
    COUNT(*) FILTER (WHERE temperature_2m_min IS NULL) AS missing_min_temp,
    COUNT(*) FILTER (WHERE precipitation_sum IS NULL) AS missing_precipitation,
    COUNT(*) FILTER (WHERE wind_speed_10m_max IS NULL) AS missing_wind_speed,
    COUNT(*) FILTER (WHERE weather_code IS NULL) AS missing_weather_code
FROM daily_weather;
```

**Actual result:**

| Field | Missing records |
|---|---:|
| City | 0 |
| Date | 0 |
| Mean temperature | 0 |
| Maximum temperature | 0 |
| Minimum temperature | 0 |
| Precipitation | 0 |
| Maximum wind speed | 0 |
| Weather code | 0 |

All checked fields had zero missing values.

### 6.3 Temperature consistency

Minimum temperature should not exceed maximum temperature.

The following query identifies records that violate this condition:

```sql
SELECT
    COUNT(*) AS invalid_temperature_records
FROM daily_weather
WHERE temperature_2m_min > temperature_2m_max;
```

**Expected result:** 0.

**Actual result:** 0.

This confirmed that all stored records satisfied the minimum/maximum temperature relationship.

## 7. Manual Backfill and Idempotency Testing

A manual backfill was used to verify that the Airflow workflow could reprocess a historical date range successfully.

### 7.1 Trigger a manual backfill

The DAG was manually triggered through the Airflow interface with:

```json
{
  "start_date": "2026-10-01",
  "end_date": "2026-10-03"
}
```

This requested three days of weather data for all three configured cities.

Expected records processed:

3 days × 3 cities = 9 city/date records.

The manual DAG run completed successfully, with all tasks successful.

### 7.2 Verify idempotency

Idempotency is important in data pipelines because the same data may need to be processed more than once.

ClimaScope uses a PostgreSQL upsert operation with:

`ON CONFLICT DO UPDATE`

The unique key `(city, date)` prevents duplicate records for the same city and date.

When an existing record is encountered, the loader updates it instead of inserting another record.

After the successful October 1–3 backfill, the database was checked again.

The following command was executed from PowerShell:

```powershell
docker compose -p weather-api-pipeline exec postgres psql -U weather_user -d weather -c "SELECT COUNT(*) AS total_records, COUNT(DISTINCT city) AS total_cities FROM daily_weather;"
```

The result was:

| Metric | Result |
|---|---:|
| Total records | 924 |
| Distinct cities | 3 |

The record count remained unchanged after the backfill.

This confirmed that rerunning the same date range did not create duplicate records.

## 8. Final Verification Results

The following checks were completed during this phase:

| Verification | Result |
|---|---|
| Automated component tests | 9/9 passed |
| Docker services | Running |
| PostgreSQL health | Healthy |
| Airflow DAG discovery | Successful |
| DAG active/unpaused status | Confirmed |
| Historical data coverage | January 1 – November 4, 2026 |
| Expected city count | 3 |
| Expected record count | 924 |
| Duplicate city/date records | None |
| Missing required values | None |
| Invalid temperature relationships | None |
| Manual October backfill | Successful |
| Idempotency verification | Successful |

## 9. Testing Scope and Limitations

The checks in this phase provide evidence that the individual components and integrated pipeline work under the tested conditions.

The following distinctions are important:

- The automated tests verify individual component behaviour, including selected failure scenarios.
- PostgreSQL queries verify the actual stored records and data quality.
- The manual backfill verifies that Airflow can orchestrate extraction, processing and loading for a selected historical range.
- The unchanged record count after the backfill verifies idempotent database loading for the tested range.

A deliberate failure of a live Airflow task was not performed as part of this phase. Failure handling was tested at component level through the automated tests.

The local deployment also depends on the computer and Docker services remaining available for scheduled execution. With `catchup=False`, missed daily runs are not automatically recreated.

## 10. Phase Outcome

Phase 9 confirmed that the ClimaScope pipeline works as an integrated system.

The automated tests passed, Airflow successfully executed a manual historical backfill, and PostgreSQL contained the expected 924 records across Accra, Kumasi and Tamale.

The database checks found no duplicate city/date combinations, missing values in the checked fields or invalid minimum/maximum temperature relationships.

The successful repeat backfill also demonstrated that the database loader updates existing records without increasing the total record count.

This completed the end-to-end integration and testing phase of ClimaScope.