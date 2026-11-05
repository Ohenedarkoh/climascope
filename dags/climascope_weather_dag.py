
import json
from datetime import date,datetime,timedelta
from pathlib import Path

import pandas as pd

from airflow.sdk import dag, task,get_current_context

# Import the functions we already built and tested.
from src.api.client import fetch_weather
from src.api.storage import save_raw_weather
from src.processing import process_weather_data
from src.database.loader import load_weather_data


# Define the cities and their approximate coordinates.
CITIES = {
    "Accra": {
        "latitude": 5.6037,
        "longitude": -0.1870,
    },
    "Kumasi": {
        "latitude": 6.6885,
        "longitude": -1.6244,
    },
    "Tamale": {
        "latitude": 9.4075,
        "longitude": -0.8533,
    },
}



# Define the workflow and its default settings.
@dag(
    dag_id="climascope_weather_pipeline",
    description="Orchestrates the ClimaScope historical weather pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False, #prevents Airflow from automatically creating all missed historical runs.
    params={
        "start_date": "",
        "end_date": "",
    },
    max_active_runs=1, #prevents overlapping runs of this DAG.
    tags=["climascope", "weather", "data-engineering"],
)
def climascope_weather_pipeline():

    # Task 1: Extract weather data from the API.
        # Task 1: Extract weather data from the API.
    @task
    def extract_weather():
        """Fetch and save historical weather data for all cities."""

        # Get information about the current Airflow run.
        context = get_current_context()
        run_params = context["params"]

        # Check whether custom dates were supplied for a backfill.
        requested_start = run_params.get("start_date")
        requested_end = run_params.get("end_date")

        if requested_start or requested_end:

            # Both dates must be supplied for a manual backfill.
            if not requested_start or not requested_end:
                raise ValueError(
                    "Both start_date and end_date must be provided."
                )

            # Convert the supplied dates into Python date objects.
            start = date.fromisoformat(requested_start)
            end = date.fromisoformat(requested_end)

            # Make sure the date range is valid.
            if start > end:
                raise ValueError(
                    "start_date cannot be later than end_date."
                )

            print(f"Running historical backfill: {start} to {end}")

        else:

            # Get the date interval assigned to this scheduled run.
            interval_start = context["data_interval_start"].date()
            interval_end = (
                context["data_interval_end"] - timedelta(days=1)
            ).date()

            # Reprocess the previous seven days.
            start = interval_start - timedelta(days=6)
            end = interval_end

            print(f"Running daily ingestion: {start} to {end}")

        # Convert the dates into the format expected by the API.
        start_date = start.isoformat()
        end_date = end.isoformat()

        # Store the paths of the saved raw JSON files.
        raw_file_paths = []

        # Fetch data for each city.
        for city, coordinates in CITIES.items():

            # Fetch historical weather data from Open-Meteo.
            weather_data = fetch_weather(
                latitude=coordinates["latitude"],
                longitude=coordinates["longitude"],
                start_date=start_date,
                end_date=end_date,
            )

            # Save the original API response as a raw JSON file.
            file_path = save_raw_weather(
                data=weather_data,
                city=city,
                start_date=start_date,
                end_date=end_date,
            )

            # Store the file path for the processing task.
            raw_file_paths.append(str(file_path))

        return raw_file_paths

    # Task 2: Process and validate the extracted data.
    @task
    def process_weather(raw_file_paths):
        """Process and validate the raw weather JSON files."""

        # Store the paths of the processed CSV files.
        processed_file_paths = []

        # Process each city's raw JSON file.
        for file_path in raw_file_paths:

            # Get the city name from the parent folder.
            # Convert it to title case (e.g. accra becomes Accra).
            city = Path(file_path).parent.name.title()

            # Open and read the raw JSON weather data.
            with open(file_path, "r", encoding="utf-8") as file:
                weather_data = json.load(file)

            # Convert the API response into a DataFrame and validate it.
            processed_data = process_weather_data(
                data=weather_data,
                city=city,
            )

            # Define where the processed CSV file will be saved.
            processed_file_path = (
                Path("data/processed") / f"{city.lower()}_weather.csv"
            )

            # Create the processed directory if it does not exist.
            processed_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Save the processed DataFrame as a CSV file.
            processed_data.to_csv(processed_file_path, index=False)

            # Store the CSV path for the loading task.
            processed_file_paths.append(str(processed_file_path))

        # Pass the processed file paths to the loading task through XCom.
        return processed_file_paths

    # Task 3: Load processed records into PostgreSQL.
    @task
    def load_weather(processed_file_paths):
        """Load processed weather CSV files into PostgreSQL."""

        # Process each CSV file returned by the processing task.
        for file_path in processed_file_paths:

            # Read the processed CSV file into a pandas DataFrame.
            weather_df = pd.read_csv(
                file_path,
                parse_dates=["date", "ingested_at"],
            )

            # Convert the date column to Python date objects.
            weather_df["date"] = weather_df["date"].dt.date

            # Load the DataFrame into PostgreSQL.
            # The loader handles connections, transactions and upserts.
            load_weather_data(weather_df)

        # Log a message after all files have been loaded.
        print("All weather data loaded successfully into PostgreSQL.")

    # Define the task dependencies and pass outputs between tasks.
    raw_file_paths = extract_weather()

    processed_file_paths = process_weather(raw_file_paths)

    load_weather(processed_file_paths)


# Register the DAG with Airflow.
climascope_weather_pipeline()
