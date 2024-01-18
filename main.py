import os
import requests
from utilities.util_methods import *
from extract import start_extraction_flow
from pathlib import Path
from transform import start_transformation_flow
from load import start_load_flow
from prefect import flow, task
from dotenv import load_dotenv


load_dotenv()


@flow(name="ETL Main Flow")
def start_etl_flow(city_name: str, start_date: str, end_date: str = "") -> None:
    """Start the ETL flow"""
    table_name = f"aqi_{city_name}"
    data_path = Path("./.sample_data")
    dataset = os.getenv("AQI_DATASET_NAME")

    start_day_unix_time = get_unix_time_from_date(start_date)

    if end_date:
        end_day_unix_time = get_unix_time_from_date(end_date)
    else:
        end_day_unix_time = get_current_time()

    # Get coordinates for the city
    if city_name:
        coordinates = get_city_coordinates(city_name)
    else:
        raise ValueError("Please enter the city name.")

    print(f"coordinates : {coordinates}")  # Extract the data
    response_file_path = start_extraction_flow(
        coordinates["lat"],
        coordinates["lon"],
        start_day_unix_time,
        end_day_unix_time,
        city_name,
    )

    # Apply transformations
    df = start_transformation_flow(response_file_path)

    # Load the data to GCS and BigQuery
    start_load_flow(df, table_name, data_path, dataset)


if __name__ == "__main__":
    city_name = os.getenv("CITY")
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    start_etl_flow(os.getenv("CITY"), start_date, end_date)
